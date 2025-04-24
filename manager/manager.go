package manager

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"orchestrator/communication"
	"orchestrator/node"
	"orchestrator/scheduler"
	"orchestrator/store"
	"orchestrator/task"
	"orchestrator/webapi"
	"os"
	"strings"
	"time"

	"github.com/docker/go-connections/nat"
	"github.com/golang-collections/collections/queue"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
)

type Manager struct {
	Pending       queue.Queue
	TaskDb        store.Store
	EventDb       store.Store
	Workers       []string
	WorkerTaskMap map[string][]uuid.UUID
	TaskWorkerMap map[uuid.UUID]string
	LastWorker    int
	WorkerNodes   []*node.Node
	Scheduler     scheduler.Scheduler
	logger        *log.Logger
}

// TODO: remove after test new method New
// func New(workers []string) *Manager {
// 	taskDb := make(map[uuid.UUID]*task.Task)
// 	eventDb := make(map[uuid.UUID]*task.TaskEvent)
// 	workerTaskMap := make(map[string][]uuid.UUID)
// 	taskWorkerMap := make(map[uuid.UUID]string)
// 	for i := range workers {
// 		workerTaskMap[workers[i]] = []uuid.UUID{}
// 	}

// 	return &Manager{
// 		Pending:       *queue.New(),
// 		Workers:       workers,
// 		TaskDb:        taskDb,
// 		EventDb:       eventDb,
// 		WorkerTaskMap: workerTaskMap,
// 		TaskWorkerMap: taskWorkerMap,
// 		logger:        log.New(os.Stdout, "[orchestrator | manager] ", log.LstdFlags),
// 	}
// }

func New(workers []string, schedulerType string, dbType string) *Manager {
	workerTaskMap := make(map[string][]uuid.UUID)
	taskWorkerMap := make(map[uuid.UUID]string)

	var nodes []*node.Node
	for worker := range workers {
		workerTaskMap[workers[worker]] = []uuid.UUID{}

		nAPI := fmt.Sprintf("http://%v", workers[worker])
		n := node.NewNode(workers[worker], nAPI, "worker")
		nodes = append(nodes, n)
	}

	var s scheduler.Scheduler
	switch schedulerType {
	case "greedy":
		s = &scheduler.Greedy{Name: "greedy"}
	case "roundrobin":
		s = &scheduler.RoundRobin{Name: "roundrobin"}
	case "epvm":
		s = &scheduler.Epvm{Name: "epvm"}
	default:
		s = &scheduler.RoundRobin{Name: "roundrobin"}
	}

	var ts store.Store
	var es store.Store
	var err error
	switch dbType {
	case "memory":
		ts = store.NewInMemoryTaskStore()
		es = store.NewInMemoryTaskEventStore()
	case "persistent":
		ts, err = store.NewTaskStore("tasks.db", 0600, "tasks")
		es, err = store.NewEventStore("events.db", 0600, "events")
	}

	if err != nil {
		log.Fatalf("unable to create task store: %v", err)
	}

	if err != nil {
		log.Fatalf("unable to create task event store: %v", err)
	}

	return &Manager{
		Pending:       *queue.New(),
		Workers:       workers,
		TaskDb:        ts,
		EventDb:       es,
		WorkerTaskMap: workerTaskMap,
		TaskWorkerMap: taskWorkerMap,
		WorkerNodes:   nodes,
		Scheduler:     s,
		logger:        log.New(os.Stdout, "[orch | manager] ", log.LstdFlags),
	}
}

func (m *Manager) SelectWorker(t task.Task) (*node.Node, error) {
	candidates := m.Scheduler.SelectCandidateNodes(t, m.WorkerNodes)
	if candidates == nil {
		msg := fmt.Sprintf(`No available candidates match resource request for task "%v"`, t.ID)
		err := errors.New(msg)
		return nil, err
	}
	scores := m.Scheduler.Score(t, candidates)
	selectedNode := m.Scheduler.Pick(scores, candidates)
	return selectedNode, nil
}

func (m *Manager) UpdateTasks() {
	for {
		func() {
			ctx := context.Background()
			ctx, span := otel.Tracer("").Start(ctx, "UpdateTasks")
			defer span.End()

			m.logger.Println("Checking for task updates from workers")
			m.updateTasks(ctx)
			m.logger.Println("Task updates completed")
		}()

		m.logger.Println("Sleeping for 15 seconds")
		time.Sleep(15 * time.Second)
	}
}

func (m *Manager) updateTasks(ctx context.Context) {
	for _, worker := range m.Workers {
		m.logger.Printf("Checking worker %v for task updates\n", worker)
		url := fmt.Sprintf("http://%s/tasks", worker)
		req := communication.NewGet(ctx, url)
		resp, err := (&http.Client{}).Do(req)
		if err != nil {
			m.logger.Printf("Error connecting to \"%v\": \"%v\"\n", worker, err)
			continue
		}

		if resp.StatusCode != http.StatusOK {
			m.logger.Printf("Error sending request: \"%v\"\n", err)
			continue
		}

		d := json.NewDecoder(resp.Body)
		var tasks []*task.Task
		err = d.Decode(&tasks)
		if err != nil {
			m.logger.Printf("Error unmarshalling tasks: %s\n", err.Error())
		}

		for _, t := range tasks {
			m.logger.Printf("Attempting to update task \"%v\"\n", t.ID)

			result, err := m.TaskDb.Get(t.ID.String())
			if err != nil {
				m.logger.Printf("Getting task error: %s\n", err)
				return
			}

			persistedTask, ok := result.(*task.Task)
			if !ok {
				m.logger.Printf("cannot convert result %v to task.Task type\n", result)
				continue
			}

			if persistedTask.State != t.State {
				persistedTask.State = t.State
			}

			persistedTask.StartTime = t.StartTime
			persistedTask.FinishTime = t.FinishTime
			persistedTask.ContainerID = t.ContainerID
			persistedTask.HostPorts = t.HostPorts

			m.TaskDb.Put(persistedTask.ID.String(), persistedTask)
		}
	}
}

func (m *Manager) UpdateNodeStats() {
	for {
		for _, node := range m.WorkerNodes {
			m.logger.Printf("Collecting stats for node %v ...", node.Name)
			_, err := node.GetStats()
			if err != nil {
				log.Printf("error updating node stats: %v", err)
			}
		}
		time.Sleep(15 * time.Second)
	}
}

func (m *Manager) DoHealthChecks() {
	for {
		func() {
			ctx := context.Background()
			ctx, span := otel.Tracer("Health checking").Start(ctx, "DoHealthChecks")
			defer span.End()

			m.logger.Println("Performing task health check")
			m.doHealthChecks(ctx)
			m.logger.Println("Task health checks completed")
		}()

		m.logger.Println("Sleeping for 60 seconds")
		time.Sleep(60 * time.Second)
	}
}

func (m *Manager) doHealthChecks(ctx context.Context) {
	tasks, err := m.TaskDb.List()
	if err != nil {
		m.logger.Panicf("Error: %s", err)
	}
	for _, t := range tasks.([]*task.Task) {
		if t.State == task.Running && t.RestartCount < 3 {
			err := m.checkTaskHealth(ctx, *t)
			if err != nil {
				if t.RestartCount < 3 {
					m.restartTask(ctx, t)
				}
			}
		} else if t.State == task.Failed && t.RestartCount < 3 {
			m.restartTask(ctx, t)
		}
	}
}

func (m *Manager) checkTaskHealth(ctx context.Context, t task.Task) error {
	ctx, span := otel.Tracer("").Start(ctx, "checkTaskHealth")
	defer span.End()

	m.logger.Printf("Calling health check for task %s: %s\n", t.ID, t.HealthCheck)

	w := m.TaskWorkerMap[t.ID]
	hostPort := getHostPort(t.HostPorts)
	worker := strings.Split(w, ":")
	if hostPort == nil {
		m.logger.Printf("Have not collected task %s host port yet. Skipping.\n", t.ID)
		return nil
	}
	url := fmt.Sprintf("http://%s:%s%s", worker[0], *hostPort, t.HealthCheck)
	m.logger.Printf("Calling health check for task \"%s\": \"%s\"\n", t.ID, url)
	resp, err := (&http.Client{}).Do(communication.NewGet(ctx, url))
	if err != nil {
		msg := fmt.Sprintf("Error connecting to health check \"%s\"", url)
		m.logger.Println(msg)
		return errors.New(msg)
	}

	if resp.StatusCode != http.StatusOK {
		msg := fmt.Sprintf("Error health check for task %s did not return 200\n", t.ID)
		m.logger.Println(msg)
		return errors.New(msg)
	}

	m.logger.Printf("Task %s health check response: %v\n", t.ID, resp.StatusCode)

	return nil
}

func getHostPort(ports nat.PortMap) *string {
	for k := range ports {
		return &ports[k][0].HostPort
	}
	return nil
}

func (m *Manager) restartTask(ctx context.Context, t *task.Task) {
	ctx, span := otel.Tracer("").Start(ctx, "restartTask")
	defer span.End()

	// Get the worker where the task was running
	w := m.TaskWorkerMap[t.ID]
	t.State = task.Scheduled
	t.RestartCount++

	// We need to overwrite the existing task to ensure it has
	// the current state
	m.TaskDb.Put(t.ID.String(), t)

	te := task.TaskEvent{
		ID:        uuid.New(),
		State:     task.Running,
		Timestamp: time.Now(),
		Task:      *t,
	}
	data, err := json.Marshal(te)
	if err != nil {
		m.logger.Printf("Unable to marshal task object: %v.\n", t)
		return
	}

	url := fmt.Sprintf("http://%s/tasks", w)
	req := communication.NewPost(ctx, url, communication.NewJSON(data))
	resp, err := (&http.Client{}).Do(req)
	if err != nil {
		m.logger.Printf("Error connecting to %v: %v\n", w, err)
		m.Pending.Enqueue(t)
		return
	}

	d := json.NewDecoder(resp.Body)
	if resp.StatusCode != http.StatusCreated {
		e := webapi.ErrResponse{}
		err := d.Decode(&e)
		if err != nil {
			m.logger.Printf("Error decoding response: %s\n", err.Error())
			return
		}
		m.logger.Printf("Response error (%d): %s\n", e.HTTPStatusCode, e.Message)
		return
	}

	newTask := task.Task{}
	err = d.Decode(&newTask)
	if err != nil {
		m.logger.Printf("Error decoding response: %s\n", err.Error())
		return
	}
	m.logger.Printf("%#v\n", t)
}

func (m *Manager) SendWork(ctx context.Context) {
	ctx, span := otel.Tracer("").Start(ctx, "SendWork")
	defer span.End()

	if m.Pending.Len() > 0 {
		e := m.Pending.Dequeue()
		te := e.(task.TaskEvent)
		err := m.EventDb.Put(te.ID.String(), &te)
		if err != nil {
			m.logger.Printf("Error attempting to store task event \"Id = %s\": %s\n", te.ID.String(), err)
			return
		}

		t := te.Task
		m.logger.Printf(`Pulled "%v" off pending queue\n`, t)

		taskWorker, ok := m.TaskWorkerMap[te.Task.ID]
		if ok {
			rawTask, err := m.TaskDb.Get(t.ID.String())
			if err != nil {
				m.logger.Printf("Unable to schedule task: %s", err)
				return
			}
			persistedTask, ok := rawTask.(*task.Task)
			if !ok {
				m.logger.Printf("Unable to convert task to task.Task type")
				return
			}

			if te.State == task.Completed && task.ValidStateTransition(persistedTask.State, te.State) {
				m.stopTask(ctx, taskWorker, te.Task.ID.String())
				return
			}
			log.Printf(`invalid request: existing task "%s" is in state "%v" and`+
				`cannot transition to the completed state\n`,
				persistedTask.ID.String(), persistedTask.State)
			return
		}

		m.SelectWorker(t)

		w, err := m.SelectWorker(t)
		if err != nil {
			m.logger.Printf(`error selecting worker for task "%s": "%v"\n`, t.ID, err)
		}

		m.WorkerTaskMap[w.Name] = append(m.WorkerTaskMap[w.Name], te.Task.ID)
		m.TaskWorkerMap[t.ID] = w.Name

		t.State = task.Scheduled
		err = m.TaskDb.Put(t.ID.String(), &t)
		if err != nil {
			m.logger.Panicf("Error: %s", err)
		}

		data, err := json.Marshal(te)
		if err != nil {
			m.logger.Printf("Unable to marshal task object: %v.\n", t)
		}

		url := fmt.Sprintf("http://%s/tasks", w.Name)
		req := communication.NewPost(ctx, url, communication.NewJSON(data))
		resp, err := (&http.Client{}).Do(req)
		if err != nil {
			m.logger.Printf("Error connecting to \"%v\": %v\n", url, err)
			m.Pending.Enqueue(te)
			return
		}

		d := json.NewDecoder(resp.Body)
		if resp.StatusCode != http.StatusCreated {
			e := webapi.ErrResponse{}
			err := d.Decode(&e)
			if err != nil {
				m.logger.Printf("Error decoding response: %s\n", err.Error())
				return
			}
			m.logger.Printf("Response error (%d): %s\n", e.HTTPStatusCode, e.Message)
			return
		}

		t = task.Task{}
		err = d.Decode(&t)
		if err != nil {
			m.logger.Printf("Error decoding response: %s\n", err.Error())
			return
		}
		m.logger.Printf("%#v\n", t)
	} else {
		m.logger.Println("No work in the queue")
	}
}

func (m *Manager) AddTask(te task.TaskEvent) {
	m.Pending.Enqueue(te)
}

func (m *Manager) GetTasks() []*task.Task {
	tasks, err := m.TaskDb.List()
	if err != nil {
		m.logger.Printf("Error getting list of tasks: %v\n", err)
		return nil
	}
	return tasks.([]*task.Task)
}

func (m *Manager) ProcessTasks() {
	for {
		func() {
			ctx := context.Background()
			tracer := otel.Tracer("Task processing")
			ctx, span := tracer.Start(ctx, "ProcessTasks")
			defer span.End()

			m.logger.Println("Processing any tasks in the queue")
			m.SendWork(ctx)
			m.logger.Println("Sleeping for 10 seconds")
		}()

		time.Sleep(10 * time.Second)
	}
}

func (m *Manager) stopTask(ctx context.Context, worker string, taskID string) {
	url := fmt.Sprintf("http://%s/tasks/%s", worker, taskID)
	req := communication.NewDelete(ctx, url)
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		m.logger.Printf(`error connecting to worker at "%s": "%v"\n`, url, err)
		return
	}
	if resp.StatusCode != 204 {
		m.logger.Printf(`Error sending request: "%v"\n`, err)
		return
	}
	m.logger.Printf("task %s has been scheduled to be stopped", taskID)
}

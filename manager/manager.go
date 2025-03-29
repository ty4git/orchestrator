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
	TaskDb        map[uuid.UUID]*task.Task
	EventDb       map[uuid.UUID]*task.TaskEvent
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

func New(workers []string, schedulerType string) *Manager {
	taskDb := make(map[uuid.UUID]*task.Task)
	eventDb := make(map[uuid.UUID]*task.TaskEvent)

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

	return &Manager{
		Pending:       *queue.New(),
		Workers:       workers,
		TaskDb:        taskDb,
		EventDb:       eventDb,
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
			m.logger.Printf("Attempting to update task %v\n", t.ID)

			_, ok := m.TaskDb[t.ID]
			if !ok {
				m.logger.Printf("Task with ID %s not found\n", t.ID)
				return
			}
			if m.TaskDb[t.ID].State != t.State {
				m.TaskDb[t.ID].State = t.State
			}

			m.TaskDb[t.ID].StartTime = t.StartTime
			m.TaskDb[t.ID].FinishTime = t.FinishTime
			m.TaskDb[t.ID].ContainerID = t.ContainerID
		}
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
	for _, t := range m.TaskDb {
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
	m.TaskDb[t.ID] = t

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
		m.EventDb[te.ID] = &te

		t := te.Task
		m.logger.Printf(`Pulled "%v" off pending queue\n`, t)

		taskWorker, ok := m.TaskWorkerMap[te.Task.ID]
		if ok {
			persistedTask := m.TaskDb[te.Task.ID]
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
		m.TaskDb[t.ID] = &t

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
	tasks := []*task.Task{}
	for _, t := range m.TaskDb {
		tasks = append(tasks, t)
	}
	return tasks
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

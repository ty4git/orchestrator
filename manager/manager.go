package manager

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"orchestrator/communication"
	"orchestrator/node"
	"orchestrator/scheduler"
	"orchestrator/store"
	"orchestrator/task"
	"orchestrator/webapi"
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
	logger        *slog.Logger
}

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
		if err != nil {
			slog.Error("Unable to create task store", "error", err)
			panic(err)
		}
		es, err = store.NewEventStore("events.db", 0600, "events")
		if err != nil {
			slog.Error("unable to create task event store", "error", err)
			panic(err)
		}
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
		logger:        slog.Default(),
	}
}

func (m *Manager) Run() *Manager {
	go m.ProcessTasks()
	go m.SynchronizeTasks()
	go m.DoHealthChecks()
	go m.UpdateNodeStats()
	return m
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

func (m *Manager) SynchronizeTasks() {
	for {
		func() {
			ctx := context.Background()
			ctx, span := otel.Tracer("").Start(ctx, "UpdateTasks")
			defer span.End()

			m.logger.Debug("Checking for task updates from workers...")
			m.synchronizeTasks(ctx)
			m.logger.Debug("Task updates completed")
		}()

		delay := 15 * time.Second
		m.logger.Debug("Sleeping...", "delay", delay)
		time.Sleep(delay)
	}
}

func (m *Manager) synchronizeTasks(ctx context.Context) {
	for _, worker := range m.Workers {
		m.logger.Debug("Checking worker for task updates", "worker", worker)

		url := fmt.Sprintf("http://%s/tasks", worker)
		req := communication.NewGet(ctx, url)
		resp, err := (&http.Client{}).Do(req) // TODO: don't create a new client each time
		if err != nil {
			m.logger.Warn("Error connecting to worker", "worker", worker, "err", err)
			continue
		}

		if resp.StatusCode != http.StatusOK {
			m.logger.Warn("Error sending request", "error", err)
			continue
		}

		d := json.NewDecoder(resp.Body)
		var tasks []*task.Task
		err = d.Decode(&tasks)
		if err != nil {
			m.logger.Error("Error unmarshalling tasks", "error", err.Error())
		}

		for _, t := range tasks {
			m.logger.Debug("Attempting to update task...", "taskId", t.ID)

			result, err := m.TaskDb.Get(t.ID.String())
			if err != nil {
				m.logger.Warn("Error while getting task", "error", err)
				return
			}

			persistedTask, ok := result.(*task.Task)
			if !ok {
				m.logger.Debug("Cannot convert stored object to task.Task type", "storedObject", result)
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
			m.logger.Debug("Collecting stats for node...", "nodeName", node.Name)
			_, err := node.GetStats()
			if err != nil {
				m.logger.Error("Error updating node stats", "error", err)
			}
		}

		delay := 15 * time.Second
		time.Sleep(delay)
	}
}

func (m *Manager) ProcessTasks() {
	for {
		func() {
			ctx := context.Background()
			tracer := otel.Tracer("Task processing")
			ctx, span := tracer.Start(ctx, "ProcessTasks")
			defer span.End()

			m.logger.Debug("Processing pending tasks...")
			m.SendWork(ctx)
		}()

		delay := 10 * time.Second
		m.logger.Debug("Sleeping...", "delay", delay)
		time.Sleep(delay)
	}
}

func (m *Manager) DoHealthChecks() {
	for {
		func() {
			ctx := context.Background()
			ctx, span := otel.Tracer("Health checking").Start(ctx, "DoHealthChecks")
			defer span.End()

			m.logger.Debug("Performing task health check...")
			m.doHealthChecks(ctx)
			m.logger.Debug("Task health checks completed")
		}()

		delay := 60 * time.Second
		m.logger.Debug("Sleeping...", "delay", delay)
		time.Sleep(delay)
	}
}

func (m *Manager) doHealthChecks(ctx context.Context) {
	tasks, err := m.TaskDb.List()
	if err != nil {
		m.logger.Error(err.Error())
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

	m.logger.Debug("Calling health check for task", "taskId", t.ID, "healthCheckPath", t.HealthCheck)

	w := m.TaskWorkerMap[t.ID]
	hostPort := getHostPort(t.HostPorts)
	worker := strings.Split(w, ":")
	if hostPort == nil {
		m.logger.Info("Have not collected task host port yet. Skipping.", "taskId", t.ID)
		return nil
	}
	url := fmt.Sprintf("http://%s:%s%s", worker[0], *hostPort, t.HealthCheck)
	m.logger.Debug("Calling health check for task", "taskId", t.ID, "healthCheckPath", url)
	resp, err := (&http.Client{}).Do(communication.NewGet(ctx, url))
	if err != nil {
		msg := "Error connecting to the health check endpoint of task"
		m.logger.Warn(msg, "healthCheckPath", url)
		return errors.New(msg)
	}

	if resp.StatusCode != http.StatusOK {
		msg := fmt.Sprintln("Error of health check result, did not return 'ok' status")
		m.logger.Warn(msg, "taskId", t.ID)
		return errors.New(msg)
	}

	m.logger.Debug("Task health check response", "taskId", t.ID, "StatusCode", resp.StatusCode)
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
		m.logger.Error("Unable to marshal task object", "task", t)
		return
	}

	url := fmt.Sprintf("http://%s/tasks", w)
	req := communication.NewPost(ctx, url, communication.NewJSON(data))
	resp, err := (&http.Client{}).Do(req)
	if err != nil {
		m.logger.Error("Error connecting to worker", "worker", w, "error", err)
		m.Pending.Enqueue(t)
		return
	}

	d := json.NewDecoder(resp.Body)
	if resp.StatusCode != http.StatusCreated {
		e := webapi.ErrResponse{}
		err := d.Decode(&e)
		if err != nil {
			m.logger.Error("Error decoding response", "error", err.Error())
			return
		}
		m.logger.Error("Response error (%d): %s\n", e.HTTPStatusCode, e.Message)
		return
	}

	newTask := task.Task{}
	err = d.Decode(&newTask)
	if err != nil {
		m.logger.Error("Error decoding response: %s\n", err.Error())
		return
	}
	m.logger.Debug("%#v\n", t)
}

func (m *Manager) SendWork(ctx context.Context) {
	ctx, span := otel.Tracer("").Start(ctx, "SendWork")
	defer span.End()

	if m.Pending.Len() > 0 {
		e := m.Pending.Dequeue()
		te := e.(task.TaskEvent)
		err := m.EventDb.Put(te.ID.String(), &te)
		if err != nil {
			m.logger.Error("Error attempting to store task event \"Id = %s\": %s\n", te.ID.String(), err)
			return
		}

		t := te.Task
		m.logger.Debug("Pulled task off pending queue (task = %v)\n", t)

		taskWorker, ok := m.TaskWorkerMap[t.ID]
		if ok {
			rawTask, err := m.TaskDb.Get(t.ID.String())
			if err != nil {
				m.logger.Error("Unable to schedule task: %s", err)
				return
			}
			persistedTask, ok := rawTask.(*task.Task)
			if !ok {
				m.logger.Error("Unable to convert task to task.Task type")
				return
			}

			if te.State == task.Completed && task.ValidStateTransition(persistedTask.State, te.State) {
				m.stopTask(ctx, taskWorker, te.Task.ID.String())
				return
			}

			m.logger.Error("invalid request: existing task (task = %s) is in state (state = %s) and"+
				" cannot transition to the completed state\n",
				persistedTask.ID.String(), persistedTask.State.String())
			return
		}

		m.SelectWorker(t)

		w, err := m.SelectWorker(t)
		if err != nil {
			m.logger.Error(`error selecting worker for task "%s": "%v"\n`, t.ID, err)
		}

		m.WorkerTaskMap[w.Name] = append(m.WorkerTaskMap[w.Name], te.Task.ID)
		m.TaskWorkerMap[t.ID] = w.Name

		t.State = task.Scheduled
		err = m.TaskDb.Put(t.ID.String(), &t)
		if err != nil {
			m.logger.Error("Error: %s", err)
		}

		data, err := json.Marshal(te)
		if err != nil {
			m.logger.Error("Unable to marshal task object: %v.\n", t)
		}

		url := fmt.Sprintf("http://%s/tasks", w.Name)
		req := communication.NewPost(ctx, url, communication.NewJSON(data))
		resp, err := (&http.Client{}).Do(req)
		if err != nil {
			m.logger.Warn("Error connecting to worker endpoint", "worker.endpoint", url, "error", err)
			m.Pending.Enqueue(te)
			return
		}

		d := json.NewDecoder(resp.Body)
		if resp.StatusCode != http.StatusCreated {
			e := webapi.ErrResponse{}
			err := d.Decode(&e)
			if err != nil {
				m.logger.Error("Error decoding response", "error", err.Error())
				return
			}
			m.logger.Warn("Response error status code", "status.code", e.HTTPStatusCode, "error", e.Message)
			return
		}

		t = task.Task{}
		err = d.Decode(&t)
		if err != nil {
			m.logger.Error("Error decoding response", "error", err.Error())
			return
		}
		m.logger.Debug("%#v\n", t)
	} else {
		m.logger.Info("No work in the queue")
	}
}

func (m *Manager) AddTask(te task.TaskEvent) {
	m.Pending.Enqueue(te)
}

func (m *Manager) GetTasks() []*task.Task {
	tasks, err := m.TaskDb.List()
	if err != nil {
		m.logger.Error("Error of getting list of tasks: %v\n", err)
		return nil
	}
	return tasks.([]*task.Task)
}

func (m *Manager) stopTask(ctx context.Context, worker string, taskID string) {
	url := fmt.Sprintf("http://%s/tasks/%s", worker, taskID)
	req := communication.NewDelete(ctx, url)
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		m.logger.Error(`error connecting to the worker endpoint`, "worker.endpoint", url, "error", err)
		return
	}
	if resp.StatusCode != 204 {
		m.logger.Error(`Error sending request: "%v"\n`, err)
		return
	}
	m.logger.Info("Task has been scheduled to be stopped", "taskId", taskID)
}

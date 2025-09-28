package manager

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"orchestrator/communication"
	"orchestrator/infrastructure"
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
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
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
	logger        *infrastructure.Logger
	client        *http.Client
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

	logger := infrastructure.NewLogger(slog.Default())
	client := http.DefaultClient
	client.Transport = otelhttp.NewTransport(http.DefaultTransport)

	return &Manager{
		Pending:       *queue.New(),
		Workers:       workers,
		TaskDb:        ts,
		EventDb:       es,
		WorkerTaskMap: workerTaskMap,
		TaskWorkerMap: taskWorkerMap,
		WorkerNodes:   nodes,
		Scheduler:     s,
		logger:        logger,
		client:        client,
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
		msg := fmt.Sprintf(`No available candidates match resource request for task "%v"`, t.Id)
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
			ctx, span := otel.Tracer("").Start(ctx, "SynchronizeTasks")
			defer span.End()

			m.logger.DebugContext(ctx, "Checking for task updates from workers...")
			m.synchronizeTasks(ctx)
			m.logger.DebugContext(ctx, "Task updates completed")
		}()

		delay := 15 * time.Second
		m.logger.Debug("Sleeping...", "delay", delay)
		time.Sleep(delay)
	}
}

func (m *Manager) synchronizeTasks(ctx context.Context) {
	_, span := otel.Tracer("").Start(ctx, "")
	defer span.End()

	for _, worker := range m.Workers {
		m.logger.Debug("Checking worker for task updates", "worker", worker)

		url := fmt.Sprintf("http://%s/tasks", worker)
		req := communication.NewGet(ctx, url)
		resp, err := m.client.Do(req) // TODO: don't create a new client each time
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			m.logger.Warn("Error connecting to worker", "worker", worker, "error", err)
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
			m.logger.Debug("Attempting to update task...", "taskId", t.Id)

			result, err := m.TaskDb.Get(t.Id.String())
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

			m.TaskDb.Put(persistedTask.Id.String(), persistedTask)
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
		tracer := otel.Tracer("")
		ctx, span := tracer.Start(context.Background(), "ProcessTasks")
		func() {
			ctx, span := tracer.Start(ctx, "Processing...")
			defer span.End()

			logger := m.logger.WithCtx(ctx)
			logger.Debug("Processing pending tasks...")
			m.sendTask(ctx)
		}()

		func() {
			ctx, span := tracer.Start(ctx, "Sleeping...")
			defer span.End()

			logger := m.logger.WithCtx(ctx)
			delay := 10 * time.Second
			logger.Debug("Sleeping...", "delay", delay)
			time.Sleep(delay)
		}()
		span.End()
	}
}

func (m *Manager) DoHealthChecks() {
	for {
		ctx := context.Background()
		func() {
			ctx, span := otel.Tracer("").Start(ctx, "DoHealthChecks")
			defer span.End()

			m.logger.DebugContext(ctx, "Performing task health check...")
			m.doHealthChecks(ctx)
			m.logger.DebugContext(ctx, "Task health checks completed")
		}()

		delay := 60 * time.Second
		m.logger.DebugContext(ctx, "Sleeping...", "delay", delay)
		time.Sleep(delay)
	}
}

func (m *Manager) doHealthChecks(ctx context.Context) {
	tasks, err := m.TaskDb.List()
	if err != nil {
		m.logger.Error(err.Error())
	}

	for _, t := range tasks.([]*task.Task) {
		maxRestartCount := byte(3)
		if t.RestartCount >= maxRestartCount {
			m.logger.WarnContext(ctx, "Task has been restarted max times, skipping health check",
				"taskId", t.Id, "restartCount", t.RestartCount, "maxRestartCount", maxRestartCount)
			continue
		}
		if t.State == task.Running {
			err := m.checkTaskHealth(ctx, *t)
			if err != nil {
				m.restartTask(ctx, t)
			}
		} else if t.State == task.Failed {
			m.restartTask(ctx, t)
		}
	}
}

func (m *Manager) checkTaskHealth(ctx context.Context, t task.Task) error {
	ctx, span := otel.Tracer("").Start(ctx, "CheckTaskHealth")
	defer span.End()
	logger := m.logger.WithCtx(ctx)

	logger.Debug("Calling health check for task", "taskId", t.Id, "healthCheckUrl", t.HealthCheck)

	w := m.TaskWorkerMap[t.Id]
	hostPort := getHostPort(t.HostPorts)
	worker := strings.Split(w, ":")
	if hostPort == nil {
		logger.Info("Have not collected task host port yet. Skipping.", "taskId", t.Id)
		return nil
	}
	url := fmt.Sprintf("http://%s:%s%s", worker[0], *hostPort, t.HealthCheck)
	logger.Debug("Calling health check for task", "taskId", t.Id, "healthCheckUrl", url)
	resp, err := m.client.Do(communication.NewGet(ctx, url))
	if err != nil {
		return fmt.Errorf("error connecting to the health check endpoint of task: %w",
			err)
	}

	if resp.StatusCode != http.StatusOK {
		msg := fmt.Sprintln("Error of health check result, did not return 'ok' status")
		logger.Warn(msg, "taskId", t.Id, "responseStatusCode", resp.StatusCode)
		return fmt.Errorf("%s: %w", msg, err)
	}

	logger.Debug("Task health check response", "taskId", t.Id, "StatusCode", resp.StatusCode)
	return nil
}

func getHostPort(ports nat.PortMap) *string {
	for k := range ports {
		return &ports[k][0].HostPort
	}
	return nil
}

func (m *Manager) restartTask(ctx context.Context, t *task.Task) {
	ctx, span := otel.Tracer("").Start(ctx, "RestartTask")
	defer span.End()

	// if task is already pending of restart
	if t.State == task.Pending {
		return
	}

	// logger := m.logger.WithCtx(ctx)

	// worker := m.TaskWorkerMap[t.Id]
	t.State = task.Pending
	t.RestartCount++

	// We need to overwrite the existing task to ensure it has
	// the current state
	m.TaskDb.Put(t.Id.String(), t)

	// te := task.TaskEvent{
	// 	Id:        uuid.New(),
	// 	State:     task.Running,
	// 	Timestamp: time.Now(),
	// 	Payload:   *t,
	// }
	// data, err := json.Marshal(te)
	// if err != nil {
	// 	logger.Error("Unable to marshal task object", "task", t, "error", err)
	// 	return
	// }

	// url := fmt.Sprintf("http://%s/tasks", worker)
	// req := communication.NewPost(ctx, url, communication.NewJSON(data))
	// resp, err := m.client.Do(req)
	// defer resp.Body.Close()
	// if err != nil {
	// 	logger.Error("Error connecting to worker", "worker", worker, "error", err)
	// 	m.Pending.Enqueue(t)
	// 	return
	// }

	// d := json.NewDecoder(resp.Body)
	// if resp.StatusCode != http.StatusCreated {
	// 	e := webapi.ErrResponse{}
	// 	err := d.Decode(&e)
	// 	if err != nil {
	// 		logger.Error("Error decoding response", "error", err.Error())
	// 		return
	// 	}
	// 	logger.Error("Response error (%d): %s\n", e.HTTPStatusCode, e.Message)
	// 	return
	// }

	// newTask := task.Task{}
	// err = d.Decode(&newTask)
	// if err != nil {
	// 	logger.Error("Error decoding response", "error", err.Error())
	// 	return
	// }
	// logger.Debug("%#v\n", t)
}

func (m *Manager) sendTask(ctx context.Context) {
	ctx, span := otel.Tracer("").Start(ctx, "SendWork")
	defer span.End()

	logger := m.logger.WithCtx(ctx)

	if m.Pending.Len() == 0 {
		logger.Info("No work in the queue")
		return
	}

	nextTaskEvent, nextTask, currentTask, err := m.getNextTaskForSending(ctx)
	taskId := currentTask.Id
	if err != nil {
		logger.Error("Error getting next task for sending", "error", err)
		return
	}
	err = m.ValidateTaskTransition(ctx, nextTaskEvent, nextTask)
	if err != nil {
		logger.Error("Invalid task transition", "error", err)
		return
	}

	// e := m.Pending.Dequeue()
	// te := e.(task.TaskEvent)
	// err := m.EventDb.Put(te.Id.String(), &te)
	// if err != nil {
	// 	logger.Error("Error while storing the task event", "task.event.id", te.Id.String(), "error", err)
	// 	return
	// }

	// var taskId uuid.UUID
	// var newTask *task.Task
	// if te.State == task.Stopped {
	// 	taskId = te.Payload.(uuid.UUID)
	// } else {
	// 	newTask = te.Payload.(*task.Task)
	// 	taskId = newTask.Id
	// 	logger.Debug("Pulled task off pending queue", "task.id", taskId)
	// }

	// rawTask, err := m.TaskDb.Get(taskId.String())
	// if err != nil {
	// 	logger.Error("Error while finding the task in the taskDb", "task.id", taskId.String(), "error", err)
	// 	return
	// }

	// existingTask, ok := rawTask.(*task.Task)
	// if !ok {
	// 	logger.Error("Error of task converting to type", "task.id", taskId.String())
	// 	return
	// }

	// newState := te.State
	// if !task.IsValidStateTransition(storedTask.State, newState) {
	// 	logger.Error("Error: existing task cannot be transited to the new state",
	// 		"task.id", storedTask.Id.String(),
	// 		"task.state", storedTask.State.String(),
	// 		"task.state.new", newState.String())
	// 	return
	// }

	worker, err := m.getWorker(currentTask, nextTask)
	if err != nil {
		logger.Error("Error getting worker for task", "task.id", nextTask.Id, "error", err)
		return
	}

	// TODO: add function "MakeTransition"
	if nextTask.State == task.Stopped {
		m.stopTask(ctx, worker, taskId.String())
	} else {
		nextTask.State = task.Scheduled
	}

	err = m.TaskDb.Put(taskId.String(), nextTask)
	if err != nil {
		logger.Error("Error while saving new task or updating existing task", "task.id",
			taskId.String(), "error", err)
		return
	}

	switch nextTaskEvent.State {
	case task.Scheduled:
		data, err := json.Marshal(nextTaskEvent)
		if err != nil {
			logger.Error("Unable to marshal task event", "error", err)
			return
		}

		url := fmt.Sprintf("http://%s/tasks", worker)
		req := communication.NewPost(ctx, url, communication.NewJSON(data))
	case task.Stopped:
		url := fmt.Sprintf("http://%s/tasks/%s", worker, taskId.String())
		req := communication.NewPut(ctx, url)
	}

	resp, err := m.client.Do(req)
	if err != nil {
		logger.Warn("Error connecting to worker endpoint", "worker.endpoint", url, "error", err)
		m.Pending.Enqueue(nextTaskEvent)
		return
	}
	defer resp.Body.Close()

	d := json.NewDecoder(resp.Body)
	if resp.StatusCode != http.StatusCreated {
		e := webapi.ErrResponse{}
		err := d.Decode(&e)
		if err != nil {
			logger.Error("Error decoding response", "error", err.Error())
			return
		}
		logger.Warn("Response error", "status.code", e.HTTPStatusCode, "error", e.Message)
		return
	}
}

func (m *Manager) getNextTaskForSending(ctx context.Context) (*task.TaskEvent, *task.Task,
	*task.Task, error) {
	e := m.Pending.Dequeue()
	nextTaskEvent := e.(task.TaskEvent)
	err := m.EventDb.Put(nextTaskEvent.Id.String(), &nextTaskEvent) // TODO: Change this, remove it
	if err != nil {
		return nil, nil, nil, fmt.Errorf("error while storing the task event (task.event.id = %s): %w",
			nextTaskEvent.Id, err)
	}

	var nextTask *task.Task
	var currentTask *task.Task
	switch nextTaskEvent.Payload.(type) {
	case *task.Task:
		nextTask = nextTaskEvent.Payload.(*task.Task)
	case uuid.UUID:
		taskId := nextTaskEvent.Payload.(uuid.UUID)
		rawTask, err := m.TaskDb.Get(taskId.String())
		if err != nil {
			err = fmt.Errorf("error while finding the task (task.id = %s): %w", taskId, err)
			break
		}

		var ok bool
		currentTask, ok = rawTask.(*task.Task)
		if !ok {
			err = fmt.Errorf("error of task converting (task.id = %s)", taskId)
		}
	default:
		err = fmt.Errorf("error of event payload converting (task.event.id = %s)",
			nextTaskEvent.Id)
	}

	return &nextTaskEvent, nextTask, currentTask, err
}

func (m *Manager) ValidateTaskTransition(ctx context.Context, nextEvent *task.TaskEvent,
	nextTask *task.Task) error {

	newState := nextEvent.State
	if !task.IsValidStateTransition(nextTask.State, newState) {
		return fmt.Errorf("invalid state transition for task (task.id = %s, from = %s, to = %s)",
			nextTask.Id.String(), nextTask.State.String(), newState.String())
	}

	return nil
}

func (m *Manager) getWorker(currentTask *task.Task, nextTask *task.Task) (string, error) {
	if currentTask != nil {
		worker, ok := m.TaskWorkerMap[currentTask.Id]
		if !ok {
			return "",
				fmt.Errorf("no worker found for task event (task.event.id = %s), but should be",
					currentTask.Id)
		}
		return worker, nil
	}
	taskId := nextTask.Id
	workerAddr, ok := m.TaskWorkerMap[taskId]
	if ok {
		return workerAddr, nil
	}
	worker, err := m.SelectWorker(*nextTask)
	if err != nil {
		return "", err
	}
	m.WorkerTaskMap[worker.Name] = append(m.WorkerTaskMap[worker.Name], taskId)
	m.TaskWorkerMap[taskId] = worker.Name
	return worker.Name, nil
}

func (m *Manager) AddTask(ctx context.Context, newTaskEvent *task.TaskEvent) (uuid.UUID,
	error) {
	if event, _ := m.EventDb.Get(newTaskEvent.Id.String()); event != nil {
		return uuid.Nil, ErrTaskEventAlreadyExists
	}
	newTask := newTaskEvent.Payload.(*task.Task)

	newTask.Id = uuid.New()
	newTask.State = task.Pending

	// TODO: Remove this. You can't do this since there is a risk that the data will be inconsistent.
	// TODO: Use goroutine to copy the task from event db to pending queue.
	m.Pending.Enqueue(
		task.TaskEvent{
			Id:        newTaskEvent.Id,
			State:     task.Pending,
			Timestamp: time.Now().UTC(),
			Payload:   newTask,
		})

	err := m.EventDb.Put(newTaskEvent.Id.String(), newTaskEvent)
	if err != nil {
		err := fmt.Errorf("error while storing task event %s: %w", newTaskEvent.Id, err)
		return uuid.Nil, err
	}

	return newTask.Id, nil
}

func (m *Manager) StopTask(ctx context.Context, id uuid.UUID) error {
	logger := m.logger.WithCtx(ctx)

	_, err := m.TaskDb.Get(id.String())
	if err != nil {
		logger.WarnContext(ctx, "No task found by Id", "taskId", id)
		return fmt.Errorf("%w: %w", ErrTaskNotFound, err)
	}

	m.Pending.Enqueue(
		task.TaskEvent{
			State:     task.Stopped,
			Timestamp: time.Now(),
			Payload:   id,
		})
	return nil
}

func (m *Manager) GetTasks() []*task.Task {
	tasks, err := m.TaskDb.List()
	if err != nil {
		m.logger.Error("Error of getting list of tasks", "error", err)
		return nil
	}
	return tasks.([]*task.Task)
}

// TODO: Add returned error
func (m *Manager) stopTask(ctx context.Context, worker string, taskId string) {
	url := fmt.Sprintf("http://%s/tasks/%s", worker, taskId)
	req := communication.NewDelete(ctx, url)
	resp, err := m.client.Do(req)
	if err != nil {
		m.logger.ErrorContext(ctx, "Error connecting to the worker endpoint",
			"worker.endpoint", url, "error", err)
		return
	}
	if resp.StatusCode != 204 {
		m.logger.ErrorContext(ctx, "Error sending request", "error", err)
		return
	}
	m.logger.Info("Task has been scheduled to be stopped", "taskId", taskId)
}

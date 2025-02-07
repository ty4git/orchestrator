package worker

import (
	"context"
	"errors"
	"fmt"
	"log"
	"orchestrator/task"
	"os"
	"time"

	"github.com/golang-collections/collections/queue"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
)

type Worker struct {
	Name      string
	Queue     queue.Queue
	Db        map[uuid.UUID]*task.Task
	TaskCount int
	Stats     *Stats
	logger    *log.Logger
}

func New() *Worker {
	return &Worker{
		Queue:  *queue.New(),
		Db:     make(map[uuid.UUID]*task.Task),
		logger: log.New(os.Stdout, "[orchestrator | worker | worker] ", log.LstdFlags),
	}
}

func (w *Worker) GetTasks(ctx context.Context) []*task.Task {
	_, span := otel.Tracer("").Start(ctx, "GetTasks")
	defer span.End()

	tasks := []*task.Task{}
	for _, task := range w.Db {
		tasks = append(tasks, task)
	}
	return tasks
}

func (w *Worker) AddTask(t task.Task) {
	w.Queue.Enqueue(t)
}

func (w *Worker) RunTasks(ctx context.Context) {
	for {
		func() {
			_, span := otel.Tracer("Run tasks...").Start(ctx, "Run tasks")
			defer span.End()
			if w.Queue.Len() != 0 {
				result := w.runTask()
				if result.Error != nil {
					w.logger.Printf("Error running task: %v\n", result.Error)
				}
			} else {
				w.logger.Printf("No tasks to process currently.\n")
			}
		}()

		w.logger.Println("Sleeping for 10 seconds.")
		time.Sleep(10 * time.Second)
	}
}

func (w *Worker) runTask() task.DockerResult {
	t := w.Queue.Dequeue()
	if t == nil {
		w.logger.Println("No tasks in the queue")
		return task.DockerResult{Error: nil}
	}

	taskQueued := t.(task.Task)

	taskPersisted := w.Db[taskQueued.ID]
	if taskPersisted == nil {
		if taskQueued.State != task.Scheduled {
			err := fmt.Errorf("task \"%v\" has state \"%v\", but expected \"%v\"", taskQueued.ID, taskQueued.State, task.Scheduled)
			return task.DockerResult{Error: err}
		}
		taskPersisted = &task.Task{
			ID:    taskQueued.ID,
			Name:  taskQueued.Name,
			State: task.Pending,
			Image: taskQueued.Image,
		}
		w.Db[taskPersisted.ID] = taskPersisted
	}

	var result task.DockerResult
	if task.ValidStateTransition(taskPersisted.State, taskQueued.State) {
		switch taskQueued.State {
		case task.Scheduled:
			result = w.StartTask(taskQueued)
		case task.Completed:
			result = w.StopTask(taskQueued)
		default:
			result.Error = errors.New("we should not get here")
		}
	} else {
		err := fmt.Errorf("invalid transition task ID \"%v\" from \"%v\" to \"%v\"",
			taskPersisted.ID, taskPersisted.State, taskQueued.State)
		result.Error = err
		return result
	}
	return result
}

func (w *Worker) StartTask(t task.Task) task.DockerResult {
	t.StartTime = time.Now().UTC()
	config := task.NewDockerConfig(&t)
	d := task.NewDocker(config)
	result := d.Run()
	if result.Error != nil {
		w.logger.Printf("Error of running task \"%v\": \"%v\"\n", t.ID, result.Error)
		t.State = task.Failed
		w.Db[t.ID] = &t
		return result
	}

	t.ContainerID = result.ContainerId
	t.State = task.Running
	w.Db[t.ID] = &t

	return result
}

func (w *Worker) StopTask(t task.Task) task.DockerResult {
	config := task.NewDockerConfig(&t)
	d := task.NewDocker(config)

	result := d.Stop(t.ContainerID)
	if result.Error != nil {
		w.logger.Printf("Error stopping container %v: %v\n", t.ContainerID, result.Error)
	}
	t.FinishTime = time.Now().UTC()
	t.State = task.Completed
	w.Db[t.ID] = &t
	w.logger.Printf("Stopped and removed container \"%v\" for task \"%v\"\n", t.ContainerID, t.ID)

	return result
}

func (w *Worker) CollectStats() {
	for {
		w.logger.Println("Collecting stats...")
		w.Stats = GetStats()
		w.Stats.TaskCount = w.TaskCount
		time.Sleep(15 * time.Second)
	}
}

func (w *Worker) InspectTask(t task.Task) *task.DockerInspectResponse {
	config := task.NewDockerConfig(&t)
	d := task.NewDocker(config)
	return d.Inspect(t.ContainerID)
}

func (w *Worker) UpdateTasks() {
	for {
		w.logger.Println("Checking status of tasks...")
		w.updateTasks()
		w.logger.Println("Task updates completed")
		w.logger.Println("Sleeping for 15 seconds...")
		time.Sleep(15 * time.Second)
	}
}

func (w *Worker) updateTasks() {
	// for each task in the worker's datastore:
	// 1. call InspectTask method
	// 2. verify task is in running state
	// 3. if task is not in running state, or not running at all, mark task as `failed`
	for id, t := range w.Db {
		if t.State == task.Running {
			resp := w.InspectTask(*t)
			if resp.Error != nil {
				fmt.Printf("ERROR: %v\n", resp.Error)
			}

			if resp.Container == nil {
				w.logger.Printf("No container for running task %s\n", id)
				w.Db[id].State = task.Failed
			}

			if resp.Container.State.Status == "exited" {
				w.logger.Printf("Container for task %s in non-running state %s\n", id, resp.Container.State.Status)
				w.Db[id].State = task.Failed
			}

			// task is running, update exposed ports
			w.Db[id].HostPorts = resp.Container.NetworkSettings.NetworkSettingsBase.Ports
		}
	}
}

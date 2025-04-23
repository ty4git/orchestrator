package worker

import (
	"context"
	"fmt"
	"log"
	"orchestrator/store"
	"orchestrator/task"
	"os"
	"time"

	"github.com/golang-collections/collections/queue"
	"go.opentelemetry.io/otel"
)

type Worker struct {
	Name      string
	Queue     queue.Queue
	Db        store.Store
	TaskCount int
	Stats     *Stats
	logger    *log.Logger
}

func New(taskDbType string) *Worker {
	var s store.Store
	switch taskDbType {
	case "memory":
		s = store.NewInMemoryTaskStore()
	}
	return &Worker{
		Queue:  *queue.New(),
		Db:     s,
		logger: log.New(os.Stdout, "[orchestrator | worker | worker] ", log.LstdFlags),
	}
}

func (w *Worker) GetTasks(ctx context.Context) []*task.Task {
	_, span := otel.Tracer("").Start(ctx, "GetTasks")
	defer span.End()

	tasks, err := w.Db.List()
	if err != nil {
		w.logger.Printf("Error getting list of tasks: %v\n", err)
		return nil
	}
	return tasks.([]*task.Task)
}

func (w *Worker) AddTask(t *task.Task) {
	w.Queue.Enqueue(*t)
}

func (w *Worker) RunTasks(ctx context.Context) {
	for {
		func() {
			_, span := otel.Tracer("Run tasks...").Start(ctx, "Run tasks")
			defer span.End()
			if w.Queue.Len() != 0 {
				result := w.runTask(ctx)
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

func (w *Worker) runTask(ctx context.Context) task.DockerResult {
	t := w.Queue.Dequeue()
	if t == nil {
		w.logger.Println("No tasks in the queue")
		return task.DockerResult{Error: nil}
	}

	taskQueued := t.(task.Task)

	err := w.Db.Put(taskQueued.ID.String(), &taskQueued)
	if err != nil {
		msg := fmt.Errorf("Error storing task %s: %v", taskQueued.ID.String(), err)
		w.logger.Println(msg)
		return task.DockerResult{Error: msg}
	}

	rawTask, err := w.Db.Get(taskQueued.ID.String())
	if err != nil {
		msg := fmt.Errorf("Error getting task %s from database: %v", taskQueued.ID.String(), err)
		w.logger.Println(msg)
		return task.DockerResult{Error: msg}
	}

	taskPersisted := rawTask.(*task.Task)

	// if taskPersisted == nil {
	// 	if taskQueued.State != task.Scheduled {
	// 		err := fmt.Errorf("task \"%v\" has state \"%v\", but expected \"%v\"", taskQueued.ID, taskQueued.State, task.Scheduled)
	// 		return task.DockerResult{Error: err}
	// 	}
	// 	taskPersisted = &task.Task{
	// 		ID:    taskQueued.ID,
	// 		Name:  taskQueued.Name,
	// 		State: task.Pending,
	// 		Image: taskQueued.Image,
	// 	}
	// 	w.Db[taskPersisted.ID] = taskPersisted
	// }

	var result task.DockerResult
	if task.ValidStateTransition(taskPersisted.State, taskQueued.State) {
		switch taskQueued.State {
		case task.Scheduled:
			result = w.StartTask(ctx, taskQueued)
		case task.Stopped:
			result = w.StopTask(taskQueued)
		case task.Deleted:
			result = w.DeleteTask(taskQueued)
		default:
			result.Error = fmt.Errorf("we should not get here, state of queued task: \"%v\"", taskQueued.State)
		}
	} else {
		err := fmt.Errorf("invalid transition task ID \"%v\" from \"%v\" to \"%v\"",
			taskPersisted.ID, taskPersisted.State, taskQueued.State)
		result.Error = err
		return result
	}
	return result
}

func (w *Worker) StartTask(ctx context.Context, t task.Task) task.DockerResult {
	t.StartTime = time.Now().UTC()
	config := task.NewDockerConfig(&t)
	d := task.NewDocker(config)
	result := d.Run(ctx)
	if result.Error != nil {
		w.logger.Printf("Error of running task \"%v\": \"%v\"\n", t.ID, result.Error)
		t.State = task.Failed
		w.Db.Put(t.ID.String(), &t)
		return result
	}

	t.ContainerID = result.ContainerId
	t.State = task.Running
	w.Db.Put(t.ID.String(), &t)

	return result
}

func (w *Worker) StopTask(t task.Task) task.DockerResult {
	config := task.NewDockerConfig(&t)
	d := task.NewDocker(config)

	stopResult := d.Stop(t.ContainerID)
	if stopResult.Error != nil {
		w.logger.Printf("Error stopping container \"%v\": \"%v\"\n", t.ContainerID, stopResult.Error)
	}
	removeResult := d.Remove(t.ContainerID)
	if removeResult.Error != nil {
		log.Printf("%v\n", removeResult.Error)
	}

	t.FinishTime = time.Now().UTC()
	t.State = task.Stopped
	w.Db.Put(t.ID.String(), &t)
	w.logger.Printf("Stopped and removed container \"%v\" for task \"%v\"\n", t.ContainerID, t.ID)

	return stopResult
}

func (w *Worker) DeleteTask(t task.Task) task.DockerResult {
	// TODO: remove from w.Db
	return task.DockerResult{Action: "delete", Result: "success", Error: nil}
}

func (w *Worker) CollectStats() {
	for {
		w.logger.Println("Collecting stats...")
		w.Stats = GetStats()
		w.Stats.TaskCount = w.TaskCount
		time.Sleep(15 * time.Second)
	}
}

func (w *Worker) InspectTask(t task.Task) *task.DockerInspectResult {
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

	tasks, err := w.Db.List()
	if err != nil {
		w.logger.Printf("Error getting list of tasks: %v\n", err)
		return
	}

	for id, t := range tasks.([]*task.Task) {
		if t.State == task.Running {
			resp := w.InspectTask(*t)
			if resp.Error != nil {
				fmt.Printf("Error: %v\n", resp.Error)
			}

			if resp.Container == nil {
				w.logger.Printf("No container for running task %s\n", id)
				t.State = task.Failed
				w.Db.Put(t.ID.String(), t)
			}

			if resp.Container.State.Status == "exited" {
				w.logger.Printf("Container for task %s in non-running state %s\n", id, resp.Container.State.Status)
				t.State = task.Failed
				w.Db.Put(t.ID.String(), t)
			}

			// task is running, update exposed ports
			t.HostPorts = resp.Container.NetworkSettings.NetworkSettingsBase.Ports
			w.Db.Put(t.ID.String(), t)
		}
	}
}

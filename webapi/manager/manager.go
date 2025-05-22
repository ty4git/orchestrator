package manager

import (
	"fmt"
	"log/slog"
	"net/http"
	"orchestrator/manager"
	"orchestrator/task"
	"os"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin"
)

type ErrResponse struct {
	HTTPStatusCode int
	Message        string
}

type Api struct {
	Host    string
	Port    string
	Manager *manager.Manager
	logger  *slog.Logger
}

func NewApi(host string, port string, manager *manager.Manager) *Api {
	logger := slog.New(
		slog.NewJSONHandler(
			os.Stdout,
			&slog.HandlerOptions{
				AddSource: true,
				Level:     slog.LevelDebug,
			},
		),
	)

	return &Api{
		Host:    host,
		Port:    port,
		Manager: manager,
		logger:  logger,
	}
}

func (api *Api) Start() {
	engine := gin.Default()
	engine.Use(otelgin.Middleware("manager API"))
	api.createRoutes(engine)
	engine.Run(fmt.Sprintf("%s:%s", api.Host, api.Port))
}

func (api *Api) createRoutes(engine *gin.Engine) {
	tasks := engine.Group("tasks")
	{
		tasks.GET("", api.GetTasks)
		tasks.POST("", api.StartTask)
		tasks.DELETE("/:taskID", api.StopTask)
	}
	nodes := engine.Group("nodes")
	{
		nodes.GET("", api.GetNodes)
	}
}

func (api *Api) GetTasks(c *gin.Context) {
	c.JSON(http.StatusOK, api.Manager.GetTasks())
}

func (api *Api) StartTask(c *gin.Context) {
	te := task.TaskEvent{}
	if err := c.BindJSON(&te); err != nil {
		msg := fmt.Sprintf("Error unmarshalling body: %v", err)
		api.logger.Warn(msg)
		e := ErrResponse{
			HTTPStatusCode: 400,
			Message:        msg,
		}
		c.JSON(http.StatusBadRequest, e)
		return
	}

	api.Manager.AddTask(te)
	api.logger.Debug("Added task", "taskId", te.Task.ID)
	c.JSON(http.StatusCreated, te.Task)
}

func (api *Api) StopTask(c *gin.Context) {
	rawID := c.Param("taskID")
	if rawID == "" {
		msg := "No taskID passed in request."
		api.logger.Info(msg)
		c.JSON(http.StatusBadRequest, gin.H{"error": msg})
	}

	id, _ := uuid.Parse(rawID)
	rawTask, err := api.Manager.TaskDb.Get(id.String())
	if err != nil {
		api.logger.Warn("No task found by Id", "taskId", id)
		c.JSON(http.StatusNotFound, gin.H{"error": "Not found!$"})
	}
	taskToStop, ok := rawTask.(*task.Task)
	if !ok {
		api.logger.Error("Error while stopping task", "task", taskToStop)
	}

	te := task.TaskEvent{
		ID:        uuid.New(),
		State:     task.Stopped,
		Timestamp: time.Now(),
	}

	taskCopy := *taskToStop
	taskCopy.State = task.Stopped
	te.Task = taskCopy
	api.Manager.AddTask(te)

	api.logger.Debug("Added task event to stop task", "taskEventId", te.ID, "taskId", taskToStop.ID)
	c.Status(http.StatusOK)
}

func (api *Api) DeleteTask(c *gin.Context) {

}

func (api *Api) GetNodes(c *gin.Context) {
	c.JSON(http.StatusOK, api.Manager.WorkerNodes)
}

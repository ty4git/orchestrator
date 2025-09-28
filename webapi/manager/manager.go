package manager

import (
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"orchestrator/manager"
	"orchestrator/task"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin"
	semconv "go.opentelemetry.io/otel/semconv/v1.20.0"
)

const (
	APIName = "orchestrator-manager-api"
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

func NewApi(manager *manager.Manager, host string, port string) *Api {
	logger := slog.Default().With(
		string(semconv.ServiceNameKey), APIName,
	)

	return &Api{
		Host:    host,
		Port:    port,
		Manager: manager,
		logger:  logger,
	}
}

func (api *Api) Start() {
	slog.Info(fmt.Sprintf("Starting \"%s\" on http://%s:%s...", APIName, api.Host, api.Port),
		"host", api.Host, "port", api.Port)

	engine := gin.Default()
	engine.Use(otelgin.Middleware(APIName))
	api.createRoutes(engine)
	engine.Run(fmt.Sprintf("%s:%s", api.Host, api.Port))
}

func (api *Api) createRoutes(engine *gin.Engine) {
	tasks := engine.Group("tasks")
	{
		tasks.GET("", api.GetTasks)
		tasks.POST("", api.StartTask)
		tasks.DELETE("/:taskId", api.StopTask)
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
	ctx := c.Request.Context()
	apiAddTask := &StartTask{}
	if err := c.BindJSON(apiAddTask); err != nil {
		msg := fmt.Sprintf("Error unmarshalling body: %v", err)
		api.logger.WarnContext(ctx, msg)
		e := ErrResponse{
			HTTPStatusCode: 400,
			Message:        msg,
		}
		c.JSON(http.StatusBadRequest, e)
		return
	}

	addTask := apiAddTask.ToManagerTask()

	addTaskEvent := &task.TaskEvent{
		Id:      apiAddTask.EventId,
		Payload: addTask,
	}

	taskId, err := api.Manager.AddTask(ctx, addTaskEvent)
	if err != nil {
		if errors.Is(err, manager.ErrTaskEventAlreadyExists) {
			api.logger.WarnContext(ctx, "Task event already exists",
				"task.event.id", apiAddTask.EventId, "error", err)
			c.Status(http.StatusConflict)
			return
		}
		api.logger.ErrorContext(ctx,
			fmt.Sprintf("Error adding task: %v", err), "task.event.id", apiAddTask.EventId,
			"task.id", taskId)
		c.Status(http.StatusInternalServerError)
		return
	}
	api.logger.DebugContext(ctx, "Added task", "task.event.id", apiAddTask.EventId,
		"task.id", taskId)
	c.JSON(http.StatusCreated, addTaskEvent)
}

func (api *Api) StopTask(c *gin.Context) {
	ctx := c.Request.Context()
	rawID := c.Param("taskId")
	if rawID == "" {
		msg := "No taskId passed in request."
		api.logger.InfoContext(ctx, msg)
		c.JSON(http.StatusBadRequest, gin.H{"error": msg})
	}

	id, _ := uuid.Parse(rawID)
	err := api.Manager.StopTask(ctx, id)
	if err != nil {
		api.logger.ErrorContext(ctx, err.Error(), "taskId", id)
		if errors.Is(err, manager.ErrTaskNotFound) {
			c.Status(http.StatusNotFound)
		} else {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		}
		return
	}

	api.logger.DebugContext(ctx, "Added the stop task", "taskId", id)
	c.Status(http.StatusOK)
}

func (api *Api) DeleteTask(c *gin.Context) {

}

func (api *Api) GetNodes(c *gin.Context) {
	c.JSON(http.StatusOK, api.Manager.WorkerNodes)
}

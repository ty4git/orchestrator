package webapi

import (
	"fmt"
	"log"
	"net/http"
	"orchestrator/task"
	"orchestrator/worker"
	"os"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin"
)

type ErrResponse struct {
	HTTPStatusCode int
	Message        string
}

type Api struct {
	Worker *worker.Worker
	Host   string
	Port   string
	logger *log.Logger
}

func NewApi(worker *worker.Worker, host string, port string) *Api {
	return &Api{
		Worker: worker,
		Host:   host,
		Port:   port,
		logger: log.New(os.Stdout, "[orchestrator | webapi | worker] ", log.LstdFlags),
	}
}

func (api *Api) Start() {
	engine := gin.Default()
	engine.Use(otelgin.Middleware("worker API"))
	api.createRoutes(engine)
	engine.Run(fmt.Sprintf("%s:%s", api.Host, api.Port))
}

func (api *Api) createRoutes(engine *gin.Engine) {
	// TODO: move it to a separate file
	engine.GET("/health", func(c *gin.Context) {
		c.JSON(200, gin.H{
			"status": "running",
		})
	})

	tasksRoot := engine.Group("/tasks")
	{
		tasksRoot.GET("", api.GetTasks)
		tasksRoot.POST("", api.StartTask)

		taskRoot := tasksRoot.Group("/:id")
		{
			taskRoot.DELETE("", api.StopTask)
			taskRoot.GET("", api.InspectTask)
		}
	}

	engine.GET("/stats", api.GetStatsHandler)
}

func (api *Api) StartTask(c *gin.Context) {
	var taskEvent task.TaskEvent
	if err := c.BindJSON(&taskEvent); err != nil {
		msg := fmt.Sprintf("Error unmarshalling body: %v", err)
		api.logger.Println(msg)
		c.JSON(http.StatusBadRequest, gin.H{"error": msg})
		return
	}

	api.Worker.AddTask(taskEvent.Task)
	api.logger.Printf("Added task %v\n", taskEvent.Task.ID)
	c.JSON(http.StatusCreated, taskEvent.Task)
}

func (a *Api) GetTasks(c *gin.Context) {
	ctx := c.Request.Context()
	c.JSON(http.StatusOK, a.Worker.GetTasks(ctx))
}

func (api *Api) StopTask(c *gin.Context) {
	rawId := c.Param("id")
	if rawId == "" {
		msg := "No \"id\" passed in request."
		api.logger.Println(msg)
		c.JSON(http.StatusBadRequest, gin.H{"error": msg})
		return
	}

	id, _ := uuid.Parse(rawId)
	deletingTask, ok := api.Worker.Db[id]
	if !ok {
		msg := fmt.Sprintf("No task with id \"%v\" found", id)
		api.logger.Println(msg)
		c.JSON(http.StatusNotFound, gin.H{"error": msg})
		return
	}

	taskCopy := *deletingTask
	taskCopy.State = task.Finished
	api.Worker.AddTask(taskCopy)

	api.logger.Printf("Added task \"%v\" to stop container \"%v\"\n", deletingTask.ID, deletingTask.ContainerID)
	c.Status(http.StatusOK)
}

func (api *Api) GetStatsHandler(c *gin.Context) {
	c.JSON(http.StatusOK, api.Worker.Stats)
}

func (api *Api) InspectTask(c *gin.Context) {
	rawID := c.Param("id")
	if rawID == "" {
		msg := "No taskID passed in request."
		api.logger.Println(msg)
		c.JSON(http.StatusBadRequest, gin.H{"error": msg})
		return
	}

	tID, _ := uuid.Parse(rawID)
	t, ok := api.Worker.Db[tID]
	if !ok {
		msg := fmt.Sprintf("No task with ID \"%v\" found", tID)
		api.logger.Println(msg)
		c.JSON(http.StatusNotFound, gin.H{"error": msg})
		return
	}

	resp := api.Worker.InspectTask(*t)
	c.JSON(http.StatusOK, resp.Container)
}

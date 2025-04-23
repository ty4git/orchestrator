package manager

import (
	"fmt"
	"log"
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
	Address string
	Port    string
	Manager *manager.Manager
	logger  *log.Logger
}

func NewApi(address string, port string, manager *manager.Manager) *Api {
	return &Api{
		Address: address,
		Port:    port,
		Manager: manager,
		logger:  log.New(os.Stdout, "[orch | manager | api] ", log.LstdFlags),
	}
}

func (api *Api) Start() {
	engine := gin.Default()
	engine.Use(otelgin.Middleware("manager API"))
	api.createRoutes(engine)
	engine.Run(fmt.Sprintf("%s:%s", api.Address, api.Port))
}

func (api *Api) createRoutes(engine *gin.Engine) {
	tasks := engine.Group("tasks")
	{
		tasks.GET("", api.GetTasks)
		tasks.POST("", api.StartTask)
		tasks.DELETE("/:taskID", api.StopTask)
	}
}

func (api *Api) GetTasks(c *gin.Context) {
	c.JSON(http.StatusOK, api.Manager.GetTasks())
}

func (api *Api) StartTask(c *gin.Context) {
	te := task.TaskEvent{}
	if err := c.BindJSON(&te); err != nil {
		msg := fmt.Sprintf("Error unmarshalling body: %v", err)
		api.logger.Println(msg)
		e := ErrResponse{
			HTTPStatusCode: 400,
			Message:        msg,
		}
		c.JSON(http.StatusBadRequest, e)
		return
	}

	api.Manager.AddTask(te)
	api.logger.Printf("Added task \"%v\"\n", te.Task.ID)
	c.JSON(http.StatusCreated, te.Task)
}

func (api *Api) StopTask(c *gin.Context) {
	rawID := c.Param("taskID")
	if rawID == "" {
		msg := "No taskID passed in request."
		api.logger.Println(msg)
		c.JSON(http.StatusBadRequest, gin.H{"error": msg})
	}

	id, _ := uuid.Parse(rawID)
	rawTask, err := api.Manager.TaskDb.Get(id.String())
	if err != nil {
		msg := fmt.Sprintf("No task with ID \"%v\" found", id)
		api.logger.Println(msg)
		c.JSON(http.StatusNotFound, gin.H{"error": msg})
	}
	taskToStop, ok := rawTask.(*task.Task)
	if !ok {
		api.logger.Panicf("Error: %s", taskToStop)
	}

	te := task.TaskEvent{
		ID:        uuid.New(),
		State:     task.Stopped,
		Timestamp: time.Now(),
	}

	// we need to make a copy so we are not modifying the task in the datastore
	taskCopy := *taskToStop
	taskCopy.State = task.Stopped
	te.Task = taskCopy
	api.Manager.AddTask(te)

	api.logger.Printf("Added task event \"%v\" to stop task \"%v\"\n", te.ID, taskToStop.ID)
	c.Status(http.StatusOK)
}

func (api *Api) DeleteTask(c *gin.Context) {

}

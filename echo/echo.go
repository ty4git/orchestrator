package main

import (
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/gin-gonic/gin"
)

type Message struct {
	Msg string
}

func main() {
	engine := gin.Default()
	engine.GET("/health", func(c *gin.Context) {
		log.Println("Health check...")
		c.Status(http.StatusOK)
	})

	engine.GET("/healthfail", func(c *gin.Context) {
		log.Println("Health check failed")
		c.Status(http.StatusInternalServerError)
	})

	engine.POST("/", func(c *gin.Context) {
		var msg Message
		if err := c.BindJSON(&msg); err != nil {
			c.JSON(http.StatusBadRequest, errors.New("unable to decode request body"))
			return
		}

		log.Printf("Received message: %v\n", msg)
		c.JSON(http.StatusOK, msg)
	})

	go func() {
		engine.Run(fmt.Sprintf(":%s", "7777"))
	}()

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGKILL, syscall.SIGTERM)
	<-c

	log.Println("Shutting down...")
}

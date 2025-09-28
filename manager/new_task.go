package manager

import (
	"orchestrator/task"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/go-connections/nat"
	"github.com/google/uuid"
)

type TaskEvent struct {
	Id        uuid.UUID
	State     task.State
	Timestamp *time.Time
	Payload   any
}

type NewTask struct {
	Id            uuid.UUID
	Name          string
	Image         string
	ExposedPorts  nat.PortSet
	HostPorts     nat.PortMap
	PortBindings  map[string]string
	RestartPolicy container.RestartPolicyMode
	RestartCount  byte
	HealthCheck   string
}

type StopTask struct {
	Id uuid.UUID
}

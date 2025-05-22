package task

import (
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/go-connections/nat"
	"github.com/google/uuid"
)

type Task struct {
	ID            uuid.UUID
	ContainerID   string
	Name          string
	State         State // TODO: Make it private for task package, no one can change it
	Image         string
	Cpu           float64
	Memory        int64
	Disk          int64
	ExposedPorts  nat.PortSet
	HostPorts     nat.PortMap
	PortBindings  map[string]string
	RestartPolicy container.RestartPolicyMode
	StartTime     time.Time
	FinishTime    time.Time
	RestartCount  byte
	HealthCheck   string
}

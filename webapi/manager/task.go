package manager

import (
	"orchestrator/task"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/go-connections/nat"
	"github.com/google/uuid"
)

// type TaskEvent struct {
// 	Id      uuid.UUID `json:"id"`
// 	Payload any       `json:"payload"`
// }

type Task struct {
	ContainerID   string
	Name          string
	Image         string
	Cpu           float64
	Memory        int64
	Disk          int64
	ExposedPorts  nat.PortSet
	HostPorts     nat.PortMap
	PortBindings  map[string]string
	RestartPolicy container.RestartPolicyMode
	HealthCheck   string
}

type StartTask struct {
	ContainerID   string
	Name          string
	Image         string
	Cpu           float64
	Memory        int64
	Disk          int64
	ExposedPorts  nat.PortSet
	HostPorts     nat.PortMap
	PortBindings  map[string]string
	RestartPolicy container.RestartPolicyMode
	HealthCheck   string
	EventId       uuid.UUID
}

func (t *StartTask) ToManagerTask() *task.Task {
	return &task.Task{
		ContainerID:   t.ContainerID,
		Name:          t.Name,
		Image:         t.Image,
		Cpu:           t.Cpu,
		Memory:        t.Memory,
		Disk:          t.Disk,
		ExposedPorts:  t.ExposedPorts,
		HostPorts:     t.HostPorts,
		PortBindings:  t.PortBindings,
		RestartPolicy: t.RestartPolicy,
		HealthCheck:   t.HealthCheck,
	}
}

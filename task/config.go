// Config struct to hold Docker container config
package task

import (
	"github.com/docker/docker/api/types/container"
	"github.com/docker/go-connections/nat"
)

type DockerConfig struct {
	// Name of the task, also used as the container name
	Name string
	// AttachStdin boolean which determines if stdin should be attached
	AttachStdin bool
	// AttachStdout boolean which determines if stdout should be attached
	AttachStdout bool
	// AttachStderr boolean which determines if stderr should be attached
	AttachStderr bool
	// ExposedPorts list of ports exposed
	ExposedPorts nat.PortSet
	// PortBindings list of port bindings
	PortBindings map[string]string
	// Cmd to be run inside container (optional)
	Cmd []string
	// Image used to run the container
	Image string
	// Cpu
	Cpu float64
	// Memory in MiBь
	Memory int64
	// Disk in GiB
	Disk int64
	// Env variables
	Env           []string
	RestartPolicy container.RestartPolicyMode
}

func NewDockerConfig(t *Task) *DockerConfig {
	return &DockerConfig{
		Name:          t.Name,
		ExposedPorts:  t.ExposedPorts,
		PortBindings:  t.PortBindings,
		Image:         t.Image,
		Cpu:           t.Cpu,
		Memory:        t.Memory,
		Disk:          t.Disk,
		RestartPolicy: t.RestartPolicy,
	}
}

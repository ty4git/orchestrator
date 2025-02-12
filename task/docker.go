package task

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/client"
	"github.com/docker/docker/pkg/stdcopy"
	"github.com/docker/go-connections/nat"
)

type Docker struct {
	Client *client.Client
	Config DockerConfig
	logger *log.Logger
}

func NewDocker(c *DockerConfig) *Docker {
	dc, _ := client.NewClientWithOpts(client.FromEnv)
	return &Docker{
		Client: dc,
		Config: *c,
		logger: log.New(os.Stdout, "[orch | task | docker] ", log.LstdFlags),
	}
}

type DockerResult struct {
	Error       error
	Action      string
	ContainerId string
	Result      string
}

type DockerInspectResponse struct {
	Error     error
	Container *types.ContainerJSON
}

func (d *Docker) Run() DockerResult {
	ctx := context.Background()
	reader, err := d.Client.ImagePull(ctx, d.Config.Image, image.PullOptions{})
	if err != nil {
		d.logger.Printf("Error pulling image %s: %v\n", d.Config.Image, err)
		return DockerResult{Error: err}
	}
	defer reader.Close()
	io.Copy(os.Stdout, reader)

	rp := container.RestartPolicy{
		Name: d.Config.RestartPolicy,
	}
	container.ValidateRestartPolicy(rp)

	r := container.Resources{
		Memory: d.Config.Memory,
	}

	containerConfig := container.Config{
		Image:        d.Config.Image,
		Tty:          false,
		Env:          d.Config.Env,
		ExposedPorts: d.Config.ExposedPorts,
	}

	ports := make([]string, 0, len(d.Config.PortBindings))
	for containerPort, hostPort := range d.Config.PortBindings {
		ports = append(ports, fmt.Sprintf("%s:%s", containerPort, hostPort))
	}
	_, bindings, err := nat.ParsePortSpecs(ports)
	if err != nil {
		d.logger.Printf("Invalid configuration of port bindings in a task configuration: %v", err)
		return DockerResult{Error: err}
	}

	hc := container.HostConfig{
		RestartPolicy:   rp,
		Resources:       r,
		PublishAllPorts: true,
		PortBindings:    bindings,
	}

	resp, err := d.Client.ContainerCreate(ctx, &containerConfig, &hc, nil, nil, d.Config.Name)
	if err != nil {
		d.logger.Printf("Error creating container using image '%s': '%v'\n", d.Config.Image, err)
		return DockerResult{Error: err}
	}

	if err = d.Client.ContainerStart(ctx, resp.ID, container.StartOptions{}); err != nil {
		d.logger.Printf("Error starting container '%s': '%v'\n", resp.ID, err)
		return DockerResult{Error: err}
	}

	out, err := d.Client.ContainerLogs(ctx, resp.ID, container.LogsOptions{ShowStdout: true, ShowStderr: true})
	if err != nil {
		d.logger.Printf("Error getting logs for container '%s': '%v'\n", resp.ID, err)
		return DockerResult{Error: err}
	}

	stdcopy.StdCopy(os.Stdout, os.Stderr, out)

	return DockerResult{ContainerId: resp.ID, Action: "start", Result: "success"}
}

func (d *Docker) Stop(id string) DockerResult {
	d.logger.Printf("Attempting to stop container (id: '%v')", id)
	ctx := context.Background()
	err := d.Client.ContainerStop(ctx, id, container.StopOptions{})
	if err != nil {
		d.logger.Printf("Error stopping container '%s': '%v'\n", id, err)
		return DockerResult{Error: err}
	}

	err = d.Client.ContainerRemove(ctx, id, container.RemoveOptions{
		RemoveVolumes: true,
		RemoveLinks:   false,
		Force:         false,
	})
	if err != nil {
		err = fmt.Errorf("Error removing container \"%s\": \"%v\"", id, err)
		d.logger.Println(err)
		return DockerResult{Error: err}
	}

	return DockerResult{Action: "stop", Result: "success", Error: nil}
}

func (d *Docker) Inspect(id string) *DockerInspectResponse {
	d.logger.Println("Inspecting container...")
	ctx := context.Background()
	resp, err := d.Client.ContainerInspect(ctx, id)
	if err != nil {
		d.logger.Printf("Error inspecting container: %s\n", err)
		return &DockerInspectResponse{
			Error: err,
		}
	}
	return &DockerInspectResponse{
		Container: &resp,
	}
}

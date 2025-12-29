package node

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"orchestrator/communication"

	"orchestrator/worker"
)

type Node struct {
	Name            string
	Ip              string
	Api             string
	Memory          int64
	MemoryAllocated int64
	Disk            int64
	DiskAllocated   int64
	Stats           *worker.Stats
	Role            string
	TaskCount       int
}

func NewNode(name string, api string, role string) *Node {
	return &Node{
		Name: name,
		Api:  api,
		Role: role,
	}
}

func (n *Node) GetStats() (*worker.Stats, error) {
	var resp *http.Response
	var err error

	url := fmt.Sprintf("%s/stats", n.Api)
	resp, err = communication.HTTPWithRetry(http.Get, url)
	if err != nil {
		msg := fmt.Sprintf("Unable to connect to \"%v\". Permanent failure.\n", n.Api)
		log.Println(msg)
		return nil, errors.New(msg)
	}

	if resp.StatusCode != 200 {
		msg := fmt.Sprintf("Error retrieving stats from \"%v\": \"%v\"", n.Api, err)
		log.Println(msg)
		return nil, errors.New(msg)
	}

	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	var stats worker.Stats
	err = json.Unmarshal(body, &stats)
	if err != nil {
		msg := fmt.Sprintf("error decoding message while getting stats for node %s", n.Name)
		log.Println(msg)
		return nil, errors.New(msg)
	}

	if stats.MemStats == nil || stats.DiskStats == nil {
		return nil, fmt.Errorf("error getting stats from node %s", n.Name)
	}

	n.Memory = int64(stats.MemTotal())
	n.Disk = int64(stats.DiskTotal())
	n.Stats = &stats

	return n.Stats, nil
}

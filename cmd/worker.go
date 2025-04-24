package cmd

import (
	"context"
	"fmt"
	"log"
	"orchestrator/webapi"
	"orchestrator/worker"

	"github.com/google/uuid"
	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(workerCmd)
	workerCmd.Flags().StringP("host", "H", "0.0.0.0", "Hostname or IP address")
	workerCmd.Flags().StringP("port", "p", "5555", "Port on which to listen")
	workerCmd.Flags().StringP("name", "n", fmt.Sprintf("worker-%s", uuid.New().String()), "Name of the worker")
	workerCmd.Flags().StringP("dbtype", "d", "memory", "Type of datastore to use for tasks (\"memory\" or \"persistent\")")
}

var workerCmd = &cobra.Command{
	Use:   "worker",
	Short: "Worker command to operate a worker node.",
	Long: `aorta worker command.

The worker runs tasks and responds to the manager's requests about task state.`,
	Run: func(cmd *cobra.Command, args []string) {
		host, _ := cmd.Flags().GetString("host")
		port, _ := cmd.Flags().GetString("port")
		name, _ := cmd.Flags().GetString("name")
		dbType, _ := cmd.Flags().GetString("dbtype")

		log.Println("Starting worker...")
		w := worker.New(name, dbType)
		api := webapi.NewApi(w, host, port)

		ctx := context.Background()
		go w.RunTasks(ctx)
		go w.CollectStats()
		go w.UpdateTasks()
		log.Printf("Starting worker API on 'http://%s:%s' ...", host, port)
		api.Start()
	},
}

package cmd

import (
	"context"
	"log/slog"
	"orchestrator/infrastructure"
	"orchestrator/manager"
	managerApi "orchestrator/webapi/manager"

	"github.com/spf13/cobra"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
)

const (
	ServiceName    = "orchestrator-manager"
	ServiceVersion = "1.0.0"

	DeploymentEnvironment = "development"
)

func init() {
	rootCmd.AddCommand(managerCmd)
	managerCmd.Flags().StringP("host", "H", "0.0.0.0", "Hostname or IP address")
	managerCmd.Flags().StringP("port", "p", "5555", "Port on which to listen")
	managerCmd.Flags().StringSliceP("workers", "w", []string{"localhost:5556"}, "List of workers on which the manager will schedule tasks.")
	managerCmd.Flags().StringP("scheduler", "s", "epvm", "Name of scheduler to use.")
	managerCmd.Flags().StringP("dbType", "d", "memory", "Type of datastore to use for events and tasks (\"memory\" or \"persistent\")")
}

var managerCmd = &cobra.Command{
	Use:   "manager",
	Short: "Manager command to operate a manager node.",
	Long: `"manager" command

The manager controls the orchestration system and is responsible for:
- Accepting tasks from users
- Scheduling tasks onto worker nodes
- Rescheduling tasks in the event of a node failure
- Periodically polling workers to get task updates`,
	Run: func(cmd *cobra.Command, args []string) {
		host, _ := cmd.Flags().GetString("host")
		port, _ := cmd.Flags().GetString("port")
		workers, _ := cmd.Flags().GetStringSlice("workers")
		scheduler, _ := cmd.Flags().GetString("scheduler")
		dbType, _ := cmd.Flags().GetString("dbType")

		slog.SetDefault(slog.Default().With(
			string(semconv.ServiceNameKey), ServiceName,
			string(semconv.ServiceVersionKey), ServiceVersion,
		))

		slog.Info("Starting manager...")

		ctx := context.Background()
		managerTracer := infrastructure.InitOpenTel(ctx, ServiceName, ServiceVersion,
			DeploymentEnvironment)
		defer managerTracer.Shutdown(ctx)

		m := manager.
			New(workers, scheduler, dbType).
			Run()

		api := managerApi.NewApi(m, host, port)
		api.Start()
	},
}

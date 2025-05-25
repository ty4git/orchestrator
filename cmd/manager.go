package cmd

import (
	"context"
	"log/slog"
	"orchestrator/manager"
	managerApi "orchestrator/webapi/manager"

	"github.com/spf13/cobra"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
)

const (
	ServiceName    = "orchestrator-manager"
	ServiceVersion = "1.0.0"
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
			slog.Group("service",
				"name", ServiceName,
				"version", ServiceVersion,
			),
		))
		slog.Info("Starting manager...")

		// mhost := os.Getenv("CUBE_MANAGER_HOST")
		// mport := os.Getenv("CUBE_MANAGER_PORT")

		ctx := context.Background()
		managerTracer := initJaeger(ctx, ServiceName)
		defer managerTracer.Shutdown(ctx)

		m := manager.New(workers, scheduler, dbType)
		api := managerApi.NewApi(host, port, m)

		go m.ProcessTasks()
		go m.SynchronizeTasks()
		go m.DoHealthChecks()
		go m.UpdateNodeStats()
		slog.Info("Starting manager API on http://{host}:{port}...", "host", host, "port", port)
		api.Start()
	},
}

func initJaeger(ctx context.Context, serviceName string) *sdktrace.TracerProvider {
	exporter, err := otlptracehttp.New(
		ctx,
		otlptracehttp.WithEndpoint("localhost:4318"),
		otlptracehttp.WithInsecure(), // TODO: only for local development
	)

	if err != nil {
		panic(err)
	}

	res := resource.NewWithAttributes(
		semconv.SchemaURL,
		semconv.ServiceNameKey.String(serviceName),
		semconv.ServiceVersionKey.String(ServiceVersion),
		semconv.DeploymentEnvironmentKey.String("development"),
	)

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),

		// TODO: change it, only for tests
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)

	otel.SetTextMapPropagator(
		propagation.NewCompositeTextMapPropagator(
			propagation.TraceContext{},
			propagation.Baggage{},
		),
	)

	otel.SetTracerProvider(tp)

	return tp
}

package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"orchestrator/manager"
	"orchestrator/webapi"
	managerApi "orchestrator/webapi/manager"
	"orchestrator/worker"
	"os"
	"strconv"

	"github.com/joho/godotenv"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
)

func main() {
	fmt.Println("Starting orch!!!...")
	fmt.Println("Starting orchestrator...")

	err := godotenv.Load()
	if err != nil {
		log.Fatalf("Ошибка при загрузке файла .env: %v", err)
	}

	partType := flag.String("name", "", "manager or worker")
	flag.Parse()

	switch *partType {
	case "manager":
		runManager()
	case "worker":
		runWorkers()
	default:
		panic("Set up which system do you want to run: manager or worker")
	}
}

func runManager() {
	fmt.Println("Starting manager...")

	mhost := os.Getenv("CUBE_MANAGER_HOST")
	mport := os.Getenv("CUBE_MANAGER_PORT")

	whost := os.Getenv("CUBE_WORKER_HOST")
	wport := os.Getenv("CUBE_WORKER_PORT")

	ctx := context.Background()
	managerTracer := initJaeger(ctx, "manager")
	defer managerTracer.Shutdown(ctx)

	workers := []string{fmt.Sprintf("%s:%s", whost, wport)}
	schedulerType := "roundrobin"
	m := manager.New(workers, schedulerType)
	mapi := managerApi.NewApi(mhost, mport, m)

	go m.ProcessTasks()
	go m.UpdateTasks()
	go m.DoHealthChecks()

	mapi.Start()
}

func runWorkers() {
	fmt.Println("Starting worker...")

	whost := os.Getenv("CUBE_WORKER_HOST")
	wport := os.Getenv("CUBE_WORKER_PORT")
	wportVal, _ := strconv.Atoi(wport)

	ctx := context.Background()
	workerTracer := initJaeger(ctx, "worker")
	defer workerTracer.Shutdown(ctx)

	w1 := worker.New()
	wapi1 := webapi.NewApi(w1, whost, wport)
	w2 := worker.New()
	wapi2 := webapi.NewApi(w2, whost, strconv.Itoa(wportVal+1))
	w3 := worker.New()
	wapi3 := webapi.NewApi(w3, whost, strconv.Itoa(wportVal+2))

	workers := []*worker.Worker{w1, w2, w3}

	for _, w := range workers {
		go w.RunTasks(ctx)
		go w.CollectStats()
		go w.UpdateTasks()
	}

	wapis := []*webapi.Api{wapi1, wapi2, wapi3}
	for _, wapi := range wapis {
		wapi.Start()
	}
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
		semconv.ServiceVersionKey.String("1.0.0"),
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

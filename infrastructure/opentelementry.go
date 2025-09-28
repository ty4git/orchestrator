package infrastructure

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
)

const (
	Development = "development"
)

func InitOpenTel(ctx context.Context, serviceName string, serviceVersion string,
	deploymentEnvironment string) *sdktrace.TracerProvider {
	options := []otlptracehttp.Option{
		otlptracehttp.WithEndpoint("jaeger:4318"),
	}
	if deploymentEnvironment == Development {
		options = append(options, otlptracehttp.WithInsecure())
	}
	exporter, err := otlptracehttp.New(
		ctx,
		options...,
	)

	if err != nil {
		panic(err)
	}

	res := resource.NewWithAttributes(
		semconv.SchemaURL,
		semconv.ServiceNameKey.String(serviceName),
		semconv.ServiceVersionKey.String(serviceVersion),
		semconv.DeploymentEnvironmentKey.String(deploymentEnvironment),
	)

	tp := sdktrace.NewTracerProvider(
		// TODO: it's only for tests then change it
		sdktrace.WithSampler(sdktrace.AlwaysSample()),

		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
	)

	otel.SetTextMapPropagator(
		propagation.NewCompositeTextMapPropagator(
			propagation.TraceContext{},
			propagation.Baggage{},
		),
	)

	otel.SetTracerProvider(tp)
	return tp

	//lp := InitOTelLog(ctx, serviceName, deploymentEnvironment, res)

	// return tp, lp
}

// func InitOTelLog(ctx context.Context, serviceName string, deploymentEnvironment string,
// 	res *resource.Resource) *log.LoggerProvider {
// 	tempLogger := slog.Default()
// 	tempLogger.Info("Configuring logger...")

// 	logOptions := []otlploghttp.Option{
// 		otlploghttp.WithEndpoint("jaeger:4318"),
// 	}
// 	if deploymentEnvironment == Development {
// 		logOptions = append(logOptions, otlploghttp.WithInsecure())
// 	}
// 	logExporter, err := otlploghttp.New(ctx, logOptions...)
// 	if err != nil {
// 		panic(err)
// 	}
// 	loggerProvider := log.NewLoggerProvider(
// 		log.WithResource(res),
// 		log.WithProcessor(log.NewBatchProcessor(logExporter)),
// 	)
// 	logger := otelslog.NewLogger(fmt.Sprintf("%s-%s", serviceName, "logger"),
// 		otelslog.WithLoggerProvider(loggerProvider),
// 		otelslog.WithSource(true))

// 	slog.SetDefault(logger)

// 	logger.InfoContext(ctx, "Hello world!")

// 	tempLogger.Info("Logger configured.")
// 	return loggerProvider
// }

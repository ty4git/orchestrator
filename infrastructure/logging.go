package infrastructure

import (
	"io"
	"log/slog"
	"os"
	"path"

	"gopkg.in/natefinch/lumberjack.v2"
)

const (
	LogFileDir  = "./logs"
	LogFileName = "orchestrator.log"

	DeploymentEnvironmentKey = "deployment.environment"
)

func NewLogger(environment string) *slog.Logger {
	tempLogger := newTempLogger(environment)

	tempLogger.Info("Configuring logger...")

	logFilePath := path.Join(LogFileDir, LogFileName)
	if err := os.MkdirAll(LogFileDir, 0755); err != nil {
		tempLogger.Error("Could not create directory of logs", "error", err)
		panic(err)
	}
	logFileWriter := &lumberjack.Logger{
		Filename:   logFilePath,
		MaxSize:    10,
		MaxBackups: 3,
		MaxAge:     28,
		Compress:   true,
		LocalTime:  true,
	}
	multiWriter := io.MultiWriter(os.Stdout, logFileWriter)
	logger := slog.New(
		slog.NewJSONHandler(multiWriter, &slog.HandlerOptions{
			AddSource: true,
			Level:     slog.LevelDebug,
		}),
	).With(
		DeploymentEnvironmentKey, environment,
	)
	slog.SetDefault(logger)
	slog.Info("Logger configured.")
	return logger
}

func newTempLogger(environment string) *slog.Logger {
	return slog.New(
		slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
			AddSource: true,
			Level:     slog.LevelInfo,
		}),
	).With(
		DeploymentEnvironmentKey, environment,
	)
}

package infrastructure

import (
	"context"
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

type Logger struct {
	*slog.Logger
	ctx context.Context
}

func NewLogger(inner *slog.Logger) *Logger {
	return &Logger{
		Logger: inner,
		ctx:    context.Background(),
	}
}

func (l *Logger) WithCtx(ctx context.Context) *Logger {
	return &Logger{
		Logger: l.Logger,
		ctx:    ctx,
	}
}

func (l *Logger) Debug(msg string, args ...any) {
	l.Log(l.ctx, slog.LevelDebug, msg, args...)
}

func (l *Logger) Info(msg string, args ...any) {
	l.Log(l.ctx, slog.LevelInfo, msg, args...)
}

func (l *Logger) Warn(msg string, args ...any) {
	l.Log(l.ctx, slog.LevelWarn, msg, args...)
}

func (l *Logger) Error(msg string, args ...any) {
	l.Log(l.ctx, slog.LevelError, msg, args...)
}

func NewSlogLogger(environment string) *slog.Logger {
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
	jsonHandler := slog.NewJSONHandler(multiWriter, &slog.HandlerOptions{
		AddSource: true,
		Level:     slog.LevelDebug,
	})
	logger := slog.New(NewOTelSlogHandler(jsonHandler)).
		With(DeploymentEnvironmentKey, environment)

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
	).With(DeploymentEnvironmentKey, environment)
}

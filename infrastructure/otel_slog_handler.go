package infrastructure

import (
	"context"
	"errors"
	"log/slog"

	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

type OTelSlogHandler struct {
	innerHandler slog.Handler
}

func NewOTelSlogHandler(inner slog.Handler) *OTelSlogHandler {
	return &OTelSlogHandler{innerHandler: inner}
}

func (h *OTelSlogHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.innerHandler.Enabled(ctx, level)
}

func (h *OTelSlogHandler) Handle(ctx context.Context, r slog.Record) error {
	span := trace.SpanFromContext(ctx)
	spanContext := span.SpanContext()
	if spanContext.IsValid() {
		r.AddAttrs(
			slog.String("trace_id", spanContext.TraceID().String()),
			slog.String("span_id", spanContext.SpanID().String()),
		)

		if r.Level == slog.LevelError {
			span.SetStatus(codes.Error, "")
			span.RecordError(errors.New(r.Message))
		}
	}
	return h.innerHandler.Handle(ctx, r)
}

func (h *OTelSlogHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return NewOTelSlogHandler(h.innerHandler.WithAttrs(attrs))
}

func (h *OTelSlogHandler) WithGroup(name string) slog.Handler {
	return NewOTelSlogHandler(h.innerHandler.WithGroup(name))
}

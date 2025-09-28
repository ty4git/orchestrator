package task

import (
	"time"

	"github.com/google/uuid"
)

type TaskEvent struct {
	Id        uuid.UUID
	State     State
	Timestamp time.Time
	Payload   any
}

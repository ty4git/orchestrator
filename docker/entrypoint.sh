#!/bin/sh

apk update
apk add --no-cache curl

# LOG_DIR="/app/logs"
# LOG_FILE="$LOG_DIR/orchestrator.log"
# mkdir -p "$LOG_DIR"

# {
# #   go build -o /app/myapp /app/main.go && /app/myapp
#   go run . manager --port ${ORCHESTRATOR_PORT}
# } 2>&1 | tee -a "$LOG_FILE"

# go run . manager --port ${ORCHESTRATOR_PORT} 2>&1 | tee -a "$LOG_FILE"

# go install github.com/air-verse/air@latest
go install github.com/go-delve/delve/cmd/dlv@latest

dlv --listen=:40000 --headless=true --api-version=2 debug \
    --continue --accept-multiclient -- manager --port ${ORCHESTRATOR_PORT}
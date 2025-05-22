#!/bin/sh

apk update
apk add --no-cache curl

## go install github.com/air-verse/air@latest
# go install github.com/go-delve/delve/cmd/dlv@latest

# dlv --listen=:40000 --headless=true --api-version=2 debug \
#     --continue --accept-multiclient -- manager --port $ORCHESTRATOR_PORT

echo "Entrypoint file!!! Port=${ORCHESTRATOR_PORT}"

go run . manager --port ${ORCHESTRATOR_PORT}
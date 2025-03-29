#!/bin/sh

# go install github.com/air-verse/air@latest
go install github.com/go-delve/delve/cmd/dlv@latest

dlv --listen=:40000 --headless=true --api-version=2 debug \
    --continue --accept-multiclient -- -name manager
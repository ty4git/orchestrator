#!/bin/bash

make run-worker &>> worker.log &
PID1=$!
echo "Waiting http://localhost:8080..."
while ! nc -z localhost 8081; do
    echo "Port is not open yet, waiting..."
    sleep 1
done

make run-manager &>> manager.log &
PID2=$!
echo "Waiting processes с PID '$PID1' и '$PID2'..."

tail -f worker.log manager.log >> all.log

trap "kill $PID1 $PID2; echo "Finished"; exit" SIGINT

wait

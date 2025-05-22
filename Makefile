include .env

REQUESTS_DIR=./requests

run-manager:
	@echo "Manager address: http://$(CUBE_MANAGER_HOST):$(CUBE_MANAGER_PORT)"
	go run . manager --port 8080

run-worker:
	@echo "Worker address: http://$(CUBE_WORKER_HOST):$(CUBE_WORKER_PORT)"
	go run . worker --port 8081

add-task:
	curl -v -X POST $(CUBE_MANAGER_HOST):$(CUBE_MANAGER_PORT)/tasks \
		-H "Content-Type: application/json" \
		-d @$(REQUESTS_DIR)/add_task.json

add-task-healthfail:
	curl -v \
		-X POST $(CUBE_MANAGER_HOST):$(CUBE_MANAGER_PORT)/tasks \
		-H "Content-Type: application/json" \
		-d @$(REQUESTS_DIR)/add_task_healthfail.json

get-tasks:
	curl -v http://localhost:$(CUBE_MANAGER_PORT)/tasks | jq

ID=bb1d59ef-9fc1-4e4b-a44d-db571eeed203
Url="http://$(CUBE_MANAGER_HOST):$(CUBE_MANAGER_PORT)/tasks/$(ID)"
stop-task:
	echo "Task id: $(ID)"
	curl -v -X DELETE $(Url)

run-all:
	docker compose up -d
rerun-manager:
	docker compose up -d --force-recreate manager
rerun-kibana:
	docker compose up -d --force-recreate elasticsearch
	docker compose up -d --force-recreate kibana
rerun-filebeat:
	docker compose up -d --force-recreate filebeat

stop-all:
	docker compose down

restart-filebeat:
	docker compose down filebeat
	docker compose build filebeat
	docker compose up -d filebeat
	docker compose logs filebeat
# or docker-compose up -d --force-recreate filebeat
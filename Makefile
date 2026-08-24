.PHONY: help up down logs gateway frontend vet build test docker-build

COMPOSE_DIR := deploy/compose
KAFKA_CONTAINER := cozy-critter-kafka

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*## "}; {printf "  %-14s %s\n", $$1, $$2}'

up: ## Start the Compose Kafka stack and wait for it to be healthy
	cd $(COMPOSE_DIR) && docker compose up -d
	@echo "waiting for $(KAFKA_CONTAINER) to become healthy..."
	@until [ "$$(docker inspect --format='{{.State.Health.Status}}' $(KAFKA_CONTAINER) 2>/dev/null)" = "healthy" ]; do sleep 2; done
	@echo "$(KAFKA_CONTAINER) is healthy"

down: ## Stop the Compose Kafka stack
	cd $(COMPOSE_DIR) && docker compose down

logs: ## Tail Kafka's logs
	docker logs -f $(KAFKA_CONTAINER)

gateway: ## Run the gateway locally (run `make up` first)
	go run ./cmd/gateway

frontend: ## Serve the static frontend on :8081
	cd frontend && python3 -m http.server 8081

vet: ## go vet
	go vet ./...

build: vet ## go build (after vet)
	go build ./...

test: vet ## go vet + go test (run `make up` first for full Kafka-backed coverage)
	go test ./...

docker-build: ## Build the gateway container image
	docker build -t cozy-critter-gateway:dev .

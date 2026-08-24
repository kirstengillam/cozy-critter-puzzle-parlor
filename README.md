# Cozy Critter Puzzle Parlor

For the product/architecture context and the "why" behind decisions, see
[cozy_critter_context.md](cozy_critter_context.md) (the PRD) and
[IMPLEMENTATION_PLAN.md](IMPLEMENTATION_PLAN.md) (the finer-grained plan and status). This file is just
the commands: how to run everything locally, and how to deploy to Kubernetes.

A `Makefile` wraps the local-dev commands below — `make help` lists all targets (`up`, `down`,
`gateway`, `frontend`, `vet`, `build`, `test`, `docker-build`). The full commands are still spelled out
here for the Kubernetes path and for anyone who'd rather not use `make`.

## Prerequisites

- Go 1.27+
- Docker
- Python 3 (only used to serve the static frontend during local dev — any static file server works)
- For the Kubernetes path: [`kind`](https://kind.sigs.k8s.io/) and `kubectl`

## Running locally (Docker Compose)

Three pieces run separately: Kafka (via Compose), the gateway (via `go run`), and the static frontend
(via any file server).

**1. Start Kafka:**

```bash
cd deploy/compose
docker compose up -d
```

Wait for it to report healthy:

```bash
docker inspect --format='{{.State.Health.Status}}' cozy-critter-kafka
```

**2. Start the gateway** (from the repo root; it creates its Kafka topics on startup):

```bash
go run ./cmd/gateway
```

Env vars (all optional, shown with defaults):

| Var | Default | Purpose |
|---|---|---|
| `KAFKA_BROKERS` | `localhost:9092` | comma-separated broker list |
| `PORT` | `8080` | gateway HTTP/WS port |
| `ALLOWED_ORIGINS` | `localhost:8081` | origins allowed to open a WebSocket connection |

**3. Serve the frontend** (from `frontend/`, on the port `ALLOWED_ORIGINS` expects — 8081 by default):

```bash
cd frontend
python3 -m http.server 8081
```

Open **http://localhost:8081**.

**Tearing down:**

```bash
cd deploy/compose
docker compose down
```
(Ctrl+C stops the gateway and the frontend server.)

## Running tests

```bash
go vet ./...
go build ./...
go test ./...
```

Room-lifecycle tests (`internal/room`, and most of `internal/gateway`) don't need Kafka running and
will just pass. The movement/chat broadcast tests (`TestMovementBroadcast`,
`TestChatApprovedBroadcastsAndRejectedStaysPrivate`) skip themselves with a clear message if Kafka isn't
reachable at `localhost:9092` — bring up Compose (step 1 above) first to actually run them.

## Building the gateway container image

```bash
docker build -t cozy-critter-gateway:dev .
```

## Deploying to Kubernetes (kind + Strimzi)

This mirrors the Compose setup (single broker, no replication) — see IMPLEMENTATION_PLAN.md's
"Kubernetes parity" section for why it's built this way.

**1. Create the cluster** (maps the gateway's NodePort to host `:8080`, same as Compose's port):

```bash
kind create cluster --name cozy-critter --config deploy/k8s/kind-config.yaml
```

**2. Install the Strimzi operator:**

```bash
kubectl create namespace kafka
curl -sL 'https://strimzi.io/install/latest?namespace=kafka' -o /tmp/strimzi-install.yaml
kubectl create -f /tmp/strimzi-install.yaml -n kafka
kubectl wait --for=condition=Available deployment/strimzi-cluster-operator -n kafka --timeout=180s
```

**3. Deploy Kafka:**

```bash
kubectl apply -f deploy/k8s/kafka.yaml -n kafka
kubectl wait --for=condition=Ready kafka/cozy-critter -n kafka --timeout=300s
```

**4. Build and load the gateway image, then deploy it:**

```bash
docker build -t cozy-critter-gateway:dev .
kind load docker-image cozy-critter-gateway:dev --name cozy-critter
kubectl create namespace cozy-critter
kubectl apply -f deploy/k8s/gateway.yaml
kubectl wait --for=condition=Available deployment/gateway -n cozy-critter --timeout=60s
```

**5. Serve the frontend** same as local dev:

```bash
cd frontend
python3 -m http.server 8081
```

Open **http://localhost:8081** — the gateway is reachable at `localhost:8080` either way, so the
frontend doesn't know or care whether Compose or `kind` is behind it.

**Useful while it's running:**

```bash
kubectl get pods -n kafka
kubectl get pods -n cozy-critter
kubectl logs -n cozy-critter deployment/gateway -f
```

**Tearing down:**

```bash
kind delete cluster --name cozy-critter
docker rmi cozy-critter-gateway:dev
```

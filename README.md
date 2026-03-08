# Distributed Data Systems Project Template

Basic project structure with Python's Flask and Redis. 
**You are free to use any web framework in any language and any database you like for this project.**

### Project structure

* `env`
    Folder containing the Redis env variables for the docker-compose deployment
    
* `helm-config` 
   Helm chart values for Redis and ingress-nginx
        
* `k8s`
    Folder containing the kubernetes deployments, apps and services for the ingress, order, payment and stock services.
    
* `order`
    Folder containing the order application logic and dockerfile. 
    
* `payment`
    Folder containing the payment application logic and dockerfile. 

* `stock`
    Folder containing the stock application logic and dockerfile. 

* `test`
    Folder containing some basic correctness tests for the entire system. (Feel free to enhance them)

### Deployment types:

#### docker-compose (local development)

After coding the REST endpoint logic run `docker-compose up --build` in the base folder to test if your logic is correct
(you can use the provided tests in the `\test` folder and change them as you wish). 

***Requirements:*** You need to have docker and docker-compose installed on your machine. 

K8s is also possible, but we do not require it as part of your submission. 

#### minikube (local k8s cluster)

This setup is for local k8s testing to see if your k8s config works before deploying to the cloud. 
First deploy your database using helm by running the `deploy-charts-minicube.sh` file (in this example the DB is Redis 
but you can find any database you want in https://artifacthub.io/ and adapt the script). Then adapt the k8s configuration files in the
`\k8s` folder to mach your system and then run `kubectl apply -f .` in the k8s folder. 

***Requirements:*** You need to have minikube (with ingress enabled) and helm installed on your machine.

#### kubernetes cluster (managed k8s cluster in the cloud)

Similarly to the `minikube` deployment but run the `deploy-charts-cluster.sh` in the helm step to also install an ingress to the cluster. 

***Requirements:*** You need to have access to kubectl of a k8s cluster.

-------------------------------------------------------------------------------------------

# Distributed Web Shop — DDS26 Team 16

A fault-tolerant, distributed e-commerce backend built with Flask microservices, Redis, and Nginx. Supports two checkout protocols: **Saga (orchestration)** and **Two-Phase Commit (2PC)**, selectable at runtime via an environment variable.


## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Services](#services)
3. [Checkout Protocols](#checkout-protocols)
4. [Fault Tolerance](#fault-tolerance)
5. [API Reference](#api-reference)
6. [Getting Started](#getting-started)
7. [Configuration](#configuration)
8. [Testing](#testing)
9.  [Technology Stack](#technology-stack)


## Architecture Overview

```
                    ┌───────────────────────────────┐
   Client           │       NGINX Gateway :8000     │
   requests  ──────►│  Round-robin + auto-failover  │
                    └──────┬──────────┬─────────────┘
                           │          │
              ┌────────────┘          └──────────────┐
              ▼                                      ▼
   ┌────────────────────┐               ┌────────────────────┐
   │  order-service-1   │               │  order-service-2   │
   │  stock-service-1   │               │  stock-service-2   │
   │  payment-service-1 │               │  payment-service-2 │
   └────────┬───────────┘               └──────────┬─────────┘
            │                                      │
            └──────────────┬───────────────────────┘
                           ▼
              ┌─────────────────────────┐
              │  Redis Master + Replica │  (one pair per service)
              │  order / stock /        │
              │  payment                │
              └─────────────────────────┘
```

Each of the three logical services (order, stock, payment) runs as **2 application replicas** behind a shared Nginx upstream pool. Each service has its own isolated **Redis master + replica + Sentinel** cluster for data persistence, replication, and automatic failover. The stack totals 19 containers (16 original + 3 Sentinels).


## Services

### Order Service
The **Saga coordinator** and **2PC coordinator**. Owns all checkout orchestration — it calls the stock and payment services in sequence and drives either compensation (Saga) or prepare/commit/abort (2PC) depending on the active mode.

### Stock Service
Owns item inventory. Exposes standard CRUD endpoints plus full **2PC participant** endpoints (`prepare_subtract`, `commit_subtract`, `abort_subtract`). Uses optimistic locking (`WATCH/MULTI/EXEC`) for all concurrent stock mutations.

### Payment Service
Owns user credit balances. Exposes standard CRUD plus full **2PC participant** endpoints (`prepare_pay`, `commit_pay`, `abort_pay`). Also uses optimistic locking for all credit mutations.

### Gateway (Nginx)
Routes `/orders/`, `/stock/`, and `/payment/` prefixes to their respective upstream pools. Configured with `proxy_next_upstream error timeout http_500 http_502 http_503` — a failed replica is transparently retried on the healthy one within the same request.


## Checkout Protocols

The active protocol is selected via the `CHECKOUT_MODE` environment variable on the order service (`saga` or `2pc`). Both protocols produce identical externally observable results — stock is deducted, credit is charged, and the order is marked `paid`. They differ in how they handle failures and guarantee atomicity.

### Saga (Orchestration)

The order service calls participants sequentially. If any step fails, it executes compensating transactions to undo completed steps.

**Idempotency guarantees:**
- `status=paid` → return 200 immediately (safe for benchmark retries)
- `status=failed` → return 400 immediately (do not re-execute)
- `status=started` → crash recovery marker, treated as failed, return 400

### Two-Phase Commit (2PC)

The order service is the 2PC **coordinator**. Stock and payment are **participants** with WAL-backed idempotent endpoints.

**Phase 1 — PREPARE:**
Each participant checks resource availability, creates a soft-reservation, and writes a WAL entry. Responds YES (200) or NO (400).

**Phase 2 — COMMIT or ABORT:**
If all participants voted YES, the coordinator durably writes `status=paid` to its own Redis **first** (the point of no return), then sends COMMIT to all participants. If any participant voted NO, ABORT is sent to all that prepared and `status=failed` is written.

**WAL keys:**
```
2pc:stock:{order_id}:{item_id}      → prepared | committed | aborted
2pc:payment:{order_id}:{user_id}    → prepared | committed | aborted
2pc:pending                          → coordinator WAL: set of in-flight order_ids
reserved:stock:{item_id}             → int  (soft-reserved units)
reserved:payment:{user_id}           → int  (soft-reserved credit)
```

**Crash recovery:** On every gunicorn worker startup, `recover_2pc()` scans `2pc:pending`. Orders with `status=paid` are re-driven to COMMIT (participants are idempotent). Orders in any other state are aborted. This guarantees forward progress after a coordinator crash at any point in the protocol.

**Double-spend prevention:** The Saga `/subtract` and `/pay` endpoints respect active 2PC soft-reservations. Before deducting, they read `reserved:stock:{item_id}` or `reserved:payment:{user_id}` and reject the deduction if it would cut into reserved amounts. A concurrent Saga checkout cannot steal resources held by an in-flight 2PC transaction.


## Fault Tolerance

### Application Replica Failure

Nginx upstream pools are configured with `max_fails=1 fail_timeout=5s` and `proxy_next_upstream error timeout http_500 http_502 http_503`. When one replica is killed or crashes, Nginx automatically retries the request on the other within the same client connection. All services have `restart: unless-stopped` so Docker re-creates crashed containers.

### Redis Master Failure (Sentinel Automatic Failover)

Each service has a **Redis master + replica + Sentinel** cluster. Redis Sentinel continuously monitors the master; if it fails to respond for 5 seconds it triggers an automatic failover — the replica is promoted to master in ~5–8 seconds. All application services use a Sentinel-aware Redis client (`REDIS_SENTINEL_HOSTS` env var) so they discover the new master transparently after failover. The old master, when it restarts, rejoins as a replica. In Kubernetes this is handled by the bitnami/redis Helm chart with `sentinel.enabled=true`.

### Concurrent Correctness

All stock and credit mutations use Redis `WATCH/MULTI/EXEC` optimistic locking with up to 10 retry attempts. If two Gunicorn workers attempt to modify the same key simultaneously, one observes a `WatchError`, discards its changes, and retries with the latest value. This prevents lost updates, negative balances, and overselling under concurrent load.


## API Reference

All paths are relative to the gateway at `http://localhost:8000`.

### Orders (`/orders/`)

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/orders/create/<user_id>` | Create a new empty order |
| `POST` | `/orders/addItem/<order_id>/<item_id>/<quantity>` | Add an item to an order |
| `GET`  | `/orders/find/<order_id>` | Get order details and status |
| `POST` | `/orders/checkout/<order_id>` | Trigger checkout (Saga or 2PC per `CHECKOUT_MODE`) |
| `POST` | `/orders/batch_init/<n>/<n_items>/<n_users>/<item_price>` | Seed n random orders |

### Stock (`/stock/`)

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/stock/item/create/<price>` | Create a new item |
| `GET`  | `/stock/find/<item_id>` | Get stock level and price |
| `POST` | `/stock/add/<item_id>/<amount>` | Add stock units |
| `POST` | `/stock/subtract/<item_id>/<amount>` | Deduct stock (Saga path) |
| `POST` | `/stock/batch_init/<n>/<starting_stock>/<item_price>` | Seed n items |
| `POST` | `/stock/prepare_subtract/<order_id>/<item_id>/<amount>` | 2PC Phase 1 — soft-reserve |
| `POST` | `/stock/commit_subtract/<order_id>/<item_id>/<amount>` | 2PC Phase 2 — commit deduction |
| `POST` | `/stock/abort_subtract/<order_id>/<item_id>/<amount>` | 2PC Phase 2 — release reservation |

### Payment (`/payment/`)

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/payment/create_user` | Create a new user with 0 credit |
| `GET`  | `/payment/find_user/<user_id>` | Get user credit balance |
| `POST` | `/payment/add_funds/<user_id>/<amount>` | Add credit |
| `POST` | `/payment/pay/<user_id>/<amount>` | Deduct credit (Saga path) |
| `POST` | `/payment/batch_init/<n>/<starting_money>` | Seed n users |
| `POST` | `/payment/prepare_pay/<order_id>/<user_id>/<amount>` | 2PC Phase 1 — soft-reserve |
| `POST` | `/payment/commit_pay/<order_id>/<user_id>/<amount>` | 2PC Phase 2 — commit deduction |
| `POST` | `/payment/abort_pay/<order_id>/<user_id>/<amount>` | 2PC Phase 2 — release reservation |


## Getting Started

### Prerequisites

- Docker ≥ 24
- Docker Compose v2 (`docker compose`, not `docker-compose`)
- `curl` and `bash` (for test scripts)

### Start the stack

```bash
docker compose down -v          # wipe old volumes (clean slate)
docker compose up --build       # build images and start all 16 containers
```

The gateway is available at `http://localhost:8000` once all services report healthy — typically 10–15 seconds.

### Quick smoke test

```bash
# Seed 10 items (stock=100, price=10) and 10 users (credit=1000)
curl -sX POST http://localhost:8000/stock/batch_init/10/100/10
curl -sX POST http://localhost:8000/payment/batch_init/10/1000

# Create a user with 500 credit
USER=$(curl -s -X POST http://localhost:8000/payment/create_user \
       | grep -o '"user_id":"[^"]*"' | cut -d'"' -f4)
curl -sX POST http://localhost:8000/payment/add_funds/$USER/500

# Create an order, add 2 units of item 0 (cost = 20), check out
ORDER=$(curl -s -X POST http://localhost:8000/orders/create/$USER \
        | grep -o '"order_id":"[^"]*"' | cut -d'"' -f4)
curl -sX POST "http://localhost:8000/orders/addItem/$ORDER/0/2"
curl -sX POST "http://localhost:8000/orders/checkout/$ORDER"

# Verify: status should be "paid", stock should be 98, credit 480
curl -s http://localhost:8000/orders/find/$ORDER
curl -s http://localhost:8000/stock/find/0
curl -s http://localhost:8000/payment/find_user/$USER
```

### Kubernetes — minikube (local)

**Prerequisites:** minikube, helm, kubectl, docker

```bash
# 1. Start minikube
minikube start

# 2. Build images + install Helm charts (Redis + nginx ingress) + apply all manifests
bash deploy-charts-minikube.sh

# 3. Watch pods come up (all should reach Running 1/1)
kubectl get pods -w
```

Once all pods are `Running`, forward the ingress port in a separate terminal, then run the tests:

```bash
# Terminal 1 — keep running
kubectl port-forward -n ingress-nginx svc/ingress-nginx-controller 8080:80

# Terminal 2
DEPLOY_MODE=kube BASE_URL=http://localhost:8080 bash test-scripts/run_all.sh
```

### Deletion of the stack

```bash
helm uninstall order-redis stock-redis payment-redis nginx
kubectl delete -f k8s/
kubectl delete configmap gateway-nginx-conf
kubectl delete pvc --all
kubectl delete pv --all

```

Or to destroy the entire minikube cluster:
```bash
minikube delete
```


## Configuration

All configuration is via environment variables defined in `docker-compose.yml`.

| Variable | Service | Description | Default |
|----------|---------|-------------|---------|
| `CHECKOUT_MODE` | order | `saga` or `2pc` | `saga` |
| `GATEWAY_URL` | order | Internal URL for inter-service calls | `http://gateway:80` |
| `REDIS_HOST` | all | Redis master hostname (direct connection fallback) | per-service name |
| `REDIS_PORT` | all | Redis port | `6379` |
| `REDIS_PASSWORD` | all | Redis auth password | `redis` |
| `REDIS_DB` | all | Redis database index | `0` |
| `REDIS_SENTINEL_HOSTS` | all | Comma-separated `host:port` list of Sentinel nodes | *(not set → direct)* |
| `REDIS_MASTER_NAME` | all | Sentinel master set name | `mymaster` |

### Switching checkout mode at runtime

```bash
# Switch order services to 2PC without touching Redis config
cat > /tmp/mode_override.yml <<EOF
services:
  order-service-1:
    environment:
      CHECKOUT_MODE: "2pc"
  order-service-2:
    environment:
      CHECKOUT_MODE: "2pc"
EOF

docker compose stop order-service-1 order-service-2
docker compose -f docker-compose.yml -f /tmp/mode_override.yml \
    up -d --no-deps order-service-1 order-service-2

# Restore Saga
docker compose stop order-service-1 order-service-2
docker compose up -d --no-deps order-service-1 order-service-2
```


## Testing

All tests are self-contained bash scripts in `test-scripts/` and require only `curl` — no Python, no extra packages.

### Run the full suite

```bash
bash test-scripts/run_all.sh
```

### Run a single test

```bash
bash test-scripts/01_happy_path.sh
bash test-scripts/09_2pc_protocol.sh
```

### Run without fault-tolerance tests (faster)

```bash
SKIP_FAULT_TESTS=1 bash test-scripts/run_all.sh
```

### Target a remote stack

```bash
BASE_URL=http://192.168.1.100:8000 bash test-scripts/run_all.sh
```

### Test suite

| Script | Description |
|--------|-------------|
| `01_happy_path.sh` | Full Saga checkout succeeds: stock decreases, credit decreases, order marked `paid` |
| `02_idempotency.sh` | Retrying a paid checkout returns 200; retrying a failed checkout returns 400 — no double-charges |
| `03_compensation_payment_fails.sh` | Payment step fails → all reserved stock is released by Saga compensation; credit untouched |
| `04_compensation_stock_fails.sh` | Stock step fails → order fails immediately; payment step never reached; no stock leaked |
| `05_fault_app_replica.sh` | Kills each app replica in turn (6 total); Nginx routes to surviving replica; checkout always succeeds |
| `06_fault_redis_master.sh` | Kills each Redis master in turn; Sentinel promotes the replica within ~5 s; checkout uninterrupted; old master rejoins as replica |
| `07_mode_flag.sh` | Switches `CHECKOUT_MODE` between `saga` and `2pc` at runtime; both modes produce correct end-to-end results and correct compensation |
| `08_consistency_check.sh` | Fires 10 concurrent checkouts against the same item; verifies no negative stock/credit and no lost updates |
| `09_2pc_protocol.sh` | Directly exercises all 2PC participant endpoints: prepare/commit/abort idempotency, double-spend prevention under reservations, reservation conflict detection |
| `10_redis_aof_persistence.sh` | Crashes all three Redis masters and verifies stock, credit, and order data is fully restored on restart — proving AOF persistence works end-to-end |

## Technology stack

| Component | Technology |
|-----------|------------|
| Application services | Python 3.12, Flask, Gunicorn (4 workers per replica) |
| Serialisation | msgspec (msgpack binary format) |
| Data store | Redis 7.2 with AOF persistence enabled |
| Data replication | Redis master + replica + Sentinel per service |
| Load balancer | Nginx 1.25 with upstream health tracking |
| Container runtime | Docker Compose v2 |
| HTTP client | Python requests with 3-attempt retry |
| Concurrency control | Redis WATCH/MULTI/EXEC optimistic locking |
| Automatic failover | Redis Sentinel (one per service in docker, bitnami chart in K8s) |

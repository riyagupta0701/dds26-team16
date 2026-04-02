# Distributed Web Shop — DDS26 Team 16

A fault-tolerant, distributed e-commerce backend built with Flask microservices, Redis, and Nginx. This architecture utilizes **Event-Driven Inter-Service Communication** via Redis Streams, supporting both **Saga (orchestration)** and **Two-Phase Commit (2PC)** protocols.

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Service Interaction: RPC over Streams](#service-interaction-rpc-over-streams)
3. [Checkout Protocols](#checkout-protocols)
4. [Fault Tolerance & Reliability](#fault-tolerance--reliability)
5. [API Reference](#api-reference)
6. [Getting Started](#getting-started)
   - [Docker Compose](#docker-compose)
   - [Kubernetes — minikube](#kubernetes--minikube-local)
7. [Configuration](#configuration)
8. [Testing](#testing)
9. [Technology Stack](#technology-stack)

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
   │   Order Service    │               │   Order Service    │
   └────────┬───────────┘               └──────────┬─────────┘
            │                                      │
            │  ┌──────────────────────────────┐    │
            └─►│  Orchestrator Service (×2)   │◄───┘
               │  WAL-backed task execution   │
               └──────────────┬───────────────┘
                              │
            ┌─────────────────┴──────────────────┐
            │      Redis Streams (MQ) Layer      │
            │  (events:stock / events:payment)   │
            └─────────────────┬──────────────────┘
                              ▼
              ┌─────────────────────────┐
              │     Stock Service       │
              │    Payment Service      │
              └────────────┬────────────┘
                           │
              ┌────────────┴────────────┐
              │  Redis Master + Replica │  (one cluster per service,
              │  + Sentinel Cluster     │   incl. orchestrator)
              └─────────────────────────┘
```

The system is composed of three logical services (Order, Stock, Payment) and an **Orchestrator** that coordinates multi-service transactions. Each service runs as **2 application replicas** behind a shared Nginx upstream pool. For data persistence and high availability, each of the three business services has its own isolated **Redis master + replica + Sentinel** cluster. The orchestrator persists its WAL state to the shared **WAL Redis** cluster and uses the shared MQ Redis for messaging — it does not have a separate Redis cluster. A shared **WAL Redis** cluster (master + replica + Sentinel) stores all Write-Ahead Log entries independently of every service's business data. The complete stack totals **22 containers**.

### Redis Architecture: State vs. Messaging

To ensure high availability and prevent head-of-line blocking, each service maintains **two distinct connection pools**:

1.  **State Connection (`db`):** Used for persistent business data (e.g., inventory counts, user credits, order status).
2.  **Messaging Connection (`mq`):** Used for inter-service communication (Redis Streams, Consumer Groups, RPC replies).

These connections are configured via separate environment variables to allow splitting traffic or scaling the messaging layer independently.

### Orchestrator Service

The **Orchestrator** is a standalone service (2 replicas) that executes multi-step transaction workflows on behalf of the Order service. When the Order service initiates a checkout, it submits a **batch** of tasks to the orchestrator via the `events:orchestrator` Redis Stream. The orchestrator:

1.  **Persists the batch to a WAL** (`wal:orch:pending` set + `wal:orch:batch:{id}` key) in the shared `wal-redis` cluster before processing.
2.  **Dispatches tasks** to participant streams (`events:stock`, `events:payment`) respecting dependency ordering.
3.  **Collects replies** via temporary `BLPOP` keys and retries failed deliveries (configurable via `ORCH_MAX_RETRIES`).
4.  **Pushes the final result** back to the Order service's `reply_to` key and cleans up the WAL.

On startup, each orchestrator replica runs a **recovery sweep**: it scans `wal:orch:pending` in `wal-redis`, acquires distributed locks (`wal:orch:lock:{batch_id}`) to prevent duplicate processing, and re-drives any incomplete batches. This ensures crash recovery even if the orchestrator dies mid-transaction.

## Service Interaction: RPC over Streams

Inter-service calls (e.g., the Order service asking the Stock service to deduct items) are implemented as **Synchronous RPC over Asynchronous Streams**:

1.  **Request:** The caller generates a unique `reply_to` key (a Redis List) and pushes a message to the target service's Stream (e.g., `events:stock`).
2.  **Consumption:** Target services run background threads using **Consumer Groups** (`XREADGROUP`) to process requests.
3.  **Response:** The target service processes the logic and pushes the result back to the `reply_to` list using `RPUSH`.
4.  **Blocking Wait:** The caller waits on the `reply_to` key using a blocking pop (`BLPOP`) with a timeout.

This pattern decouples the services, provides automatic load balancing via Consumer Groups, and ensures that inter-service traffic does not interfere with the HTTP gateway.

## Checkout Protocols

The active protocol is selected via the `CHECKOUT_MODE` environment variable (`saga` or `2pc`).

### Saga (Orchestration)
The Order service submits a batch of tasks (stock subtraction, payment charge) to the **Orchestrator**, which dispatches them sequentially to Stock and Payment via Redis Streams. If a step fails (e.g., insufficient credit), the Order service sends **compensating transactions** (rollbacks) to restore the system state. Each retry attempt uses an **incremental transaction ID** (`{order_id}_{attempt}`) so that tombstones from previous attempts don't block retries.

### Two-Phase Commit (2PC)
A high-integrity protocol for atomicity across services, also coordinated through the Orchestrator.
*   **Phase 1 (PREPARE):** The Orchestrator dispatches prepare tasks to participants, which soft-reserve resources and write WAL entries.
*   **Phase 2 (COMMIT/ABORT):** Once the Order service durably logs the decision, the Orchestrator broadcasts commit/abort to all participants.

## Fault Tolerance & Reliability

### Tombstone Pattern (Idempotency)
A critical challenge in distributed systems is message reordering or retries. If a rollback command arrives *before* the original request, the system uses **Tombstones**:
*   When a rollback arrives for an unknown transaction, the service records a "Tombstone" (e.g., `TX_ROLLED_BACK`).
*   If the original request arrives later, the service sees the Tombstone and ignores the request.

### Decoupled Write-Ahead Log (WAL)

Every WAL entry in the system lives in a **dedicated `wal-redis` cluster** that is completely separate from each service's business-data Redis. This is the core persistence guarantee:

| What | Where stored | Key namespace |
|------|-------------|---------------|
| Saga in-flight orders | `wal-redis` | `wal:order:saga:pending` (Set) |
| 2PC coordinator in-flight | `wal-redis` | `wal:order:2pc:pending` (Set) |
| Saga tx-id per attempt | `wal-redis` | `wal:order:saga:tx:{order_id}` |
| Stock 2PC state machine | `wal-redis` | `wal:stock:2pc:{order_id}` |
| Payment 2PC state machine | `wal-redis` | `wal:payment:2pc:{order_id}:{user_id}` |
| Orchestrator batch state | `wal-redis` | `wal:orch:batch:{batch_id}` (JSON) |
| Orchestrator in-flight set | `wal-redis` | `wal:orch:pending` (Set) |
| Orchestrator recovery locks | `wal-redis` | `wal:orch:lock:{batch_id}` (TTL key) |

**Why this matters:** if `order-redis`, `stock-redis`, or `payment-redis` is wiped or corrupted, the WAL is untouched. On restart, the service reads its business data (which may be partially restored via AOF/replica) and reads the WAL from `wal-redis` to determine which transactions need to be committed or rolled back. The two stores are never written in the same pipeline or transaction.

**Separation rule enforced in code:** every service has a `wal.py` module that opens an independent Redis connection to `wal-redis`. Business writes go to `db` (business Redis); WAL writes go to `wal` (WAL Redis). These are never mixed in the same `WATCH/MULTI/EXEC` pipeline.

**`--appendfsync always`:** `wal-redis` is configured with `appendfsync always` (fsync on every write) rather than `everysec`. This guarantees zero WAL data loss even on a hard crash, at the cost of slightly higher latency on WAL writes.

### Background Recovery

On startup, each `order-service` replica scans its WAL (`wal:order:saga:pending` or `wal:order:2pc:pending`) in `wal-redis`. If it finds transactions that were in-flight during a crash, it deterministically re-drives them. Each **orchestrator** replica scans `wal:orch:pending` in the same `wal-redis`, acquires distributed locks (`wal:orch:lock:{batch_id}`) to prevent duplicate processing, and re-dispatches any incomplete batches. All participant services are idempotent, so re-driving a batch that was partially completed is safe.

### Application Replica Failure
Nginx upstream pools are configured with `max_fails=1 fail_timeout=5s` and `proxy_next_upstream error timeout http_500 http_502 http_503`. When one replica crashes, Nginx automatically retries the request on the healthy one. All containers use `restart: unless-stopped`.

### Redis Master Failure (Sentinel Automatic Failover)
Each service has a **Redis master + replica + Sentinel** cluster. Redis Sentinel monitors the master; if it fails, it triggers an automatic failover, promoting the replica to master in ~5–8 seconds. Application services use a Sentinel-aware client to discover the new master transparently.

### Concurrent Correctness (Optimistic Locking)
All state mutations use Redis `WATCH/MULTI/EXEC` optimistic locking with up to 10 retry attempts. This prevents lost updates and inconsistent state under concurrent load.

## API Reference

| Service | Prefix | Key Endpoints |
|---------|--------|---------------|
| **Order** | `/orders/` | `create`, `addItem`, `find`, `checkout`, `health` |
| **Stock** | `/stock/` | `item/create`, `find`, `add`, `subtract`, `health` |
| **Payment**| `/payment/`| `create_user`, `find_user`, `add_funds`, `pay`, `health` |

*Note: All inter-service logic is handled via the MQ and is not exposed directly on the HTTP gateway. Each service provides a `/health` endpoint that checks both its DB and MQ connectivity.*

## Getting Started

### Docker Compose

Three compose files are provided targeting different CPU budgets. All expose the gateway on `http://localhost:8000`.

**Prerequisites:** Docker ≥ 24, Docker Compose v2.

#### Small — single instance (~5 CPUs)
One replica and one gunicorn worker per service, one orchestrator instance, standalone Redis per service (no Sentinel).

```bash
docker compose -f docker-compose-small.yml down -v
docker compose -f docker-compose-small.yml up --build
```

#### Medium — 50 CPUs (hard limit)
Four replicas per service (8 workers each, 3.5 CPU cap), 2 orchestrator replicas (1.4 CPU each), full Redis Sentinel HA for all services including orchestrator. Total hard limit: 49.9 CPUs.

```bash
docker compose -f docker-compose-medium.yml down -v
docker compose -f docker-compose-medium.yml up --build
```

#### Large — 90 CPUs (hard limit)
Eight replicas per service (8 workers each, 3.1 CPU cap), 4 orchestrator replicas (1.5 CPU each), full Redis Sentinel HA for all services including orchestrator. Designed for a 96-core machine, leaving ~6 CPUs for locust clients. Total hard limit: 89.8 CPUs.

```bash
docker compose -f docker-compose-large.yml down -v
docker compose -f docker-compose-large.yml up --build
```

**Run tests** (any tier):
```bash
bash test-scripts/run_all.sh
```

### Kubernetes — minikube (Local)

**Prerequisites:** minikube, kubectl, docker.

1.  **Start minikube:**
    ```bash
    minikube start
    ```

2.  **Deploy the stack:**
    ```bash
    bash deploy-charts-minikube.sh
    ```
    This builds the images into minikube's Docker daemon and applies all manifests in `k8s/` (Redis Sentinel clusters, gateway, and app services).

3.  **Access the Gateway:**
    In a separate terminal, forward the ingress port:
    ```bash
    kubectl port-forward -n ingress-nginx svc/ingress-nginx-controller 8080:80
    ```
    Then run tests targeting the minikube gateway:
    ```bash
    BASE_URL=http://localhost:8080 bash test-scripts/run_all.sh
    ```

## Configuration

Configuration is managed via environment variables in `docker-compose.yml` or K8s manifests.

| Variable | Service | Description | Default |
|----------|---------|-------------|---------|
| `CHECKOUT_MODE` | order | `saga` or `2pc` | `saga` |
| `GATEWAY_URL` | order | Internal URL for inter-service calls | `http://gateway:80` |
| `REDIS_HOST` | all | Redis master hostname (fallback) | per-service name |
| `REDIS_PORT` | all | Redis port | `6379` |
| `REDIS_PASSWORD`| all | Redis auth password | `redis` |
| `REDIS_SENTINEL_HOSTS` | all | Comma-separated `host:port` list of Sentinel nodes | *(optional)* |
| `REDIS_MASTER_NAME` | all | Sentinel master set name | `mymaster` |
| `WAL_REDIS_HOST` | order, stock, payment | Decoupled WAL Redis hostname (fallback) | `wal-redis-master` |
| `WAL_REDIS_PORT` | order, stock, payment | WAL Redis port | `6379` |
| `WAL_REDIS_PASSWORD` | order, stock, payment | WAL Redis auth password | `redis` |
| `WAL_REDIS_DB` | order, stock, payment | WAL Redis database index | `0` |
| `WAL_SENTINEL_HOSTS` | order, stock, payment | WAL Sentinel `host:port` nodes | `wal-redis-sentinel:26379` |
| `WAL_MASTER_NAME` | order, stock, payment | WAL Sentinel master set name | `wal-master` |
| `MQ_REDIS_HOST` | all | Host for the MQ connection (Redis Streams) | *(required)* |
| `MQ_REDIS_PORT` | all | Port for the MQ connection | `6379` |
| `MQ_REDIS_PASSWORD`| all | Password for the MQ connection | `redis` |
| `MQ_REDIS_DB` | all | DB index for the MQ connection | `0` |
| `ORCH_MAX_RETRIES` | orchestrator | Max retry attempts per task delivery | `3` |
| `ORCH_TASK_TIMEOUT_S` | orchestrator | Timeout (seconds) waiting for a task reply | `5` |
| `ORCH_CONSUMER_NAME` | orchestrator | Unique consumer name per replica | `orchestrator-1` |

## Testing Suite

| Script | Description |
|--------|-------------|
| `01_happy_path.sh` | Full Saga checkout success: stock decreases, credit decreases, order marked `paid`. |
| `02_idempotency.sh` | Retrying a paid checkout returns 200; retrying a failed checkout returns 400. |
| `03_compensation_payment_fails.sh` | Payment failure triggers stock compensation; stock fully restored. |
| `04_compensation_stock_fails.sh` | Out-of-stock checkout fails; stock and credit unchanged. |
| `05_fault_app_replica.sh` | Kills app replicas; Nginx routes to survivors; checkout always succeeds. |
| `06_fault_redis_master.sh` | Kills Redis master; Sentinel promotes replica; checkout uninterrupted. |
| `07_mode_flag.sh` | Switches between Saga and 2PC at runtime; both modes pass end-to-end. |
| `08_consistency_check.sh` | Fires concurrent checkouts and verifies stock/credit consistency. |
| `09_2pc_protocol.sh` | Exercises 2PC participant endpoints: prepare/commit/abort idempotency and soft-reservations. |
| `10_redis_aof_persistence.sh` | Crashes Redis masters and verifies data is restored on restart via AOF. |
| `11_native_mq_2pc.sh` | Tests 2PC participant protocol directly over MQ (prepare, commit, abort). |
| `12_orchestrator.sh` | Orchestrator integration tests: saga/2PC flows, concurrency, pause/resume, WAL recovery. |
| `13_microservices_pytest.sh` | Python integration tests for stock, payment, and order service correctness. |
| `14_wal_decoupled.sh` | Verifies WAL keys live only in `wal-redis` for all services and orchestrator; confirms WAL survives killing business-data Redis and both orchestrator containers. |

## Technology Stack

| Component | Technology |
|-----------|------------|
| **App Logic** | Python 3.12, Flask, Gunicorn |
| **Inter-Service Comm** | Redis Streams (MQ) + Consumer Groups |
| **Data Store** | Redis 7.2 with AOF (Persistence) |
| **Data Replication** | Redis Master + Replica + Sentinel |
| **Serialization** | msgspec (msgpack binary format) |
| **Load Balancer** | Nginx 1.25 with upstream health tracking |
| **Orchestration** | WAL-backed task orchestrator with distributed locking and crash recovery |
| **Reliability** | 2PC, Saga, Tombstones, WAL Recovery, Sentinel Failover |

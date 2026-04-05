# Distributed Web Shop — DDS26 Team 16

A fault-tolerant, distributed e-commerce backend built with Flask microservices, Redis, and Nginx. This architecture utilizes **Event-Driven Inter-Service Communication** via Redis Streams, supporting both **Saga (orchestration)** and **Two-Phase Commit (2PC)** protocols.

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Service Interaction: RPC over Streams](#service-interaction-rpc-over-streams)
3. [Checkout Protocols](#checkout-protocols)
4. [Fault Tolerance & Reliability](#fault-tolerance--reliability)
5. [API Reference](#api-reference)
6. [Getting Started](#getting-started)
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
            │      Redis Streams (MQ) Layer        │
            │  (events:stock / events:payment)     │
            └──────────────┬───────────────────────┘
                           ▼
              ┌─────────────────────────┐
              │     Stock Service       │
              │    Payment Service      │
              └────────────┬────────────┘
                           │
              ┌────────────┴────────────┐
              │  Redis Master + Replica │  (one pair per service)
              │  + Sentinel Cluster     │  (order / stock / payment)
              └─────────────────────────┘
```

The system is composed of three logical services (Order, Stock, Payment). Each service runs as one or more application replicas behind a shared Nginx upstream pool — the number of replicas depends on the chosen compose file (1 / 4 / 8 for small / medium / large). For data persistence and high availability, each service has its own isolated **Redis master + replica + Sentinel** cluster.

### Redis Architecture: State vs. Messaging

To ensure high availability and prevent head-of-line blocking, each service maintains **two distinct connection pools**:

1.  **State Connection (`db`):** Used for persistent business data (e.g., inventory counts, user credits, order status).
2.  **Messaging Connection (`mq`):** Used for inter-service communication (Redis Streams, Consumer Groups, RPC replies).

These connections are configured via separate environment variables to allow splitting traffic or scaling the messaging layer independently.

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
The Order service orchestrates the flow by sending sequential RPC commands to Stock and Payment. If a step fails (e.g., insufficient credit), the Order service sends **compensating transactions** (rollbacks) to the MQ to restore the system state.

### Two-Phase Commit (2PC)
A high-integrity protocol for atomicity across services.
*   **Phase 1 (PREPARE):** Order service asks participants to soft-reserve resources. Participants write a WAL (Write-Ahead Log) entry.
*   **Phase 2 (COMMIT/ABORT):** Once the Order service durably logs the decision to its own Redis (the point of no return), it broadcasts the final result to all participants.

## Fault Tolerance & Reliability

### Tombstone Pattern (Idempotency)
A critical challenge in distributed systems is message reordering or retries. If a rollback command arrives *before* the original request, the system uses **Tombstones**:
*   When a rollback arrives for an unknown transaction, the service records a "Tombstone" (e.g., `TX_ROLLED_BACK`).
*   If the original request arrives later, the service sees the Tombstone and ignores the request.

### Background Recovery
On startup, each `order-service` replica scans its WAL (`2pc:pending` or `saga:pending`). If it finds transactions that were in-flight during a crash, it deterministically re-drives them to completion (either committing or rolling back).

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

Three compose files are provided targeting different CPU budgets, plus a general-purpose compose for local testing. All expose the gateway on `http://localhost:8000`.

**Prerequisites:** Docker ≥ 24, Docker Compose v2.

#### Testing - general docker compose setup
Docker compose setup that is similar to "medium" and "large" configuration but with fewer resources such that it can be run on our laptops for testing.

**Prerequisites:** For the tests you need to install pytest and requests:
```bash
pip install pytest requests
```

**Run tests** (using the default `docker-compose.yml`):
```bash
docker compose up --build
bash test-scripts/run_all.sh
```

#### Small — single instance (~5 CPUs)
One replica and one gunicorn worker per service. Standalone Redis per service (no Sentinel).

```bash
docker compose -f docker-compose-small.yml down -v
docker compose -f docker-compose-small.yml up --build
```

#### Medium — 50 CPUs (hard limit)
Four replicas per service, each capped at 3.75 CPUs. Full Redis Sentinel HA (3 sentinels per cluster, quorum=2).

```bash
docker compose -f docker-compose-medium.yml down -v
docker compose -f docker-compose-medium.yml up --build
```

#### Large — 90 CPUs (hard limit)
Eight replicas per service, each capped at 3.4 CPUs. Designed for 96-core machine, leaving ~6 CPUs for locust clients. Full Redis Sentinel HA (3 sentinels per cluster, quorum=2).

```bash
docker compose -f docker-compose-large.yml down -v
docker compose -f docker-compose-large.yml up --build
```

## Configuration

Configuration is managed via environment variables in `docker-compose.yml`.

| Variable | Service | Description | Default |
|----------|---------|-------------|---------|
| `CHECKOUT_MODE` | order | `saga` or `2pc` | `saga` |
| `GATEWAY_URL` | order | Internal URL for inter-service calls | `http://gateway:80` |
| `REDIS_HOST` | all | Redis master hostname (fallback) | per-service name |
| `REDIS_PORT` | all | Redis port | `6379` |
| `REDIS_PASSWORD`| all | Redis auth password | `redis` |
| `REDIS_SENTINEL_HOSTS` | all | Comma-separated `host:port` list of Sentinel nodes | *(optional)* |
| `REDIS_MASTER_NAME` | all | Sentinel master set name | `mymaster` |
| `MQ_REDIS_HOST` | all | Host for the MQ connection (Redis Streams) | *(required)* |
| `MQ_REDIS_PORT` | all | Port for the MQ connection | `6379` |
| `MQ_REDIS_PASSWORD`| all | Password for the MQ connection | `redis` |
| `MQ_REDIS_DB` | all | DB index for the MQ connection | `0` |

## Testing Suite

**Prerequisites:** `pip install pytest requests`

The test suite runs core correctness tests (01–04, 08) in **both saga and 2pc modes**. It switches modes at runtime via the `/orders/mode/<mode>` API — no restart needed. Fault tolerance tests run in saga mode (the default), since the infrastructure-level fault handling (nginx failover, Sentinel, container restart) is mode-agnostic.

Run all tests:
```bash
bash test-scripts/run_all.sh
```

Skip fault tolerance tests (faster):
```bash
SKIP_FAULT_TESTS=1 bash test-scripts/run_all.sh
```

| Script | Description |
|--------|-------------|
| `01_happy_path.sh` | Full Saga checkout success: stock decreases, credit decreases, order marked `paid`. |
| `02_idempotency.sh` | Retrying a paid checkout returns 200; retrying a failed checkout returns 400. |
| `03_compensation_payment_fails.sh` | Checkout with insufficient credit; verifies stock is rolled back. |
| `04_compensation_stock_fails.sh` | Checkout with insufficient stock; verifies credit is not deducted. |
| `05_fault_app_replica.sh` | Kills app replicas; Nginx routes to survivors; checkout always succeeds. |
| `06_fault_redis_master.sh` | Kills Redis master; Sentinel promotes replica; checkout uninterrupted. |
| `07_mode_flag.sh` | Switches `CHECKOUT_MODE` between saga and 2pc; verifies both work. |
| `08_consistency_check.sh` | Concurrent checkouts; verifies stock and credit totals are consistent. |
| `09_2pc_protocol.sh` | Exercises 2PC participant endpoints: prepare/commit/abort idempotency. |
| `10_redis_aof_persistence.sh` | Crashes Redis masters and verifies data is restored on restart via AOF. |
| `11_native_mq_2pc.sh` | Exercises 2PC participant endpoints over MQ: prepare/commit/abort idempotency. |
| `12_microservices_pytest.sh` | Python unittest suite covering stock, payment, and order service correctness. |
| `13_fault_mq_redis.sh` | Kills MQ Redis; verifies checkouts fail; restarts and verifies recovery + consistency. |
| `14_fault_kill_process.sh` | Kills PID 1 inside service containers; verifies Docker restarts and checkouts recover. |
| `15_fault_sequential_kills.sh` | Kills services one-by-one with recovery between each; verifies stock/credit consistency. |

## Technology Stack

| Component | Technology |
|-----------|------------|
| **App Logic** | Python 3.12, Flask, Gunicorn |
| **Inter-Service Comm** | Redis Streams (MQ) + Consumer Groups |
| **Data Store** | Redis 7.2 with AOF (Persistence) |
| **Data Replication** | Redis Master + Replica + Sentinel |
| **Serialization** | msgspec (msgpack binary format) |
| **Load Balancer** | Nginx 1.25 with upstream health tracking |
| **Reliability** | 2PC, Saga, Tombstones, Sentinel Failover |

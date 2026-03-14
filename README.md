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
              │  order / stock /        │
              │  payment                │
              └─────────────────────────┘
```

The system is composed of three logical services (Order, Stock, Payment), each horizontally scaled with 2 replicas. Unlike traditional REST architectures where services call each other via HTTP, this stack uses **Redis Streams** as a high-performance message bus.

### Redis Architecture: State vs. Messaging

To ensure high availability and prevent head-of-line blocking, each service maintains **two distinct connection pools**:

1.  **State Connection (`db`):** Used for persistent business data (e.g., inventory counts, user credits, order status).
2.  **Messaging Connection (`mq`):** Used for inter-service communication (Redis Streams, Consumer Groups, RPC replies).

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
A critical challenge in distributed systems is message reordering or retries. If a rollback command arrives *before* the original request (due to a network hiccup or coordinator timeout), the system uses **Tombstones**:
*   When a rollback arrives for an unknown transaction, the service records a "Tombstone" (e.g., `TX_ROLLED_BACK`).
*   If the original request arrives later, the service sees the Tombstone and ignores the request, preventing a "ghost" deduction from occurring after a rollback.

### Background Recovery
On startup, each `order-service` replica scans its WAL (`2pc:pending` or `saga:pending`). If it finds transactions that were in-flight during a crash, it deterministically re-drives them to completion (either committing or rolling back) using the idempotent participant endpoints.

### Optimistic Locking
All state mutations use Redis `WATCH/MULTI/EXEC`. If two requests attempt to modify the same item or user simultaneously, Redis detects the collision, and the service retries the operation with the latest data.

## API Reference

| Service | Prefix | Key Endpoints |
|---------|--------|---------------|
| **Order** | `/orders/` | `create`, `addItem`, `find`, `checkout` |
| **Stock** | `/stock/` | `item/create`, `find`, `add`, `subtract` |
| **Payment**| `/payment/`| `create_user`, `find_user`, `add_funds`, `pay` |

*Note: All inter-service logic (like `prepare_subtract` or `rollback_stock`) is handled via the MQ and is not exposed directly on the HTTP gateway.*

## Technology Stack

| Component | Technology |
|-----------|------------|
| **App Logic** | Python 3.12, Flask, Gunicorn |
| **Inter-Service Comm** | Redis Streams (MQ) + Consumer Groups |
| **Data Store** | Redis 7.2 with AOF (Persistence) |
| **Serialization** | msgspec (msgpack binary format) |
| **Load Balancer** | Nginx 1.25 |
| **Reliability** | 2PC, Saga, Tombstones, WAL Recovery |

## Getting Started

```bash
docker compose down -v
docker compose up --build
```
The gateway is available at `http://localhost:8000`. Run the full test suite with:
```bash
bash test-scripts/run_all.sh
```

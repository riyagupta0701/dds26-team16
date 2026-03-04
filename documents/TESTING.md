# Testing Guide

All tests are automated via scripts in the `test-scripts/` folder.
You can also run everything manually using the curl commands below.

---

## Prerequisites

- Docker Desktop (or Docker Engine + Compose plugin)
- `curl` and `bash` (macOS / Linux / WSL)
- ~4 GB RAM free

---

## Start the stack

```bash
docker compose up --build
```

Wait ~20 seconds for all containers to be healthy, then verify:

```bash
docker compose ps
```

All 13 containers should show `running`:
gateway, order-service-1/2, order-redis-master/replica,
stock-service-1/2, stock-redis-master/replica,
payment-service-1/2, payment-redis-master/replica.

---

## Run all tests

### Seed data

```bash
curl -X POST http://localhost:8000/stock/batch_init/10/100/10
curl -X POST http://localhost:8000/payment/batch_init/10/1000
curl -X POST http://localhost:8000/orders/batch_init/10/10/10/10
```

```bash
# Make scripts executable (first time only)
chmod +x test-scripts/*.sh

# Run every test in sequence
test-scripts/run_all.sh
```

Or run individual test scripts:

```bash
test-scripts/01_happy_path.sh
test-scripts/02_idempotency.sh
test-scripts/03_compensation_payment_fails.sh
test-scripts/04_compensation_stock_fails.sh
test-scripts/05_fault_app_replica.sh
# test-scripts/06_fault_redis_master.sh
test-scripts/07_mode_flag.sh
```

---

## Manual curl tests

### Seed data

```bash
curl -X POST http://localhost:8000/stock/batch_init/10/100/10
curl -X POST http://localhost:8000/payment/batch_init/10/1000
curl -X POST http://localhost:8000/orders/batch_init/10/10/10/10
```

### Happy path

```bash
USER_ID=$(curl -s -X POST http://localhost:8000/payment/create_user | grep -o '"user_id":"[^"]*"' | cut -d'"' -f4)
curl -s -X POST http://localhost:8000/payment/add_funds/$USER_ID/100

ORDER_ID=$(curl -s -X POST http://localhost:8000/orders/create/$USER_ID | grep -o '"order_id":"[^"]*"' | cut -d'"' -f4)
curl -s -X POST http://localhost:8000/orders/addItem/$ORDER_ID/0/1

curl -X POST http://localhost:8000/orders/checkout/$ORDER_ID
# → 200 Checkout successful

curl http://localhost:8000/orders/find/$ORDER_ID
# → "status": "paid", "paid": true
```

### Compensation — payment fails

```bash
USER_ID=$(curl -s -X POST http://localhost:8000/payment/create_user | grep -o '"user_id":"[^"]*"' | cut -d'"' -f4)
# No funds added

ORDER_ID=$(curl -s -X POST http://localhost:8000/orders/create/$USER_ID | grep -o '"order_id":"[^"]*"' | cut -d'"' -f4)
curl -s -X POST http://localhost:8000/orders/addItem/$ORDER_ID/1/2

STOCK_BEFORE=$(curl -s http://localhost:8000/stock/find/1 | grep -o '"stock":[0-9]*' | cut -d: -f2)

curl -X POST http://localhost:8000/orders/checkout/$ORDER_ID
# → 400 User out of credit

STOCK_AFTER=$(curl -s http://localhost:8000/stock/find/1 | grep -o '"stock":[0-9]*' | cut -d: -f2)
# STOCK_BEFORE should equal STOCK_AFTER — stock was released by compensation
```

---

## Container reference

| Container | What killing it tests |
|---|---|
| `order-service-1` or `order-service-2` | App replica failover |
| `stock-service-1` or `stock-service-2` | App replica failover |
| `payment-service-1` or `payment-service-2` | App replica failover |
| `order-redis-master` | Redis Sentinel failover (~5s recovery) |
| `stock-redis-master` | Redis Sentinel failover |
| `payment-redis-master` | Redis Sentinel failover |
| `order-redis-replica` | Replica loss — master unaffected |
<!-- | `order-sentinel` | Sentinel loss — already promoted master unaffected | -->

---

## Teardown

```bash
# Stop and remove all containers + volumes (clears Redis data)
docker compose down -v

# Stop only (keep data)
docker compose down
```

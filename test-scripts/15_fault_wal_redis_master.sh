#!/usr/bin/env bash
# Test 15: Fault tolerance — kill wal-redis-master, Sentinel promotes the replica.
#
# The WAL Redis is shared by all services (order, stock, payment, orchestrator).
# Killing the master triggers Sentinel failover. During the ~5s failover window,
# checkouts will fail, but afterwards the system must resume correctly with no
# data loss (wal-redis uses appendfsync=always).
#
# Requires: medium or large docker-compose (with wal-redis Sentinel HA).
source "$(dirname "$0")/helpers.sh"

header "TEST 15 — Fault Tolerance: WAL Redis Master Failure (Sentinel Failover)"

seed_data

# ── Pre-failure: verify a checkout works ────────────────────────────────────
USER1=$(create_funded_user 200)
ORDER1=$(create_order_with_item "$USER1" 0 1)
CODE=$(post "/orders/checkout/$ORDER1")
assert_http "Pre-failure checkout succeeds" "200" "$CODE"

STOCK_BEFORE=$(json_field "$(get_body /stock/find/0)" "stock")
yellow "Stock before wal-redis kill: $STOCK_BEFORE"

# ── Kill wal-redis-master ───────────────────────────────────────────────────
yellow "Killing wal-redis-master..."
docker compose stop wal-redis-master > /dev/null 2>&1

# ── Wait for Sentinel failover ──────────────────────────────────────────────
yellow "Waiting for wal-redis Sentinel failover..."
sleep 8

# ── Post-failover: verify checkouts resume ──────────────────────────────────
# Retry a few times — services may need a moment to reconnect to new master
RECOVERED=0
for i in $(seq 1 10); do
  USER2=$(create_funded_user 200)
  ORDER2=$(create_order_with_item "$USER2" 0 1)
  CODE=$(post "/orders/checkout/$ORDER2")
  if [ "$CODE" = "200" ]; then
    RECOVERED=1
    break
  fi
  sleep 2
done

if [ "$RECOVERED" -eq 1 ]; then
  green "Checkout succeeded after wal-redis failover (attempt $i)"
  PASS=$((PASS+1))
else
  red "Checkout did not recover after wal-redis failover"
  FAIL=$((FAIL+1))
fi

# ── Verify order state is consistent ────────────────────────────────────────
if [ "$RECOVERED" -eq 1 ]; then
  ORDER_STATUS=$(json_field "$(get_body /orders/find/$ORDER2)" "status")
  assert_eq "Order correctly marked paid after wal-redis failover" "paid" "$ORDER_STATUS"
fi

# ── Verify pre-failure order is still intact ────────────────────────────────
ORDER1_STATUS=$(json_field "$(get_body /orders/find/$ORDER1)" "status")
assert_eq "Pre-failure order still shows paid" "paid" "$ORDER1_STATUS"

# ── Restart wal-redis-master (rejoins as replica) ───────────────────────────
yellow "Restarting wal-redis-master..."
docker compose start wal-redis-master > /dev/null 2>&1
sleep 5

# ── Final checkout to confirm full recovery ─────────────────────────────────
USER3=$(create_funded_user 200)
ORDER3=$(create_order_with_item "$USER3" 0 1)
CODE=$(post "/orders/checkout/$ORDER3")
assert_http "Checkout works after wal-redis-master rejoin" "200" "$CODE"

summary

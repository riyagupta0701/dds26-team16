#!/usr/bin/env bash
# Test 6: Fault tolerance — kill a Redis master, Sentinel promotes replica
source "$(dirname "$0")/helpers.sh"

header "TEST 6 — Fault Tolerance: Redis Master Failure (Sentinel Failover)"

FAILOVER_WAIT=8  # seconds to allow Sentinel to elect new master

seed_data

MASTERS=("order-redis-master" "stock-redis-master" "payment-redis-master")

for MASTER in "${MASTERS[@]}"; do
  yellow "Stopping $MASTER..."
  docker compose stop "$MASTER" > /dev/null 2>&1

  yellow "Waiting ${FAILOVER_WAIT}s for Sentinel failover..."
  sleep "$FAILOVER_WAIT"

  USER_ID=$(create_funded_user 200)
  ORDER_ID=$(create_order_with_item "$USER_ID" 0 1)
  CODE=$(post "/orders/checkout/$ORDER_ID")
  assert_http "Checkout works after $MASTER failover" "200" "$CODE"

  ORDER_STATUS=$(json_field "$(get_body /orders/find/$ORDER_ID)" "status")
  assert_eq "Order correctly marked paid after failover" "paid" "$ORDER_STATUS"

  yellow "Restarting $MASTER (will rejoin as replica)..."
  docker compose start "$MASTER" > /dev/null 2>&1
  sleep 5  # allow resync before next test
done

summary

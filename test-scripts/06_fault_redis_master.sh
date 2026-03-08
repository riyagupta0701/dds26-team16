#!/usr/bin/env bash
# Test 6: Fault tolerance — kill a Redis master, Sentinel promotes the replica.
#
# Kills the Redis master for each service in turn and verifies that checkouts
# continue to work through the Sentinel-elected new master. After each test
# the old master is restarted (it rejoins as a replica).
#
# Works in both DEPLOY_MODE=docker and DEPLOY_MODE=kube.
#   docker: stops/starts the {svc}-redis-master container
#   kube:   deletes/waits on the {svc}-redis-node-0 pod (bitnami Sentinel StatefulSet)
source "$(dirname "$0")/helpers.sh"

header "TEST 6 — Fault Tolerance: Redis Master Failure (Sentinel Failover)"

FAILOVER_WAIT=8  # seconds for Sentinel to elect a new master

seed_data

SERVICES=("order" "stock" "payment")

for SVC in "${SERVICES[@]}"; do
  header "Failing $SVC Redis master"

  redis_kill_master "$SVC"

  yellow "Waiting ${FAILOVER_WAIT}s for Sentinel failover..."
  sleep "$FAILOVER_WAIT"

  USER_ID=$(create_funded_user 200)
  ORDER_ID=$(create_order_with_item "$USER_ID" 0 1)
  CODE=$(post "/orders/checkout/$ORDER_ID")
  assert_http "Checkout works after $SVC Redis master failover" "200" "$CODE"

  ORDER_STATUS=$(json_field "$(get_body /orders/find/$ORDER_ID)" "status")
  assert_eq "Order correctly marked paid after $SVC failover" "paid" "$ORDER_STATUS"

  redis_restart_master "$SVC"
  yellow "Waiting 5s for $SVC replica resync..."
  sleep 5
done

summary

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

seed_data

SERVICES=("order" "stock" "payment")

# Wait for Sentinel failover to complete for the affected service.
# Polls a service-specific endpoint until it stops returning HTTP 400 (DB error):
#   stock   → GET  /stock/find/0            (200 when up, 400 when Redis down)
#   payment → POST /payment/create_user     (200 when up, 400 when Redis down)
#   order   → POST /orders/create/probe-id  (200 when up, 400 when Redis down)
_wait_for_recovery() {
  local svc="$1"
  yellow "Polling until $svc Redis recovers from Sentinel failover..."
  for i in $(seq 1 30); do
    case "$svc" in
      stock)   _c=$(get_code "/stock/find/0") ;;
      payment) _c=$(post "/payment/create_user") ;;
      order)   _c=$(post "/orders/create/probe-id") ;;
    esac
    if [ "$_c" = "200" ]; then
      green "$svc service recovered after Sentinel failover"
      return 0
    fi
    sleep 1
  done
  red "$svc Redis did not recover within 30s"
  FAIL=$((FAIL+1))
  return 1
}

for SVC in "${SERVICES[@]}"; do
  header "Failing $SVC Redis master"

  redis_kill_master "$SVC"

  if _wait_for_recovery "$SVC"; then
    USER_ID=$(create_funded_user 200)
    ORDER_ID=$(create_order_with_item "$USER_ID" 0 1)
    CODE=$(post "/orders/checkout/$ORDER_ID")
    assert_http "Checkout works after $SVC Redis master failover" "200" "$CODE"

    ORDER_STATUS=$(json_field "$(get_body /orders/find/$ORDER_ID)" "status")
    assert_eq "Order correctly marked paid after $SVC failover" "paid" "$ORDER_STATUS"
  fi

  # Always wait for master pod to rejoin before the next iteration,
  # even if the checkout test was skipped — avoids cascading failures.
  redis_restart_master "$SVC"
  yellow "Waiting 5s for $SVC replica resync..."
  sleep 5
done

summary

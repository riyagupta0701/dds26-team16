#!/usr/bin/env bash
# Test 5: Fault tolerance — kill one app replica, system keeps working
source "$(dirname "$0")/helpers.sh"

header "TEST 5 — Fault Tolerance: App Replica Failure"

seed_data

SERVICES=("order-service-1" "order-service-2" "stock-service-1" "stock-service-2" "payment-service-1" "payment-service-2")

for SERVICE in "${SERVICES[@]}"; do
  yellow "Stopping $SERVICE..."
  service_stop "$SERVICE"
  sleep 2  # let nginx detect the failure

  USER_ID=$(create_funded_user 200)
  ORDER_ID=$(create_order_with_item "$USER_ID" 0 1)
  CODE=$(post "/orders/checkout/$ORDER_ID")
  assert_http "Checkout works with $SERVICE down" "200" "$CODE"

  yellow "Restarting $SERVICE..."
  service_start "$SERVICE"
  sleep 3
done

summary

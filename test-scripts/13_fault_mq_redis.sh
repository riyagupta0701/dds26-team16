#!/usr/bin/env bash
# Test 13: Fault tolerance — kill mq-redis (message queue broker).
#
# mq-redis is a single instance with no replication. Killing it means all
# in-flight streams and reply keys are lost. Services should reconnect after
# mq-redis restarts, and saga compensation should handle any stuck transactions.
source "$(dirname "$0")/helpers.sh"

header "TEST 13 — Fault Tolerance: MQ Redis Failure"

seed_data

# ── Pre-failure baseline ────────────────────────────────────────────────────
USER1=$(create_funded_user 200)
ORDER1=$(create_order_with_item "$USER1" 0 1)
CODE=$(post "/orders/checkout/$ORDER1")
assert_http "Pre-failure checkout succeeds" "200" "$CODE"

STOCK_BEFORE=$(json_field "$(get_body /stock/find/0)" "stock")
CREDIT_BEFORE=$(json_field "$(get_body /payment/find_user/$USER1)" "credit")
yellow "Stock before: $STOCK_BEFORE  |  Credit after checkout: $CREDIT_BEFORE"

# ── Kill mq-redis ───────────────────────────────────────────────────────────
yellow "Killing mq-redis..."
docker compose stop mq-redis > /dev/null 2>&1
sleep 2

# ── Verify system is down (checkouts should fail/timeout) ───────────────────
yellow "Attempting checkout while mq-redis is down (expect failure)..."
USER_DOWN=$(create_funded_user 200)
ORDER_DOWN=$(create_order_with_item "$USER_DOWN" 0 1)
CODE=$(curl -s -o /dev/null -w "%{http_code}" -m 10 -X POST "$BASE_URL/orders/checkout/$ORDER_DOWN" 2>/dev/null || echo "000")
if [ "$CODE" != "200" ]; then
  green "Checkout correctly fails while mq-redis is down (HTTP $CODE)"
  PASS=$((PASS+1))
else
  red "Checkout unexpectedly succeeded while mq-redis is down"
  FAIL=$((FAIL+1))
fi

# ── Restart mq-redis ───────────────────────────────────────────────────────
yellow "Restarting mq-redis..."
docker compose start mq-redis > /dev/null 2>&1
sleep 5

# ── Wait for services to reconnect ─────────────────────────────────────────
yellow "Waiting for services to reconnect to mq-redis..."
RECOVERED=0
for i in $(seq 1 15); do
  USER3=$(create_funded_user 200)
  ORDER3=$(create_order_with_item "$USER3" 0 1)
  CODE=$(post "/orders/checkout/$ORDER3")
  if [ "$CODE" = "200" ]; then
    RECOVERED=1
    break
  fi
  sleep 2
done

if [ "$RECOVERED" -eq 1 ]; then
  green "Checkout recovered after mq-redis restart (attempt $i)"
  PASS=$((PASS+1))
else
  red "Checkout did not recover after mq-redis restart"
  FAIL=$((FAIL+1))
fi

# ── Verify consistency: pre-failure order still intact ──────────────────────
ORDER1_STATUS=$(json_field "$(get_body /orders/find/$ORDER1)" "status")
assert_eq "Pre-failure order still shows paid" "paid" "$ORDER1_STATUS"

# ── Verify stock/credit consistency ─────────────────────────────────────────
# The order that was attempted while mq-redis was down should NOT have
# deducted stock or credit (it never reached the participants).
ORDER_DOWN_STATUS=$(json_field "$(get_body /orders/find/$ORDER_DOWN)" "status")
yellow "Order attempted during outage has status: $ORDER_DOWN_STATUS"
if [ "$ORDER_DOWN_STATUS" != "paid" ]; then
  green "Order during outage was not incorrectly paid"
  PASS=$((PASS+1))
else
  red "Order during outage was incorrectly marked as paid"
  FAIL=$((FAIL+1))
fi

summary

#!/usr/bin/env bash
# Test 2: Idempotency — retrying checkout never double-charges
source "$(dirname "$0")/helpers.sh"

header "TEST 2 — Idempotency"

seed_data

USER_ID=$(create_funded_user 200)
ORDER_ID=$(create_order_with_item "$USER_ID" 0 1)

# First checkout
CODE=$(post "/orders/checkout/$ORDER_ID")
assert_http "First checkout returns 200" "200" "$CODE"

STOCK_AFTER_1=$(json_field "$(get_body /stock/find/0)" "stock")
CREDIT_AFTER_1=$(json_field "$(get_body /payment/find_user/$USER_ID)" "credit")

# Retry the same checkout multiple times
for i in 1 2 3; do
  CODE=$(post "/orders/checkout/$ORDER_ID")
  assert_http "Retry #$i returns 200 (idempotent)" "200" "$CODE"
done

STOCK_AFTER_N=$(json_field "$(get_body /stock/find/0)" "stock")
CREDIT_AFTER_N=$(json_field "$(get_body /payment/find_user/$USER_ID)" "credit")

assert_eq "Stock unchanged after retries" "$STOCK_AFTER_1" "$STOCK_AFTER_N"
assert_eq "Credit unchanged after retries" "$CREDIT_AFTER_1" "$CREDIT_AFTER_N"

# A failed order should also be idempotent
USER2_ID=$(curl -s -X POST "$BASE_URL/payment/create_user" | grep -o '"user_id":"[^"]*"' | cut -d'"' -f4)
ORDER2_ID=$(create_order_with_item "$USER2_ID" 1 2)

CODE=$(post "/orders/checkout/$ORDER2_ID")
assert_http "Failed checkout returns 400" "400" "$CODE"

for i in 1 2 3; do
  CODE=$(post "/orders/checkout/$ORDER2_ID")
  assert_http "Retry on failed order #$i returns 400 (idempotent)" "400" "$CODE"
done

summary

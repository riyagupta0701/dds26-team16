#!/usr/bin/env bash
# Test 3: Saga compensation — payment fails, stock must be fully released
source "$(dirname "$0")/helpers.sh"

header "TEST 3 — Compensation: Payment Fails"

seed_data

# User with zero credit
USER_ID=$(curl -s -X POST "$BASE_URL/payment/create_user" | grep -o '"user_id":"[^"]*"' | cut -d'"' -f4)
yellow "Broke user: $USER_ID (0 credit)"

ORDER_ID=$(create_order_with_item "$USER_ID" 2 2)
yellow "Order with 2x item-2: $ORDER_ID"

STOCK_BEFORE=$(json_field "$(get_body /stock/find/2)" "stock")
CREDIT_BEFORE=$(json_field "$(get_body /payment/find_user/$USER_ID)" "credit")
yellow "Stock before: $STOCK_BEFORE  |  Credit before: $CREDIT_BEFORE"

CODE=$(post "/orders/checkout/$ORDER_ID")
assert_http "Checkout fails with 400" "400" "$CODE"

STOCK_AFTER=$(json_field "$(get_body /stock/find/2)" "stock")
CREDIT_AFTER=$(json_field "$(get_body /payment/find_user/$USER_ID)" "credit")

assert_eq "Stock fully restored after compensation" "$STOCK_BEFORE" "$STOCK_AFTER"
assert_eq "Credit unchanged (payment was never charged)" "$CREDIT_BEFORE" "$CREDIT_AFTER"

ORDER_STATUS=$(json_field "$(get_body /orders/find/$ORDER_ID)" "status")
assert_eq "Order status is failed" "failed" "$ORDER_STATUS"

summary

#!/usr/bin/env bash
# Test 1: Happy path — successful end-to-end checkout
source "$(dirname "$0")/helpers.sh"

header "TEST 1 — Happy Path"

seed_data

USER_ID=$(create_funded_user 200)
yellow "Created user: $USER_ID"

STOCK_BEFORE=$(json_field "$(get_body /stock/find/0)" "stock")
CREDIT_BEFORE=$(json_field "$(get_body /payment/find_user/$USER_ID)" "credit")
yellow "Stock before: $STOCK_BEFORE  |  Credit before: $CREDIT_BEFORE"

ORDER_ID=$(create_order_with_item "$USER_ID" 0 1)
yellow "Created order: $ORDER_ID"

CODE=$(post "/orders/checkout/$ORDER_ID")
assert_http "Checkout returns 200" "200" "$CODE"

ORDER_JSON=$(get_body "/orders/find/$ORDER_ID")
STATUS=$(json_field "$ORDER_JSON" "status")
PAID=$(json_field "$ORDER_JSON" "paid")
assert_eq "Order status is paid" "paid" "$STATUS"
assert_eq "Order paid flag is true" "true" "$PAID"

STOCK_AFTER=$(json_field "$(get_body /stock/find/0)" "stock")
CREDIT_AFTER=$(json_field "$(get_body /payment/find_user/$USER_ID)" "credit")

assert_lte "Stock decreased" "$STOCK_BEFORE" "$STOCK_AFTER"
assert_lte "Credit decreased" "$CREDIT_BEFORE" "$CREDIT_AFTER"

summary

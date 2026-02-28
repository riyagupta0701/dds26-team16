#!/usr/bin/env bash
# Test 4: Saga compensation — stock fails, payment must never be charged
source "$(dirname "$0")/helpers.sh"

header "TEST 4 — Compensation: Stock Fails"

seed_data

# Create a scarce item with only 1 unit
ITEM_BODY=$(curl -s -X POST "$BASE_URL/stock/item/create/50")
SCARCE_ITEM=$(json_field "$ITEM_BODY" "item_id")
curl -s -X POST "$BASE_URL/stock/add/$SCARCE_ITEM/1" > /dev/null
yellow "Scarce item: $SCARCE_ITEM (1 unit, price 50)"

USER_ID=$(create_funded_user 1000)
yellow "Rich user: $USER_ID (1000 credit)"

ORDER_ID=$(create_order_with_item "$USER_ID" "$SCARCE_ITEM" 5)
yellow "Order requesting 5 units of scarce item: $ORDER_ID"

STOCK_BEFORE=$(json_field "$(get_body /stock/find/$SCARCE_ITEM)" "stock")
CREDIT_BEFORE=$(json_field "$(get_body /payment/find_user/$USER_ID)" "credit")
yellow "Stock before: $STOCK_BEFORE  |  Credit before: $CREDIT_BEFORE"

CODE=$(post "/orders/checkout/$ORDER_ID")
assert_http "Checkout fails with 400 (out of stock)" "400" "$CODE"

STOCK_AFTER=$(json_field "$(get_body /stock/find/$SCARCE_ITEM)" "stock")
CREDIT_AFTER=$(json_field "$(get_body /payment/find_user/$USER_ID)" "credit")

assert_eq "Stock unchanged (subtract rolled back)" "$STOCK_BEFORE" "$STOCK_AFTER"
assert_eq "Credit unchanged (payment step never ran)" "$CREDIT_BEFORE" "$CREDIT_AFTER"

ORDER_STATUS=$(json_field "$(get_body /orders/find/$ORDER_ID)" "status")
assert_eq "Order status is failed" "failed" "$ORDER_STATUS"

summary

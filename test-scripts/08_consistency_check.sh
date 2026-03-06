#!/usr/bin/env bash
# Test 8: Consistency — run concurrent checkouts and verify no money or stock lost
source "$(dirname "$0")/helpers.sh"

header "TEST 8 — Consistency Under Concurrent Load"

seed_data

CONCURRENCY=10
ITEM_ID=5
ITEM_PRICE=10
STARTING_CREDIT=500
STARTING_STOCK=100

# Seed a known item and users
curl -s -X POST "$BASE_URL/stock/batch_init/10/$STARTING_STOCK/$ITEM_PRICE" > /dev/null

yellow "Firing $CONCURRENCY concurrent checkouts against item $ITEM_ID..."

# Use a temp dir to capture subshell results — subshell variables don't
# propagate to the parent, so each worker writes its own result file.
TMPDIR=$(mktemp -d)
trap 'rm -rf "$TMPDIR"' EXIT

declare -a PIDS

for i in $(seq 1 $CONCURRENCY); do
  (
    UID_VAL=$(create_funded_user $STARTING_CREDIT)
    OID_VAL=$(create_order_with_item "$UID_VAL" "$ITEM_ID" 1)
    CODE=$(post "/orders/checkout/$OID_VAL")
    echo "$UID_VAL $OID_VAL $CODE" > "$TMPDIR/result_$i"
  ) &
  PIDS+=($!)
done

# Wait for all workers to finish
for PID in "${PIDS[@]}"; do
  wait "$PID"
done

# Read back results and print a summary line per checkout
TOTAL_CREDIT_SPENT=0
TOTAL_STOCK_SOLD=0

for i in $(seq 1 $CONCURRENCY); do
  read -r UID_VAL OID_VAL CODE < "$TMPDIR/result_$i"
  yellow "  $UID_VAL $OID_VAL $CODE"
done

# Check consistency against the UIDs that actually placed orders
yellow "  User/Item $ITEM_ID: verifying stock and credit for each user..."

ALL_NON_NEGATIVE=true

for i in $(seq 1 $CONCURRENCY); do
  read -r UID_VAL OID_VAL CODE < "$TMPDIR/result_$i"

  CREDIT=$(json_field "$(get_body /payment/find_user/$UID_VAL)" "credit")
  STOCK=$(json_field "$(get_body /stock/find/$ITEM_ID)" "stock")

  CREDIT_SPENT=$((STARTING_CREDIT - CREDIT))
  STOCK_SOLD=$((STARTING_STOCK - STOCK))

  TOTAL_CREDIT_SPENT=$((TOTAL_CREDIT_SPENT + CREDIT_SPENT))

  # Core invariant: credit must not go negative
  if [ "$CREDIT" -lt 0 ] 2>/dev/null; then
    red "User $UID_VAL has negative credit: $CREDIT"
    FAIL=$((FAIL+1))
    ALL_NON_NEGATIVE=false
  else
    PASS=$((PASS+1))
  fi
done

# Check item stock is non-negative (only need to check once after all workers done)
FINAL_STOCK=$(json_field "$(get_body /stock/find/$ITEM_ID)" "stock")
if [ "$FINAL_STOCK" -lt 0 ] 2>/dev/null; then
  red "Item $ITEM_ID has negative stock: $FINAL_STOCK"
  FAIL=$((FAIL+1))
  ALL_NON_NEGATIVE=false
else
  PASS=$((PASS+1))
fi

TOTAL_STOCK_SOLD=$((STARTING_STOCK - FINAL_STOCK))
yellow "  Item $ITEM_ID: stock_sold=$TOTAL_STOCK_SOLD total_credit_spent=$TOTAL_CREDIT_SPENT"

if $ALL_NON_NEGATIVE; then
  green "All values are non-negative"
fi
yellow "Check logs above for any anomalies"

summary

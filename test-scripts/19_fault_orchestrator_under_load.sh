#!/usr/bin/env bash
# Test 19: Fault tolerance — kill orchestrator during active checkouts.
#
# Starts a burst of concurrent checkouts, kills the orchestrator mid-flight,
# waits for recovery, then verifies no money or stock was lost.
source "$(dirname "$0")/helpers.sh"

header "TEST 19 — Fault Tolerance: Kill Orchestrator Under Load"

# ── Setup ───────────────────────────────────────────────────────────────────
curl -s -X POST "$BASE_URL/stock/batch_init/5/100/10" > /dev/null
curl -s -X POST "$BASE_URL/payment/batch_init/5/1000" > /dev/null

# Record initial totals
INITIAL_STOCK=0
for i in $(seq 0 4); do
  S=$(json_field "$(get_body /stock/find/$i)" "stock")
  INITIAL_STOCK=$((INITIAL_STOCK + S))
done

INITIAL_CREDIT=0
for i in $(seq 0 4); do
  C=$(json_field "$(get_body /payment/find_user/$i)" "credit")
  INITIAL_CREDIT=$((INITIAL_CREDIT + C))
done

yellow "Initial stock: $INITIAL_STOCK  |  Initial credit: $INITIAL_CREDIT"

# ── Fire concurrent checkouts in background ─────────────────────────────────
RESULT_DIR=$(mktemp -d)
NUM_ORDERS=10

yellow "Launching $NUM_ORDERS concurrent checkouts..."
for i in $(seq 1 $NUM_ORDERS); do
  USER_ID="$((i % 5))"
  ITEM_ID="$((i % 5))"
  (
    ORDER_BODY=$(post_body "/orders/create/$USER_ID")
    ORDER_ID=$(json_field "$ORDER_BODY" "order_id")
    post "/orders/addItem/$ORDER_ID/$ITEM_ID/1" > /dev/null
    CODE=$(curl -s -o /dev/null -w "%{http_code}" -m 90 -X POST "$BASE_URL/orders/checkout/$ORDER_ID" 2>/dev/null || echo "000")
    echo "$CODE" > "$RESULT_DIR/$i.code"
    echo "$ORDER_ID" > "$RESULT_DIR/$i.order"
  ) &
done

# ── Kill orchestrator while checkouts are in flight ─────────────────────────
sleep 1  # let some checkouts reach the orchestrator
yellow "Killing orchestrator..."

# Try both naming patterns
ORCH_CTR="dds26-team16-orchestrator-1"
if ! docker inspect "$ORCH_CTR" > /dev/null 2>&1; then
  ORCH_CTR="dds26-team16-orchestrator-1-1"
fi
docker exec "$ORCH_CTR" kill 1 2>/dev/null || true

# ── Wait for all checkouts to complete (they may timeout) ───────────────────
yellow "Waiting for checkouts to complete..."
wait

# ── Let recovery run ────────────────────────────────────────────────────────
yellow "Waiting for WAL recovery..."
sleep 10

# ── Count results ──────────────────────────────────────────────────────────
SUCCESSES=0
FAILURES=0
for i in $(seq 1 $NUM_ORDERS); do
  CODE=$(cat "$RESULT_DIR/$i.code" 2>/dev/null || echo "000")
  if [ "$CODE" = "200" ]; then
    SUCCESSES=$((SUCCESSES + 1))
  else
    FAILURES=$((FAILURES + 1))
  fi
done

yellow "Results: $SUCCESSES succeeded, $FAILURES failed"

# Some checkouts may have failed (orchestrator was dead) — that's OK.
# The key invariant: no money or stock lost.

# ── Count actually paid orders ──────────────────────────────────────────────
PAID_ITEMS=0
PAID_COST=0
for i in $(seq 1 $NUM_ORDERS); do
  ORDER_ID=$(cat "$RESULT_DIR/$i.order" 2>/dev/null || echo "")
  if [ -n "$ORDER_ID" ]; then
    STATUS=$(json_field "$(get_body /orders/find/$ORDER_ID)" "status")
    if [ "$STATUS" = "paid" ]; then
      PAID_ITEMS=$((PAID_ITEMS + 1))
      PAID_COST=$((PAID_COST + 10))
    fi
  fi
done

yellow "Actually paid orders: $PAID_ITEMS"

# ── Consistency verification ────────────────────────────────────────────────
header "Consistency After Orchestrator Kill"

FINAL_STOCK=0
for i in $(seq 0 4); do
  S=$(json_field "$(get_body /stock/find/$i)" "stock")
  FINAL_STOCK=$((FINAL_STOCK + S))
done

FINAL_CREDIT=0
for i in $(seq 0 4); do
  C=$(json_field "$(get_body /payment/find_user/$i)" "credit")
  FINAL_CREDIT=$((FINAL_CREDIT + C))
done

EXPECTED_STOCK=$((INITIAL_STOCK - PAID_ITEMS))
EXPECTED_CREDIT=$((INITIAL_CREDIT - PAID_COST))

yellow "Expected stock: $EXPECTED_STOCK  |  Actual: $FINAL_STOCK"
yellow "Expected credit: $EXPECTED_CREDIT  |  Actual: $FINAL_CREDIT"

assert_eq "Stock consistent after orchestrator kill" "$EXPECTED_STOCK" "$FINAL_STOCK"
assert_eq "Credit consistent after orchestrator kill" "$EXPECTED_CREDIT" "$FINAL_CREDIT"

# ── Verify system still works after recovery ────────────────────────────────
USER_POST=$(create_funded_user 200)
ORDER_POST=$(create_order_with_item "$USER_POST" 0 1)
CODE=$(post "/orders/checkout/$ORDER_POST")
assert_http "Post-recovery checkout succeeds" "200" "$CODE"

# Cleanup
rm -rf "$RESULT_DIR"

summary

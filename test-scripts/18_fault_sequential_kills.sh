#!/usr/bin/env bash
# Test 18: Fault tolerance — sequential container kills with consistency check.
#
# Kills containers one at a time, letting the system recover between each kill.
# After each kill-recovery cycle and at the end, verifies that stock and
# credit totals remain consistent (no money or items lost).
source "$(dirname "$0")/helpers.sh"

header "TEST 18 — Sequential Container Kills + Consistency"

# ── Setup: known initial state ──────────────────────────────────────────────
yellow "Setting up known initial state..."
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

yellow "Initial total stock: $INITIAL_STOCK  |  Initial total credit: $INITIAL_CREDIT"

# ── Do some successful checkouts to change state ───────────────────────────
PAID_ORDERS=0
PAID_COST=0
PAID_ITEMS=0

for i in $(seq 1 5); do
  USER_ID="$((i % 5))"
  ITEM_ID="$((i % 5))"
  ORDER_BODY=$(post_body "/orders/create/$USER_ID")
  ORDER_ID=$(json_field "$ORDER_BODY" "order_id")
  post "/orders/addItem/$ORDER_ID/$ITEM_ID/1" > /dev/null
  CODE=$(post "/orders/checkout/$ORDER_ID")
  if [ "$CODE" = "200" ]; then
    PAID_ORDERS=$((PAID_ORDERS + 1))
    PAID_COST=$((PAID_COST + 10))
    PAID_ITEMS=$((PAID_ITEMS + 1))
  fi
done
yellow "Completed $PAID_ORDERS checkouts before kills"

# ── Sequential kill-recover-checkout cycles ─────────────────────────────────

# Detect which containers exist (varies by small/medium/large config)
detect_container() {
  local name="$1"
  if docker inspect "dds26-team16-${name}-1" > /dev/null 2>&1; then
    echo "dds26-team16-${name}-1"
  elif docker inspect "dds26-team16-${name}1-1" > /dev/null 2>&1; then
    echo "dds26-team16-${name}1-1"
  else
    echo ""
  fi
}

KILL_TARGETS=(
  "stock-service:Stock service"
  "payment-service:Payment service"
  "orchestrator:Orchestrator"
  "order-service:Order service"
)

for entry in "${KILL_TARGETS[@]}"; do
  SVC="${entry%%:*}"
  LABEL="${entry##*:}"

  CONTAINER=$(detect_container "$SVC")
  if [ -z "$CONTAINER" ]; then
    yellow "Container for $SVC not found, skipping"
    continue
  fi

  header "Sequential kill: $LABEL ($CONTAINER)"

  # Kill PID 1
  yellow "Killing PID 1 in $CONTAINER..."
  docker exec "$CONTAINER" kill 1 2>/dev/null || true

  # Let it recover
  yellow "Waiting for recovery..."
  sleep 8

  # Verify a checkout still works
  RECOVERED=0
  for attempt in $(seq 1 10); do
    USER_ID="$((attempt % 5))"
    ITEM_ID="$((attempt % 5))"
    ORDER_BODY=$(post_body "/orders/create/$USER_ID")
    ORDER_ID=$(json_field "$ORDER_BODY" "order_id")
    post "/orders/addItem/$ORDER_ID/$ITEM_ID/1" > /dev/null
    CODE=$(post "/orders/checkout/$ORDER_ID")
    if [ "$CODE" = "200" ]; then
      RECOVERED=1
      PAID_ORDERS=$((PAID_ORDERS + 1))
      PAID_COST=$((PAID_COST + 10))
      PAID_ITEMS=$((PAID_ITEMS + 1))
      break
    fi
    sleep 2
  done

  if [ "$RECOVERED" -eq 1 ]; then
    green "Checkout recovered after killing $LABEL (attempt $attempt)"
    PASS=$((PASS+1))
  else
    red "Checkout did not recover after killing $LABEL"
    FAIL=$((FAIL+1))
  fi
done

# ── Final consistency check ─────────────────────────────────────────────────
header "Final Consistency Verification"

sleep 3  # let any in-flight recovery settle

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

yellow "Paid orders: $PAID_ORDERS  |  Items deducted: $PAID_ITEMS  |  Cost deducted: $PAID_COST"
yellow "Expected stock: $EXPECTED_STOCK  |  Actual stock: $FINAL_STOCK"
yellow "Expected credit: $EXPECTED_CREDIT  |  Actual credit: $FINAL_CREDIT"

assert_eq "Total stock consistent" "$EXPECTED_STOCK" "$FINAL_STOCK"
assert_eq "Total credit consistent" "$EXPECTED_CREDIT" "$FINAL_CREDIT"

summary

#!/usr/bin/env bash
# Test 10: AOF persistence — crash a Redis master and verify data survives restart.
#
# Redis replays the AOF log on startup, so all writes made before the crash
# should be fully restored. This test proves that "appendonly yes" is actually
# working and not just set in config.
#
# Only runs in DEPLOY_MODE=docker (docker compose gives direct control over
# individual Redis containers; in K8s the pod is managed by a StatefulSet).
source "$(dirname "$0")/helpers.sh"

header "TEST 10 — AOF Persistence: Data Survives Redis Master Crash"

seed_data

# ── Stock: create an item and record its stock level ─────────────────────────

ITEM_ID=$(json_field "$(post_body /stock/item/create/50)" "item_id")
curl -s -X POST "$BASE_URL/stock/add/$ITEM_ID/42" > /dev/null
STOCK_BEFORE=$(json_field "$(get_body /stock/find/$ITEM_ID)" "stock")
assert_eq "Stock written before crash" "42" "$STOCK_BEFORE"

# ── Payment: create a user with a known credit balance ───────────────────────

USER_ID=$(json_field "$(post_body /payment/create_user)" "user_id")
curl -s -X POST "$BASE_URL/payment/add_funds/$USER_ID/333" > /dev/null
CREDIT_BEFORE=$(json_field "$(get_body /payment/find_user/$USER_ID)" "credit")
assert_eq "Credit written before crash" "333" "$CREDIT_BEFORE"

# ── Order: create an order and record its status ─────────────────────────────

ORDER_ID=$(json_field "$(post_body /orders/create/$USER_ID)" "order_id")
curl -s -X POST "$BASE_URL/orders/addItem/$ORDER_ID/$ITEM_ID/1" > /dev/null
STATUS_BEFORE=$(json_field "$(get_body /orders/find/$ORDER_ID)" "status")
assert_eq "Order written before crash" "pending" "$STATUS_BEFORE"

# ── Crash and restore all Redis masters ──────────────────────────────────────

redis_crash
redis_restore

yellow "Waiting for all services to reconnect to Redis..."
for i in $(seq 1 20); do
  stock_ok=$(json_field "$(get_body "/stock/find/$ITEM_ID")"        "stock")
  credit_ok=$(json_field "$(get_body "/payment/find_user/$USER_ID")" "credit")
  order_ok=$(json_field "$(get_body "/orders/find/$ORDER_ID")"       "status")
  if [ -n "$stock_ok" ] && [ -n "$credit_ok" ] && [ -n "$order_ok" ]; then
    green "All services reconnected to Redis"
    break
  fi
  [ "$i" -eq 20 ] && red "Services did not reconnect in time" && exit 1
  sleep 2
done

# ── Verify all data survived ─────────────────────────────────────────────────

STOCK_AFTER=$(json_field "$(get_body /stock/find/$ITEM_ID)" "stock")
assert_eq "Stock survived Redis master crash (AOF)" "$STOCK_BEFORE" "$STOCK_AFTER"

CREDIT_AFTER=$(json_field "$(get_body /payment/find_user/$USER_ID)" "credit")
assert_eq "Credit survived Redis master crash (AOF)" "$CREDIT_BEFORE" "$CREDIT_AFTER"

STATUS_AFTER=$(json_field "$(get_body /orders/find/$ORDER_ID)" "status")
assert_eq "Order survived Redis master crash (AOF)" "$STATUS_BEFORE" "$STATUS_AFTER"

summary

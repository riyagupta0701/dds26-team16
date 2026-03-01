#!/usr/bin/env bash
# Test 7: Mode flag — verify both SAGA and 2PC end-to-end checkout produce
#          correct results (stock deducted, credit charged, order marked paid).
#
# Mode switching is done via a temporary docker-compose override file that
# merges only CHECKOUT_MODE into the existing service definition, preserving
# all other env vars (REDIS_HOST, GATEWAY_URL, etc.).
#
# The script always restores saga mode on exit — even if a test assertion fails.

source "$(dirname "$0")/helpers.sh"

header "TEST 7 — Mode Flag (SAGA vs 2PC)"

seed_data

# ── Helpers ───────────────────────────────────────────────────────────────────

_set_checkout_mode() {
  local mode="$1"
  local override="/tmp/checkout_mode_override_$$.yml"

  # Map-format override so docker compose MERGES rather than replaces the
  # base service environment (list format would wipe REDIS_HOST etc.).
  cat > "$override" <<YAML
services:
  order-service-1:
    environment:
      CHECKOUT_MODE: "${mode}"
  order-service-2:
    environment:
      CHECKOUT_MODE: "${mode}"
YAML

  docker compose stop order-service-1 order-service-2 > /dev/null 2>&1

  if [ "$mode" = "saga" ]; then
    # Restore from the canonical compose file (no override needed).
    docker compose up -d --no-deps order-service-1 order-service-2 > /dev/null 2>&1
  else
    docker compose -f docker-compose.yml -f "$override" \
      up -d --no-deps order-service-1 order-service-2 > /dev/null 2>&1
  fi

  rm -f "$override"
  sleep 4  # allow gunicorn workers to start and run 2PC recovery
}

# Always restore saga mode on script exit so subsequent tests are unaffected.
_restore_saga() { _set_checkout_mode saga 2>/dev/null; }
trap _restore_saga EXIT

# ── Section A: SAGA end-to-end ────────────────────────────────────────────────

yellow "[ A ] SAGA mode end-to-end"

USER_SAGA=$(create_funded_user 200)
ORDER_SAGA=$(create_order_with_item "$USER_SAGA" 0 1)

STOCK_BEFORE=$(json_field "$(get_body /stock/find/0)" "stock")
CREDIT_BEFORE=$(json_field "$(get_body /payment/find_user/$USER_SAGA)" "credit")

CODE=$(post "/orders/checkout/$ORDER_SAGA")
assert_http "SAGA checkout returns 200" "200" "$CODE"

ORDER_JSON=$(get_body "/orders/find/$ORDER_SAGA")
assert_eq  "SAGA order.status = paid" "paid" "$(json_field "$ORDER_JSON" "status")"
assert_eq  "SAGA order.paid = true"   "true" "$(json_field "$ORDER_JSON" "paid")"

STOCK_AFTER=$(json_field  "$(get_body /stock/find/0)"           "stock")
CREDIT_AFTER=$(json_field "$(get_body /payment/find_user/$USER_SAGA)" "credit")

assert_lte "SAGA stock decreased"  "$STOCK_BEFORE"  "$STOCK_AFTER"
assert_lte "SAGA credit decreased" "$CREDIT_BEFORE" "$CREDIT_AFTER"

# ── Section B: 2PC end-to-end ─────────────────────────────────────────────────

yellow "[ B ] Switching to CHECKOUT_MODE=2pc..."
_set_checkout_mode 2pc
yellow "    Order services restarted with CHECKOUT_MODE=2pc"

USER_2PC=$(create_funded_user 200)
ORDER_2PC=$(create_order_with_item "$USER_2PC" 1 1)

STOCK_BEFORE_2=$(json_field "$(get_body /stock/find/1)"              "stock")
CREDIT_BEFORE_2=$(json_field "$(get_body /payment/find_user/$USER_2PC)" "credit")

CODE=$(post "/orders/checkout/$ORDER_2PC")
assert_http "2PC checkout returns 200" "200" "$CODE"

ORDER_JSON_2=$(get_body "/orders/find/$ORDER_2PC")
assert_eq "2PC order.status = paid" "paid" "$(json_field "$ORDER_JSON_2" "status")"
assert_eq "2PC order.paid = true"   "true" "$(json_field "$ORDER_JSON_2" "paid")"

STOCK_AFTER_2=$(json_field  "$(get_body /stock/find/1)"              "stock")
CREDIT_AFTER_2=$(json_field "$(get_body /payment/find_user/$USER_2PC)" "credit")

assert_lte "2PC stock decreased"  "$STOCK_BEFORE_2"  "$STOCK_AFTER_2"
assert_lte "2PC credit decreased" "$CREDIT_BEFORE_2" "$CREDIT_AFTER_2"

# Idempotent retry of a paid order must still return 200 in 2PC mode.
CODE=$(post "/orders/checkout/$ORDER_2PC")
assert_http "2PC checkout retry (idempotent) returns 200" "200" "$CODE"

# ── Section C: 2PC compensation in 2PC mode ───────────────────────────────────

yellow "[ C ] 2PC compensation — payment fails in 2PC mode"

USER_BROKE=$(curl -s -X POST "$BASE_URL/payment/create_user" \
             | grep -o '"user_id":"[^"]*"' | cut -d'"' -f4)
ORDER_BROKE=$(create_order_with_item "$USER_BROKE" 2 1)

STOCK_BEFORE_C=$(json_field "$(get_body /stock/find/2)" "stock")

CODE=$(post "/orders/checkout/$ORDER_BROKE")
assert_http "2PC checkout with no credit returns 400" "400" "$CODE"

STOCK_AFTER_C=$(json_field "$(get_body /stock/find/2)" "stock")
assert_eq "2PC abort: stock reservation released, stock unchanged" \
          "$STOCK_BEFORE_C" "$STOCK_AFTER_C"

STATUS_BROKE=$(json_field "$(get_body /orders/find/$ORDER_BROKE)" "status")
assert_eq "2PC failed order.status = failed" "failed" "$STATUS_BROKE"

# ── Restore saga (trap also fires on exit, this is belt-and-suspenders) ───────

yellow "[ D ] Restoring CHECKOUT_MODE=saga..."
_set_checkout_mode saga
yellow "    Order services restored"

USER_SAGA2=$(create_funded_user 200)
ORDER_SAGA2=$(create_order_with_item "$USER_SAGA2" 3 1)
CODE=$(post "/orders/checkout/$ORDER_SAGA2")
assert_http "SAGA checkout works again after restore" "200" "$CODE"

summary

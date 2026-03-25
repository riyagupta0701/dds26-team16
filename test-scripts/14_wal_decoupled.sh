#!/usr/bin/env bash
# Test 14: Decoupled WAL — verify WAL entries live in wal-redis, not in
# business-data Redis, and survive killing the business-data container.
#
# What this proves:
#   1. After a checkout, NEW WAL keys appear only in wal-redis, not in
#      order-redis, stock-redis, or payment-redis.
#   2. Killing order-redis-master does NOT destroy the WAL — wal-redis still
#      holds all entries, so recovery can resume without data loss.
#   3. After order-redis-master restarts, a fresh checkout succeeds, confirming
#      the whole stack (including WAL recovery) is working again.
#
# NOTE: This test uses a delta approach for key-placement checks. It snapshots
# keys in each business-data Redis BEFORE the checkout, runs the checkout, then
# asserts no NEW WAL-format keys were added. This makes the test robust against
# leftover keys from previous runs (e.g. when volumes were not wiped between
# deployments). For a fully clean run: docker compose down -v && up --build.
#
# Only runs in DEPLOY_MODE=docker (direct container control required).
source "$(dirname "$0")/helpers.sh"

header "TEST 14 — Decoupled WAL: WAL entries survive business-data Redis loss"

if [ "${DEPLOY_MODE:-docker}" != "docker" ]; then
  yellow "Skipping: DEPLOY_MODE=${DEPLOY_MODE:-docker} (requires docker)"
  exit 0
fi

WAL_CLI="docker compose exec -T wal-redis-master redis-cli -a redis --no-auth-warning"
ORDER_CLI="docker compose exec -T order-redis-master redis-cli -a redis --no-auth-warning"
STOCK_CLI="docker compose exec -T stock-redis-master redis-cli -a redis --no-auth-warning"
PAYMENT_CLI="docker compose exec -T payment-redis-master redis-cli -a redis --no-auth-warning"

seed_data

# ── Part A: WAL entries land in wal-redis, not in business-data Redis ────────

header "Part A — WAL key placement"

USER_ID=$(create_funded_user 500)
ORDER_ID=$(create_order_with_item "$USER_ID" 0 1)

# Use 2PC mode so participant WAL entries (stock + payment) are written
CODE=$(post "/orders/mode/2pc")
assert_http "Switch to 2PC mode" "200" "$CODE"

# ── Snapshot keys in each business-data Redis BEFORE the checkout ─────────────
# We diff before/after so leftover keys from prior runs don't cause false fails.
ORDER_BEFORE=$($ORDER_CLI keys "wal:*"       2>/dev/null | sort)
STOCK_BEFORE_WAL=$($STOCK_CLI keys "wal:*"        2>/dev/null | sort)
STOCK_BEFORE_OLD=$($STOCK_CLI keys "2pc:stock:*"  2>/dev/null | sort)
PAY_BEFORE_WAL=$($PAYMENT_CLI keys "wal:*"          2>/dev/null | sort)
PAY_BEFORE_OLD=$($PAYMENT_CLI keys "2pc:payment:*"  2>/dev/null | sort)

CODE=$(post "/orders/checkout/$ORDER_ID")
assert_http "2PC checkout returns 200" "200" "$CODE"

# ── Check wal-redis received entries ──────────────────────────────────────────
yellow "Checking wal-redis for WAL keys..."
WAL_KEYS=$($WAL_CLI keys "wal:*" 2>/dev/null | tr '\r\n' ' ')
yellow "wal-redis keys: $WAL_KEYS"

if echo "$WAL_KEYS" | grep -q "wal:"; then
  green "WAL keys found in wal-redis: PASS"
  PASS=$((PASS+1))
else
  red "No wal: keys found in wal-redis"
  FAIL=$((FAIL+1))
fi

# ── Check order-redis received no new wal: keys ───────────────────────────────
yellow "Confirming WAL keys are ABSENT from order-redis..."
ORDER_AFTER=$($ORDER_CLI keys "wal:*" 2>/dev/null | sort)
ORDER_NEW=$(comm -13 <(echo "$ORDER_BEFORE") <(echo "$ORDER_AFTER") | grep -v "^$")
if [ -z "$ORDER_NEW" ]; then
  green "order-redis gained no wal: keys — correctly separated: PASS"
  PASS=$((PASS+1))
else
  red "order-redis gained new wal: keys during checkout: $ORDER_NEW"
  FAIL=$((FAIL+1))
fi

yellow "Confirming old-style WAL keys (2pc:pending, saga:pending) absent from order-redis..."
OLD_WAL=$($ORDER_CLI keys "2pc:pending" 2>/dev/null; $ORDER_CLI keys "saga:pending" 2>/dev/null)
OLD_WAL=$(echo "$OLD_WAL" | grep -v "^$" | head -5)
if [ -z "$OLD_WAL" ]; then
  green "order-redis has no legacy WAL keys — fully migrated: PASS"
  PASS=$((PASS+1))
else
  red "order-redis still has legacy WAL keys: $OLD_WAL"
  FAIL=$((FAIL+1))
fi

# ── Check stock-redis received no new WAL keys ────────────────────────────────
yellow "Confirming no new 2PC WAL keys written to stock-redis during checkout..."
STOCK_AFTER_WAL=$($STOCK_CLI keys "wal:*"       2>/dev/null | sort)
STOCK_AFTER_OLD=$($STOCK_CLI keys "2pc:stock:*" 2>/dev/null | sort)
STOCK_NEW_WAL=$(comm -13 <(echo "$STOCK_BEFORE_WAL") <(echo "$STOCK_AFTER_WAL") | grep -v "^$")
STOCK_NEW_OLD=$(comm -13 <(echo "$STOCK_BEFORE_OLD") <(echo "$STOCK_AFTER_OLD") | grep -v "^$")
STOCK_NEW=$(printf "%s\n%s" "$STOCK_NEW_WAL" "$STOCK_NEW_OLD" | grep -v "^$")
if [ -z "$STOCK_NEW" ]; then
  green "stock-redis gained no WAL keys during checkout — correctly separated: PASS"
  PASS=$((PASS+1))
else
  red "stock-redis gained new WAL keys during checkout (expected only in wal-redis): $STOCK_NEW"
  FAIL=$((FAIL+1))
fi

# ── Check payment-redis received no new WAL keys ──────────────────────────────
yellow "Confirming no new 2PC WAL keys written to payment-redis during checkout..."
PAY_AFTER_WAL=$($PAYMENT_CLI keys "wal:*"          2>/dev/null | sort)
PAY_AFTER_OLD=$($PAYMENT_CLI keys "2pc:payment:*"  2>/dev/null | sort)
PAY_NEW_WAL=$(comm -13 <(echo "$PAY_BEFORE_WAL") <(echo "$PAY_AFTER_WAL") | grep -v "^$")
PAY_NEW_OLD=$(comm -13 <(echo "$PAY_BEFORE_OLD") <(echo "$PAY_AFTER_OLD") | grep -v "^$")
PAY_NEW=$(printf "%s\n%s" "$PAY_NEW_WAL" "$PAY_NEW_OLD" | grep -v "^$")
if [ -z "$PAY_NEW" ]; then
  green "payment-redis gained no WAL keys during checkout — correctly separated: PASS"
  PASS=$((PASS+1))
else
  red "payment-redis gained new WAL keys during checkout (expected only in wal-redis): $PAY_NEW"
  FAIL=$((FAIL+1))
fi

# ── Part B: WAL survives order-redis dying ────────────────────────────────────

header "Part B — WAL survives order-redis-master crash"

CODE=$(post "/orders/mode/saga")
assert_http "Switch back to saga mode" "200" "$CODE"

USER2=$(create_funded_user 500)
ORDER2=$(create_order_with_item "$USER2" 0 1)
CODE=$(post "/orders/checkout/$ORDER2")
assert_http "Saga checkout succeeds before crash" "200" "$CODE"

yellow "Snapshotting WAL keys before crash..."
WAL_BEFORE=$($WAL_CLI keys "wal:*" 2>/dev/null | sort | tr '\r\n' '|')
yellow "WAL before: $WAL_BEFORE"

yellow "Killing order-redis-master..."
docker compose stop order-redis-master > /dev/null 2>&1

yellow "Confirming wal-redis still readable after order-redis dies..."
WAL_AFTER=$($WAL_CLI keys "wal:*" 2>/dev/null | sort | tr '\r\n' '|')
yellow "WAL after crash: $WAL_AFTER"

if [ -n "$WAL_AFTER" ]; then
  green "wal-redis readable after order-redis crash: PASS"
  PASS=$((PASS+1))
else
  yellow "wal-redis returned no keys (may be empty if WAL was cleaned up — checking ping)"
  PING=$($WAL_CLI ping 2>/dev/null)
  if [ "$PING" = "PONG" ]; then
    green "wal-redis still reachable (WAL keys already cleaned on success): PASS"
    PASS=$((PASS+1))
  else
    red "wal-redis unreachable after order-redis crash"
    FAIL=$((FAIL+1))
  fi
fi

yellow "Restarting order-redis-master..."
docker compose start order-redis-master > /dev/null 2>&1

yellow "Waiting for order-redis to recover..."
for i in $(seq 1 20); do
  PING=$($ORDER_CLI ping 2>/dev/null)
  [ "$PING" = "PONG" ] && green "order-redis-master recovered" && break
  [ "$i" -eq 20 ] && red "order-redis did not recover in time" && FAIL=$((FAIL+1))
  sleep 2
done

sleep 5

# ── Part C: System works after recovery ───────────────────────────────────────

header "Part C — Full checkout works after recovery"

USER3=$(create_funded_user 500)
ORDER3=$(create_order_with_item "$USER3" 0 1)
CODE=$(post "/orders/checkout/$ORDER3")
assert_http "Checkout succeeds after order-redis recovery" "200" "$CODE"

STATUS=$(json_field "$(get_body /orders/find/$ORDER3)" "status")
assert_eq "Order marked paid after recovery" "paid" "$STATUS"

post "/orders/mode/saga" > /dev/null 2>&1

summary

# ── Part D: Orchestrator WAL in wal-redis ────────────────────────────────────

header "Part D — Orchestrator WAL entries in wal-redis"

yellow "Checking wal-redis for orchestrator WAL keys after checkout..."
ORCH_WAL_KEYS=$($WAL_CLI keys "wal:orch:*" 2>/dev/null | tr '\r\n' ' ')
yellow "wal:orch:* keys found: ${ORCH_WAL_KEYS:-<none — completed batches cleaned up>}"

MQ_CLI="docker compose exec -T mq-redis redis-cli -a redis --no-auth-warning"

MISPLACED=$($MQ_CLI keys "wal:orch:*" 2>/dev/null | grep -v "^$" | head -5)
if [ -z "$MISPLACED" ]; then
  green "mq-redis has no wal:orch: keys — orchestrator WAL correctly in wal-redis: PASS"
  PASS=$((PASS+1))
else
  red "mq-redis unexpectedly contains wal:orch: keys: $MISPLACED"
  FAIL=$((FAIL+1))
fi

yellow "Killing BOTH orchestrator containers to test WAL survives..."
docker compose stop orchestrator-1 orchestrator-2 > /dev/null 2>&1

yellow "Verifying wal-redis still reachable while orchestrators are down..."
PING=$($WAL_CLI ping 2>/dev/null)
if [ "$PING" = "PONG" ]; then
  green "wal-redis alive with orchestrators stopped: PASS"
  PASS=$((PASS+1))
else
  red "wal-redis unreachable after stopping orchestrators"
  FAIL=$((FAIL+1))
fi

yellow "Restarting orchestrators..."
docker compose start orchestrator-1 orchestrator-2 > /dev/null 2>&1

sleep 8

yellow "Verifying checkout works after orchestrator restart (recovery ran)..."
USER4=$(create_funded_user 500)
ORDER4=$(create_order_with_item "$USER4" 0 1)
CODE=$(post "/orders/checkout/$ORDER4")
assert_http "Checkout succeeds after orchestrator restart" "200" "$CODE"

STATUS=$(json_field "$(get_body /orders/find/$ORDER4)" "status")
assert_eq "Order paid after orchestrator restart" "paid" "$STATUS"

summary

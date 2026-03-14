#!/usr/bin/env bash
# Test 11: Native MQ 2PC participant protocol
#
# This test verifies the 2PC protocol by sending messages directly to the Redis Streams.
# This bypasses the HTTP layer and tests the MQ listener and service logic directly.

source "$(dirname "$0")/helpers.sh"

header "TEST 11 — Native MQ 2PC Participant Protocol"

# ── Helpers ───────────────────────────────────────────────────────────────────

txn() { echo "mq-2pc-test-$1-$$-$RANDOM"; }

fresh_item() {
  local stock="${1:-100}" price="${2:-10}"
  local body=$(post_body "/stock/item/create/$price")
  local id=$(json_field "$body" "item_id")
  post "/stock/add/$id/$stock" > /dev/null
  echo "$id"
}

# ── Section A: Stock participant ──────────────────────────────────────────────

yellow "[ A ] Stock participant — prepare / commit / abort"

ITEM=$(fresh_item 100 10)
yellow "  Fresh item: $ITEM (stock=100)"

TXN1=$(txn "commit")
ITEMS_JSON="{\"$ITEM\": 20}"

# A1. prepare → commit
RESP=$(mq_rpc_2pc "events:stock" "prepare_subtract_batch" "$TXN1" "$ITEMS_JSON")
CODE=$(json_field "$RESP" "status_code")
assert_eq "A1 prepare_subtract votes YES (200)" "200" "$CODE"

# Check side-effect: reservation exists in stock-redis-master
RES_VAL=$(docker exec dds26-team16-stock-redis-master-1 redis-cli -a redis get "reserved:stock:$ITEM")
assert_eq "A1 reservation recorded in Redis" "20" "$RES_VAL"

RESP=$(mq_rpc_2pc "events:stock" "commit_subtract_batch" "$TXN1" "$ITEMS_JSON")
CODE=$(json_field "$RESP" "status_code")
assert_eq "A1 commit_subtract returns 200" "200" "$CODE"

STOCK_AFTER=$(json_field "$(get_body /stock/find/$ITEM)" "stock")
assert_eq "A1 stock decreased by 20 after commit (100→80)" "80" "$STOCK_AFTER"

RES_VAL_AFTER=$(docker exec dds26-team16-stock-redis-master-1 redis-cli -a redis get "reserved:stock:$ITEM")
assert_eq "A1 reservation cleared in Redis" "0" "${RES_VAL_AFTER:-0}"

# A2. commit is idempotent
RESP=$(mq_rpc_2pc "events:stock" "commit_subtract_batch" "$TXN1" "$ITEMS_JSON")
CODE=$(json_field "$RESP" "status_code")
assert_eq "A2 commit_subtract idempotent (200)" "200" "$CODE"

# A3. prepare → abort
TXN2=$(txn "abort")
ITEMS_JSON_2="{\"$ITEM\": 15}"
RESP=$(mq_rpc_2pc "events:stock" "prepare_subtract_batch" "$TXN2" "$ITEMS_JSON_2")
assert_eq "A3 prepare votes YES (200)" "200" "$(json_field "$RESP" "status_code")"

RESP=$(mq_rpc_2pc "events:stock" "abort_subtract_batch" "$TXN2" "$ITEMS_JSON_2")
assert_eq "A3 abort returns 200" "200" "$(json_field "$RESP" "status_code")"

STOCK_AFTER_ABORT=$(json_field "$(get_body /stock/find/$ITEM)" "stock")
assert_eq "A3 stock unchanged after abort (still 80)" "80" "$STOCK_AFTER_ABORT"

# A6. prepare votes NO when stock is insufficient
TXN4=$(txn "noresource")
ITEMS_JSON_FAIL="{\"$ITEM\": 99999}"
RESP=$(mq_rpc_2pc "events:stock" "prepare_subtract_batch" "$TXN4" "$ITEMS_JSON_FAIL")
assert_eq "A6 prepare votes NO on insufficient stock (400)" "400" "$(json_field "$RESP" "status_code")"

# ── Section C: Payment participant ────────────────────────────────────────────

yellow "[ C ] Payment participant — prepare / commit / abort"

USER=$(create_funded_user 100)
yellow "  Fresh user: $USER (credit=100)"

TXN5=$(txn "pay-commit")

# C1. prepare → commit
RESP=$(mq_rpc_2pc "events:payment" "prepare_pay" "$TXN5" "" "$USER" "40")
assert_eq "C1 prepare_pay votes YES (200)" "200" "$(json_field "$RESP" "status_code")"

# Check side-effect in payment-redis-master
RES_VAL=$(docker exec dds26-team16-payment-redis-master-1 redis-cli -a redis get "reserved:payment:$USER")
assert_eq "C1 credit reservation recorded in Redis" "40" "$RES_VAL"

RESP=$(mq_rpc_2pc "events:payment" "commit_pay" "$TXN5" "" "$USER" "40")
assert_eq "C1 commit_pay returns 200" "200" "$(json_field "$RESP" "status_code")"

CREDIT_AFTER=$(json_field "$(get_body /payment/find_user/$USER)" "credit")
assert_eq "C1 credit decreased by 40 after commit (100→60)" "60" "$CREDIT_AFTER"

# C3. prepare → abort
TXN6=$(txn "pay-abort")
RESP=$(mq_rpc_2pc "events:payment" "prepare_pay" "$TXN6" "" "$USER" "30")
assert_eq "C3 prepare_pay votes YES (200)" "200" "$(json_field "$RESP" "status_code")"

RESP=$(mq_rpc_2pc "events:payment" "abort_pay" "$TXN6" "" "$USER" "30")
assert_eq "C3 abort_pay returns 200" "200" "$(json_field "$RESP" "status_code")"

CREDIT_AFTER_ABORT=$(json_field "$(get_body /payment/find_user/$USER)" "credit")
assert_eq "C3 credit unchanged after abort (still 60)" "60" "$CREDIT_AFTER_ABORT"

summary

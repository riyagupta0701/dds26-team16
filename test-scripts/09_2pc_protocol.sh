#!/usr/bin/env bash
# Test 9: 2PC participant protocol — direct tests of prepare/commit/abort
#
# These endpoints exist and work regardless of CHECKOUT_MODE.  We test them
# directly to verify the participant-level protocol without needing to restart
# any containers.
#
# Properties verified:
#   1. prepare→commit  : resource actually deducted, WAL idempotent
#   2. prepare→abort   : reservation released, resource unchanged, WAL idempotent
#   3. abort on un-prepared txn : 200 (safe for coordinator recovery re-drives)
#   4. prepare fails on insufficient resource  : 400 (vote NO)
#   5. Reservation blocks double-spend         : second prepare fails when
#      combined reservations would exceed total
#   6. Reservation released by abort unblocks a subsequent prepare

source "$(dirname "$0")/helpers.sh"

header "TEST 9 — 2PC Participant Protocol"

# ── Helpers ───────────────────────────────────────────────────────────────────

# Generate a transaction ID that is unique per test run.
txn() { echo "2pc-test-$1-$$-$RANDOM"; }

# Create a fresh stock item with known stock; print its item_id.
fresh_item() {
  local stock="${1:-100}" price="${2:-10}"
  local body
  body=$(post_body "/stock/item/create/$price")
  local id
  id=$(json_field "$body" "item_id")
  post "/stock/add/$id/$stock" > /dev/null
  echo "$id"
}

# ── Section A: Stock participant ──────────────────────────────────────────────

yellow "[ A ] Stock participant — prepare / commit / abort"

ITEM=$(fresh_item 100 10)
yellow "  Fresh item: $ITEM (stock=100)"

TXN1=$(txn "commit")
TXN2=$(txn "abort")
TXN3=$(txn "never-prepared")
TXN4=$(txn "noresource")

# A1. prepare → commit: stock must decrease by the committed amount.
CODE=$(post "/stock/prepare_subtract/$TXN1/$ITEM/20")
assert_http "A1 prepare_subtract votes YES (200)" "200" "$CODE"

# Visible stock is unchanged before commit (reservation is soft).
STOCK_MID=$(json_field "$(get_body /stock/find/$ITEM)" "stock")
assert_eq "A1 stock unchanged before commit (still 100)" "100" "$STOCK_MID"

CODE=$(post "/stock/commit_subtract/$TXN1/$ITEM/20")
assert_http "A1 commit_subtract returns 200" "200" "$CODE"

STOCK_AFTER_COMMIT=$(json_field "$(get_body /stock/find/$ITEM)" "stock")
assert_eq "A1 stock decreased by 20 after commit (100→80)" "80" "$STOCK_AFTER_COMMIT"

# A2. commit is idempotent: calling again must not double-deduct.
CODE=$(post "/stock/commit_subtract/$TXN1/$ITEM/20")
assert_http "A2 commit_subtract idempotent (200)" "200" "$CODE"
STOCK_IDEMPOTENT=$(json_field "$(get_body /stock/find/$ITEM)" "stock")
assert_eq "A2 idempotent commit does not double-deduct (still 80)" "80" "$STOCK_IDEMPOTENT"

# A3. prepare → abort: stock must remain unchanged.
CODE=$(post "/stock/prepare_subtract/$TXN2/$ITEM/15")
assert_http "A3 prepare before abort returns 200" "200" "$CODE"

CODE=$(post "/stock/abort_subtract/$TXN2/$ITEM/15")
assert_http "A3 abort_subtract returns 200" "200" "$CODE"

STOCK_AFTER_ABORT=$(json_field "$(get_body /stock/find/$ITEM)" "stock")
assert_eq "A3 stock unchanged after abort (still 80)" "80" "$STOCK_AFTER_ABORT"

# A4. abort is idempotent.
CODE=$(post "/stock/abort_subtract/$TXN2/$ITEM/15")
assert_http "A4 abort_subtract idempotent (200)" "200" "$CODE"

# A5. abort on never-prepared txn returns 200 (safe for recovery re-drives).
CODE=$(post "/stock/abort_subtract/$TXN3/$ITEM/10")
assert_http "A5 abort on un-prepared txn returns 200 (no-op)" "200" "$CODE"

# A6. prepare votes NO when stock is insufficient.
CODE=$(post "/stock/prepare_subtract/$TXN4/$ITEM/99999")
assert_http "A6 prepare votes NO on insufficient stock (400)" "400" "$CODE"

# ── Section B: Reservation blocks concurrent double-spend ─────────────────────

yellow "[ B ] Reservation conflict — double-spend prevention"

ITEM2=$(fresh_item 10 10)
yellow "  Scarce item: $ITEM2 (stock=10)"

TXN_A=$(txn "ds-A")
TXN_B=$(txn "ds-B")

# B1. First prepare uses 8 of 10 units → succeeds.
CODE=$(post "/stock/prepare_subtract/$TXN_A/$ITEM2/8")
assert_http "B1 first prepare (8/10) votes YES (200)" "200" "$CODE"

# B2. Second prepare requests 5 units; only 2 unreserved → must fail.
CODE=$(post "/stock/prepare_subtract/$TXN_B/$ITEM2/5")
assert_http "B2 second prepare (5/10, 8 reserved) votes NO (400)" "400" "$CODE"

# B3. After aborting the first prepare the reservation is freed.
CODE=$(post "/stock/abort_subtract/$TXN_A/$ITEM2/8")
assert_http "B3 abort first prepare returns 200" "200" "$CODE"

# B4. Now the second prepare should succeed (10 unreserved again).
CODE=$(post "/stock/prepare_subtract/$TXN_B/$ITEM2/5")
assert_http "B4 second prepare succeeds after first aborted (200)" "200" "$CODE"

# Clean up: commit TXN_B so the reservation doesn't leak into other tests.
CODE=$(post "/stock/commit_subtract/$TXN_B/$ITEM2/5")
assert_http "B4 cleanup commit returns 200" "200" "$CODE"

# ── Section C: Payment participant ────────────────────────────────────────────

yellow "[ C ] Payment participant — prepare / commit / abort"

USER=$(create_funded_user 100)
yellow "  Fresh user: $USER (credit=100)"

TXN5=$(txn "pay-commit")
TXN6=$(txn "pay-abort")
TXN7=$(txn "pay-never-prepared")
TXN8=$(txn "pay-noresource")

# C1. prepare → commit: credit must decrease.
CODE=$(post "/payment/prepare_pay/$TXN5/$USER/40")
assert_http "C1 prepare_pay votes YES (200)" "200" "$CODE"

# Credit is unchanged before commit (reservation is soft).
CREDIT_MID=$(json_field "$(get_body /payment/find_user/$USER)" "credit")
assert_eq "C1 credit unchanged before commit (still 100)" "100" "$CREDIT_MID"

CODE=$(post "/payment/commit_pay/$TXN5/$USER/40")
assert_http "C1 commit_pay returns 200" "200" "$CODE"

CREDIT_AFTER_COMMIT=$(json_field "$(get_body /payment/find_user/$USER)" "credit")
assert_eq "C1 credit decreased by 40 after commit (100→60)" "60" "$CREDIT_AFTER_COMMIT"

# C2. commit is idempotent.
CODE=$(post "/payment/commit_pay/$TXN5/$USER/40")
assert_http "C2 commit_pay idempotent (200)" "200" "$CODE"
CREDIT_IDEMPOTENT=$(json_field "$(get_body /payment/find_user/$USER)" "credit")
assert_eq "C2 idempotent commit does not double-deduct (still 60)" "60" "$CREDIT_IDEMPOTENT"

# C3. prepare → abort: credit must remain unchanged.
CODE=$(post "/payment/prepare_pay/$TXN6/$USER/30")
assert_http "C3 prepare before abort returns 200" "200" "$CODE"

CODE=$(post "/payment/abort_pay/$TXN6/$USER/30")
assert_http "C3 abort_pay returns 200" "200" "$CODE"

CREDIT_AFTER_ABORT=$(json_field "$(get_body /payment/find_user/$USER)" "credit")
assert_eq "C3 credit unchanged after abort (still 60)" "60" "$CREDIT_AFTER_ABORT"

# C4. abort is idempotent.
CODE=$(post "/payment/abort_pay/$TXN6/$USER/30")
assert_http "C4 abort_pay idempotent (200)" "200" "$CODE"

# C5. abort on never-prepared txn returns 200.
CODE=$(post "/payment/abort_pay/$TXN7/$USER/10")
assert_http "C5 abort on un-prepared txn returns 200" "200" "$CODE"

# C6. prepare votes NO when credit is insufficient.
CODE=$(post "/payment/prepare_pay/$TXN8/$USER/99999")
assert_http "C6 prepare votes NO on insufficient credit (400)" "400" "$CODE"

# ── Section D: Reservation blocks payment double-spend ────────────────────────

yellow "[ D ] Reservation conflict — payment double-spend prevention"

USER2=$(create_funded_user 10)
yellow "  Low-credit user: $USER2 (credit=10)"

TXN_PA=$(txn "pay-ds-A")
TXN_PB=$(txn "pay-ds-B")

# D1. First prepare uses 8 of 10 credit → succeeds.
CODE=$(post "/payment/prepare_pay/$TXN_PA/$USER2/8")
assert_http "D1 first prepare (8/10 credit) votes YES (200)" "200" "$CODE"

# D2. Second prepare requests 5; only 2 unreserved → must fail.
CODE=$(post "/payment/prepare_pay/$TXN_PB/$USER2/5")
assert_http "D2 second prepare (5 credit, 8 reserved) votes NO (400)" "400" "$CODE"

# D3. Abort first; second should now succeed.
CODE=$(post "/payment/abort_pay/$TXN_PA/$USER2/8")
assert_http "D3 abort first prepare returns 200" "200" "$CODE"

CODE=$(post "/payment/prepare_pay/$TXN_PB/$USER2/5")
assert_http "D4 second prepare succeeds after first aborted (200)" "200" "$CODE"

# Clean up.
post "/payment/abort_pay/$TXN_PB/$USER2/5" > /dev/null

summary

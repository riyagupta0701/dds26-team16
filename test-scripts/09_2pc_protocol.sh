#!/usr/bin/env bash
# Test 9: 2PC participant protocol — direct HTTP tests of prepare/commit/abort
#
# The payment service exposes HTTP endpoints for 2PC (prepare/commit/abort).
# Stock 2PC is MQ-only (tested in test 11).
#
# Properties verified:
#   1. prepare→commit  : resource actually deducted, idempotent
#   2. prepare→abort   : reservation released, resource unchanged, idempotent
#   3. abort on un-prepared txn : 200 (safe for coordinator recovery re-drives)
#   4. prepare fails on insufficient resource  : 400 (vote NO)
#   5. Reservation blocks double-spend         : second prepare fails when
#      combined reservations would exceed total
#   6. Reservation released by abort unblocks a subsequent prepare

source "$(dirname "$0")/helpers.sh"

header "TEST 9 — 2PC Participant Protocol (Payment HTTP)"

# ── Helpers ───────────────────────────────────────────────────────────────────

# Generate a transaction ID that is unique per test run.
txn() { echo "2pc-test-$1-$$-$RANDOM"; }

# ── Section A: Payment participant ────────────────────────────────────────────

yellow "[ A ] Payment participant — prepare / commit / abort"

USER=$(create_funded_user 100)
yellow "  Fresh user: $USER (credit=100)"

TXN1=$(txn "pay-commit")
TXN2=$(txn "pay-abort")
TXN3=$(txn "pay-never-prepared")
TXN4=$(txn "pay-noresource")

# A1. prepare → commit: credit must decrease.
CODE=$(post "/payment/prepare_pay/$TXN1/$USER/40")
assert_http "A1 prepare_pay votes YES (200)" "200" "$CODE"

# Credit is unchanged before commit (reservation is soft).
CREDIT_MID=$(json_field "$(get_body /payment/find_user/$USER)" "credit")
assert_eq "A1 credit unchanged before commit (still 100)" "100" "$CREDIT_MID"

CODE=$(post "/payment/commit_pay/$TXN1/$USER/40")
assert_http "A1 commit_pay returns 200" "200" "$CODE"

CREDIT_AFTER_COMMIT=$(json_field "$(get_body /payment/find_user/$USER)" "credit")
assert_eq "A1 credit decreased by 40 after commit (100→60)" "60" "$CREDIT_AFTER_COMMIT"

# A2. commit is idempotent.
CODE=$(post "/payment/commit_pay/$TXN1/$USER/40")
assert_http "A2 commit_pay idempotent (200)" "200" "$CODE"
CREDIT_IDEMPOTENT=$(json_field "$(get_body /payment/find_user/$USER)" "credit")
assert_eq "A2 idempotent commit does not double-deduct (still 60)" "60" "$CREDIT_IDEMPOTENT"

# A3. prepare → abort: credit must remain unchanged.
CODE=$(post "/payment/prepare_pay/$TXN2/$USER/30")
assert_http "A3 prepare before abort returns 200" "200" "$CODE"

CODE=$(post "/payment/abort_pay/$TXN2/$USER/30")
assert_http "A3 abort_pay returns 200" "200" "$CODE"

CREDIT_AFTER_ABORT=$(json_field "$(get_body /payment/find_user/$USER)" "credit")
assert_eq "A3 credit unchanged after abort (still 60)" "60" "$CREDIT_AFTER_ABORT"

# A4. abort is idempotent.
CODE=$(post "/payment/abort_pay/$TXN2/$USER/30")
assert_http "A4 abort_pay idempotent (200)" "200" "$CODE"

# A5. abort on never-prepared txn returns 200.
CODE=$(post "/payment/abort_pay/$TXN3/$USER/10")
assert_http "A5 abort on un-prepared txn returns 200" "200" "$CODE"

# A6. prepare votes NO when credit is insufficient.
CODE=$(post "/payment/prepare_pay/$TXN4/$USER/99999")
assert_http "A6 prepare votes NO on insufficient credit (400)" "400" "$CODE"

# ── Section B: Reservation blocks payment double-spend ────────────────────────

yellow "[ B ] Reservation conflict — payment double-spend prevention"

USER2=$(create_funded_user 10)
yellow "  Low-credit user: $USER2 (credit=10)"

TXN_PA=$(txn "pay-ds-A")
TXN_PB=$(txn "pay-ds-B")

# B1. First prepare uses 8 of 10 credit → succeeds.
CODE=$(post "/payment/prepare_pay/$TXN_PA/$USER2/8")
assert_http "B1 first prepare (8/10 credit) votes YES (200)" "200" "$CODE"

# B2. Second prepare requests 5; only 2 unreserved → must fail.
CODE=$(post "/payment/prepare_pay/$TXN_PB/$USER2/5")
assert_http "B2 second prepare (5 credit, 8 reserved) votes NO (400)" "400" "$CODE"

# B3. Abort first; second should now succeed.
CODE=$(post "/payment/abort_pay/$TXN_PA/$USER2/8")
assert_http "B3 abort first prepare returns 200" "200" "$CODE"

CODE=$(post "/payment/prepare_pay/$TXN_PB/$USER2/5")
assert_http "B4 second prepare succeeds after first aborted (200)" "200" "$CODE"

# Clean up.
post "/payment/abort_pay/$TXN_PB/$USER2/5" > /dev/null

summary

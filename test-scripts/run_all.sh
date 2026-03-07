#!/usr/bin/env bash
# Runs all tests in order and prints a final summary
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BASE_URL="${BASE_URL:-http://localhost:8000}"
export BASE_URL

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[0;33m'
BOLD='\033[1m'
RESET='\033[0m'

TOTAL_PASS=0
TOTAL_FAIL=0

run_test() {
  local script="$1"
  local name
  name=$(basename "$script" .sh)
  echo ""
  echo -e "${BOLD}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${RESET}"
  echo -e "${BOLD}  Running: $name${RESET}"
  echo -e "${BOLD}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${RESET}"

  set +e
  bash "$script"
  EXIT_CODE=$?
  set -e

  if [ "$EXIT_CODE" -eq 0 ]; then
    echo -e "${GREEN}  ✔  $name PASSED${RESET}"
    TOTAL_PASS=$((TOTAL_PASS+1))
  else
    echo -e "${RED}  ✘  $name FAILED${RESET}"
    TOTAL_FAIL=$((TOTAL_FAIL+1))
  fi
}

echo -e "${BOLD}"
echo "╔══════════════════════════════════════════════════╗"
echo "║                    Test Suite                    ║"
echo "╚══════════════════════════════════════════════════╝"
echo -e "${RESET}"
echo "Target: $BASE_URL"

# Wait for stack
echo ""
echo -e "${YELLOW}→  Waiting for stack to be ready...${RESET}"
for i in $(seq 1 30); do
  CODE=$(curl -s -o /dev/null -w "%{http_code}" "$BASE_URL/stock/find/0" 2>/dev/null || echo "000")
  if [ "$CODE" = "200" ]; then
    echo -e "${GREEN}✔  Stack is ready${RESET}"
    break
  fi
  if [ "$i" -eq 30 ]; then
    echo -e "${RED}✘  Stack not ready after 60s. Is docker compose up --build running?${RESET}"
    exit 1
  fi
  sleep 2
done

# Core correctness tests (fast, no container killing)
run_test "$SCRIPT_DIR/01_happy_path.sh"
run_test "$SCRIPT_DIR/02_idempotency.sh"
run_test "$SCRIPT_DIR/03_compensation_payment_fails.sh"
run_test "$SCRIPT_DIR/04_compensation_stock_fails.sh"
run_test "$SCRIPT_DIR/08_consistency_check.sh"

# 2PC participant protocol (no container restart required)
run_test "$SCRIPT_DIR/09_2pc_protocol.sh"

# Fault tolerance tests (stop/start containers — slower)
if [ "${SKIP_FAULT_TESTS:-0}" != "1" ]; then
  run_test "$SCRIPT_DIR/05_fault_app_replica.sh"
  # run_test "$SCRIPT_DIR/06_fault_redis_master.sh"
else
  echo ""
  echo -e "${YELLOW}→  Skipping fault tests (SKIP_FAULT_TESTS=1)${RESET}"
fi

# Mode flag test — switches CHECKOUT_MODE between saga and 2pc (restarts order services)
run_test "$SCRIPT_DIR/07_mode_flag.sh"

# Final summary
echo ""
echo -e "${BOLD}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${RESET}"
echo -e "${BOLD}  FINAL RESULTS${RESET}"
echo -e "${BOLD}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${RESET}"
echo -e "  Test suites passed: ${GREEN}${BOLD}$TOTAL_PASS${RESET}"
echo -e "  Test suites failed: ${RED}${BOLD}$TOTAL_FAIL${RESET}"
echo ""

[ "$TOTAL_FAIL" -eq 0 ] \
  && echo -e "${GREEN}${BOLD}  ALL TESTS PASSED ✔${RESET}" \
  || echo -e "${RED}${BOLD}  SOME TESTS FAILED ✘${RESET}"

echo ""
[ "$TOTAL_FAIL" -eq 0 ] && exit 0 || exit 1

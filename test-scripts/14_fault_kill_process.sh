#!/usr/bin/env bash
# Test 14: Fault tolerance — kill the main process inside containers.
#
# Kills the main process (PID 1) inside containers rather than using
# docker compose stop. This is a harder failure mode — no graceful shutdown,
# the process dies immediately and Docker restarts it via restart: unless-stopped.
source "$(dirname "$0")/helpers.sh"

header "TEST 14 — Fault Tolerance: Kill Main Process (PID 1)"

seed_data

# ── Helper: kill PID 1 inside a container, wait for restart ─────────────────
kill_and_wait() {
  local container="$1"
  local label="$2"
  yellow "Killing PID 1 in $container..."
  docker exec "$container" kill 1 2>/dev/null || true
  sleep 3  # let Docker restart the container
  # Wait until container is running again
  for i in $(seq 1 10); do
    STATUS=$(docker inspect -f '{{.State.Running}}' "$container" 2>/dev/null || echo "false")
    [ "$STATUS" = "true" ] && break
    sleep 1
  done
}

# ── Kill each service type and verify recovery ─────────────────────────────
CONTAINERS=(
  "dds26-team16-order-service-1:Order service"
  "dds26-team16-stock-service-1:Stock service"
  "dds26-team16-payment-service-1:Payment service"
)

for entry in "${CONTAINERS[@]}"; do
  CONTAINER="${entry%%:*}"
  LABEL="${entry##*:}"

  # Check container exists
  if ! docker inspect "$CONTAINER" > /dev/null 2>&1; then
    # Try alternate naming (e.g. without -1 suffix for small config)
    ALT="${CONTAINER%-1}"
    if docker inspect "dds26-team16-${ALT##*-}-1" > /dev/null 2>&1; then
      CONTAINER="dds26-team16-${ALT##*-}-1"
    else
      yellow "Container $CONTAINER not found, skipping"
      continue
    fi
  fi

  header "Kill PID 1: $LABEL"

  kill_and_wait "$CONTAINER" "$LABEL"

  # Wait for service to be fully ready
  sleep 5

  # Attempt checkout
  RECOVERED=0
  for attempt in $(seq 1 8); do
    USER_ID=$(create_funded_user 200)
    ORDER_ID=$(create_order_with_item "$USER_ID" 0 1)
    CODE=$(post "/orders/checkout/$ORDER_ID")
    if [ "$CODE" = "200" ]; then
      RECOVERED=1
      break
    fi
    sleep 2
  done

  if [ "$RECOVERED" -eq 1 ]; then
    green "Checkout works after killing $LABEL (attempt $attempt)"
    PASS=$((PASS+1))

    ORDER_STATUS=$(json_field "$(get_body /orders/find/$ORDER_ID)" "status")
    assert_eq "Order paid after $LABEL kill" "paid" "$ORDER_STATUS"
  else
    red "Checkout failed after killing $LABEL"
    FAIL=$((FAIL+1))
  fi
done

summary

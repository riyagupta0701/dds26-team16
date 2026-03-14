#!/usr/bin/env bash
# Shared helpers for all test scripts

BASE_URL="${BASE_URL:-http://localhost:8000}"
DEPLOY_MODE="${DEPLOY_MODE:-docker}"   # "docker" or "kube"
PASS=0
FAIL=0

green()  { echo -e "\033[0;32m✔  $*\033[0m"; }
red()    { echo -e "\033[0;31m✘  $*\033[0m"; }
yellow() { echo -e "\033[0;33m→  $*\033[0m"; }
header() { echo -e "\n\033[1;34m══════════════════════════════════════\033[0m"; echo -e "\033[1;34m  $*\033[0m"; echo -e "\033[1;34m══════════════════════════════════════\033[0m"; }

assert_eq() {
  local label="$1" expected="$2" actual="$3"
  if [ "$actual" = "$expected" ]; then
    green "$label: '$actual'"
    PASS=$((PASS+1))
  else
    red  "$label: expected='$expected' got='$actual'"
    FAIL=$((FAIL+1))
  fi
}

assert_http() {
  local label="$1" expected="$2" actual="$3"
  assert_eq "$label (HTTP $expected)" "$expected" "$actual"
}

assert_gt() {
  local label="$1" expected="$2" actual="$3"
  if [ "$actual" -gt "$expected" ] 2>/dev/null; then
    green "$label: $actual > $expected"
    PASS=$((PASS+1))
  else
    red "$label: expected >$expected got='$actual'"
    FAIL=$((FAIL+1))
  fi
}

assert_lte() {
  local label="$1" expected="$2" actual="$3"
  if [ "$actual" -le "$expected" ] 2>/dev/null; then
    green "$label: $actual <= $expected"
    PASS=$((PASS+1))
  else
    red "$label: expected <=$expected got='$actual'"
    FAIL=$((FAIL+1))
  fi
}

summary() {
  echo ""
  echo -e "\033[1m Results: \033[0;32m$PASS passed\033[0m  \033[0;31m$FAIL failed\033[0m"
  [ "$FAIL" -eq 0 ] && exit 0 || exit 1
}

# Extract a JSON field value (simple, no jq dependency)
json_field() {
  local json="$1" field="$2"
  echo "$json" | grep -o "\"$field\":[^,}]*" | head -1 | sed 's/.*://;s/[" ]//g'
}

post() { curl -s -o /dev/null -w "%{http_code}" -X POST "$BASE_URL$1"; }
post_body() { curl -s -X POST "$BASE_URL$1"; }
get_body() { curl -s "$BASE_URL$1"; }
get_code() { curl -s -o /dev/null -w "%{http_code}" "$BASE_URL$1"; }

wait_for_stack() {
  yellow "Waiting for stack to be ready..."
  for i in $(seq 1 30); do
    code=$(curl -s -o /dev/null -w "%{http_code}" "$BASE_URL/stock/find/0" 2>/dev/null || echo "000")
    [ "$code" != "000" ] && green "Stack is ready" && return 0
    sleep 2
  done
  red "Stack did not become ready in time"
  exit 1
}

seed_data() {
  yellow "Seeding data..."
  curl -s -X POST "$BASE_URL/stock/batch_init/10/100/10"  > /dev/null
  curl -s -X POST "$BASE_URL/payment/batch_init/10/1000"  > /dev/null
  curl -s -X POST "$BASE_URL/orders/batch_init/10/10/10/10" > /dev/null
  green "Data seeded"
}

create_funded_user() {
  local amount="${1:-200}"
  local body
  body=$(post_body "/payment/create_user")
  local uid
  uid=$(json_field "$body" "user_id")
  curl -s -X POST "$BASE_URL/payment/add_funds/$uid/$amount" > /dev/null
  echo "$uid"
}

create_order_with_item() {
  local user_id="$1" item_id="${2:-0}" qty="${3:-1}"
  local body
  body=$(post_body "/orders/create/$user_id")
  local oid
  oid=$(json_field "$body" "order_id")
  curl -s -X POST "$BASE_URL/orders/addItem/$oid/$item_id/$qty" > /dev/null
  echo "$oid"
}

# ---------------------------------------------------------------------------
# Message Queue helpers
# ---------------------------------------------------------------------------

# Send an RPC call over Redis Streams and wait for a reply on a temporary list.
# Usage: mq_rpc <stream> <type> <payload_json_escaped>
# Returns: The raw JSON response from the service.
mq_rpc() {
  local stream="$1"
  local type="$2"
  local payload="$3"
  local req_id=$(head /dev/urandom | LC_ALL=C tr -dc 'a-zA-Z0-9' | head -c 16)
  local reply_key="reply:test:$req_id"

  docker exec -e REDISCLI_AUTH=redis dds26-team16-mq-redis-1 redis-cli \
    XADD "$stream" "*" \
    type "$type" \
    reply_to "$reply_key" \
    payload "$payload" > /dev/null

  local resp=$(docker exec -e REDISCLI_AUTH=redis dds26-team16-mq-redis-1 redis-cli BLPOP "$reply_key" 5)
  echo "$resp" | tail -n 1 | tr -d '\r'
}

# Special version for 2PC which often passes order_id/items as top-level fields
mq_rpc_2pc() {
  local stream="$1"
  local type="$2"
  local order_id="$3"
  local items_json="$4"
  local user_id="$5"
  local amount="$6"
  
  local req_id=$(head /dev/urandom | LC_ALL=C tr -dc 'a-zA-Z0-9' | head -c 16)
  local reply_key="reply:test:$req_id"

  local cmd=("XADD" "$stream" "*" "type" "$type" "reply_to" "$reply_key")
  [ -n "$order_id" ] && cmd+=("order_id" "$order_id")
  [ -n "$items_json" ] && cmd+=("items" "$items_json")
  [ -n "$user_id" ] && cmd+=("user_id" "$user_id")
  [ -n "$amount" ] && cmd+=("amount" "$amount")

  docker exec -e REDISCLI_AUTH=redis dds26-team16-mq-redis-1 redis-cli "${cmd[@]}" > /dev/null
  local resp=$(docker exec -e REDISCLI_AUTH=redis dds26-team16-mq-redis-1 redis-cli BLPOP "$reply_key" 5)
  echo "$resp" | tail -n 1 | tr -d '\r'
}

# ---------------------------------------------------------------------------
# Runtime helpers — work in both DEPLOY_MODE=docker and DEPLOY_MODE=kube
# ---------------------------------------------------------------------------

# Maps a docker-compose service name to a Kubernetes resource:
#   app services  → component label (used by Deployments)
#   redis masters → StatefulSet pod name (one StatefulSet per Helm release)
_kube_resource() {
  case "$1" in
    *order-redis*)   echo "pod/order-redis-master-0"   ;;
    *stock-redis*)   echo "pod/stock-redis-master-0"   ;;
    *payment-redis*) echo "pod/payment-redis-master-0" ;;
    *order*)         echo "label:component=order"      ;;
    *stock*)         echo "label:component=stock"      ;;
    *payment*)       echo "label:component=payment"    ;;
  esac
}

# Kill one replica of a service.
# docker: docker compose stop <name>
# kube:   delete the pod (K8s / StatefulSet recreates it automatically)
service_stop() {
  local svc="$1"
  if [ "$DEPLOY_MODE" = "kube" ]; then
    local resource pod
    resource=$(_kube_resource "$svc")
    if [[ "$resource" == pod/* ]]; then
      pod="$resource"
    else
      local label="${resource#label:}"
      pod=$(kubectl get pod -l "$label" -o name | head -1)
    fi
    yellow "Deleting $pod..."
    kubectl delete "$pod" --grace-period=0 --wait=false > /dev/null 2>&1
  else
    docker compose stop "$svc" > /dev/null 2>&1
  fi
}

# Restore a service replica.
# docker: docker compose start <name>
# kube:   wait for the pod/deployment to reach full readiness
service_start() {
  local svc="$1"
  if [ "$DEPLOY_MODE" = "kube" ]; then
    local resource
    resource=$(_kube_resource "$svc")
    if [[ "$resource" == pod/* ]]; then
      kubectl wait "${resource}" --for=condition=Ready --timeout=60s > /dev/null 2>&1
    else
      local label="${resource#label:}"
      local component="${label#component=}"
      kubectl rollout status "deployment/${component}-deployment" --timeout=60s > /dev/null 2>&1
    fi
  else
    docker compose start "$svc" > /dev/null 2>&1
  fi
}

# Crash ALL Redis masters (simulates a full data-store outage / power loss).
# docker: stops all three per-service masters
# kube:   deletes the master-0 pod of each bitnami StatefulSet (each is recreated automatically)
redis_crash() {
  yellow "Crashing Redis masters..."
  if [ "$DEPLOY_MODE" = "kube" ]; then
    kubectl delete pod/order-redis-master-0 pod/stock-redis-master-0 pod/payment-redis-master-0 \
      --grace-period=0 --wait=false > /dev/null 2>&1
  else
    docker compose stop order-redis-master stock-redis-master payment-redis-master > /dev/null 2>&1
  fi
}

# Restart ALL Redis masters and wait until they are ready (AOF replay complete).
# docker: starts all three masters
# kube:   waits for all three StatefulSet pods to become Ready again
redis_restore() {
  yellow "Restarting Redis masters (AOF replay on startup)..."
  if [ "$DEPLOY_MODE" = "kube" ]; then
    kubectl wait pod/order-redis-master-0 pod/stock-redis-master-0 pod/payment-redis-master-0 \
      --for=condition=Ready --timeout=60s > /dev/null 2>&1
  else
    docker compose start order-redis-master stock-redis-master payment-redis-master > /dev/null 2>&1
  fi
}

# Switch CHECKOUT_MODE on the order service.
# docker: rewrites the service via a compose override file
# kube:   kubectl set env + waits for rolling update to complete
set_checkout_mode() {
    local mode=$1
    curl -s -X POST "$BASE_URL/orders/mode/$mode" > /dev/null
}

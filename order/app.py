import logging
import os
import atexit
import random
import uuid
import json
from collections import defaultdict

import redis

from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response

DB_ERROR_STR = "DB error"
REQ_ERROR_STR = "Requests error"


# CHECKOUT_MODE: "saga" (default) or "2pc"
CHECKOUT_MODE = os.environ.get('CHECKOUT_MODE', 'saga').lower()

app = Flask("order-service")

db: redis.Redis = redis.Redis(host=os.environ['REDIS_HOST'],
                              port=int(os.environ['REDIS_PORT']),
                              password=os.environ['REDIS_PASSWORD'],
                              db=int(os.environ['REDIS_DB']))

# Separate connection for Message Queue operations
# Defaults to the same host as DB, but allows splitting in production
mq: redis.Redis = redis.Redis(host=os.environ.get('MQ_REDIS_HOST', os.environ['REDIS_HOST']),
                              port=int(os.environ.get('MQ_REDIS_PORT', os.environ['REDIS_PORT'])),
                              password=os.environ.get('MQ_REDIS_PASSWORD', os.environ['REDIS_PASSWORD']),
                              db=int(os.environ.get('MQ_REDIS_DB', os.environ['REDIS_DB'])))

def close_db_connection():
    db.close()
    mq.close()


atexit.register(close_db_connection)

# Order status values — shared by Saga and 2PC
STATUS_PENDING = "pending"   # created, not yet checked out
STATUS_STARTED = "started"   # checkout started, no side-effects yet
STATUS_PAID    = "paid"      # completed successfully
STATUS_FAILED  = "failed"    # rolled back

# Redis key for the coordinator WAL: a set of order_ids currently in-flight.
# On startup, unresolved entries are used to recover hanging transactions.
COORD_PENDING_KEY = "2pc:pending"
SAGA_PENDING_KEY = "saga:pending"


class OrderValue(Struct, kw_only=True):
    paid: bool
    items: dict[str, int]
    user_id: str
    total_cost: int
    status: str = STATUS_PENDING


# DB helpers
def get_order_from_db(order_id: str) -> OrderValue | None:
    try:
        # get serialized data
        raw_entry: bytes = db.get(order_id)
    except redis.exceptions.RedisError:
        abort(500, DB_ERROR_STR)
    # deserialize data if it exists else return null
    entry: OrderValue | None = msgpack.decode(raw_entry, type=OrderValue) if raw_entry else None
    if entry is None:
        app.logger.warning("Order entry not found: %s", order_id)
        # if order does not exist in the database; abort
        abort(400, f"Order: {order_id} not found!")
    return entry

# Order endpoints
@app.post('/create/<user_id>')
def create_order(user_id: str):
    key = str(uuid.uuid4())
    value = msgpack.encode(OrderValue(paid=False, items={}, user_id=user_id, total_cost=0, status=STATUS_PENDING))
    try:
        db.set(key, value)
    except redis.exceptions.RedisError:
        app.logger.error("Failed to save order: %s", key)
        return abort(500, DB_ERROR_STR)
    return jsonify({'order_id': key})


@app.post('/batch_init/<n>/<n_items>/<n_users>/<item_price>')
def batch_init_orders(n: int, n_items: int, n_users: int, item_price: int):

    n = int(n)
    n_items = int(n_items)
    n_users = int(n_users)
    item_price = int(item_price)

    def generate_entry() -> OrderValue:
        user_id = random.randint(0, n_users - 1)
        item1_id = random.randint(0, n_items - 1)
        item2_id = random.randint(0, n_items - 1)
        
        items_dict = defaultdict(int)
        items_dict[f"{item1_id}"] += 1
        items_dict[f"{item2_id}"] += 1
        
        return OrderValue(paid=False,
                          items=items_dict,
                          user_id=f"{user_id}",
                          total_cost=2 * item_price,
                          status=STATUS_PENDING)

    kv_pairs: dict[str, bytes] = {f"{i}": msgpack.encode(generate_entry())
                                  for i in range(n)}
    try:
        db.mset(kv_pairs)
    except redis.exceptions.RedisError:
        app.logger.error("Failed to save order: %s", kv_pairs)
        return abort(500, DB_ERROR_STR)
    return jsonify({"msg": "Batch init for orders successful"})


@app.get('/find/<order_id>')
def find_order(order_id: str):
    order_entry: OrderValue = get_order_from_db(order_id)
    return jsonify(
        {
            "order_id": order_id,
            "paid": order_entry.paid,
            "items": order_entry.items,
            "user_id": order_entry.user_id,
            "total_cost": order_entry.total_cost,
            "status": order_entry.status
        }
    )


@app.get('/health')
def health_check():
    try:
        db.ping()
        return jsonify({"status": "healthy", "db": "connected"}), 200
    except redis.exceptions.RedisError as e:
        app.logger.error(f"Health check failed: {e}")
        return jsonify({"status": "unhealthy", "db": "disconnected"}), 500

# HTTP helpers with retry
class RpcResponse:
    def __init__(self, status_code, json_data=None, error_msg=None):
        self.status_code = status_code
        self._json = json_data
        self.text = error_msg or ""

    def json(self):
        return self._json

def send_rpc(stream: str, action: str, data: dict, retries: int = 3) -> RpcResponse | None:
    """
    Sends a command to a Redis Stream and waits for a reply on a temporary list.
    Acts as a synchronous RPC wrapper over async streams.
    """
    req_id = str(uuid.uuid4())
    reply_key = f"reply:{req_id}"

    # Prepare message for Redis Stream (flat dict of strings)
    message = {
        "type": action,
        "reply_to": reply_key,
        **{k: str(v) for k, v in data.items()}
    }

    for attempt in range(retries):
        try:
            mq.xadd(stream, message)

            # Blocking pop with timeout (matches previous HTTP timeout)
            resp = mq.blpop(reply_key, timeout=5)

            if resp:
                # resp is a tuple (key, value)
                val = json.loads(resp[1])
                return RpcResponse(
                    val.get('status_code', 500),
                    json_data=val.get('body'),
                    error_msg=val.get('error')
                )
            else:
                app.logger.warning(f"RPC {action} attempt {attempt + 1} timed out")

        except Exception as e:
            app.logger.error(f"RPC {action} failed: {e}")

    # Cleanup (best effort, consumer sets TTL anyway)
    mq.delete(reply_key)
    return None


@app.post('/addItem/<order_id>/<item_id>/<quantity>')
def add_item(order_id: str, item_id: str, quantity: int):
    # Fetch item price first (outside the Redis transaction — read-only, no lock needed).
    item_reply = send_rpc('events:stock', 'find_item', {'item_id': item_id})
    if item_reply is None or item_reply.status_code != 200:
        abort(400, f"Item: {item_id} does not exist!")
    item_json: dict = item_reply.json()
    price_per_unit = int(quantity) * item_json["price"]

    # Use WATCH/MULTI/EXEC so that two concurrent addItem calls on the same
    # order_id cannot overwrite each other.
    for _ in range(10):
        try:
            with db.pipeline() as pipe:
                pipe.watch(order_id)
                raw = pipe.get(order_id)
                if raw is None:
                    pipe.unwatch()
                    abort(400, f"Order: {order_id} not found!")
                order_entry = msgpack.decode(raw, type=OrderValue)
                order_entry.items[item_id] = order_entry.items.get(item_id, 0) + int(quantity)
                order_entry.total_cost += price_per_unit
                pipe.multi()
                pipe.set(order_id, msgpack.encode(order_entry))
                pipe.execute()
                return Response(
                    f"Item: {item_id} added to: {order_id} price updated to: {order_entry.total_cost}",
                    status=200,
                )
        except redis.WatchError:
            continue  # concurrent write detected — retry with fresh state
        except redis.exceptions.RedisError:
            abort(500, DB_ERROR_STR)
    abort(400, "Conflict: could not add item after retries")


def rollback_stock(items: dict[str, int], transaction_id: str | None = None):
    # Best-effort: use _recovery_post so a network hiccup during compensation
    # does not raise an HTTPException that would prevent STATUS_FAILED from
    # being persisted by the caller.
    payload = {'items': json.dumps(items), 'transaction_id': transaction_id} if transaction_id else {'items': json.dumps(items)}
    _recovery_rpc('events:stock', 'add_stock_batch', payload)


# Checkout dispatcher
@app.post('/checkout/<order_id>')
def checkout(order_id: str):
    app.logger.debug(f"Checkout {order_id} using mode={CHECKOUT_MODE}")
    if CHECKOUT_MODE == '2pc':
        return checkout_2pc(order_id)
    elif CHECKOUT_MODE == 'saga':
        return checkout_saga(order_id)
    else: abort(501, "Select a valid checkout mode.")


def checkout_saga(order_id: str):
    """
    Orchestration-based Saga.

    Forward transitions:
        STATUS_PENDING -> STATUS_STARTED
        STATUS_STARTED -> (Reserve Stock) -> (Deduct Payment) -> STATUS_PAID

    Backward / compensation transitions on failure:
        Deduct Payment failed  -> Refund Payment (no-op here, never charged)
                               -> Release Stock
                               -> STATUS_FAILED
        Reserve Stock failed   -> Release already-reserved Stock
                               -> STATUS_FAILED
        Final db.set failed    -> Refund Payment -> Release Stock -> STATUS_FAILED

    Idempotency:
        STATUS_PAID    -> return 200 immediately (benchmark retry-safe)
        STATUS_FAILED  -> return 400 immediately (do not re-execute)
        STATUS_STARTED -> crash recovery: treat as failed, return 400
                          (order was in-flight when a container died)
    """
    order_entry: OrderValue = get_order_from_db(order_id)
    # get the quantity per item

    # --- Idempotency guards ---
    if order_entry.status == STATUS_PAID:
        return Response("Checkout successful", status=200)
    if order_entry.status in (STATUS_FAILED, STATUS_STARTED):
        abort(400, f"Order {order_id} is in terminal/in-progress state: {order_entry.status}")

    # --- Mark in-flight before any side effects (crash-safe marker) ---
    order_entry.status = STATUS_STARTED
    try:
        with db.pipeline(transaction=True) as pipe:
            pipe.set(order_id, msgpack.encode(order_entry))
            pipe.sadd(SAGA_PENDING_KEY, order_id)
            pipe.execute()
    except redis.exceptions.RedisError:
        abort(500, DB_ERROR_STR)

    # items are already aggregated in a dictionary
    items_quantities = order_entry.items
    
    # FORWARD step 1: Reserve Stock (Batch)
    # We send a single event with all items.
    stock_payload = {'items': json.dumps(items_quantities), 'transaction_id': order_id}
    stock_reply = send_rpc('events:stock', 'subtract_stock_batch', stock_payload)
    
    if stock_reply is None or stock_reply.status_code != 200:
        # Atomic batch failure (Network or Out-of-stock):
        # Nothing was subtracted, so no rollback needed for stock.
        order_entry.status = STATUS_FAILED
        try:
            with db.pipeline(transaction=True) as pipe:
                pipe.set(order_id, msgpack.encode(order_entry))
                pipe.srem(SAGA_PENDING_KEY, order_id)
                pipe.execute()
        except redis.exceptions.RedisError:
            app.logger.error(f"Failed to save FAILED status for saga {order_id}")
        abort(400, f"Batch stock subtraction failed: {stock_reply.text if stock_reply else REQ_ERROR_STR}")

    # FORWARD step 2: Deduct Payment
    user_reply = send_rpc('events:payment', 'pay', {'user_id': order_entry.user_id, 'amount': order_entry.total_cost, 'transaction_id': order_id})
    if user_reply is None or user_reply.status_code != 200:
        # Network failure (None) or insufficient credit (non-200): compensate then abort.
        rollback_stock(items_quantities, order_id)
        order_entry.status = STATUS_FAILED
        try:
            with db.pipeline(transaction=True) as pipe:
                pipe.set(order_id, msgpack.encode(order_entry))
                pipe.srem(SAGA_PENDING_KEY, order_id)
                pipe.execute()
        except redis.exceptions.RedisError:
            app.logger.error(f"Failed to save FAILED status for saga {order_id}")
        abort(400, "User out of credit" if user_reply else REQ_ERROR_STR)

    # Finalize: mark paid in DB
    order_entry.paid = True
    order_entry.status = STATUS_PAID
    try:
        with db.pipeline(transaction=True) as pipe:
            pipe.set(order_id, msgpack.encode(order_entry))
            pipe.srem(SAGA_PENDING_KEY, order_id)
            pipe.execute()
    except redis.exceptions.RedisError as e:
        app.logger.error(f"Failed to persist PAID status for order {order_id}: {e}")
        # Critical failure: The system thinks the order is not paid, but services might have been charged.
        # We must trigger compensation for both Stock and Payment.
        rollback_stock(items_quantities, order_id)
        _recovery_rpc('events:payment', 'add_credit', {'user_id': order_entry.user_id, 'amount': order_entry.total_cost, 'transaction_id': order_id})
        order_entry.status = STATUS_FAILED
        try:
            with db.pipeline(transaction=True) as pipe:
                pipe.set(order_id, msgpack.encode(order_entry))
                pipe.srem(SAGA_PENDING_KEY, order_id)
                pipe.execute()
        except redis.exceptions.RedisError:
            app.logger.error(f"Failed to save FAILED status for saga {order_id}")
            pass
        abort(500, f"Transaction failed at commit: {DB_ERROR_STR}")

    app.logger.debug(f"Checkout {order_id} successful")
    return Response("Checkout successful", status=200)


# ─── 2PC Coordinator ──────────────────────────────────────────────────────────
#
# Protocol overview:
#   Phase 1 PREPARE:  coordinator asks each participant to soft-lock resources.
#                     If any vote NO → send ABORT to all that voted YES.
#   Phase 2 COMMIT:   coordinator durably logs COMMIT (STATUS_PAID in its Redis)
#                     *before* sending COMMIT to participants.  This means if
#                     the coordinator crashes after the log write, recovery can
#                     always re-drive the commits (participants are idempotent).
#
# Coordinator WAL:  a Redis SET "2pc:pending" of in-flight order_ids.
#   • Added when the transaction starts.
#   • Removed only after all participants have been told COMMIT or ABORT.
#   • On startup, recover_2pc() scans this set and resolves any leftovers.
#
# The order_id is threaded through every participant call so participants can:
#   • Record their own WAL entry keyed on (order_id, resource_id).
#   • Return 200 idempotently when the same call arrives twice (retries / recovery).

def checkout_2pc(order_id: str):
    # Use WATCH so that only one of multiple concurrent workers (gunicorn) can
    # claim a STATUS_PENDING order.  Without WATCH, two workers could both read
    # STATUS_PENDING, both pass the guards, and both act as coordinator for the
    # same transaction.  With WATCH, the second worker's MULTI/EXEC will get a
    # WatchError because the first worker already updated the key, and the
    # second will re-read the now-STATUS_STARTED order and return 400.
    try:
        with db.pipeline() as pipe:
            while True:
                try:
                    pipe.watch(order_id)
                    raw = pipe.get(order_id)   # immediate execution in WATCH mode
                    if not raw:
                        pipe.reset()
                        abort(400, f"Order: {order_id} not found!")
                    order_entry: OrderValue = msgpack.decode(raw, type=OrderValue)

                    if order_entry.status == STATUS_PAID:
                        pipe.reset()
                        return Response("Checkout successful", status=200)
                    if order_entry.status in (STATUS_STARTED, STATUS_FAILED):
                        pipe.reset()
                        abort(400, f"Order {order_id} is in terminal/in-progress state: {order_entry.status}")

                    # Atomically claim the order and add it to the coordinator WAL.
                    # If another worker modified the key since WATCH, WatchError is
                    # raised and we loop back to re-read the updated status.
                    order_entry.status = STATUS_STARTED
                    pipe.multi()
                    pipe.set(order_id, msgpack.encode(order_entry))
                    pipe.sadd(COORD_PENDING_KEY, order_id)
                    pipe.execute()
                    break   # successfully claimed
                except redis.exceptions.WatchError:
                    continue  # another worker changed the key; re-read and retry
    except redis.exceptions.RedisError:
        abort(500, DB_ERROR_STR)

    items_quantities = order_entry.items

    # ── Phase 1: PREPARE ──────────────────────────────────────────────────────
    # Ask every participant to lock resources and vote yes/no.
    # If any vote NO, abort everything that already prepared.

    # 1. Prepare Stock (Batch)
    stock_payload = {'order_id': order_id, 'items': json.dumps(items_quantities)}
    reply = send_rpc('events:stock', 'prepare_subtract_batch', stock_payload)
    if reply is None or reply.status_code != 200:
        _abort_2pc(order_id, order_entry, items_quantities)
        abort(400, f"2PC prepare failed: {reply.text if reply else REQ_ERROR_STR}")

    # 2. Prepare Payment
    reply = send_rpc('events:payment', 'prepare_pay', {'order_id': order_id, 'user_id': order_entry.user_id, 'amount': order_entry.total_cost})
    if reply is None or reply.status_code != 200:
        _abort_2pc(order_id, order_entry, items_quantities)
        abort(400, "2PC prepare failed: user out of credit" if reply else REQ_ERROR_STR)

    # ── Phase 2: COMMIT ───────────────────────────────────────────────────────
    # Durably log the COMMIT decision *first*.  Once STATUS_PAID is persisted,
    # we are past the point of no return: recovery will re-drive commits even
    # if we crash before reaching all participants.

    order_entry.status = STATUS_PAID
    order_entry.paid   = True
    try:
        db.set(order_id, msgpack.encode(order_entry))
    except redis.exceptions.RedisError:
        # Failed to write commit decision — still safe to abort because no
        # participant has committed yet.
        _abort_2pc(order_id, order_entry, items_quantities)
        abort(500, DB_ERROR_STR)

    # Send COMMIT to all participants (idempotent on their end).
    # Use best-effort sends: STATUS_PAID is already durable, so the client
    # always gets 200 regardless of delivery.  If a send fails, the order stays
    # in the coordinator WAL and recover_2pc() re-drives commits on next startup.
    all_committed = True
    
    # Commit Stock (Batch)
    if not _recovery_rpc('events:stock', 'commit_subtract_batch', stock_payload):
        all_committed = False

    if not _recovery_rpc('events:payment', 'commit_pay', {'order_id': order_id, 'user_id': order_entry.user_id, 'amount': order_entry.total_cost}):
        all_committed = False

    if all_committed:
        try:
            db.srem(COORD_PENDING_KEY, order_id)
        except redis.exceptions.RedisError:
            # Non-fatal: recovery will find STATUS_PAID and re-drive commits
            # (idempotent), then clean up the WAL entry.
            app.logger.warning(f"Could not remove order {order_id} from coordinator WAL")

    app.logger.debug(f"2PC checkout {order_id} committed successfully")
    return Response("Checkout successful", status=200)


def _abort_2pc(
    order_id: str,
    order_entry: OrderValue,
    items_dict: dict[str, int],
):
    """
    Send ABORT to every participant that voted YES, then mark the order FAILED.

    Uses best-effort sends (_recovery_post) so a network error never raises an
    HTTPException that would skip persisting STATUS_FAILED or prevent the caller
    from returning the correct error to the client.

    WAL removal strategy: only remove from coordinator WAL when every abort was
    delivered successfully.  If any send fails, the order stays in the WAL so
    recover_2pc() can re-drive aborts on the next startup.
    """
    order_entry.status = STATUS_FAILED
    try:
        db.set(order_id, msgpack.encode(order_entry))
    except redis.exceptions.RedisError:
        app.logger.error(f"Failed to persist FAILED status for order {order_id}")

    all_aborted = True
    payload = {'order_id': order_id, 'items': json.dumps(items_dict)}
    if not _recovery_rpc('events:stock', 'abort_subtract_batch', payload):
        all_aborted = False

    if not _recovery_rpc('events:payment', 'abort_pay', {'order_id': order_id, 'user_id': order_entry.user_id, 'amount': order_entry.total_cost}):
        all_aborted = False

    if all_aborted:
        try:
            db.srem(COORD_PENDING_KEY, order_id)
        except redis.exceptions.RedisError:
            app.logger.warning(f"Could not remove order {order_id} from coordinator WAL")

def _recovery_rpc(stream: str, action: str, data: dict) -> bool:
    """
    Best-effort RPC. Returns True only when a 200 response is received.
    """
    resp = send_rpc(stream, action, data)
    if resp and resp.status_code == 200:
        return True
    return False

# ─── Coordinator Recovery ─────────────────────────────────────────────────────
#
# Called once at module load time (i.e. once per gunicorn worker process).
# Scans the coordinator WAL for any orders that were in-flight when a previous
# coordinator instance crashed, then resolves them deterministically:
#
#   STATUS_PAID    → commit decision was already logged; re-drive all COMMITs.
#   anything else  → coordinator crashed during PREPARE; safe to ABORT everyone.
#
# Because participant endpoints are idempotent, it is safe for multiple workers
# to run recovery concurrently for the same order.
def recover_2pc():
    try:
        pending = db.smembers(COORD_PENDING_KEY)
    except redis.exceptions.RedisError as exc:
        app.logger.error(f"Recovery: cannot read coordinator WAL: {exc}")
        return

    if not pending:
        return

    app.logger.info(f"Recovery: found {len(pending)} in-flight 2PC transaction(s)")

    for order_id_bytes in pending:
        order_id = order_id_bytes.decode()
        try:
            raw = db.get(order_id)
            if not raw:
                app.logger.warning(f"Recovery: order {order_id} not found in DB — removing from WAL")
                db.srem(COORD_PENDING_KEY, order_id)
                continue
            order = msgpack.decode(raw, type=OrderValue)
        except Exception as exc:
            app.logger.error(f"Recovery: failed to read order {order_id}: {exc}")
            continue

        items_quantities = order.items

        if order.status == STATUS_PAID:
            # Commit decision was durably logged; re-drive COMMITs to all participants.
            app.logger.info(f"Recovery: re-driving COMMIT for order {order_id}")
            all_committed = True
            if not _recovery_rpc('events:stock', 'commit_subtract_batch', {'order_id': order_id, 'items': json.dumps(items_quantities)}):
                all_committed = False
            if not _recovery_rpc('events:payment', 'commit_pay', {'order_id': order_id, 'user_id': order.user_id, 'amount': order.total_cost}):
                all_committed = False

            if all_committed:
                try:
                    db.srem(COORD_PENDING_KEY, order_id)
                except redis.exceptions.RedisError:
                    app.logger.warning(f"Recovery: could not remove {order_id} from WAL")
            # else: leave in WAL; next recovery pass will retry commits.

        else:
            # STATUS_STARTED (or any unexpected state): coordinator crashed
            # before writing the commit decision.  Abort all participants.
            # Abort is safe even for participants that never received PREPARE.
            app.logger.info(f"Recovery: re-driving ABORT for order {order_id} (status={order.status})")
            all_aborted = True
            if not _recovery_rpc('events:stock', 'abort_subtract_batch', {'order_id': order_id, 'items': json.dumps(items_quantities)}):
                all_aborted = False
            if not _recovery_rpc('events:payment', 'abort_pay', {'order_id': order_id, 'user_id': order.user_id, 'amount': order.total_cost}):
                all_aborted = False

            if all_aborted:
                order.status = STATUS_FAILED
                try:
                    with db.pipeline(transaction=True) as pipe:
                        pipe.set(order_id, msgpack.encode(order))
                        pipe.srem(COORD_PENDING_KEY, order_id)
                        pipe.execute()
                except redis.exceptions.RedisError:
                    app.logger.error(f"Recovery: failed to persist FAILED for order {order_id}")

    app.logger.info("Recovery: complete")


def recover_saga():
    """
    Scans the Saga WAL for orders stuck in STATUS_STARTED (implies coordinator crash).
    Using checkpoints, we can determine which compensation logic to run.
    they are not stuck indefinitely.
    """
    try:
        pending = db.smembers(SAGA_PENDING_KEY)
    except redis.exceptions.RedisError as exc:
        app.logger.error(f"Saga Recovery: cannot read WAL: {exc}")
        return

    if not pending:
        return

    app.logger.info(f"Saga Recovery: found {len(pending)} in-flight Saga(s)")

    for order_id_bytes in pending:
        order_id = order_id_bytes.decode()
        try:
            raw = db.get(order_id)
            if not raw:
                db.srem(SAGA_PENDING_KEY, order_id)
                continue

            order = msgpack.decode(raw, type=OrderValue)

            # We trigger a blind rollback.
            # The Stock service's tombstone logic handles whether to act or ignore.
            if order.status == STATUS_STARTED:
                app.logger.warning(f"Saga Recovery: Order {order_id} found in {order.status}. Blindly firing rollback.")
                # Re-calculate items dict to pass to rollback
                items_quantities = order.items

                # Fire-and-forget ROLLBACK to Stock Service
                # We do not use send_rpc because we don't want to wait for a reply.
                mq.xadd('events:stock', {
                    'type': 'add_stock_batch',
                    'items': json.dumps(items_quantities),
                    'transaction_id': order_id
                })

                # Fire-and-forget ROLLBACK to Payment Service
                # Relies on Payment Service Tombstone to handle if charge never happened.
                mq.xadd('events:payment', {
                    'type': 'add_credit',
                    'user_id': order.user_id,
                    'amount': str(order.total_cost),
                    'transaction_id': order_id
                })
                
                order.status = STATUS_FAILED
                db.set(order_id, msgpack.encode(order))

            db.srem(SAGA_PENDING_KEY, order_id)

        except Exception as e:
            app.logger.error(f"Saga Recovery: error processing {order_id}: {e}")

def _run_recovery():
    """Run recovery; never raises so a startup failure cannot kill the worker."""

    # Attempt to set a key. NX=True means it only succeeds if the key DOES NOT exist.
    # EX=30 means the lock automatically deletes itself after 30 seconds to prevent deadlocks.
    lock_acquired = db.set("order_startup_lock", "locked", nx=True, ex=30)

    if lock_acquired:
        app.logger.info(f"Worker {os.getpid()} acquired the startup lock. Executing WAL recovery...")
        try:
            recover_2pc()
            recover_saga()
        except Exception as exc:
            app.logger.error(f"Recovery failed at startup: {exc}")
        finally:
            db.delete("order_startup_lock")
    else:
        app.logger.info(f"Worker {os.getpid()} skipping recovery (another worker is handling it).")

if __name__ == '__main__':
    _run_recovery()
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    # Configure logging first so recover_2pc() log output goes to gunicorn.
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
    _run_recovery()

import logging
import os
import atexit
import random
import uuid
from collections import defaultdict

import redis
import requests

from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response

DB_ERROR_STR = "DB error"
REQ_ERROR_STR = "Requests error"

GATEWAY_URL = os.environ['GATEWAY_URL']

# CHECKOUT_MODE: "saga" (default) or "2pc"
CHECKOUT_MODE = os.environ.get('CHECKOUT_MODE', 'saga').lower()

app = Flask("order-service")

_REDIS_PASSWORD    = os.environ.get('REDIS_PASSWORD', '')
_REDIS_DB          = int(os.environ.get('REDIS_DB', '0'))
_SENTINEL_HOSTS    = os.environ.get('REDIS_SENTINEL_HOSTS', '')
_REDIS_MASTER_NAME = os.environ.get('REDIS_MASTER_NAME', 'mymaster')

if _SENTINEL_HOSTS:
    from redis.sentinel import Sentinel as _Sentinel
    _peers = [(h.split(':')[0], int(h.split(':')[1])) for h in _SENTINEL_HOSTS.split(',')]
    db: redis.Redis = _Sentinel(
        _peers,
        password=_REDIS_PASSWORD,
        db=_REDIS_DB,
    ).master_for(_REDIS_MASTER_NAME, socket_timeout=0.5)
else:
    db: redis.Redis = redis.Redis(
        host=os.environ['REDIS_HOST'],
        port=int(os.environ['REDIS_PORT']),
        password=_REDIS_PASSWORD,
        db=_REDIS_DB,
    )


def close_db_connection():
    db.close()


atexit.register(close_db_connection)

# Order status values — shared by Saga and 2PC
STATUS_PENDING = "pending"   # created, not yet checked out
STATUS_STARTED = "started"   # checkout in progress (crash-safe marker)
STATUS_PAID    = "paid"      # completed successfully
STATUS_FAILED  = "failed"    # rolled back

# Redis key for the coordinator WAL: a set of order_ids currently in-flight.
# On startup, unresolved entries are used to recover hanging transactions.
COORD_PENDING_KEY = "2pc:pending"


class OrderValue(Struct, kw_only=True):
    paid: bool
    items: list[tuple[str, int]]
    user_id: str
    total_cost: int
    status: str = STATUS_PENDING


# DB helpers
def get_order_from_db(order_id: str) -> OrderValue | None:
    try:
        # get serialized data
        entry: bytes = db.get(order_id)
    except redis.exceptions.RedisError:
        abort(400, DB_ERROR_STR)
    # deserialize data if it exists else return null
    entry: OrderValue | None = msgpack.decode(entry, type=OrderValue) if entry else None
    if entry is None:
        app.logger.warning("Order entry not found: %s", order_id)
        # if order does not exist in the database; abort
        abort(400, f"Order: {order_id} not found!")
    return entry

def save_order(order_id: str, order: OrderValue):
    try:
        db.set(order_id, msgpack.encode(order))
    except redis.exceptions.RedisError:
        app.logger.error("Failed to save order: %s", order_id)
        abort(400, DB_ERROR_STR)


# Order endpoints
@app.post('/create/<user_id>')
def create_order(user_id: str):
    key = str(uuid.uuid4())
    value = msgpack.encode(OrderValue(paid=False, items=[], user_id=user_id, total_cost=0, status=STATUS_PENDING))
    try:
        db.set(key, value)
    except redis.exceptions.RedisError:
        app.logger.error("Failed to save order: %s", key)
        return abort(400, DB_ERROR_STR)
    return jsonify({'order_id': key})


@app.post('/batch_init/<n>/<n_items>/<n_users>/<item_price>')
def batch_init_users(n: int, n_items: int, n_users: int, item_price: int):

    n = int(n)
    n_items = int(n_items)
    n_users = int(n_users)
    item_price = int(item_price)

    def generate_entry() -> OrderValue:
        user_id = random.randint(0, n_users - 1)
        item1_id = random.randint(0, n_items - 1)
        item2_id = random.randint(0, n_items - 1)
        return OrderValue(paid=False,
                          items=[(f"{item1_id}", 1), (f"{item2_id}", 1)],
                          user_id=f"{user_id}",
                          total_cost=2 * item_price,
                          status=STATUS_PENDING)

    kv_pairs: dict[str, bytes] = {f"{i}": msgpack.encode(generate_entry())
                                  for i in range(n)}
    try:
        db.mset(kv_pairs)
    except redis.exceptions.RedisError:
        app.logger.error("Failed to save order: %s", kv_pairs)
        return abort(400, DB_ERROR_STR)
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

# HTTP helpers with retry
def send_post_request(url: str, retries: int = 3) -> requests.Response | None:
    for attempt in range(retries):
        try:
            response = requests.post(url, timeout=5)
            return response
        except requests.exceptions.RequestException as exc:
            app.logger.warning(f"POST {url} attempt {attempt + 1}/{retries} failed: {exc}")
    return None

def send_get_request(url: str, retries: int = 3) -> requests.Response | None:
    for attempt in range(retries):
        try:
            response = requests.get(url, timeout=5)
            return response
        except requests.exceptions.RequestException as exc:
            app.logger.warning(f"GET {url} attempt {attempt + 1}/{retries} failed: {exc}")
    return None


@app.post('/addItem/<order_id>/<item_id>/<quantity>')
def add_item(order_id: str, item_id: str, quantity: int):
    # Fetch item price first (outside the Redis transaction — read-only, no lock needed).
    item_reply = send_get_request(f"{GATEWAY_URL}/stock/find/{item_id}")
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
                order_entry.items.append((item_id, int(quantity)))
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
            abort(400, DB_ERROR_STR)
    abort(400, "Conflict: could not add item after retries")


def rollback_stock(removed_items: list[tuple[str, int]]):
    # Best-effort: use _recovery_post so a network hiccup during compensation
    # does not raise an HTTPException that would prevent STATUS_FAILED from
    # being persisted by the caller.
    for item_id, quantity in removed_items:
        _recovery_post(f"{GATEWAY_URL}/stock/add/{item_id}/{quantity}")


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
    save_order(order_id, order_entry)

    # Aggregate quantities across duplicate item entries
    items_quantities: dict[str, int] = defaultdict(int)
    for item_id, quantity in order_entry.items:
        items_quantities[item_id] += quantity

    # FORWARD step 1: Reserve Stock
    removed_items: list[tuple[str, int]] = []
    for item_id, quantity in items_quantities.items():
        stock_reply = send_post_request(f"{GATEWAY_URL}/stock/subtract/{item_id}/{quantity}")
        if stock_reply is None or stock_reply.status_code != 200:
            # Network failure (None) or out-of-stock (non-200): compensate then abort.
            rollback_stock(removed_items)
            order_entry.status = STATUS_FAILED
            save_order(order_id, order_entry)
            abort(400, f"Out of stock on item_id: {item_id}" if stock_reply else REQ_ERROR_STR)
        removed_items.append((item_id, quantity))

    # FORWARD step 2: Deduct Payment
    user_reply = send_post_request(f"{GATEWAY_URL}/payment/pay/{order_entry.user_id}/{order_entry.total_cost}")
    if user_reply is None or user_reply.status_code != 200:
        # Network failure (None) or insufficient credit (non-200): compensate then abort.
        rollback_stock(removed_items)
        order_entry.status = STATUS_FAILED
        save_order(order_id, order_entry)
        abort(400, "User out of credit" if user_reply else REQ_ERROR_STR)

    # Finalize: mark paid in DB
    order_entry.paid = True
    order_entry.status = STATUS_PAID
    try:
        db.set(order_id, msgpack.encode(order_entry))
    except redis.exceptions.RedisError:
        # Backward: Refund Payment then Release Stock.
        # Use _recovery_post so a network error here doesn't abort() and skip
        # the STATUS_FAILED write below.
        _recovery_post(f"{GATEWAY_URL}/payment/add_funds/{order_entry.user_id}/{order_entry.total_cost}")
        rollback_stock(removed_items)
        order_entry.paid = False
        order_entry.status = STATUS_FAILED
        try:
            db.set(order_id, msgpack.encode(order_entry))
        except redis.exceptions.RedisError:
            pass  # best-effort; status was set in memory
        abort(400, DB_ERROR_STR)

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
                    if order_entry.status in (STATUS_FAILED, STATUS_STARTED):
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
        abort(400, DB_ERROR_STR)

    items_quantities: dict[str, int] = defaultdict(int)
    for item_id, quantity in order_entry.items:
        items_quantities[item_id] += quantity

    # ── Phase 1: PREPARE ──────────────────────────────────────────────────────
    # Ask every participant to lock resources and vote yes/no.
    # If any vote NO, abort everything that already prepared.

    prepared_stock: list[tuple[str, int]] = []
    payment_prepared: bool = False

    for item_id, quantity in items_quantities.items():
        reply = send_post_request(
            f"{GATEWAY_URL}/stock/prepare_subtract/{order_id}/{item_id}/{quantity}"
        )
        if reply is None or reply.status_code != 200:
            _abort_2pc(order_id, order_entry, prepared_stock, payment_prepared)
            abort(400, f"2PC prepare failed: out of stock on item {item_id}" if reply else REQ_ERROR_STR)
        prepared_stock.append((item_id, quantity))

    reply = send_post_request(
        f"{GATEWAY_URL}/payment/prepare_pay/{order_id}/{order_entry.user_id}/{order_entry.total_cost}"
    )
    if reply is None or reply.status_code != 200:
        _abort_2pc(order_id, order_entry, prepared_stock, payment_prepared)
        abort(400, "2PC prepare failed: user out of credit" if reply else REQ_ERROR_STR)

    payment_prepared = True

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
        _abort_2pc(order_id, order_entry, prepared_stock, payment_prepared)
        abort(400, DB_ERROR_STR)

    # Send COMMIT to all participants (idempotent on their end).
    # Use best-effort sends: STATUS_PAID is already durable, so the client
    # always gets 200 regardless of delivery.  If a send fails, the order stays
    # in the coordinator WAL and recover_2pc() re-drives commits on next startup.
    all_committed = True
    for item_id, quantity in prepared_stock:
        if not _recovery_post(f"{GATEWAY_URL}/stock/commit_subtract/{order_id}/{item_id}/{quantity}"):
            all_committed = False

    if not _recovery_post(
        f"{GATEWAY_URL}/payment/commit_pay/{order_id}/{order_entry.user_id}/{order_entry.total_cost}"
    ):
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
    prepared_stock: list[tuple[str, int]],
    payment_prepared: bool,
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
    all_aborted = True
    for item_id, quantity in prepared_stock:
        if not _recovery_post(
            f"{GATEWAY_URL}/stock/abort_subtract/{order_id}/{item_id}/{quantity}"
        ):
            all_aborted = False

    if payment_prepared:
        if not _recovery_post(
            f"{GATEWAY_URL}/payment/abort_pay/{order_id}/{order_entry.user_id}/{order_entry.total_cost}"
        ):
            all_aborted = False

    order_entry.status = STATUS_FAILED
    try:
        db.set(order_id, msgpack.encode(order_entry))
    except redis.exceptions.RedisError:
        app.logger.error(f"Failed to persist FAILED status for order {order_id}")

    if all_aborted:
        try:
            db.srem(COORD_PENDING_KEY, order_id)
        except redis.exceptions.RedisError:
            app.logger.warning(f"Could not remove order {order_id} from coordinator WAL")


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

def _recovery_post(url: str) -> bool:
    """
    Best-effort POST with retries.  Returns True only when a 200 response is
    received.  Never raises — callers use the return value to decide whether
    to keep the order in the coordinator WAL for the next recovery pass.
    """
    for _ in range(3):
        try:
            resp = requests.post(url, timeout=5)
            return resp.status_code == 200
        except Exception as exc:
            app.logger.warning(f"POST failed {url}: {exc}")
    return False


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

        items_quantities: dict[str, int] = defaultdict(int)
        for item_id, quantity in order.items:
            items_quantities[item_id] += quantity

        if order.status == STATUS_PAID:
            # Commit decision was durably logged; re-drive COMMITs to all participants.
            app.logger.info(f"Recovery: re-driving COMMIT for order {order_id}")
            all_committed = True
            for item_id, quantity in items_quantities.items():
                if not _recovery_post(
                    f"{GATEWAY_URL}/stock/commit_subtract/{order_id}/{item_id}/{quantity}"
                ):
                    all_committed = False
            if not _recovery_post(
                f"{GATEWAY_URL}/payment/commit_pay/{order_id}/{order.user_id}/{order.total_cost}"
            ):
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
            for item_id, quantity in items_quantities.items():
                if not _recovery_post(
                    f"{GATEWAY_URL}/stock/abort_subtract/{order_id}/{item_id}/{quantity}"
                ):
                    all_aborted = False
            if not _recovery_post(
                f"{GATEWAY_URL}/payment/abort_pay/{order_id}/{order.user_id}/{order.total_cost}"
            ):
                all_aborted = False

            order.status = STATUS_FAILED
            status_saved = False
            try:
                db.set(order_id, msgpack.encode(order))
                status_saved = True
            except redis.exceptions.RedisError:
                app.logger.error(f"Recovery: failed to persist FAILED for order {order_id}")

            # Only remove from WAL when both the status write and all abort
            # sends succeeded.  Otherwise leave it for the next recovery pass.
            if all_aborted and status_saved:
                try:
                    db.srem(COORD_PENDING_KEY, order_id)
                except redis.exceptions.RedisError:
                    app.logger.warning(f"Recovery: could not remove {order_id} from WAL")

    app.logger.info("Recovery: complete")


def _run_recovery():
    """Run 2PC recovery; never raises so a startup failure cannot kill the worker."""
    try:
        recover_2pc()
    except Exception as exc:
        app.logger.error(f"Recovery failed at startup: {exc}")


if __name__ == '__main__':
    _run_recovery()
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    # Configure logging first so recover_2pc() log output goes to gunicorn.
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
    _run_recovery()

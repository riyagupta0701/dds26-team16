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

db: redis.Redis = redis.Redis(host=os.environ['REDIS_HOST'],
                              port=int(os.environ['REDIS_PORT']),
                              password=os.environ['REDIS_PASSWORD'],
                              db=int(os.environ['REDIS_DB']))


def close_db_connection():
    db.close()


atexit.register(close_db_connection)

# Order status values — shared by Saga and 2PC
STATUS_PENDING = "pending"   # created, not yet checked out
STATUS_STARTED = "started"   # checkout in progress (crash-safe marker)
STATUS_PAID    = "paid"      # completed successfully
STATUS_FAILED  = "failed"    # rolled back

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
        # if order does not exist in the database; abort
        abort(400, f"Order: {order_id} not found!")
    return entry

def save_order(order_id: str, order: OrderValue):
    try:
        db.set(order_id, msgpack.encode(order))
    except redis.exceptions.RedisError:
        abort(400, DB_ERROR_STR)


# Order endpoints
@app.post('/create/<user_id>')
def create_order(user_id: str):
    key = str(uuid.uuid4())
    value = msgpack.encode(OrderValue(paid=False, items=[], user_id=user_id, total_cost=0, status=STATUS_PENDING))
    try:
        db.set(key, value)
    except redis.exceptions.RedisError:
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
def send_post_request(url: str, retries: int = 3) -> requests.Response:
    for attempt in range(retries):
        try:
            response = requests.post(url, timeout=5)
            return response
        except requests.exceptions.RequestException:
            if attempt == retries - 1:
                abort(400, REQ_ERROR_STR)


def send_get_request(url: str, retries: int = 3) -> requests.Response:
    for attempt in range(retries):
        try:
            response = requests.get(url, timeout=5)
            return response
        except requests.exceptions.RequestException:
            if attempt == retries - 1:
                abort(400, REQ_ERROR_STR)


@app.post('/addItem/<order_id>/<item_id>/<quantity>')
def add_item(order_id: str, item_id: str, quantity: int):
    order_entry: OrderValue = get_order_from_db(order_id)
    item_reply = send_get_request(f"{GATEWAY_URL}/stock/find/{item_id}")
    if item_reply.status_code != 200:
        # Request failed because item does not exist
        abort(400, f"Item: {item_id} does not exist!")
    item_json: dict = item_reply.json()
    order_entry.items.append((item_id, int(quantity)))
    order_entry.total_cost += int(quantity) * item_json["price"]
    save_order(order_id, order_entry)
    return Response(f"Item: {item_id} added to: {order_id} price updated to: {order_entry.total_cost}",
                    status=200)


def rollback_stock(removed_items: list[tuple[str, int]]):
    for item_id, quantity in removed_items:
        send_post_request(f"{GATEWAY_URL}/stock/add/{item_id}/{quantity}")


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
        if stock_reply.status_code != 200:
            # If one item does not have enough stock we need to rollback
            rollback_stock(removed_items)
            order_entry.status = STATUS_FAILED
            save_order(order_id, order_entry)
            abort(400, f"Out of stock on item_id: {item_id}")
        removed_items.append((item_id, quantity))

    # FORWARD step 2: Deduct Payment
    user_reply = send_post_request(f"{GATEWAY_URL}/payment/pay/{order_entry.user_id}/{order_entry.total_cost}")
    if user_reply.status_code != 200:
        # Backward: Release all reserved stock
        rollback_stock(removed_items)
        order_entry.status = STATUS_FAILED
        save_order(order_id, order_entry)
        abort(400, "User out of credit")

    # Finalize: mark paid in DB
    order_entry.paid = True
    order_entry.status = STATUS_PAID
    try:
        db.set(order_id, msgpack.encode(order_entry))
    except redis.exceptions.RedisError:
        # Backward: Refund Payment then Release Stock
        send_post_request(f"{GATEWAY_URL}/payment/add_funds/{order_entry.user_id}/{order_entry.total_cost}")
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


def checkout_2pc(order_id: str):
    order_entry: OrderValue = get_order_from_db(order_id)

    if order_entry.status == STATUS_PAID:
        return Response("Checkout successful", status=200)
    if order_entry.status in (STATUS_FAILED, STATUS_STARTED):
        abort(400, f"Order {order_id} is in terminal/in-progress state: {order_entry.status}")

    # Mark as in-progress (coordinator log — survives coordinator crash)
    order_entry.status = STATUS_STARTED
    try:
        db.set(order_id, msgpack.encode(order_entry))
    except redis.exceptions.RedisError:
        abort(400, DB_ERROR_STR)

    items_quantities: dict[str, int] = defaultdict(int)
    for item_id, quantity in order_entry.items:
        items_quantities[item_id] += quantity

    # ── Phase 1: PREPARE ──────────────────────────────────────────────────────
    # Ask every participant to lock resources and vote yes/no.
    # If any votes NO, we abort everything.

    prepared_stock: list[tuple[str, int]] = []
    prepared_payment: bool = False

    # Prepare stock for each item
    for item_id, quantity in items_quantities.items():
        reply = send_post_request(
            # Todo: Need to element /prepare_subtract
            f"{GATEWAY_URL}/stock/prepare_subtract/{item_id}/{quantity}"
        )
        if reply.status_code != 200:
            # Participant voted NO — abort all that already prepared
            _abort_stock(prepared_stock)
            order_entry.status = STATUS_FAILED
            db.set(order_id, msgpack.encode(order_entry))
            abort(400, f"2PC prepare failed: out of stock on item {item_id}")
        prepared_stock.append((item_id, quantity))

    # Prepare payment
    reply = send_post_request(
        # Todo: Implement /prepare_pay
        f"{GATEWAY_URL}/payment/prepare_pay/{order_entry.user_id}/{order_entry.total_cost}"
    )
    if reply.status_code != 200:
        # Payment voted NO — abort all stock preparations
        _abort_stock(prepared_stock)
        order_entry.status = STATUS_FAILED
        db.set(order_id, msgpack.encode(order_entry))
        abort(400, "2PC prepare failed: user out of credit")

    prepared_payment = True

    # ── Phase 2: COMMIT ───────────────────────────────────────────────────────
    # All participants voted YES. Durably log the COMMIT decision first,
    # then send commit to each participant.
    # Even if we crash here, recovery can re-send commits (idempotent).

    order_entry.status = STATUS_PAID
    order_entry.paid = True
    try:
        db.set(order_id, msgpack.encode(order_entry))
    except redis.exceptions.RedisError:
        # Failed to write commit decision — safe to abort since no participant
        # has committed yet.
        _abort_stock(prepared_stock)
        if prepared_payment:
            send_post_request(
                # Todo: Is this not the same as add?
                f"{GATEWAY_URL}/payment/abort_pay/{order_entry.user_id}/{order_entry.total_cost}"
            )
        order_entry.status = STATUS_FAILED
        db.set(order_id, msgpack.encode(order_entry))
        abort(400, DB_ERROR_STR)

    # Send commit to all participants (best-effort; they must be idempotent)
    for item_id, quantity in prepared_stock:
        # Todo: Batch op?
        send_post_request(f"{GATEWAY_URL}/stock/commit_subtract/{item_id}/{quantity}")

    send_post_request(
        f"{GATEWAY_URL}/payment/commit_pay/{order_entry.user_id}/{order_entry.total_cost}"
    )

    app.logger.debug(f"2PC checkout {order_id} committed successfully")
    return Response("Checkout successful", status=200)


def _abort_stock(prepared_stock: list[tuple[str, int]]):
    """Send abort to all stock participants that successfully prepared."""
    for item_id, quantity in prepared_stock:
        # Todo: Implement this. Can it be a batch op instead?
        send_post_request(f"{GATEWAY_URL}/stock/abort_subtract/{item_id}/{quantity}")

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)

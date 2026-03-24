import atexit
import logging
import os
import random
import threading
import time
import uuid
from collections import defaultdict

import redis
from flask import Flask, Response, abort, jsonify
from msgspec import msgpack

from db import (db, mq, OrderValue, get_order,
                STATUS_PENDING, DB_ERROR_STR)
from wal import wal
from rpc import send_rpc
from saga import checkout_saga, recover_saga
from twopc import checkout_2pc, recover_2pc

app = Flask("order-service")
atexit.register(lambda: (db.close(), mq.close(), wal.close()))

# ── Routes ─────────────────────────────────────────────────────────────────────

@app.post('/create/<user_id>')
def create_order(user_id: str):
    key   = str(uuid.uuid4())
    value = msgpack.encode(OrderValue(paid=False, items={}, user_id=user_id,
                                      total_cost=0, status=STATUS_PENDING))
    try:
        db.set(key, value)
    except redis.exceptions.RedisError:
        abort(500, DB_ERROR_STR)
    return jsonify({'order_id': key})


@app.post('/batch_init/<n>/<n_items>/<n_users>/<item_price>')
def batch_init_orders(n: int, n_items: int, n_users: int, item_price: int):
    n, n_items, n_users, item_price = int(n), int(n_items), int(n_users), int(item_price)

    def random_order() -> OrderValue:
        items = defaultdict(int)
        items[str(random.randint(0, n_items - 1))] += 1
        items[str(random.randint(0, n_items - 1))] += 1
        return OrderValue(paid=False, items=items,
                          user_id=str(random.randint(0, n_users - 1)),
                          total_cost=2 * item_price, status=STATUS_PENDING)

    kv_pairs = {str(i): msgpack.encode(random_order()) for i in range(n)}
    try:
        db.mset(kv_pairs)
    except redis.exceptions.RedisError:
        abort(500, DB_ERROR_STR)
    return jsonify({"msg": "Batch init for orders successful"})


@app.get('/find/<order_id>')
def find_order(order_id: str):
    o = get_order(order_id)
    return jsonify({"order_id": order_id, "paid": o.paid, "items": o.items,
                    "user_id": o.user_id, "total_cost": o.total_cost, "status": o.status})


@app.post('/addItem/<order_id>/<item_id>/<quantity>')
def add_item(order_id: str, item_id: str, quantity: int):
    item_reply = send_rpc('events:stock', 'find_item', {'item_id': item_id})
    if item_reply is None or item_reply.status_code != 200:
        abort(400, f"Item: {item_id} does not exist!")
    price_delta = int(quantity) * item_reply.json()["price"]

    for _ in range(10):
        try:
            with db.pipeline() as pipe:
                pipe.watch(order_id)
                raw = pipe.get(order_id)
                if raw is None:
                    pipe.unwatch()
                    abort(400, f"Order: {order_id} not found!")
                order = msgpack.decode(raw, type=OrderValue)
                order.items[item_id] = order.items.get(item_id, 0) + int(quantity)
                order.total_cost += price_delta
                pipe.multi()
                pipe.set(order_id, msgpack.encode(order))
                pipe.execute()
                return Response(
                    f"Item: {item_id} added to: {order_id} price updated to: {order.total_cost}",
                    status=200)
        except redis.WatchError:
            continue
        except redis.exceptions.RedisError:
            abort(500, DB_ERROR_STR)
    abort(400, "Conflict: could not add item after retries")


@app.post('/checkout/<order_id>')
def checkout(order_id: str):
    try:
        raw_mode = db.get("system:checkout_mode")
        mode = raw_mode.decode() if raw_mode else 'saga'
    except redis.exceptions.RedisError:
        abort(500, DB_ERROR_STR)

    if mode == 'saga':
        return checkout_saga(order_id)
    if mode == '2pc':
        return checkout_2pc(order_id)
    abort(501, "Select a valid checkout mode.")


@app.post('/mode/<new_mode>')
def set_mode(new_mode: str):
    mode = new_mode.lower()
    if mode not in ('saga', '2pc'):
        abort(400, "Invalid mode. Use 'saga' or '2pc'.")
    try:
        db.set("system:checkout_mode", mode)
        return Response(f"Mode set to {mode}", status=200)
    except redis.exceptions.RedisError:
        abort(500, DB_ERROR_STR)


@app.get('/health')
def health_check():
    try:
        db.ping()
        mq.ping()
        return jsonify({"status": "healthy", "db": "connected", "mq": "connected"}), 200
    except redis.exceptions.RedisError as e:
        app.logger.error("Health check failed: %s", e)
        return jsonify({"status": "unhealthy"}), 500


# ── Startup recovery ───────────────────────────────────────────────────────────

def _run_recovery():
    time.sleep(5)
    try:
        recover_2pc()
        recover_saga()
    except Exception as e:
        app.logger.error("Recovery failed at startup: %s", e)


def start_background_recovery():
    app.logger.info("Worker %d starting background recovery", os.getpid())
    threading.Thread(target=_run_recovery, daemon=True).start()


# ── Entry point ────────────────────────────────────────────────────────────────

if __name__ == '__main__':
    start_background_recovery()
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
    logging.getLogger('order-service').handlers = gunicorn_logger.handlers
    logging.getLogger('order-service').setLevel(gunicorn_logger.level)
    start_background_recovery()

import logging
import os
import atexit
import uuid
import threading
import json
import time

import redis

from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response


DB_ERROR_STR = "DB error"

# 2PC WAL state values stored in Redis
WAL_PREPARED  = b"prepared"
WAL_COMMITTED = b"committed"
WAL_ABORTED   = b"aborted"

app = Flask("stock-service")

db: redis.Redis = redis.Redis(host=os.environ['REDIS_HOST'],
                              port=int(os.environ['REDIS_PORT']),
                              password=os.environ['REDIS_PASSWORD'],
                              db=int(os.environ['REDIS_DB']))

# Separate connection for Message Queue operations
# Defaults to the same host as DB, but allows splitting in production
mq: redis.Redis = redis.Redis(host=os.environ['MQ_REDIS_HOST'],
                              port=int(os.environ['MQ_REDIS_PORT']),
                              password=os.environ['MQ_REDIS_PASSWORD'],
                              db=int(os.environ['MQ_REDIS_DB']))

def close_db_connection():
    db.close()
    mq.close()


# Do not register atexit immediately if we plan to use threads that might need the connection
# or handle it gracefully. For now, we keep it but ensure threads are daemon.
atexit.register(close_db_connection)


class StockValue(Struct):
    stock: int
    price: int

# ─── Logic Helpers ────────────────────────────────────────────────────────────

def response_success(body, status_code=200):
    return {"status_code": status_code, "body": body}

def response_error(error_msg, status_code=400):
    return {"status_code": status_code, "error": error_msg}

def get_item_from_db(item_id: str) -> StockValue | None:
    # get serialized data
    entry: bytes = db.get(item_id)
    # deserialize data if it exists else return null
    entry: StockValue | None = msgpack.decode(entry, type=StockValue) if entry else None
    return entry


def create_item_logic(price: int):
    key = str(uuid.uuid4())
    app.logger.debug(f"Item: {key} created")
    value = msgpack.encode(StockValue(stock=0, price=int(price)))
    try:
        db.set(key, value)
    except redis.exceptions.RedisError:
        return response_error(DB_ERROR_STR)
    return response_success({'item_id': key})

def batch_init_logic(n: int, starting_stock: int, item_price: int):
    n = int(n)
    starting_stock = int(starting_stock)
    item_price = int(item_price)
    kv_pairs: dict[str, bytes] = {f"{i}": msgpack.encode(StockValue(stock=starting_stock, price=item_price))
                                  for i in range(n)}
    try:
        db.mset(kv_pairs)
    except redis.exceptions.RedisError:
        return response_error(DB_ERROR_STR)
    return response_success({"msg": "Batch init for stock successful"})

def find_item_logic(item_id: str):
    try:
        item_entry: StockValue = get_item_from_db(item_id)
    except redis.exceptions.RedisError:
        return response_error(DB_ERROR_STR)
    if item_entry is None:
        return response_error(f"Item: {item_id} not found!", 400)
    return response_success({
        "stock": item_entry.stock,
        "price": item_entry.price
    })

def add_stock_logic(item_id: str, amount: int):
    """
    Atomically add stock using optimistic locking.
    """
    amount = int(amount)
    for _ in range(10):
        try:
            with db.pipeline() as pipe:
                pipe.watch(item_id)
                raw = pipe.get(item_id)
                if raw is None:
                    pipe.reset()
                    return response_error(f"Item: {item_id} not found!")
                item_entry = msgpack.decode(raw, type=StockValue)
                pipe.multi()
                item_entry.stock += amount
                pipe.set(item_id, msgpack.encode(item_entry))
                pipe.execute()
                return response_success(f"Item: {item_id} stock updated to: {item_entry.stock}")
        except redis.WatchError:
            continue
        except redis.exceptions.RedisError:
            return response_error(DB_ERROR_STR)
    return response_error("Conflict: could not update stock after retries")

def remove_stock_logic(item_id: str, amount: int):
    """
    Atomically subtract stock using optimistic locking.
    Respects 2PC soft-reservations: only unreserved stock may be subtracted.
    Returns 400 if stock would fall below the currently reserved amount.
    """
    amount = int(amount)
    app.logger.debug(f"Subtracting {amount} from item: {item_id}")
    reserved_key = _stock_reserved_key(item_id)
    for _ in range(10):
        try:
            with db.pipeline() as pipe:
                pipe.watch(item_id, reserved_key)
                raw = pipe.get(item_id)
                if raw is None:
                    pipe.unwatch()
                    return response_error(f"Item: {item_id} not found!")
                item_entry = msgpack.decode(raw, type=StockValue)
                reserved  = int(pipe.get(reserved_key) or 0)
                # Guard: cannot subtract below what is already soft-reserved.
                if item_entry.stock - amount < reserved:
                    pipe.unwatch()
                    return response_error(f"Item: {item_id} stock cannot get reduced below zero!")
                pipe.multi()
                item_entry.stock -= amount
                pipe.set(item_id, msgpack.encode(item_entry))
                pipe.execute()
                return response_success(f"Item: {item_id} stock updated to: {item_entry.stock}")
        except redis.WatchError:
            continue
        except redis.exceptions.RedisError:
            return response_error(DB_ERROR_STR)
    return response_error("Conflict: could not subtract stock after retries")


# ─── 2PC Participant Endpoints ─────────────────────────────────────────────────
#
# Soft-reservations are stored as plain Redis integers under:
#   reserved:stock:{item_id}
#
# Per-transaction WAL entries live under:
#   2pc:stock:{order_id}:{item_id}  → b"prepared" | b"committed" | b"aborted"
#
# All three endpoints use WATCH/MULTI/EXEC for optimistic concurrency control so
# multiple gunicorn workers never corrupt each other's reads and writes.

def _stock_wal_key(order_id: str, item_id: str) -> str:
    return f"2pc:stock:{order_id}:{item_id}"


def _stock_reserved_key(item_id: str) -> str:
    return f"reserved:stock:{item_id}"

def prepare_subtract_logic(order_id: str, item_id: str, amount: int):
    """
    2PC Phase 1 – PREPARE (participant: stock).

    Atomically checks that item_id has enough unreserved stock, then
    soft-reserves `amount` units and records a WAL entry so the decision
    survives participant crashes.

    Idempotent: a duplicate call for the same (order_id, item_id) returns 200.
    """
    amount = int(amount)
    wal_key      = _stock_wal_key(order_id, item_id)
    reserved_key = _stock_reserved_key(item_id)

    try:
        with db.pipeline() as pipe:
            while True:
                try:
                    # WATCH the three keys involved so any concurrent write
                    # causes our EXEC to fail and we retry.
                    pipe.watch(item_id, reserved_key, wal_key)

                    # ── Idempotency check ──────────────────────────────────
                    wal_state = pipe.get(wal_key)
                    if wal_state in (WAL_PREPARED, WAL_COMMITTED):
                        pipe.unwatch()
                        return response_success("Prepare: already done")
                    if wal_state == WAL_ABORTED:
                        pipe.unwatch()
                        return response_error(f"Transaction {order_id} already aborted for item {item_id}")

                    # ── Availability check ─────────────────────────────────
                    raw = pipe.get(item_id)
                    if not raw:
                        pipe.unwatch()
                        return response_error(f"Item: {item_id} not found!")
                    item      = msgpack.decode(raw, type=StockValue)
                    reserved  = int(pipe.get(reserved_key) or 0)
                    available = item.stock - reserved

                    if available < amount:
                        pipe.unwatch()
                        return response_error(f"Insufficient stock for item {item_id}: "
                                   f"available={available}, requested={amount}")

                    # ── Atomic soft-reserve + WAL ──────────────────────────
                    pipe.multi()
                    pipe.set(reserved_key, reserved + amount)
                    pipe.set(wal_key, WAL_PREPARED)
                    pipe.execute()
                    break  # success

                except redis.WatchError:
                    continue  # a concurrent writer touched a watched key; retry

    except redis.exceptions.RedisError:
        return response_error(DB_ERROR_STR)

    app.logger.debug(f"2PC prepare OK  order={order_id} item={item_id} qty={amount}")
    return response_success("Prepare: OK")

def commit_subtract_logic(order_id: str, item_id: str, amount: int):
    """
    2PC Phase 2 – COMMIT (participant: stock).

    Permanently deducts `amount` from item stock and releases the soft-
    reservation.  Safe to call multiple times (idempotent via WAL).
    """
    amount = int(amount)
    wal_key      = _stock_wal_key(order_id, item_id)
    reserved_key = _stock_reserved_key(item_id)

    try:
        with db.pipeline() as pipe:
            while True:
                try:
                    pipe.watch(item_id, reserved_key, wal_key)

                    wal_state = pipe.get(wal_key)
                    if wal_state == WAL_COMMITTED:
                        pipe.unwatch()
                        return response_success("Commit: already done")
                    if wal_state == WAL_ABORTED:
                        pipe.unwatch()
                        return response_error(f"Cannot commit: transaction {order_id} was aborted for item {item_id}")

                    # WAL_PREPARED (or None on coordinator recovery re-drive)
                    raw = pipe.get(item_id)
                    if not raw:
                        pipe.unwatch()
                        return response_error(f"Item: {item_id} not found!")
                    item      = msgpack.decode(raw, type=StockValue)
                    reserved  = int(pipe.get(reserved_key) or 0)

                    new_stock    = item.stock - amount
                    new_reserved = max(0, reserved - amount)

                    if new_stock < 0:
                        # The coordinator has already durably logged COMMIT, so
                        # we must commit regardless.  This path should be
                        # unreachable now that /subtract checks reservations, but
                        # if something bypassed the reservation, log and proceed.
                        app.logger.error(
                            f"2PC commit: stock underflow for item {item_id} order {order_id} "
                            f"(stock={item.stock}, reserved={reserved}, amount={amount}). "
                            f"Committing anyway to preserve 2PC durability."
                        )

                    item.stock = new_stock

                    pipe.multi()
                    pipe.set(item_id, msgpack.encode(item))
                    pipe.set(reserved_key, new_reserved)
                    pipe.set(wal_key, WAL_COMMITTED)
                    pipe.execute()
                    break

                except redis.WatchError:
                    continue

    except redis.exceptions.RedisError:
        return response_error(DB_ERROR_STR)

    app.logger.debug(f"2PC commit OK   order={order_id} item={item_id} qty={amount}")
    return response_success("Commit: OK")

def abort_subtract_logic(order_id: str, item_id: str, amount: int):
    """
    2PC Phase 2 – ABORT (participant: stock).

    Releases the soft-reservation without touching actual stock.
    Safe to call even if prepare was never received (WAL=None → no-op).
    Idempotent: calling twice returns 200 both times.
    """
    amount = int(amount)
    wal_key      = _stock_wal_key(order_id, item_id)
    reserved_key = _stock_reserved_key(item_id)

    try:
        with db.pipeline() as pipe:
            while True:
                try:
                    pipe.watch(reserved_key, wal_key)

                    wal_state = pipe.get(wal_key)
                    if wal_state == WAL_ABORTED or wal_state is None:
                        # Already aborted or never prepared — nothing to release.
                        pipe.unwatch()
                        return response_success("Abort: already done or not prepared")
                    if wal_state == WAL_COMMITTED:
                        pipe.unwatch()
                        return response_error(f"Cannot abort: transaction {order_id} already committed for item {item_id}")

                    # WAL_PREPARED: release the reservation.
                    reserved     = int(pipe.get(reserved_key) or 0)
                    new_reserved = max(0, reserved - amount)

                    pipe.multi()
                    pipe.set(reserved_key, new_reserved)
                    pipe.set(wal_key, WAL_ABORTED)
                    pipe.execute()
                    break

                except redis.WatchError:
                    continue

    except redis.exceptions.RedisError:
        return response_error(DB_ERROR_STR)

    return response_success("Abort: OK")


# ─── Flask Routes (Wrappers) ──────────────────────────────────────────────────

@app.post('/item/create/<price>')
def create_item(price: int):
    res = create_item_logic(price)
    return jsonify(res['body']) if res['status_code'] == 200 else abort(res['status_code'], res.get('error'))

@app.post('/batch_init/<n>/<starting_stock>/<item_price>')
def batch_init_users(n: int, starting_stock: int, item_price: int):
    res = batch_init_logic(n, starting_stock, item_price)
    return jsonify(res['body']) if res['status_code'] == 200 else abort(res['status_code'], res.get('error'))

@app.get('/find/<item_id>')
def find_item(item_id: str):
    res = find_item_logic(item_id)
    return jsonify(res['body']) if res['status_code'] == 200 else abort(res['status_code'], res.get('error'))

@app.post('/add/<item_id>/<amount>')
def add_stock(item_id: str, amount: int):
    res = add_stock_logic(item_id, amount)
    return Response(res['body'], status=200) if res['status_code'] == 200 else abort(res['status_code'], res.get('error'))

@app.post('/subtract/<item_id>/<amount>')
def remove_stock(item_id: str, amount: int):
    res = remove_stock_logic(item_id, amount)
    return Response(res['body'], status=200) if res['status_code'] == 200 else abort(res['status_code'], res.get('error'))

@app.post('/prepare_subtract/<order_id>/<item_id>/<amount>')
def prepare_subtract(order_id: str, item_id: str, amount: int):
    res = prepare_subtract_logic(order_id, item_id, amount)
    return Response(res['body'], status=200) if res['status_code'] == 200 else abort(res['status_code'], res.get('error'))

@app.post('/commit_subtract/<order_id>/<item_id>/<amount>')
def commit_subtract(order_id: str, item_id: str, amount: int):
    res = commit_subtract_logic(order_id, item_id, amount)
    return Response(res['body'], status=200) if res['status_code'] == 200 else abort(res['status_code'], res.get('error'))

@app.post('/abort_subtract/<order_id>/<item_id>/<amount>')
def abort_subtract(order_id: str, item_id: str, amount: int):
    res = abort_subtract_logic(order_id, item_id, amount)
    return Response(res['body'], status=200) if res['status_code'] == 200 else abort(res['status_code'], res.get('error'))


@app.get('/health')
def health_check():
    try:
        db.ping()
        mq.ping()
        return jsonify({"status": "healthy", "db": "connected", "mq": "connected"}), 200
    except redis.exceptions.RedisError as e:
        app.logger.error(f"Health check failed: {e}")
        return jsonify({"status": "unhealthy", "db": "disconnected", "mq": "disconnected"}), 500


# ─── Message Queue Listener ───────────────────────────────────────────────────

def run_event_listener():
    stream_key = 'events:stock'
    group_name = 'stock_group'
    consumer_name = f"stock_consumer_{uuid.uuid4()}"

    try:
        mq.xgroup_create(stream_key, group_name, mkstream=True)
        app.logger.info(f"Created consumer group {group_name}")
    except redis.exceptions.ResponseError as e:
        if "BUSYGROUP" not in str(e):
            app.logger.error(f"Failed to create consumer group: {e}")

    app.logger.info(f"Starting MQ listener: {consumer_name}")

    while True:
        try:
            # Read new messages from the group
            streams = mq.xreadgroup(group_name, consumer_name, {stream_key: '>'}, count=1, block=5000)
            
            if not streams:
                continue

            for _, messages in streams:
                for message_id, data in messages:
                    reply_to = None
                    try:
                        # Decode byte keys/values from Redis
                        payload = {k.decode('utf-8'): v.decode('utf-8') for k, v in data.items()}
                        msg_type = payload.get('type')
                        reply_to = payload.get('reply_to')
                        
                        result = {"status_code": 400, "error": "Unknown action"}

                        if msg_type == 'create_item':
                            result = create_item_logic(int(payload['price']))
                        elif msg_type == 'batch_init':
                            result = batch_init_logic(int(payload['n']), int(payload['starting_stock']), int(payload['item_price']))
                        elif msg_type == 'find_item':
                            result = find_item_logic(payload['item_id'])
                        elif msg_type == 'add_stock':
                            result = add_stock_logic(payload['item_id'], int(payload['amount']))
                        elif msg_type == 'subtract_stock':
                            result = remove_stock_logic(payload['item_id'], int(payload['amount']))
                        elif msg_type == 'prepare_subtract':
                            result = prepare_subtract_logic(payload['order_id'], payload['item_id'], int(payload['amount']))
                        elif msg_type == 'commit_subtract':
                            result = commit_subtract_logic(payload['order_id'], payload['item_id'], int(payload['amount']))
                        elif msg_type == 'abort_subtract':
                            result = abort_subtract_logic(payload['order_id'], payload['item_id'], int(payload['amount']))
                        
                        # Push response if reply_to is present
                        if reply_to:
                            mq.rpush(reply_to, json.dumps(result))
                            mq.expire(reply_to, 60) # Set TTL for reply list

                        # Acknowledge the message
                        mq.xack(stream_key, group_name, message_id)

                    except Exception as e:
                        app.logger.error(f"Error processing MQ message {message_id}: {e}")
                        # Optionally we could push an error response to reply_to here
                        if reply_to:
                             error_res = response_error(f"Internal processing error: {str(e)}", 500)
                             mq.rpush(reply_to, json.dumps(error_res))
                             mq.expire(reply_to, 60)
                        # We still ack to prevent poison pills from looping forever, 
                        # or we could implement a dead-letter queue mechanism.
                        mq.xack(stream_key, group_name, message_id)

        except Exception as e:
            app.logger.error(f"MQ Listener loop error: {e}")
            time.sleep(2) # Backoff

if __name__ == '__main__':
    # Start background listener thread
    t = threading.Thread(target=run_event_listener, daemon=True)
    t.start()
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
    
    # Start background listener thread when running with Gunicorn
    t = threading.Thread(target=run_event_listener, daemon=True)
    t.start()

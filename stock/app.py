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

TX_DEDUCTED = b"DEDUCTED"
TX_ROLLED_BACK = b"ROLLED_BACK"

app = Flask("stock-service")

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

def _stock_tx_key(tx_id: str) -> str:
    return f"stock:tx:{tx_id}"


def create_item_logic(price: int):
    key = str(uuid.uuid4())
    app.logger.debug(f"Item: {key} created")
    value = msgpack.encode(StockValue(stock=0, price=int(price)))
    try:
        db.set(key, value)
    except redis.exceptions.RedisError:
        return response_error(DB_ERROR_STR, 500)
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
        return response_error(DB_ERROR_STR, 500)
    return response_success({"msg": "Batch init for stock successful"})

def find_item_logic(item_id: str):
    try:
        item_entry: StockValue = get_item_from_db(item_id)
    except redis.exceptions.RedisError:
        return response_error(DB_ERROR_STR, 500)
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
            return response_error(DB_ERROR_STR, 500)
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
            return response_error(DB_ERROR_STR, 500)
    return response_error("Conflict: could not subtract stock after retries")

def add_stock_batch_logic(items_json: str, transaction_id: str | None = None):
    """
    Atomically add stock for multiple items.
    payload: {'items': '{"item_id": amount, ...}'}
    """
    try:
        items: dict[str, int] = json.loads(items_json)
    except json.JSONDecodeError:
        return response_error("Invalid JSON format for items")

    keys = list(items.keys())
    tx_key = _stock_tx_key(transaction_id) if transaction_id else None
    
    for _ in range(10):
        try:
            with db.pipeline() as pipe:
                if tx_key:
                    pipe.watch(tx_key, *keys)
                    state = pipe.get(tx_key)
                    
                    if state == TX_DEDUCTED:
                        # Proceed to add stock (actual rollback)
                        pass 
                    elif state == TX_ROLLED_BACK:
                        pipe.unwatch()
                        return response_success("Rollback already processed (idempotent)")
                    else:
                        # Key does not exist (or unknown state). 
                        # Set Tombstone to prevent future deductions.
                        pipe.multi()
                        pipe.set(tx_key, TX_ROLLED_BACK)
                        pipe.execute()
                        return response_success("Rollback tombstone set (deduction never happened)")
                else:
                    pipe.watch(*keys)
                
                current_values = {}

                for key in keys:
                    raw = pipe.get(key)
                    if raw is None:
                        pipe.unwatch()
                        return response_error(f"Item: {key} not found!")
                    current_values[key] = msgpack.decode(raw, type=StockValue)

                pipe.multi()
                for key, amount in items.items():
                    entry = current_values[key]
                    entry.stock += int(amount)
                    pipe.set(key, msgpack.encode(entry))
                if tx_key:
                    pipe.set(tx_key, TX_ROLLED_BACK)
                pipe.execute()
                return response_success(f"Batch stock added for {len(items)} items")
        except redis.WatchError:
            continue
        except redis.exceptions.RedisError:
            return response_error(DB_ERROR_STR, 500)
    return response_error("Conflict: could not update stock batch after retries")

def subtract_stock_batch_logic(items_json: str, transaction_id: str | None = None):
    """
    Atomically subtract stock for multiple items.
    """
    try:
        items: dict[str, int] = json.loads(items_json)
    except json.JSONDecodeError:
        return response_error("Invalid JSON format for items")

    keys = list(items.keys())
    reserved_keys = [_stock_reserved_key(k) for k in keys]
    all_watch_keys = keys + reserved_keys
    tx_key = _stock_tx_key(transaction_id) if transaction_id else None

    for _ in range(10):
        try:
            with db.pipeline() as pipe:
                if tx_key:
                    pipe.watch(tx_key, *all_watch_keys)
                    state = pipe.get(tx_key)
                    
                    if state == TX_DEDUCTED:
                        pipe.unwatch()
                        return response_success("Stock already deducted (idempotent)")
                    if state == TX_ROLLED_BACK:
                        pipe.unwatch()
                        return response_success("Ignored deduction (rolled back via tombstone)")
                else:
                    pipe.watch(*all_watch_keys)
                
                current_values = {}
                current_reserved = {}
                
                for key in keys:
                    raw = pipe.get(key)
                    if raw is None:
                        pipe.unwatch()
                        return response_error(f"Item: {key} not found!")
                    current_values[key] = msgpack.decode(raw, type=StockValue)
                    
                    r_key = _stock_reserved_key(key)
                    res_val = pipe.get(r_key)
                    current_reserved[key] = int(res_val or 0)

                    entry = current_values[key]
                    amount = items.get(key)
                    if entry.stock - int(amount) < current_reserved[key]:
                        pipe.unwatch()
                        return response_error(f"Item: {key} insufficient stock/reserved conflict")

                pipe.multi()
                for key, amount in items.items():
                    entry = current_values[key]
                    entry.stock -= int(amount)
                    pipe.set(key, msgpack.encode(entry))
                if tx_key:
                    pipe.set(tx_key, TX_DEDUCTED)
                pipe.execute()
                return response_success(f"Batch stock subtracted for {len(items)} items")
        except redis.WatchError:
            continue
        except redis.exceptions.RedisError:
            return response_error(DB_ERROR_STR, 500)
    return response_error("Conflict: could not subtract stock batch after retries")


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

def _stock_wal_key(order_id: str) -> str:
    return f"2pc:stock:{order_id}"

def _stock_reserved_key(item_id: str) -> str:
    return f"reserved:stock:{item_id}"

def prepare_subtract_batch_logic(order_id: str, items_json: str):
    """
    2PC Phase 1 – PREPARE BATCH (participant: stock).
    Atomically checks availability and reserves stock for all items in the batch.
    """
    try:
        items: dict[str, int] = json.loads(items_json)
    except json.JSONDecodeError:
        return response_error("Invalid JSON format for items")

    keys = list(items.keys())
    wal_key = _stock_wal_key(order_id)
    reserved_keys = {k: _stock_reserved_key(k) for k in keys}
    
    # Watch all involved keys
    watch_keys = keys + list(reserved_keys.values()) + [wal_key]

    for _ in range(10):
        try:
            with db.pipeline() as pipe:
                pipe.watch(*watch_keys)
                
                # ── Idempotency check ──────────────────────────────────
                # If all are already prepared/committed, return success.
                # If any is aborted, fail.
                wal_state = pipe.get(wal_key)
                if wal_state == WAL_ABORTED:
                    pipe.unwatch()
                    return response_error(f"Transaction {order_id} already aborted")
                
                if wal_state in (WAL_PREPARED, WAL_COMMITTED):
                    pipe.unwatch()
                    return response_success("Prepare batch: already done")

                # ── Availability check ─────────────────────────────────
                current_reserved = {}
                for k, amount in items.items():
                    raw = pipe.get(k)
                    if not raw:
                        pipe.unwatch()
                        return response_error(f"Item: {k} not found!")
                    item = msgpack.decode(raw, type=StockValue)
                    res_val = pipe.get(reserved_keys[k])
                    current_reserved[k] = int(res_val or 0)
                    available = item.stock - current_reserved[k]
                    
                    if available < int(amount):
                        pipe.unwatch()
                        return response_error(f"Insufficient stock for item {k}")

                # ── Atomic soft-reserve + WAL ──────────────────────────
                pipe.multi()
                for k, amount in items.items():
                    pipe.set(reserved_keys[k], current_reserved[k] + int(amount))
                pipe.set(wal_key, WAL_PREPARED)
                pipe.execute()
                return response_success("Prepare batch: OK")
        except redis.WatchError:
            continue
        except redis.exceptions.RedisError:
            return response_error(DB_ERROR_STR, 500)
    return response_error("Conflict: could not prepare batch after retries")

def commit_subtract_batch_logic(order_id: str, items_json: str):
    """
    2PC Phase 2 – COMMIT BATCH (participant: stock).
    """
    try:
        items: dict[str, int] = json.loads(items_json)
    except json.JSONDecodeError:
        return response_error("Invalid JSON format for items")

    keys = list(items.keys())
    wal_key = _stock_wal_key(order_id)
    reserved_keys = {k: _stock_reserved_key(k) for k in keys}
    watch_keys = keys + list(reserved_keys.values()) + [wal_key]

    for _ in range(10):
        try:
            with db.pipeline() as pipe:
                pipe.watch(*watch_keys)

                wal_state = pipe.get(wal_key)
                if wal_state == WAL_ABORTED:
                    pipe.unwatch()
                    return response_error(f"Cannot commit: transaction {order_id} aborted")

                if wal_state == WAL_COMMITTED:
                    pipe.unwatch()
                    return response_success("Commit batch: already done")

                current_reserved = {}

                for k, amount in items.items():
                    raw = pipe.get(k)
                    if not raw:
                        pipe.unwatch()
                        return response_error(f"Item: {k} not found!")
                    res_val = pipe.get(reserved_keys[k])
                    current_reserved[k] = int(res_val or 0)

                pipe.multi()
                for k, amount in items.items():
                    item = msgpack.decode(raw, type=StockValue)
                    item.stock -= int(amount)
                    if item.stock < 0:
                        # The coordinator has already durably logged COMMIT, so
                        # we must commit regardless.  This path should be
                        # unreachable, but if
                        # something bypassed the reservation, log and proceed.
                        app.logger.error(
                            f"2PC commit: stock underflow for item {k} order {order_id} "
                            f"(stock={item.stock}, reserved={current_reserved[k]}, amount={amount}). "
                            f"Committing anyway to preserve 2PC durability."
                        )
                    new_reserved = max(0, current_reserved[k] - int(amount))

                    pipe.set(k, msgpack.encode(item))
                    pipe.set(reserved_keys[k], new_reserved)
                pipe.set(wal_key, WAL_COMMITTED)
                pipe.execute()
                return response_success("Commit batch: OK")
        except redis.WatchError:
            continue
        except redis.exceptions.RedisError:
            return response_error(DB_ERROR_STR, 500)
    return response_error("Conflict: could not commit batch after retries")

def abort_subtract_batch_logic(order_id: str, items_json: str):
    """
    2PC Phase 2 – ABORT BATCH (participant: stock).
    """
    try:
        items: dict[str, int] = json.loads(items_json)
    except json.JSONDecodeError:
        return response_error("Invalid JSON format for items")

    keys = list(items.keys())
    wal_key = _stock_wal_key(order_id)
    reserved_keys = {k: _stock_reserved_key(k) for k in keys}
    watch_keys = list(reserved_keys.values()) + [wal_key]

    for _ in range(10):
        try:
            with db.pipeline() as pipe:
                pipe.watch(*watch_keys)
                
                wal_state = pipe.get(wal_key)
                if wal_state == WAL_COMMITTED:
                    pipe.unwatch()
                    return response_error(f"Cannot abort: transaction {order_id}, already committed")
                elif wal_state != WAL_ABORTED:
                    current_reserved = {}
                    for k, amount in items.items():
                        raw = pipe.get(k)
                        if not raw:
                            pipe.unwatch()
                            return response_error(f"Item: {k} not found!")
                        res_val = pipe.get(reserved_keys[k])
                        current_reserved[k] = int(res_val or 0)
                    pipe.multi()
                    pipe.set(wal_key, WAL_ABORTED)
                    for k, amount in items.items():
                        new_reserved = max(0, current_reserved[k] - int(amount))
                        pipe.set(reserved_keys[k], new_reserved)
                    pipe.execute()
                return response_success("Abort batch: OK")
        except redis.WatchError:
            continue
        except redis.exceptions.RedisError:
            return response_error(DB_ERROR_STR, 500)
    return response_error("Conflict: could not abort batch after retries")

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

# ─── Message Queue Listener ───────────────────────────────────────────────────

def run_event_listener():
    stream_key = 'events:stock'
    group_name = 'stock_group'
    consumer_name = f"stock_consumer_{os.environ.get('HOSTNAME', 'local')}"

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
            streams = mq.xreadgroup(group_name, consumer_name, {stream_key: '>'}, count=1, block=5000, noack=True)
            
            if not streams:
                continue

            for _, messages in streams:
                for message_id, data in messages:
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
                        elif msg_type == 'add_stock_batch':
                            tx_id = payload.get('transaction_id')
                            result = add_stock_batch_logic(payload['items'], tx_id)
                        elif msg_type == 'subtract_stock_batch':
                            tx_id = payload.get('transaction_id')
                            result = subtract_stock_batch_logic(payload['items'], tx_id)
                        elif msg_type == 'prepare_subtract_batch':
                            result = prepare_subtract_batch_logic(payload['order_id'], payload['items'])
                        elif msg_type == 'commit_subtract_batch':
                            result = commit_subtract_batch_logic(payload['order_id'], payload['items'])
                        elif msg_type == 'abort_subtract_batch':
                            result = abort_subtract_batch_logic(payload['order_id'], payload['items'])
                        
                        # Push response if reply_to is present
                        if reply_to:
                            mq.rpush(reply_to, json.dumps(result))
                            mq.expire(reply_to, 60) # Set TTL for reply list

                    except Exception as e:
                        app.logger.error(f"Error processing MQ message {message_id}: {e}")
                        # Optionally we could push an error response to reply_to here
                        if reply_to:
                             error_res = response_error(f"Internal processing error: {str(e)}", 500)
                             mq.rpush(reply_to, json.dumps(error_res))
                             mq.expire(reply_to, 60)

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

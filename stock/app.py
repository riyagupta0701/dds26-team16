import logging
import os
import atexit
import uuid

import redis

from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response


DB_ERROR_STR = "DB error"

# 2PC WAL state values stored in Redis
WAL_PREPARED  = b"prepared"
WAL_COMMITTED = b"committed"
WAL_ABORTED   = b"aborted"

app = Flask("stock-service")

_REDIS_PASSWORD    = os.environ.get('REDIS_PASSWORD', '')
_REDIS_DB          = int(os.environ.get('REDIS_DB', '0'))
_SENTINEL_HOSTS    = os.environ.get('REDIS_SENTINEL_HOSTS', '')
_REDIS_MASTER_NAME = os.environ.get('REDIS_MASTER_NAME', 'mymaster')

if _SENTINEL_HOSTS:
    from redis.sentinel import Sentinel as _Sentinel
    from redis.retry import Retry
    from redis.backoff import NoBackoff
    _peers = [(h.split(':')[0], int(h.split(':')[1])) for h in _SENTINEL_HOSTS.split(',')]
    db: redis.Redis = _Sentinel(
        _peers,
        password=_REDIS_PASSWORD,
        db=_REDIS_DB,
    ).master_for(
        _REDIS_MASTER_NAME,
        socket_timeout=1.5,
        retry=Retry(NoBackoff(), 3),
        retry_on_error=[redis.exceptions.ConnectionError, redis.exceptions.TimeoutError],
    )
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


class StockValue(Struct):
    stock: int
    price: int


def get_item_from_db(item_id: str) -> StockValue | None:
    # get serialized data
    try:
        entry: bytes = db.get(item_id)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    # deserialize data if it exists else return null
    entry: StockValue | None = msgpack.decode(entry, type=StockValue) if entry else None
    if entry is None:
        # if item does not exist in the database; abort
        abort(400, f"Item: {item_id} not found!")
    return entry


@app.post('/item/create/<price>')
def create_item(price: int):
    key = str(uuid.uuid4())
    app.logger.debug(f"Item: {key} created")
    value = msgpack.encode(StockValue(stock=0, price=int(price)))
    try:
        db.set(key, value)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({'item_id': key})


@app.post('/batch_init/<n>/<starting_stock>/<item_price>')
def batch_init_users(n: int, starting_stock: int, item_price: int):
    n = int(n)
    starting_stock = int(starting_stock)
    item_price = int(item_price)
    kv_pairs: dict[str, bytes] = {f"{i}": msgpack.encode(StockValue(stock=starting_stock, price=item_price))
                                  for i in range(n)}
    try:
        db.mset(kv_pairs)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({"msg": "Batch init for stock successful"})


@app.get('/find/<item_id>')
def find_item(item_id: str):
    item_entry: StockValue = get_item_from_db(item_id)
    return jsonify(
        {
            "stock": item_entry.stock,
            "price": item_entry.price
        }
    )


@app.post('/add/<item_id>/<amount>')
def add_stock(item_id: str, amount: int):
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
                    abort(400, f"Item: {item_id} not found!")
                item_entry = msgpack.decode(raw, type=StockValue)
                pipe.multi()
                item_entry.stock += amount
                pipe.set(item_id, msgpack.encode(item_entry))
                pipe.execute()
                return Response(f"Item: {item_id} stock updated to: {item_entry.stock}", status=200)
        except redis.WatchError:
            continue
        except redis.exceptions.RedisError:
            abort(400, DB_ERROR_STR)
    abort(400, "Conflict: could not update stock after retries")


@app.post('/subtract/<item_id>/<amount>')
def remove_stock(item_id: str, amount: int):
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
                    abort(400, f"Item: {item_id} not found!")
                item_entry = msgpack.decode(raw, type=StockValue)
                reserved  = int(pipe.get(reserved_key) or 0)
                # Guard: cannot subtract below what is already soft-reserved.
                if item_entry.stock - amount < reserved:
                    pipe.unwatch()
                    abort(400, f"Item: {item_id} stock cannot get reduced below zero!")
                pipe.multi()
                item_entry.stock -= amount
                pipe.set(item_id, msgpack.encode(item_entry))
                pipe.execute()
                return Response(f"Item: {item_id} stock updated to: {item_entry.stock}", status=200)
        except redis.WatchError:
            continue
        except redis.exceptions.RedisError:
            abort(400, DB_ERROR_STR)
    abort(400, "Conflict: could not subtract stock after retries")


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


@app.post('/prepare_subtract/<order_id>/<item_id>/<amount>')
def prepare_subtract(order_id: str, item_id: str, amount: int):
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
                        return Response("Prepare: already done", status=200)
                    if wal_state == WAL_ABORTED:
                        pipe.unwatch()
                        abort(400, f"Transaction {order_id} already aborted for item {item_id}")

                    # ── Availability check ─────────────────────────────────
                    raw = pipe.get(item_id)
                    if not raw:
                        pipe.unwatch()
                        abort(400, f"Item: {item_id} not found!")
                    item      = msgpack.decode(raw, type=StockValue)
                    reserved  = int(pipe.get(reserved_key) or 0)
                    available = item.stock - reserved

                    if available < amount:
                        pipe.unwatch()
                        abort(400, f"Insufficient stock for item {item_id}: "
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
        abort(400, DB_ERROR_STR)

    app.logger.debug(f"2PC prepare OK  order={order_id} item={item_id} qty={amount}")
    return Response("Prepare: OK", status=200)


@app.post('/commit_subtract/<order_id>/<item_id>/<amount>')
def commit_subtract(order_id: str, item_id: str, amount: int):
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
                        return Response("Commit: already done", status=200)
                    if wal_state == WAL_ABORTED:
                        pipe.unwatch()
                        abort(400, f"Cannot commit: transaction {order_id} was aborted for item {item_id}")

                    # WAL_PREPARED (or None on coordinator recovery re-drive)
                    raw = pipe.get(item_id)
                    if not raw:
                        pipe.unwatch()
                        abort(400, f"Item: {item_id} not found!")
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
        abort(400, DB_ERROR_STR)

    app.logger.debug(f"2PC commit OK   order={order_id} item={item_id} qty={amount}")
    return Response("Commit: OK", status=200)


@app.post('/abort_subtract/<order_id>/<item_id>/<amount>')
def abort_subtract(order_id: str, item_id: str, amount: int):
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
                        return Response("Abort: already done or not prepared", status=200)
                    if wal_state == WAL_COMMITTED:
                        pipe.unwatch()
                        abort(400, f"Cannot abort: transaction {order_id} already committed for item {item_id}")

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
        abort(400, DB_ERROR_STR)

    app.logger.debug(f"2PC abort OK    order={order_id} item={item_id} qty={amount}")
    return Response("Abort: OK", status=200)


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)

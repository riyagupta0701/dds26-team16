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

app = Flask("payment-service")

db: redis.Redis = redis.Redis(host=os.environ['REDIS_HOST'],
                              port=int(os.environ['REDIS_PORT']),
                              password=os.environ['REDIS_PASSWORD'],
                              db=int(os.environ['REDIS_DB']))


def close_db_connection():
    db.close()


atexit.register(close_db_connection)


class UserValue(Struct):
    credit: int


def get_user_from_db(user_id: str) -> UserValue | None:
    try:
        # get serialized data
        entry: bytes = db.get(user_id)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    # deserialize data if it exists else return null
    entry: UserValue | None = msgpack.decode(entry, type=UserValue) if entry else None
    if entry is None:
        # if user does not exist in the database; abort
        abort(400, f"User: {user_id} not found!")
    return entry


@app.post('/create_user')
def create_user():
    key = str(uuid.uuid4())
    value = msgpack.encode(UserValue(credit=0))
    try:
        db.set(key, value)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({'user_id': key})


@app.post('/batch_init/<n>/<starting_money>')
def batch_init_users(n: int, starting_money: int):
    n = int(n)
    starting_money = int(starting_money)
    kv_pairs: dict[str, bytes] = {f"{i}": msgpack.encode(UserValue(credit=starting_money))
                                  for i in range(n)}
    try:
        db.mset(kv_pairs)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({"msg": "Batch init for users successful"})


@app.get('/find_user/<user_id>')
def find_user(user_id: str):
    user_entry: UserValue = get_user_from_db(user_id)
    return jsonify(
        {
            "user_id": user_id,
            "credit": user_entry.credit
        }
    )


@app.post('/add_funds/<user_id>/<amount>')
def add_credit(user_id: str, amount: int):
    """
    Idempotency: uses a Redis WATCH/MULTI/EXEC optimistic lock so concurrent
    retries from the benchmark cannot double-add funds.
    """
    amount = int(amount)
    for _ in range(10):  # retry loop for optimistic lock
        try:
            with db.pipeline() as pipe:
                pipe.watch(user_id)
                raw = pipe.get(user_id)
                if raw is None:
                    pipe.reset()
                    abort(400, f"User: {user_id} not found!")
                user_entry = msgpack.decode(raw, type=UserValue)
                pipe.multi()
                user_entry.credit += amount
                pipe.set(user_id, msgpack.encode(user_entry))
                pipe.execute()
                return Response(f"User: {user_id} credit updated to: {user_entry.credit}", status=200)
        except redis.WatchError:
            continue
        except redis.exceptions.RedisError:
            abort(400, DB_ERROR_STR)
    abort(400, "Conflict: could not update credit after retries")


@app.post('/pay/<user_id>/<amount>')
def remove_credit(user_id: str, amount: int):
    """
    Atomically deduct credit using optimistic locking.
    Returns 400 if credit would go below zero.
    """
    amount = int(amount)
    app.logger.debug(f"Removing {amount} credit from user: {user_id}")
    for _ in range(10):
        try:
            with db.pipeline() as pipe:
                pipe.watch(user_id)
                raw = pipe.get(user_id)
                if raw is None:
                    pipe.reset()
                    abort(400, f"User: {user_id} not found!")
                user_entry = msgpack.decode(raw, type=UserValue)
                if user_entry.credit - amount < 0:
                    pipe.reset()
                    abort(400, f"User: {user_id} credit cannot get reduced below zero!")
                pipe.multi()
                user_entry.credit -= amount
                pipe.set(user_id, msgpack.encode(user_entry))
                pipe.execute()
                return Response(f"User: {user_id} credit updated to: {user_entry.credit}", status=200)
        except redis.WatchError:
            continue
        except redis.exceptions.RedisError:
            abort(400, DB_ERROR_STR)
    abort(400, "Conflict: could not deduct credit after retries")


# ─── 2PC Participant Endpoints ─────────────────────────────────────────────────
#
# Soft-reservations are stored as plain Redis integers under:
#   reserved:payment:{user_id}
#
# Per-transaction WAL entries live under:
#   2pc:payment:{order_id}:{user_id}  → b"prepared" | b"committed" | b"aborted"
#
# All three endpoints use WATCH/MULTI/EXEC for optimistic concurrency control.

def _payment_wal_key(order_id: str, user_id: str) -> str:
    return f"2pc:payment:{order_id}:{user_id}"


def _payment_reserved_key(user_id: str) -> str:
    return f"reserved:payment:{user_id}"


@app.post('/prepare_pay/<order_id>/<user_id>/<amount>')
def prepare_pay(order_id: str, user_id: str, amount: int):
    """
    2PC Phase 1 – PREPARE (participant: payment).

    Atomically checks that user_id has enough unreserved credit, then
    soft-reserves `amount` and records a WAL entry so the decision
    survives participant crashes.

    Idempotent: a duplicate call for the same (order_id, user_id) returns 200.
    """
    amount = int(amount)
    wal_key      = _payment_wal_key(order_id, user_id)
    reserved_key = _payment_reserved_key(user_id)

    try:
        with db.pipeline() as pipe:
            while True:
                try:
                    pipe.watch(user_id, reserved_key, wal_key)

                    # ── Idempotency check ──────────────────────────────────
                    wal_state = pipe.get(wal_key)
                    if wal_state in (WAL_PREPARED, WAL_COMMITTED):
                        pipe.unwatch()
                        return Response("Prepare: already done", status=200)
                    if wal_state == WAL_ABORTED:
                        pipe.unwatch()
                        abort(400, f"Transaction {order_id} already aborted for user {user_id}")

                    # ── Availability check ─────────────────────────────────
                    raw = pipe.get(user_id)
                    if not raw:
                        pipe.unwatch()
                        abort(400, f"User: {user_id} not found!")
                    user      = msgpack.decode(raw, type=UserValue)
                    reserved  = int(pipe.get(reserved_key) or 0)
                    available = user.credit - reserved

                    if available < amount:
                        pipe.unwatch()
                        abort(400, f"Insufficient credit for user {user_id}: "
                                   f"available={available}, requested={amount}")

                    # ── Atomic soft-reserve + WAL ──────────────────────────
                    pipe.multi()
                    pipe.set(reserved_key, reserved + amount)
                    pipe.set(wal_key, WAL_PREPARED)
                    pipe.execute()
                    break

                except redis.WatchError:
                    continue  # concurrent writer; retry

    except redis.exceptions.RedisError:
        abort(400, DB_ERROR_STR)

    app.logger.debug(f"2PC prepare OK  order={order_id} user={user_id} amount={amount}")
    return Response("Prepare: OK", status=200)


@app.post('/commit_pay/<order_id>/<user_id>/<amount>')
def commit_pay(order_id: str, user_id: str, amount: int):
    """
    2PC Phase 2 – COMMIT (participant: payment).

    Permanently deducts `amount` from user credit and releases the soft-
    reservation.  Safe to call multiple times (idempotent via WAL).
    """
    amount = int(amount)
    wal_key      = _payment_wal_key(order_id, user_id)
    reserved_key = _payment_reserved_key(user_id)

    try:
        with db.pipeline() as pipe:
            while True:
                try:
                    pipe.watch(user_id, reserved_key, wal_key)

                    wal_state = pipe.get(wal_key)
                    if wal_state == WAL_COMMITTED:
                        pipe.unwatch()
                        return Response("Commit: already done", status=200)
                    if wal_state == WAL_ABORTED:
                        pipe.unwatch()
                        abort(400, f"Cannot commit: transaction {order_id} was aborted for user {user_id}")

                    # WAL_PREPARED (or None on coordinator recovery re-drive)
                    raw = pipe.get(user_id)
                    if not raw:
                        pipe.unwatch()
                        abort(400, f"User: {user_id} not found!")
                    user      = msgpack.decode(raw, type=UserValue)
                    reserved  = int(pipe.get(reserved_key) or 0)

                    new_credit   = user.credit - amount
                    new_reserved = max(0, reserved - amount)

                    if new_credit < 0:
                        pipe.unwatch()
                        abort(400, f"Credit underflow during commit for user {user_id}")

                    user.credit = new_credit

                    pipe.multi()
                    pipe.set(user_id, msgpack.encode(user))
                    pipe.set(reserved_key, new_reserved)
                    pipe.set(wal_key, WAL_COMMITTED)
                    pipe.execute()
                    break

                except redis.WatchError:
                    continue

    except redis.exceptions.RedisError:
        abort(400, DB_ERROR_STR)

    app.logger.debug(f"2PC commit OK   order={order_id} user={user_id} amount={amount}")
    return Response("Commit: OK", status=200)


@app.post('/abort_pay/<order_id>/<user_id>/<amount>')
def abort_pay(order_id: str, user_id: str, amount: int):
    """
    2PC Phase 2 – ABORT (participant: payment).

    Releases the soft-reservation without touching actual credit.
    Safe to call even if prepare was never received (WAL=None → no-op).
    Idempotent: calling twice returns 200 both times.
    """
    amount = int(amount)
    wal_key      = _payment_wal_key(order_id, user_id)
    reserved_key = _payment_reserved_key(user_id)

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
                        abort(400, f"Cannot abort: transaction {order_id} already committed for user {user_id}")

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

    app.logger.debug(f"2PC abort OK    order={order_id} user={user_id} amount={amount}")
    return Response("Abort: OK", status=200)


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)

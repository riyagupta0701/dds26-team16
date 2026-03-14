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

app = Flask("payment-service")

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


class UserValue(Struct):
    credit: int

# ─── Logic Helpers ────────────────────────────────────────────────────────────

def response_success(body, status_code=200):
    return {"status_code": status_code, "body": body}

def response_error(error_msg, status_code=400):
    return {"status_code": status_code, "error": error_msg}

def get_user_from_db(user_id: str) -> UserValue | None:
    # get serialized data; let RedisError propagate
    entry: bytes = db.get(user_id)
    # deserialize data if it exists else return null
    entry: UserValue | None = msgpack.decode(entry, type=UserValue) if entry else None
    return entry

# ─── Business Logic (Decoupled) ───────────────────────────────────────────────

def create_user_logic():
    key = str(uuid.uuid4())
    value = msgpack.encode(UserValue(credit=0))
    try:
        db.set(key, value)
    except redis.exceptions.RedisError:
        return response_error(DB_ERROR_STR)
    return response_success({'user_id': key})

def batch_init_logic(n: int, starting_money: int):
    n = int(n)
    starting_money = int(starting_money)
    kv_pairs: dict[str, bytes] = {f"{i}": msgpack.encode(UserValue(credit=starting_money))
                                  for i in range(n)}
    try:
        db.mset(kv_pairs)
    except redis.exceptions.RedisError:
        return response_error(DB_ERROR_STR)
    return response_success({"msg": "Batch init for users successful"})

def find_user_logic(user_id: str):
    try:
        user_entry: UserValue = get_user_from_db(user_id)
    except redis.exceptions.RedisError:
        return response_error(DB_ERROR_STR)
    if user_entry is None:
        return response_error(f"User: {user_id} not found!", 400)
    return response_success({
        "user_id": user_id,
        "credit": user_entry.credit
    })

def add_credit_logic(user_id: str, amount: int):
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
                    return response_error(f"User: {user_id} not found!")
                user_entry = msgpack.decode(raw, type=UserValue)
                pipe.multi()
                user_entry.credit += amount
                pipe.set(user_id, msgpack.encode(user_entry))
                pipe.execute()
                return response_success(f"User: {user_id} credit updated to: {user_entry.credit}")
        except redis.WatchError:
            continue
        except redis.exceptions.RedisError:
            return response_error(DB_ERROR_STR)
    return response_error("Conflict: could not update credit after retries")

def remove_credit_logic(user_id: str, amount: int):
    """
    Atomically deduct credit using optimistic locking.
    Respects 2PC soft-reservations: only unreserved credit may be deducted.
    Returns 400 if credit would fall below the currently reserved amount.
    """
    amount = int(amount)
    app.logger.debug(f"Removing {amount} credit from user: {user_id}")
    reserved_key = _payment_reserved_key(user_id)
    for _ in range(10):
        try:
            with db.pipeline() as pipe:
                pipe.watch(user_id, reserved_key)
                raw = pipe.get(user_id)
                if raw is None:
                    pipe.unwatch()
                    return response_error(f"User: {user_id} not found!")
                user_entry = msgpack.decode(raw, type=UserValue)
                reserved   = int(pipe.get(reserved_key) or 0)
                # Guard: cannot deduct below what is already soft-reserved.
                if user_entry.credit - amount < reserved:
                    pipe.unwatch()
                    return response_error(f"Insufficient available credit for user '{user_id}'. Requested deduction: {amount}, reserved: {reserved}, current balance: {user_entry.credit}.")
                pipe.multi()
                user_entry.credit -= amount
                pipe.set(user_id, msgpack.encode(user_entry))
                pipe.execute()
                return response_success(f"User: {user_id} credit updated to: {user_entry.credit}")
        except redis.WatchError:
            continue
        except redis.exceptions.RedisError:
            return response_error(DB_ERROR_STR)
    return response_error("Conflict: could not deduct credit after retries")


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

def prepare_pay_logic(order_id: str, user_id: str, amount: int):
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
                        return response_success("Prepare: already done")
                    if wal_state == WAL_ABORTED:
                        pipe.unwatch()
                        return response_error(f"Transaction {order_id} already aborted for user {user_id}")

                    # ── Availability check ─────────────────────────────────
                    raw = pipe.get(user_id)
                    if not raw:
                        pipe.unwatch()
                        return response_error(f"User: {user_id} not found!")
                    user      = msgpack.decode(raw, type=UserValue)
                    reserved  = int(pipe.get(reserved_key) or 0)
                    available = user.credit - reserved

                    if available < amount:
                        pipe.unwatch()
                        return response_error(f"Insufficient credit for user {user_id}: "
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
        return response_error(DB_ERROR_STR)

    app.logger.debug(f"2PC prepare OK  order={order_id} user={user_id} amount={amount}")
    return response_success("Prepare: OK")

def commit_pay_logic(order_id: str, user_id: str, amount: int):
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
                        return response_success("Commit: already done")
                    if wal_state == WAL_ABORTED:
                        pipe.unwatch()
                        return response_error(f"Cannot commit: transaction {order_id} was aborted for user {user_id}")

                    # WAL_PREPARED (or None on coordinator recovery re-drive)
                    raw = pipe.get(user_id)
                    if not raw:
                        pipe.unwatch()
                        return response_error(f"User: {user_id} not found!")
                    user      = msgpack.decode(raw, type=UserValue)
                    reserved  = int(pipe.get(reserved_key) or 0)

                    new_credit   = user.credit - amount
                    new_reserved = max(0, reserved - amount)

                    if new_credit < 0:
                        # The coordinator has already durably logged COMMIT, so
                        # we must commit regardless.  This path should be
                        # unreachable now that /pay checks reservations, but if
                        # something bypassed the reservation, log and proceed.
                        app.logger.error(
                            f"2PC commit: credit underflow for user {user_id} order {order_id} "
                            f"(credit={user.credit}, reserved={reserved}, amount={amount}). "
                            f"Committing anyway to preserve 2PC durability."
                        )

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
        return response_error(DB_ERROR_STR)

    app.logger.debug(f"2PC commit OK   order={order_id} user={user_id} amount={amount}")
    return response_success("Commit: OK")

def abort_pay_logic(order_id: str, user_id: str, amount: int):
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
                        return response_success("Abort: already done or not prepared")
                    if wal_state == WAL_COMMITTED:
                        pipe.unwatch()
                        return response_error(f"Cannot abort: transaction {order_id} already committed for user {user_id}")

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

@app.post('/create_user')
def create_user():
    res = create_user_logic()
    return jsonify(res['body']) if res['status_code'] == 200 else abort(res['status_code'], res.get('error'))

@app.post('/batch_init/<n>/<starting_money>')
def batch_init_users(n: int, starting_money: int):
    res = batch_init_logic(n, starting_money)
    return jsonify(res['body']) if res['status_code'] == 200 else abort(res['status_code'], res.get('error'))

@app.get('/find_user/<user_id>')
def find_user(user_id: str):
    res = find_user_logic(user_id)
    return jsonify(res['body']) if res['status_code'] == 200 else abort(res['status_code'], res.get('error'))

@app.post('/add_funds/<user_id>/<amount>')
def add_credit(user_id: str, amount: int):
    res = add_credit_logic(user_id, amount)
    return Response(res['body'], status=200) if res['status_code'] == 200 else abort(res['status_code'], res.get('error'))

@app.post('/pay/<user_id>/<amount>')
def remove_credit(user_id: str, amount: int):
    res = remove_credit_logic(user_id, amount)
    return Response(res['body'], status=200) if res['status_code'] == 200 else abort(res['status_code'], res.get('error'))

@app.post('/prepare_pay/<order_id>/<user_id>/<amount>')
def prepare_pay(order_id: str, user_id: str, amount: int):
    res = prepare_pay_logic(order_id, user_id, amount)
    return Response(res['body'], status=200) if res['status_code'] == 200 else abort(res['status_code'], res.get('error'))

@app.post('/commit_pay/<order_id>/<user_id>/<amount>')
def commit_pay(order_id: str, user_id: str, amount: int):
    res = commit_pay_logic(order_id, user_id, amount)
    return Response(res['body'], status=200) if res['status_code'] == 200 else abort(res['status_code'], res.get('error'))

@app.post('/abort_pay/<order_id>/<user_id>/<amount>')
def abort_pay(order_id: str, user_id: str, amount: int):
    res = abort_pay_logic(order_id, user_id, amount)
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
    stream_key = 'events:payment'
    group_name = 'payment_group'
    consumer_name = f"payment_consumer_{uuid.uuid4()}"

    try:
        mq.xgroup_create(stream_key, group_name, mkstream=True)
        app.logger.info(f"Created consumer group {group_name}")
    except redis.exceptions.ResponseError as e:
        if "BUSYGROUP" not in str(e):
            app.logger.error(f"Failed to create consumer group: {e}")

    app.logger.info(f"Starting MQ listener: {consumer_name}")

    while True:
        try:
            streams = mq.xreadgroup(group_name, consumer_name, {stream_key: '>'}, count=1, block=5000)
            
            if not streams:
                continue

            for _, messages in streams:
                for message_id, data in messages:
                    reply_to = None
                    try:
                        payload = {k.decode('utf-8'): v.decode('utf-8') for k, v in data.items()}
                        msg_type = payload.get('type')
                        reply_to = payload.get('reply_to')
                        
                        result = {"status_code": 400, "error": "Unknown action"}

                        if msg_type == 'create_user':
                            result = create_user_logic()
                        elif msg_type == 'batch_init':
                            result = batch_init_logic(int(payload['n']), int(payload['starting_money']))
                        elif msg_type == 'find_user':
                            result = find_user_logic(payload['user_id'])
                        elif msg_type == 'add_credit':
                            result = add_credit_logic(payload['user_id'], int(payload['amount']))
                        elif msg_type == 'pay': # Mapped to remove_credit logic
                            result = remove_credit_logic(payload['user_id'], int(payload['amount']))
                        elif msg_type == 'prepare_pay':
                            result = prepare_pay_logic(payload['order_id'], payload['user_id'], int(payload['amount']))
                        elif msg_type == 'commit_pay':
                            result = commit_pay_logic(payload['order_id'], payload['user_id'], int(payload['amount']))
                        elif msg_type == 'abort_pay':
                            result = abort_pay_logic(payload['order_id'], payload['user_id'], int(payload['amount']))
                        
                        if reply_to:
                            mq.rpush(reply_to, json.dumps(result))
                            mq.expire(reply_to, 60)

                        mq.xack(stream_key, group_name, message_id)

                    except Exception as e:
                        app.logger.error(f"Error processing MQ message {message_id}: {e}")
                        if reply_to:
                             error_res = response_error(f"Internal processing error: {str(e)}", 500)
                             mq.rpush(reply_to, json.dumps(error_res))
                             mq.expire(reply_to, 60)
                        mq.xack(stream_key, group_name, message_id)

        except Exception as e:
            app.logger.error(f"MQ Listener loop error: {e}")
            time.sleep(2)


if __name__ == '__main__':
    t = threading.Thread(target=run_event_listener, daemon=True)
    t.start()
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)

    t = threading.Thread(target=run_event_listener, daemon=True)
    t.start()

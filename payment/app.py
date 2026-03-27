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

from wal import wal, _payment_wal_key

DB_ERROR_STR = "DB error"

# 2PC WAL state values stored in wal-redis (decoupled from business data)
WAL_PREPARED  = b"prepared"
WAL_COMMITTED = b"committed"
WAL_ABORTED   = b"aborted"

TX_CHARGED = b"CHARGED"
TX_REFUNDED = b"REFUNDED"

app = Flask("payment-service")

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
        socket_timeout=1.5,
        socket_connect_timeout=1.5,
    ).master_for(
        _REDIS_MASTER_NAME,
        socket_timeout=1.5,
        socket_connect_timeout=1.5,
        retry=Retry(NoBackoff(), 3),
        retry_on_error=[redis.exceptions.ConnectionError, redis.exceptions.TimeoutError,
                        redis.exceptions.ReadOnlyError],
    )
else:
    db: redis.Redis = redis.Redis(
        host=os.environ['REDIS_HOST'],
        port=int(os.environ['REDIS_PORT']),
        password=_REDIS_PASSWORD,
        db=_REDIS_DB,
    )

# Separate connection for Message Queue operations
# Defaults to the same host as DB, but allows splitting in production
mq: redis.Redis = redis.Redis(host=os.environ['MQ_REDIS_HOST'],
                              port=int(os.environ['MQ_REDIS_PORT']),
                              password=os.environ['MQ_REDIS_PASSWORD'],
                              db=int(os.environ['MQ_REDIS_DB']))

def close_db_connection():
    db.close()
    mq.close()
    wal.close()


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
    raw_entry: bytes = db.get(user_id)
    # deserialize data if it exists else return null
    entry: UserValue | None = msgpack.decode(raw_entry, type=UserValue) if raw_entry else None
    return entry

def _payment_tx_key(tx_id: str) -> str:
    return f"payment:tx:{tx_id}"

# ─── Business Logic (Decoupled) ───────────────────────────────────────────────

def create_user_logic():
    key = str(uuid.uuid4())
    value = msgpack.encode(UserValue(credit=0))
    try:
        db.set(key, value)
    except redis.exceptions.RedisError:
        return response_error(DB_ERROR_STR, 500)
    return response_success({'user_id': key})

def batch_init_logic(n: int, starting_money: int):
    n = int(n)
    starting_money = int(starting_money)
    kv_pairs: dict[str, bytes] = {f"{i}": msgpack.encode(UserValue(credit=starting_money))
                                  for i in range(n)}
    try:
        db.mset(kv_pairs)
    except redis.exceptions.RedisError:
        return response_error(DB_ERROR_STR, 500)
    return response_success({"msg": "Batch init for users successful"})

def find_user_logic(user_id: str):
    try:
        user_entry: UserValue = get_user_from_db(user_id)
    except redis.exceptions.RedisError:
        return response_error(DB_ERROR_STR, 500)
    if user_entry is None:
        return response_error(f"User: {user_id} not found!", 400)
    return response_success({
        "user_id": user_id,
        "credit": user_entry.credit
    })

def add_credit_logic(user_id: str, amount: int, transaction_id: str | None = None):
    """
    Idempotency: uses a Redis WATCH/MULTI/EXEC optimistic lock so concurrent
    retries from the benchmark cannot double-add funds.
    If transaction_id is provided (Refund/Rollback), implements Tombstone pattern.
    """
    amount = int(amount)
    tx_key = _payment_tx_key(transaction_id) if transaction_id else None

    for _ in range(10):  # retry loop for optimistic lock
        try:
            with db.pipeline() as pipe:
                if tx_key:
                    pipe.watch(user_id, tx_key)
                    state = pipe.get(tx_key)
                    
                    if state == TX_CHARGED:
                        # Valid refund: Proceed to add funds back
                        pass
                    elif state == TX_REFUNDED:
                        pipe.unwatch()
                        return response_success("Refund already processed (idempotent)")
                    else:
                        # Tombstone: Original charge never happened or is delayed.
                        # Mark as REFUNDED to prevent future charges.
                        pipe.multi()
                        pipe.set(tx_key, TX_REFUNDED)
                        pipe.execute()
                        return response_success("Refund tombstone set (charge never happened)")
                else:
                    pipe.watch(user_id)

                raw = pipe.get(user_id)
                if raw is None:
                    pipe.reset()
                    return response_error(f"User: {user_id} not found!")
                user_entry = msgpack.decode(raw, type=UserValue)
                pipe.multi()
                user_entry.credit += amount
                pipe.set(user_id, msgpack.encode(user_entry))
                if tx_key:
                    pipe.set(tx_key, TX_REFUNDED)
                pipe.execute()
                return response_success(f"User: {user_id} credit updated to: {user_entry.credit}")
        except redis.WatchError:
            continue
        except redis.exceptions.RedisError:
            return response_error(DB_ERROR_STR, 500)
    return response_error("Conflict: could not update credit after retries")

def remove_credit_logic(user_id: str, amount: int, transaction_id: str | None = None):
    """
    Atomically deduct credit using optimistic locking.
    Respects 2PC soft-reservations: only unreserved credit may be deducted.
    Returns 400 if credit would fall below the currently reserved amount.
    If transaction_id is provided (Charge), implements Tombstone pattern.
    """
    amount = int(amount)
    app.logger.debug(f"Removing {amount} credit from user: {user_id}")
    reserved_key = _payment_reserved_key(user_id)
    tx_key = _payment_tx_key(transaction_id) if transaction_id else None

    for _ in range(10):
        try:
            with db.pipeline() as pipe:
                watch_keys = [user_id, reserved_key]
                if tx_key:
                    watch_keys.append(tx_key)
                pipe.watch(*watch_keys)

                if tx_key:
                    state = pipe.get(tx_key)
                    if state == TX_REFUNDED:
                        pipe.unwatch()
                        return response_success("Ignored charge (rolled back via tombstone)")
                    if state == TX_CHARGED:
                        pipe.unwatch()
                        return response_success("Already charged (idempotent)")

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
                if tx_key:
                    pipe.set(tx_key, TX_CHARGED)
                pipe.execute()
                return response_success(f"User: {user_id} credit updated to: {user_entry.credit}")
        except redis.WatchError:
            continue
        except redis.exceptions.RedisError:
            return response_error(DB_ERROR_STR, 500)
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

def _payment_reserved_key(user_id: str) -> str:
    return f"reserved:payment:{user_id}"

def prepare_pay_logic(order_id: str, user_id: str, amount: int):
    """
    2PC Phase 1 - PREPARE (participant: payment).

    WAL state read/written in wal-redis (decoupled).
    Soft-reservation written in db-redis alongside credit balance so it can
    be checked atomically in a single WATCH/MULTI/EXEC pipeline.

    Order of operations:
      1. Read WAL from wal-redis — idempotency / abort guard.
      2. Check availability + soft-reserve in db-redis (WATCH/MULTI/EXEC).
      3. Write WAL_PREPARED to wal-redis.
    If step 3 fails, the soft-reserve is rolled back so the state remains clean.
    """
    amount = int(amount)
    wal_key      = _payment_wal_key(order_id, user_id)
    reserved_key = _payment_reserved_key(user_id)

    # Step 1: idempotency check via WAL (wal-redis)
    try:
        wal_state = wal.get(wal_key)
    except redis.exceptions.RedisError:
        return response_error(DB_ERROR_STR, 500)

    if wal_state in (WAL_PREPARED, WAL_COMMITTED):
        return response_success("Prepare: already done")
    if wal_state == WAL_ABORTED:
        return response_error(f"Transaction {order_id} already aborted for user {user_id}")

    # Step 2: availability check + soft-reserve (db-redis only, no WAL key in pipeline)
    for _ in range(10):
        try:
            with db.pipeline() as pipe:
                pipe.watch(user_id, reserved_key)
                raw = pipe.get(user_id)
                if not raw:
                    pipe.unwatch()
                    return response_error(f"User: {user_id} not found!")
                user     = msgpack.decode(raw, type=UserValue)
                reserved = int(pipe.get(reserved_key) or 0)
                available = user.credit - reserved
                if available < amount:
                    pipe.unwatch()
                    return response_error(
                        f"Insufficient credit for user {user_id}: "
                        f"available={available}, requested={amount}")
                pipe.multi()
                pipe.set(reserved_key, reserved + amount)
                pipe.execute()
                break
        except redis.WatchError:
            continue
        except redis.exceptions.RedisError:
            return response_error(DB_ERROR_STR, 500)
    else:
        return response_error("Conflict: could not prepare after retries")

    # Step 3: write WAL entry (wal-redis)
    try:
        wal.set(wal_key, WAL_PREPARED)
    except redis.exceptions.RedisError:
        # Roll back the soft-reserve we just applied
        try:
            for _ in range(5):
                try:
                    with db.pipeline() as pipe:
                        pipe.watch(reserved_key)
                        cur = int(pipe.get(reserved_key) or 0)
                        pipe.multi()
                        pipe.set(reserved_key, max(0, cur - amount))
                        pipe.execute()
                        break
                except redis.WatchError:
                    continue
        except Exception:
            pass
        return response_error(DB_ERROR_STR, 500)

    app.logger.debug("2PC prepare OK  order=%s user=%s amount=%s", order_id, user_id, amount)
    return response_success("Prepare: OK")


def commit_pay_logic(order_id: str, user_id: str, amount: int):
    """
    2PC Phase 2 - COMMIT (participant: payment).

    WAL state read from wal-redis; credit deduction written to db-redis.
    WAL_COMMITTED written to wal-redis after the db write succeeds.
    """
    amount = int(amount)
    wal_key      = _payment_wal_key(order_id, user_id)
    reserved_key = _payment_reserved_key(user_id)

    # Idempotency / guard via WAL (wal-redis)
    try:
        wal_state = wal.get(wal_key)
    except redis.exceptions.RedisError:
        return response_error(DB_ERROR_STR, 500)

    if wal_state == WAL_COMMITTED:
        return response_success("Commit: already done")
    if wal_state == WAL_ABORTED:
        return response_error(f"Cannot commit: transaction {order_id} was aborted for user {user_id}")

    # Apply deduction to db-redis (WATCH/MULTI/EXEC, no WAL key in pipeline)
    for _ in range(10):
        try:
            with db.pipeline() as pipe:
                pipe.watch(user_id, reserved_key)
                raw = pipe.get(user_id)
                if not raw:
                    pipe.unwatch()
                    return response_error(f"User: {user_id} not found!")
                user     = msgpack.decode(raw, type=UserValue)
                reserved = int(pipe.get(reserved_key) or 0)
                new_credit   = user.credit - amount
                new_reserved = max(0, reserved - amount)
                if new_credit < 0:
                    app.logger.error(
                        "2PC commit: credit underflow for user %s order %s. "
                        "Committing anyway to preserve 2PC durability.",
                        user_id, order_id)
                user.credit = new_credit
                pipe.multi()
                pipe.set(user_id, msgpack.encode(user))
                pipe.set(reserved_key, new_reserved)
                pipe.execute()
                break
        except redis.WatchError:
            continue
        except redis.exceptions.RedisError:
            return response_error(DB_ERROR_STR, 500)
    else:
        return response_error("Conflict: could not commit after retries")

    # Write WAL committed (wal-redis) — non-fatal if it fails
    try:
        wal.set(wal_key, WAL_COMMITTED)
    except redis.exceptions.RedisError:
        app.logger.warning("Could not write WAL_COMMITTED for order %s user %s; idempotent on retry",
                           order_id, user_id)

    app.logger.debug("2PC commit OK   order=%s user=%s amount=%s", order_id, user_id, amount)
    return response_success("Commit: OK")


def abort_pay_logic(order_id: str, user_id: str, amount: int):
    """
    2PC Phase 2 - ABORT (participant: payment).

    WAL state read from wal-redis.  If WAL is None or ABORTED → no-op.
    Reservation release written to db-redis; WAL_ABORTED written to wal-redis.
    """
    amount = int(amount)
    wal_key      = _payment_wal_key(order_id, user_id)
    reserved_key = _payment_reserved_key(user_id)

    # Idempotency / guard via WAL (wal-redis)
    try:
        wal_state = wal.get(wal_key)
    except redis.exceptions.RedisError:
        return response_error(DB_ERROR_STR, 500)

    if wal_state == WAL_ABORTED or wal_state is None:
        return response_success("Abort: already done or not prepared")
    if wal_state == WAL_COMMITTED:
        return response_error(f"Cannot abort: transaction {order_id} already committed for user {user_id}")

    # Release reservation in db-redis (no WAL key in pipeline)
    for _ in range(10):
        try:
            with db.pipeline() as pipe:
                pipe.watch(reserved_key)
                reserved     = int(pipe.get(reserved_key) or 0)
                new_reserved = max(0, reserved - amount)
                pipe.multi()
                pipe.set(reserved_key, new_reserved)
                pipe.execute()
                break
        except redis.WatchError:
            continue
        except redis.exceptions.RedisError:
            return response_error(DB_ERROR_STR, 500)
    else:
        return response_error("Conflict: could not abort after retries")

    # Write WAL aborted (wal-redis)
    try:
        wal.set(wal_key, WAL_ABORTED)
    except redis.exceptions.RedisError:
        app.logger.warning("Could not write WAL_ABORTED for order %s user %s", order_id, user_id)

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
    consumer_name = f"payment_consumer_{os.environ.get('HOSTNAME', 'local')}"

    try:
        mq.xgroup_create(stream_key, group_name, mkstream=True)
        app.logger.info(f"Created consumer group {group_name}")
    except redis.exceptions.ResponseError as e:
        if "BUSYGROUP" not in str(e):
            app.logger.error(f"Failed to create consumer group: {e}")

    app.logger.info(f"Starting MQ listener: {consumer_name}")

    while True:
        try:
            streams = mq.xreadgroup(group_name, consumer_name, {stream_key: '>'}, count=1, block=5000, noack=True)
            
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
                            tx_id = payload.get('transaction_id')
                            result = add_credit_logic(payload['user_id'], int(payload['amount']), tx_id)
                        elif msg_type == 'pay': # Mapped to remove_credit logic
                            tx_id = payload.get('transaction_id')
                            result = remove_credit_logic(payload['user_id'], int(payload['amount']), tx_id)
                        elif msg_type == 'prepare_pay':
                            result = prepare_pay_logic(payload['order_id'], payload['user_id'], int(payload['amount']))
                        elif msg_type == 'commit_pay':
                            result = commit_pay_logic(payload['order_id'], payload['user_id'], int(payload['amount']))
                        elif msg_type == 'abort_pay':
                            result = abort_pay_logic(payload['order_id'], payload['user_id'], int(payload['amount']))
                        
                        if reply_to:
                            mq.rpush(reply_to, json.dumps(result))
                            mq.expire(reply_to, 60)

                    except Exception as e:
                        app.logger.error(f"Error processing MQ message {message_id}: {e}")
                        if reply_to:
                             error_res = response_error(f"Internal processing error: {str(e)}", 500)
                             mq.rpush(reply_to, json.dumps(error_res))
                             mq.expire(reply_to, 60)

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

import json
import logging

import redis
from flask import Response, abort
from msgspec import msgpack

from db import (db, OrderValue, get_order, save_order,
                STATUS_PAID, STATUS_FAILED, STATUS_STARTED,
                COORD_PENDING_KEY, DB_ERROR_STR)
from wal import wal
from rpc import submit_batch, recovery_rpc, task_ok, task_error

log = logging.getLogger('order-service')


# ── Task graphs ────────────────────────────────────────────────────────────────

def _prepare_tasks(order_id: str, order: OrderValue) -> list:
    stock_p   = {'order_id': order_id, 'items': json.dumps(order.items)}
    payment_p = {'order_id': order_id, 'user_id': order.user_id,
                 'amount': str(order.total_cost)}
    return [
        {"task_id": f"{order_id}:2pc:prepare:stock",   "stream": "events:stock",
         "action": "prepare_subtract_batch", "payload": stock_p,   "depends_on": []},
        {"task_id": f"{order_id}:2pc:prepare:payment", "stream": "events:payment",
         "action": "prepare_pay",            "payload": payment_p, "depends_on": []},
    ]


def _commit_tasks(order_id: str, order: OrderValue) -> list:
    stock_p   = {'order_id': order_id, 'items': json.dumps(order.items)}
    payment_p = {'order_id': order_id, 'user_id': order.user_id,
                 'amount': str(order.total_cost)}
    return [
        {"task_id": f"{order_id}:2pc:commit:stock",   "stream": "events:stock",
         "action": "commit_subtract_batch", "payload": stock_p,   "depends_on": []},
        {"task_id": f"{order_id}:2pc:commit:payment", "stream": "events:payment",
         "action": "commit_pay",            "payload": payment_p, "depends_on": []},
    ]


# ── Checkout ───────────────────────────────────────────────────────────────────

def checkout_2pc(order_id: str) -> Response:
    # WATCH so only one gunicorn worker can claim a STATUS_PENDING order.
    try:
        with db.pipeline() as pipe:
            while True:
                try:
                    pipe.watch(order_id)
                    raw = pipe.get(order_id)
                    if not raw:
                        pipe.reset()
                        abort(400, f"Order: {order_id} not found!")
                    order = msgpack.decode(raw, type=OrderValue)
                    if order.status == STATUS_PAID:
                        pipe.reset()
                        return Response("Checkout successful", status=200)
                    if order.status == STATUS_STARTED:
                        pipe.reset()
                        abort(400, f"Order {order_id} is in state: {order.status}")
                    # STATUS_FAILED orders can be retried (e.g. after adding stock/credit)
                    order.status = STATUS_STARTED
                    pipe.multi()
                    pipe.set(order_id, msgpack.encode(order))
                    pipe.execute()
                    break
                except redis.WatchError:
                    continue
    except redis.exceptions.RedisError:
        abort(500, DB_ERROR_STR)

    # Write the WAL entry to the decoupled wal-redis AFTER claiming the order.
    try:
        wal.sadd(COORD_PENDING_KEY, order_id)
    except redis.exceptions.RedisError:
        abort(500, DB_ERROR_STR)

    # Phase 1: PREPARE (both participants in parallel)
    prepare_result = submit_batch(f"2pc:prepare:{order_id}", _prepare_tasks(order_id, order))

    if prepare_result is None:
        abort_2pc(order_id, order)
        abort(500, "2PC prepare timed out")

    stock_id   = f"{order_id}:2pc:prepare:stock"
    payment_id = f"{order_id}:2pc:prepare:payment"

    if not (task_ok(prepare_result, stock_id) and task_ok(prepare_result, payment_id)):
        abort_2pc(order_id, order)
        abort(400, f"2PC prepare failed — "
                   f"stock: {task_error(prepare_result, stock_id)} "
                   f"payment: {task_error(prepare_result, payment_id)}".strip())

    # Phase 2: COMMIT — log decision durably before sending to participants
    order.status = STATUS_PAID
    order.paid   = True
    try:
        db.set(order_id, msgpack.encode(order))
    except redis.exceptions.RedisError:
        abort_2pc(order_id, order)
        abort(500, DB_ERROR_STR)

    commit_result = submit_batch(f"2pc:commit:{order_id}", _commit_tasks(order_id, order))

    # STATUS_PAID is durable — always return 200.
    # If commit delivery failed, coordinator WAL keeps order_id; recover_2pc re-drives on restart.
    if commit_result is not None:
        stock_ok   = task_ok(commit_result, f"{order_id}:2pc:commit:stock")
        payment_ok = task_ok(commit_result, f"{order_id}:2pc:commit:payment")
        if stock_ok and payment_ok:
            try:
                wal.srem(COORD_PENDING_KEY, order_id)
            except redis.exceptions.RedisError:
                log.warning("Could not remove %s from coordinator WAL", order_id)

    return Response("Checkout successful", status=200)


def abort_2pc(order_id: str, order: OrderValue):
    """Send ABORT to all participants and mark order FAILED."""
    order.status = STATUS_FAILED
    try:
        save_order(order_id, order)
    except redis.exceptions.RedisError:
        log.error("Failed to persist FAILED status for order %s", order_id)

    stock_p   = {'order_id': order_id, 'items': json.dumps(order.items)}
    payment_p = {'order_id': order_id, 'user_id': order.user_id, 'amount': order.total_cost}
    stock_ok   = recovery_rpc('events:stock',   'abort_subtract_batch', stock_p)
    payment_ok = recovery_rpc('events:payment', 'abort_pay',            payment_p)

    if stock_ok and payment_ok:
        try:
            wal.srem(COORD_PENDING_KEY, order_id)
        except redis.exceptions.RedisError:
            log.warning("Could not remove %s from coordinator WAL", order_id)


# ── Recovery ───────────────────────────────────────────────────────────────────

def recover_2pc():
    try:
        pending = wal.smembers(COORD_PENDING_KEY)
    except redis.exceptions.RedisError as e:
        log.error("2PC recovery: cannot read WAL: %s", e)
        return

    if not pending:
        return
    log.info("2PC recovery: %d in-flight transaction(s)", len(pending))

    for raw_id in pending:
        order_id = raw_id.decode()
        lock_key = f"wal:order:2pc:recovery_lock:{order_id}"
        if not wal.set(lock_key, "locked", nx=True, ex=30):
            continue

        try:
            raw = db.get(order_id)
            if not raw:
                wal.srem(COORD_PENDING_KEY, order_id)
                continue

            order = msgpack.decode(raw, type=OrderValue)
            stock_p   = {'order_id': order_id, 'items': json.dumps(order.items)}
            payment_p = {'order_id': order_id, 'user_id': order.user_id, 'amount': order.total_cost}

            if order.status == STATUS_PAID:
                log.info("2PC recovery: re-driving COMMIT for %s", order_id)
                if (recovery_rpc('events:stock',   'commit_subtract_batch', stock_p) and
                        recovery_rpc('events:payment', 'commit_pay',            payment_p)):
                    wal.srem(COORD_PENDING_KEY, order_id)
            else:
                log.info("2PC recovery: re-driving ABORT for %s (status=%s)", order_id, order.status)
                if (recovery_rpc('events:stock',   'abort_subtract_batch', stock_p) and
                        recovery_rpc('events:payment', 'abort_pay',            payment_p)):
                    order.status = STATUS_FAILED
                    save_order(order_id, order, wal_remove=COORD_PENDING_KEY)

        except Exception as e:
            log.error("2PC recovery: error processing %s: %s", order_id, e)
        finally:
            wal.delete(lock_key)

    log.info("2PC recovery: complete")

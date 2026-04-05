import json
import logging
import redis
from flask import Response, abort
from msgspec import msgpack

from db import (db, OrderValue, get_order, save_order,
                STATUS_PAID, STATUS_FAILED, STATUS_STARTED,
                SAGA_PENDING_KEY, DB_ERROR_STR)
from wal import wal, SAGA_TX_PREFIX, SAGA_ATTEMPT_PREFIX
from rpc import submit_batch, recovery_rpc, task_ok, task_error

log = logging.getLogger('order-service')

def _saga_tasks(order_id: str, order: OrderValue, tx_id: str) -> list:
    return [
        {
            "task_id": f"{order_id}:saga:stock",
            "stream": "events:stock",
            "action": "subtract_stock_batch",
            "payload": {"items": json.dumps(order.items), "transaction_id": tx_id},
            "depends_on": [],
        },
        {
            "task_id": f"{order_id}:saga:payment",
            "stream": "events:payment",
            "action": "pay",
            "payload": {"user_id": order.user_id, "amount": str(order.total_cost),
                        "transaction_id": tx_id},
            "depends_on": [f"{order_id}:saga:stock"],
        },
    ]

def checkout_saga(order_id: str) -> Response:
    order = get_order(order_id)

    if order.status == STATUS_PAID:
        return Response("Checkout successful", status=200)
    if order.status == STATUS_STARTED:
        abort(400, f"Order {order_id} is in state: {order.status}")
    # STATUS_FAILED orders can be retried (e.g. after adding stock/credit)

    try:
        attempt = wal.incr(f"{SAGA_ATTEMPT_PREFIX}{order_id}")
        tx_id = f"{order_id}_{attempt}"
        wal.set(f"{SAGA_TX_PREFIX}{order_id}", tx_id)
    except redis.exceptions.RedisError:
        abort(500, DB_ERROR_STR)

    order.status = STATUS_STARTED
    try:
        save_order(order_id, order, wal_add=SAGA_PENDING_KEY)
    except redis.exceptions.RedisError:
        abort(500, DB_ERROR_STR)

    result = submit_batch(f"saga:{tx_id}", _saga_tasks(order_id, order, tx_id))

    if result is None:
        log.error("Saga %s: orchestrator timed out", order_id)
        abort(500, "Checkout timed out waiting for orchestrator")

    stock_id   = f"{order_id}:saga:stock"
    payment_id = f"{order_id}:saga:payment"

    if task_ok(result, stock_id) and task_ok(result, payment_id):
        order.status = STATUS_PAID
        order.paid   = True
        try:
            save_order(order_id, order, wal_remove=SAGA_PENDING_KEY)
        except redis.exceptions.RedisError:
            abort(500, DB_ERROR_STR)
        return Response("Checkout successful", status=200)

    compensated = True
    if task_ok(result, stock_id):  # stock ok but payment failed — compensate
        compensated = recovery_rpc('events:stock', 'add_stock_batch',
                                   {'items': json.dumps(order.items), 'transaction_id': tx_id})

    order.status = STATUS_FAILED
    try:
        save_order(order_id, order,
                   wal_remove=SAGA_PENDING_KEY if compensated else None)
    except redis.exceptions.RedisError:
        log.error("Failed to persist FAILED status for saga %s", order_id)

    abort(400, task_error(result, payment_id) or task_error(result, stock_id) or "Checkout failed")

def recover_saga():
    try:
        pending = wal.smembers(SAGA_PENDING_KEY)
    except redis.exceptions.RedisError as e:
        log.error("Saga recovery: cannot read WAL: %s", e)
        return

    if not pending:
        return
    log.info("Saga recovery: %d in-flight saga(s)", len(pending))

    for raw_id in pending:
        order_id = raw_id.decode()
        lock_key = f"wal:order:saga:recovery_lock:{order_id}"
        if not wal.set(lock_key, "locked", nx=True, ex=30):
            continue

        try:
            raw = db.get(order_id)
            if not raw:
                wal.srem(SAGA_PENDING_KEY, order_id)
                continue

            order = msgpack.decode(raw, type=OrderValue)

            if order.status != STATUS_STARTED:
                wal.srem(SAGA_PENDING_KEY, order_id)
                continue

            raw_tx = wal.get(f"{SAGA_TX_PREFIX}{order_id}")
            tx_id = raw_tx.decode() if raw_tx else order_id

            log.warning("Saga recovery: rolling back order %s (tx=%s)", order_id, tx_id)
            stock_ok   = recovery_rpc('events:stock', 'add_stock_batch',
                                      {'items': json.dumps(order.items),
                                       'transaction_id': tx_id})
            payment_ok = recovery_rpc('events:payment', 'add_credit',
                                      {'user_id': order.user_id,
                                       'amount': order.total_cost,
                                       'transaction_id': tx_id})
            if stock_ok and payment_ok:
                order.status = STATUS_FAILED
                save_order(order_id, order, wal_remove=SAGA_PENDING_KEY)

        except Exception as e:
            log.error("Saga recovery: error processing %s: %s", order_id, e)
        finally:
            wal.delete(lock_key)

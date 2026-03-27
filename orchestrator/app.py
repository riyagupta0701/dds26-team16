import json
import logging
import os
import time
import threading

import redis

from wal import (
    wal, persist_batch, load_batch, complete_batch,
    acquire_lock, release_lock, pending_batch_ids, remove_from_pending,
    PENDING_KEY,
)

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

# ── Configuration ──────────────────────────────────────────────────────────────
MQ_REDIS_HOST     = os.environ['MQ_REDIS_HOST']
MQ_REDIS_PORT     = int(os.environ['MQ_REDIS_PORT'])
MQ_REDIS_PASSWORD = os.environ['MQ_REDIS_PASSWORD']
MQ_REDIS_DB       = int(os.environ['MQ_REDIS_DB'])

MAX_RETRIES    = int(os.environ.get('ORCH_MAX_RETRIES', '3'))
TASK_TIMEOUT_S = int(os.environ.get('ORCH_TASK_TIMEOUT_S', '5'))

ORCH_STREAM   = 'events:orchestrator'
ORCH_GROUP    = 'orchestrator_group'
ORCH_CONSUMER = os.environ.get('ORCH_CONSUMER_NAME', 'orchestrator-1')

# Task states
S_PENDING         = 'PENDING'
S_IN_PROGRESS     = 'IN_PROGRESS'
S_DELIVERED_OK    = 'DELIVERED_OK'
S_DELIVERED_ERR   = 'DELIVERED_ERR'
S_DELIVERY_FAILED = 'DELIVERY_FAILED'
S_SKIPPED         = 'SKIPPED'

TERMINAL_STATES = {S_DELIVERED_OK, S_DELIVERED_ERR, S_DELIVERY_FAILED, S_SKIPPED}
FAILURE_STATES  = {S_DELIVERED_ERR, S_DELIVERY_FAILED}

# Batch statuses
B_RUNNING   = 'RUNNING'
B_COMPLETED = 'COMPLETED'
B_FAILED    = 'FAILED'

# ── MQ connection (transient — not the WAL store) ──────────────────────────────
mq: redis.Redis = redis.Redis(
    host=MQ_REDIS_HOST, port=MQ_REDIS_PORT,
    password=MQ_REDIS_PASSWORD, db=MQ_REDIS_DB,
)

# ── Task dispatch ──────────────────────────────────────────────────────────────
def dispatch_task(task: dict) -> dict:
    """
    Send one task to its target stream and block on a per-attempt reply key.
    Retries up to MAX_RETRIES times on timeout only.
    Any response (200 or non-200) is final — no retry on error responses.
    Returns updated task dict with final state.
    """
    task_id = task['task_id']

    for attempt in range(MAX_RETRIES):
        reply_key = f'reply:task:{task_id}:attempt:{attempt}'
        message = {
            'type': task['action'],
            'reply_to': reply_key,
            **{k: str(v) for k, v in task['payload'].items()},
        }
        try:
            mq.xadd(task['stream'], message)
            resp = mq.blpop(reply_key, timeout=TASK_TIMEOUT_S)
            if resp is not None:
                val = json.loads(resp[1])
                status_code = val.get('status_code', 500)
                state = S_DELIVERED_OK if status_code == 200 else S_DELIVERED_ERR
                logger.info('Task %s attempt %d: %s (code=%s)', task_id, attempt, state, status_code)
                return {**task, 'state': state, 'status_code': status_code,
                        'body': val.get('body'), 'error': val.get('error')}
            else:
                logger.warning('Task %s attempt %d timed out, retrying', task_id, attempt)
        except Exception as exc:
            logger.error('Task %s attempt %d exception: %s', task_id, attempt, exc)

    logger.error('Task %s DELIVERY_FAILED after %d attempts', task_id, MAX_RETRIES)
    return {**task, 'state': S_DELIVERY_FAILED, 'status_code': None, 'body': None,
            'error': f'No response after {MAX_RETRIES} attempts'}


# ── Batch processor ────────────────────────────────────────────────────────────
def _all_terminal(tasks_by_id: dict) -> bool:
    return all(t['state'] in TERMINAL_STATES for t in tasks_by_id.values())


def _cascade_skipped(tasks_by_id: dict) -> None:
    """Propagate SKIPPED to tasks whose dependency ended in a failure state."""
    changed = True
    while changed:
        changed = False
        for task in tasks_by_id.values():
            if task['state'] != S_PENDING:
                continue
            for dep_id in task.get('depends_on', []):
                dep = tasks_by_id.get(dep_id)
                if dep and dep['state'] in FAILURE_STATES:
                    task['state'] = S_SKIPPED
                    logger.info('Task %s SKIPPED (dep %s → %s)',
                                task['task_id'], dep_id, dep['state'])
                    changed = True
                    break


def process_batch(batch: dict) -> None:
    """
    Wave-based dependency executor. Runs in its own daemon thread per batch.

    WAL contract (all writes go to wal-redis via wal.py):
      1. persist_batch() is called with status=IN_PROGRESS BEFORE dispatching
         each wave, so a crash during dispatch leaves the WAL in a state that
         recovery can re-drive (tasks reset from IN_PROGRESS → PENDING).
      2. persist_batch() is called again AFTER each wave with the results.
      3. complete_batch() is called on final success or failure; it updates the
         JSON snapshot and removes the batch from wal:orch:pending.

    The MQ reply (rpush to reply_to) happens AFTER the WAL write, so the caller
    (order service) only gets a result once the WAL is settled.
    """
    batch_id = batch['batch_id']
    reply_to = batch['reply_to']

    # Normalize tasks to a dict keyed by task_id
    raw_tasks = batch.get('tasks', {})
    if isinstance(raw_tasks, list):
        tasks_by_id = {t['task_id']: dict(t) for t in raw_tasks}
    else:
        tasks_by_id = {k: dict(v) for k, v in raw_tasks.items()}

    logger.info('process_batch %s: %d task(s)', batch_id, len(tasks_by_id))

    while not _all_terminal(tasks_by_id):
        _cascade_skipped(tasks_by_id)
        if _all_terminal(tasks_by_id):
            break

        # Tasks ready to dispatch: PENDING with all deps DELIVERED_OK
        ready = [
            t for t in tasks_by_id.values()
            if t['state'] == S_PENDING
            and all(
                tasks_by_id.get(d, {}).get('state') == S_DELIVERED_OK
                for d in t.get('depends_on', [])
            )
        ]

        if not ready:
            logger.error('process_batch %s: no progress possible, marking remaining DELIVERY_FAILED', batch_id)
            for t in tasks_by_id.values():
                if t['state'] == S_PENDING:
                    t['state'] = S_DELIVERY_FAILED
                    t['error'] = 'Deadlock: no progress possible'
            break

        # ── Pre-wave WAL write ─────────────────────────────────────────────────
        # Mark tasks IN_PROGRESS and persist to WAL *before* dispatching.
        # If we crash during dispatch, recovery sees IN_PROGRESS and resets to PENDING.
        for t in ready:
            tasks_by_id[t['task_id']]['state'] = S_IN_PROGRESS
        batch['tasks'] = tasks_by_id
        try:
            persist_batch(batch)
        except redis.exceptions.RedisError:
            logger.error('process_batch %s: cannot write pre-wave WAL, aborting batch', batch_id)
            # Cannot proceed safely without a WAL entry — mark failed and exit.
            for t in tasks_by_id.values():
                if t['state'] == S_IN_PROGRESS:
                    t['state'] = S_DELIVERY_FAILED
                    t['error'] = 'WAL write failed before dispatch'
            break

        # ── Dispatch ready tasks concurrently ─────────────────────────────────
        results: list[dict] = []
        lock = threading.Lock()

        def _dispatch(t, results=results, lock=lock):
            r = dispatch_task(t)
            with lock:
                results.append(r)

        threads = [threading.Thread(target=_dispatch, args=(dict(t),), daemon=True) for t in ready]
        for th in threads:
            th.start()
        for th in threads:
            th.join()

        # Merge results back and cascade
        for r in results:
            tasks_by_id[r['task_id']] = r
        _cascade_skipped(tasks_by_id)

        # ── Post-wave WAL write ────────────────────────────────────────────────
        batch['tasks'] = tasks_by_id
        try:
            persist_batch(batch)
        except redis.exceptions.RedisError:
            logger.error('process_batch %s: cannot write post-wave WAL (continuing)', batch_id)

    # ── Finalise ───────────────────────────────────────────────────────────────
    has_delivery_failed = any(t['state'] == S_DELIVERY_FAILED for t in tasks_by_id.values())
    batch_status = B_FAILED if has_delivery_failed else B_COMPLETED
    batch['status'] = batch_status
    batch['tasks']  = tasks_by_id

    result_tasks = {
        tid: {
            'state':       t['state'],
            'status_code': t.get('status_code'),
            'body':        t.get('body'),
            'error':       t.get('error'),
        }
        for tid, t in tasks_by_id.items()
    }
    reply_payload = json.dumps({
        'batch_id': batch_id,
        'status':   batch_status,
        'tasks':    result_tasks,
    })

    try:
        # Write final WAL state (removes from pending) before replying to caller.
        complete_batch(batch)
        mq.rpush(reply_to, reply_payload)
        mq.expire(reply_to, 120)
    except Exception as exc:
        logger.error('process_batch %s: failed to complete WAL or push reply: %s', batch_id, exc)
        # Best-effort: try pushing the reply even if WAL cleanup failed.
        # wal:orch:pending still holds the batch_id; recovery will re-drive
        # (idempotent) and clean up on next startup.
        try:
            mq.rpush(reply_to, reply_payload)
            mq.expire(reply_to, 120)
        except Exception:
            pass

    logger.info('process_batch %s: %s', batch_id, batch_status)


# ── Recovery ───────────────────────────────────────────────────────────────────
def recover_batches() -> None:
    """
    Startup recovery sweep. Reads wal:orch:pending from wal-redis and
    re-dispatches any batch that was in-flight when a previous instance crashed.

    Uses distributed locks (wal:orch:lock:{batch_id}) so two orchestrator
    replicas racing at startup do not both re-drive the same batch.

    Participant services are all idempotent (tombstone / WAL state machine),
    so re-driving a batch that was partially completed is safe.
    """
    time.sleep(5)  # give other services time to come up first

    batch_ids = pending_batch_ids()
    if not batch_ids:
        logger.info('Recovery: no in-flight batches')
        return

    logger.info('Recovery: found %d in-flight batch(es)', len(batch_ids))

    for batch_id in batch_ids:
        if not acquire_lock(batch_id):
            logger.info('Recovery: batch %s claimed by another instance, skipping', batch_id)
            continue

        try:
            batch = load_batch(batch_id)
            if not batch:
                logger.warning('Recovery: batch %s not found in WAL, removing from pending', batch_id)
                remove_from_pending(batch_id)
                continue

            if batch.get('status') in (B_COMPLETED, B_FAILED):
                # Terminal but pending set was not cleaned — just clean up.
                remove_from_pending(batch_id)
                continue

            # Reset any IN_PROGRESS tasks to PENDING so they are re-dispatched.
            # Participant idempotency (tombstones / WAL state machines in wal-redis)
            # prevents double-apply of already-committed tasks.
            tasks = batch.get('tasks', {})
            if isinstance(tasks, dict):
                for t in tasks.values():
                    if t.get('state') == S_IN_PROGRESS:
                        t['state'] = S_PENDING
                        logger.info('Recovery: reset task %s to PENDING', t['task_id'])
                batch['tasks'] = tasks

            logger.info('Recovery: re-dispatching batch %s', batch_id)
            threading.Thread(target=process_batch, args=(batch,), daemon=True).start()

        except Exception as exc:
            logger.error('Recovery: failed to process batch %s: %s', batch_id, exc)
        finally:
            release_lock(batch_id)


# ── Stream listener ────────────────────────────────────────────────────────────
def run_listener() -> None:
    """
    Consume messages from events:orchestrator stream.
    For each submit_batch message:
      1. Build batch dict with all tasks in PENDING state.
      2. Write to WAL (persist_batch) and add to wal:orch:pending BEFORE
         spawning the processing thread. This guarantees the batch is
         recoverable even if the process crashes between accept and dispatch.
      3. Spawn a daemon thread to process the batch.
    """
    try:
        mq.xgroup_create(ORCH_STREAM, ORCH_GROUP, id='0', mkstream=True)
        logger.info('Created consumer group %s on %s', ORCH_GROUP, ORCH_STREAM)
    except redis.exceptions.ResponseError as exc:
        if 'BUSYGROUP' in str(exc):
            logger.info('Consumer group %s already exists', ORCH_GROUP)
        else:
            raise

    logger.info('Orchestrator listener started (consumer=%s)', ORCH_CONSUMER)

    while True:
        try:
            streams = mq.xreadgroup(
                ORCH_GROUP, ORCH_CONSUMER,
                {ORCH_STREAM: '>'},
                count=10, block=5000, noack=True,
            )
            if not streams:
                continue

            for _stream_name, messages in streams:
                for msg_id, fields in messages:
                    try:
                        msg_type = fields.get(b'type', b'').decode()
                        if msg_type != 'submit_batch':
                            logger.warning('Unknown message type: %s (id=%s)', msg_type, msg_id)
                            continue

                        batch_id = fields[b'batch_id'].decode()
                        reply_to = fields[b'reply_to'].decode()
                        tasks_raw = json.loads(fields[b'tasks'].decode())

                        # Normalize list → dict with initial PENDING state
                        if isinstance(tasks_raw, list):
                            tasks = {
                                t['task_id']: {
                                    **t,
                                    'state': S_PENDING,
                                    'status_code': None,
                                    'body': None,
                                    'error': None,
                                }
                                for t in tasks_raw
                            }
                        else:
                            tasks = tasks_raw

                        batch = {
                            'batch_id': batch_id,
                            'reply_to': reply_to,
                            'status': B_RUNNING,
                            'tasks': tasks,
                        }

                        # Write WAL entry BEFORE spawning the processing thread.
                        # If we crash after this write, recovery will find and
                        # re-drive the batch. If persist_batch fails, we skip
                        # spawning — the caller will time out and retry.
                        try:
                            persist_batch(batch)
                        except redis.exceptions.RedisError as exc:
                            logger.error('Cannot write WAL for batch %s: %s — dropping', batch_id, exc)
                            continue

                        threading.Thread(
                            target=process_batch, args=(batch,), daemon=True
                        ).start()
                        logger.info('Accepted batch %s', batch_id)

                    except Exception as exc:
                        logger.error('Error processing message %s: %s', msg_id, exc)

        except redis.exceptions.ConnectionError as exc:
            logger.error('Connection error in listener: %s', exc)
            time.sleep(1)
        except Exception as exc:
            logger.error('Listener error: %s', exc)
            time.sleep(1)


# ── Entry point ────────────────────────────────────────────────────────────────
def main() -> None:
    threading.Thread(target=recover_batches, daemon=True).start()
    run_listener()


if __name__ == '__main__':
    main()

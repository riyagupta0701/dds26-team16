import json
import logging
import os
import time
import threading

import redis

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

# ── Configuration ──────────────────────────────────────────────────────────────
MQ_REDIS_HOST     = os.environ['MQ_REDIS_HOST']
MQ_REDIS_PORT     = int(os.environ['MQ_REDIS_PORT'])
MQ_REDIS_PASSWORD = os.environ['MQ_REDIS_PASSWORD']
MQ_REDIS_DB       = int(os.environ['MQ_REDIS_DB'])

ORCH_REDIS_HOST     = os.environ.get('ORCH_REDIS_HOST', '')
ORCH_REDIS_PORT     = int(os.environ.get('ORCH_REDIS_PORT', '6379'))
ORCH_REDIS_PASSWORD = os.environ.get('ORCH_REDIS_PASSWORD', '')
ORCH_REDIS_DB       = int(os.environ.get('ORCH_REDIS_DB', '0'))
ORCH_SENTINEL_HOSTS = os.environ.get('ORCH_SENTINEL_HOSTS', '')
ORCH_MASTER_NAME    = os.environ.get('ORCH_MASTER_NAME', 'orch-master')

MAX_RETRIES    = int(os.environ.get('ORCH_MAX_RETRIES', '3'))
TASK_TIMEOUT_S = int(os.environ.get('ORCH_TASK_TIMEOUT_S', '5'))

ORCH_STREAM   = 'events:orchestrator'
ORCH_GROUP    = 'orchestrator_group'
ORCH_CONSUMER = 'orchestrator-1'
PENDING_KEY   = 'orch:pending'

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

# ── Redis connections ──────────────────────────────────────────────────────────
mq: redis.Redis = redis.Redis(
    host=MQ_REDIS_HOST, port=MQ_REDIS_PORT,
    password=MQ_REDIS_PASSWORD, db=MQ_REDIS_DB,
)

if ORCH_SENTINEL_HOSTS:
    from redis.sentinel import Sentinel as _Sentinel
    from redis.retry import Retry
    from redis.backoff import NoBackoff
    _orch_peers = [(h.split(':')[0], int(h.split(':')[1])) for h in ORCH_SENTINEL_HOSTS.split(',')]
    orch: redis.Redis = _Sentinel(
        _orch_peers,
        password=ORCH_REDIS_PASSWORD,
        db=ORCH_REDIS_DB,
        socket_timeout=1.5,
        socket_connect_timeout=1.5,
    ).master_for(
        ORCH_MASTER_NAME,
        socket_timeout=1.5,
        socket_connect_timeout=1.5,
        retry=Retry(NoBackoff(), 3),
        retry_on_error=[redis.exceptions.ConnectionError, redis.exceptions.TimeoutError,
                        redis.exceptions.ReadOnlyError],
    )
else:
    orch: redis.Redis = redis.Redis(
        host=ORCH_REDIS_HOST, port=ORCH_REDIS_PORT,
        password=ORCH_REDIS_PASSWORD, db=ORCH_REDIS_DB,
    )

# ── WAL helpers ────────────────────────────────────────────────────────────────
def _wal_key(batch_id: str) -> str:
    return f'orch:batch:{batch_id}'

def _persist_batch(batch: dict):
    orch.set(_wal_key(batch['batch_id']), json.dumps(batch))

def _load_batch(batch_id: str) -> dict | None:
    raw = orch.get(_wal_key(batch_id))
    return json.loads(raw) if raw else None

# ── Task dispatch ──────────────────────────────────────────────────────────────
def dispatch_task(task: dict) -> dict:
    """
    Send one task to its target stream and block on a per-attempt reply key.
    Retries up to MAX_RETRIES times on timeout only.
    Any response (200 or non-200) is final — no retry.
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
                logger.info(f'Task {task_id} attempt {attempt}: {state} (code={status_code})')
                return {**task, 'state': state, 'status_code': status_code,
                        'body': val.get('body'), 'error': val.get('error')}
            else:
                logger.warning(f'Task {task_id} attempt {attempt} timed out, retrying')
        except Exception as e:
            logger.error(f'Task {task_id} attempt {attempt} exception: {e}')

    logger.error(f'Task {task_id} DELIVERY_FAILED after {MAX_RETRIES} attempts')
    return {**task, 'state': S_DELIVERY_FAILED, 'status_code': None, 'body': None,
            'error': f'No response after {MAX_RETRIES} attempts'}

# ── Batch processor ────────────────────────────────────────────────────────────
def _all_terminal(tasks_by_id: dict) -> bool:
    return all(t['state'] in TERMINAL_STATES for t in tasks_by_id.values())

def _cascade_skipped(tasks_by_id: dict):
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
                    logger.info(f'Task {task["task_id"]} SKIPPED (dep {dep_id} → {dep["state"]})')
                    changed = True
                    break

def process_batch(batch: dict):
    """
    Wave-based dependency executor.  Runs in its own daemon thread per batch.
    Persists state to WAL after every wave so recovery can resume mid-flight.
    """
    batch_id = batch['batch_id']
    reply_to = batch['reply_to']

    # Normalize tasks to a dict keyed by task_id
    raw_tasks = batch.get('tasks', {})
    if isinstance(raw_tasks, list):
        tasks_by_id = {t['task_id']: dict(t) for t in raw_tasks}
    else:
        tasks_by_id = {k: dict(v) for k, v in raw_tasks.items()}

    logger.info(f'process_batch {batch_id}: {len(tasks_by_id)} task(s)')

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
            logger.error(f'process_batch {batch_id}: no progress possible, marking remaining DELIVERY_FAILED')
            for t in tasks_by_id.values():
                if t['state'] == S_PENDING:
                    t['state'] = S_DELIVERY_FAILED
                    t['error'] = 'Deadlock: no progress possible'
            break

        # Mark IN_PROGRESS and persist WAL before dispatching
        for t in ready:
            tasks_by_id[t['task_id']]['state'] = S_IN_PROGRESS
        batch['tasks'] = tasks_by_id
        _persist_batch(batch)

        # Dispatch ready tasks concurrently
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

        # Persist WAL after wave
        batch['tasks'] = tasks_by_id
        _persist_batch(batch)

    # Compute final batch status
    has_delivery_failed = any(t['state'] == S_DELIVERY_FAILED for t in tasks_by_id.values())
    batch_status = B_FAILED if has_delivery_failed else B_COMPLETED
    batch['status'] = batch_status

    # Build result payload (omit internal payload field)
    result_tasks = {
        tid: {
            'state': t['state'],
            'status_code': t.get('status_code'),
            'body': t.get('body'),
            'error': t.get('error'),
        }
        for tid, t in tasks_by_id.items()
    }
    reply_payload = json.dumps({
        'batch_id': batch_id,
        'status': batch_status,
        'tasks': result_tasks,
    })

    try:
        mq.rpush(reply_to, reply_payload)
        mq.expire(reply_to, 120)
        orch.srem(PENDING_KEY, batch_id)
        batch['tasks'] = tasks_by_id
        _persist_batch(batch)
    except Exception as e:
        logger.error(f'process_batch {batch_id}: failed to push reply or clean WAL: {e}')

    logger.info(f'process_batch {batch_id}: {batch_status}')

# ── Recovery ───────────────────────────────────────────────────────────────────
def recover_batches():
    time.sleep(5)
    try:
        pending = orch.smembers(PENDING_KEY)
    except Exception as e:
        logger.error(f'Recovery: cannot read pending set: {e}')
        return

    if not pending:
        logger.info('Recovery: no in-flight batches')
        return

    logger.info(f'Recovery: found {len(pending)} in-flight batch(es)')

    for batch_id_bytes in pending:
        batch_id = batch_id_bytes.decode()
        lock_key = f'orch:lock:{batch_id}'
        if not orch.set(lock_key, 'locked', nx=True, ex=30):
            continue  # another instance claimed this batch

        try:
            batch = _load_batch(batch_id)
            if not batch:
                logger.warning(f'Recovery: batch {batch_id} not found in WAL, removing from pending')
                orch.srem(PENDING_KEY, batch_id)
                continue

            if batch.get('status') in (B_COMPLETED, B_FAILED):
                # Terminal but pending set not cleaned — just clean up
                orch.srem(PENDING_KEY, batch_id)
                continue

            # Reset any IN_PROGRESS tasks to PENDING so they are re-dispatched.
            # Participant idempotency (tombstones) prevents double-apply.
            tasks = batch.get('tasks', {})
            if isinstance(tasks, dict):
                for t in tasks.values():
                    if t.get('state') == S_IN_PROGRESS:
                        t['state'] = S_PENDING
                        logger.info(f'Recovery: reset task {t["task_id"]} to PENDING')
                batch['tasks'] = tasks

            logger.info(f'Recovery: re-dispatching batch {batch_id}')
            threading.Thread(target=process_batch, args=(batch,), daemon=True).start()
        except Exception as e:
            logger.error(f'Recovery: failed to process batch {batch_id}: {e}')
        finally:
            orch.delete(lock_key)

# ── Stream listener ────────────────────────────────────────────────────────────
def run_listener():
    # Create consumer group idempotently
    try:
        mq.xgroup_create(ORCH_STREAM, ORCH_GROUP, id='0', mkstream=True)
        logger.info(f'Created consumer group {ORCH_GROUP} on {ORCH_STREAM}')
    except redis.exceptions.ResponseError as e:
        if 'BUSYGROUP' in str(e):
            logger.info(f'Consumer group {ORCH_GROUP} already exists')
        else:
            raise

    logger.info('Orchestrator listener started')

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
                            logger.warning(f'Unknown message type: {msg_type} (id={msg_id})')
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

                        _persist_batch(batch)
                        orch.sadd(PENDING_KEY, batch_id)
                        threading.Thread(
                            target=process_batch, args=(batch,), daemon=True
                        ).start()
                        logger.info(f'Accepted batch {batch_id}')
                    except Exception as e:
                        logger.error(f'Error processing message {msg_id}: {e}')

        except redis.exceptions.ConnectionError as e:
            logger.error(f'Connection error in listener: {e}')
            time.sleep(1)
        except Exception as e:
            logger.error(f'Listener error: {e}')
            time.sleep(1)

# ── Entry point ────────────────────────────────────────────────────────────────
def main():
    threading.Thread(target=recover_batches, daemon=True).start()
    run_listener()

if __name__ == '__main__':
    main()

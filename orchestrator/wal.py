"""
All orchestrator WAL entries live in the shared wal-redis cluster, which is
completely independent of the orchestrator containers and the MQ Redis.
Killing both orchestrator-1 and orchestrator-2 does not touch wal-redis.

Key namespace (all prefixed wal:orch: to avoid collisions with service WALs):
  wal:orch:pending               → Redis Set  (batch_ids currently in-flight)
  wal:orch:batch:{batch_id}      → String     (JSON snapshot of full batch state)
  wal:orch:lock:{batch_id}       → String/TTL (recovery mutex, expires in 30s)
"""

import json
import logging
import os

import redis
from redis.retry import Retry
from redis.backoff import NoBackoff

logger = logging.getLogger(__name__)


PENDING_KEY = 'wal:orch:pending'


def _batch_key(batch_id: str) -> str:
    return f'wal:orch:batch:{batch_id}'


def _lock_key(batch_id: str) -> str:
    return f'wal:orch:lock:{batch_id}'

def _build_wal_connection() -> redis.Redis:
    password = os.environ.get('WAL_REDIS_PASSWORD', '')
    db_num   = int(os.environ.get('WAL_REDIS_DB', '0'))
    sentinel = os.environ.get('WAL_SENTINEL_HOSTS', '')
    name     = os.environ.get('WAL_MASTER_NAME', 'wal-master')

    if sentinel:
        from redis.sentinel import Sentinel as _Sentinel
        peers = [(h.split(':')[0], int(h.split(':')[1]))
                 for h in sentinel.split(',')]
        return _Sentinel(
            peers,
            password=password,
            db=db_num,
            socket_timeout=1.5,
            socket_connect_timeout=1.5,
        ).master_for(
            name,
            socket_timeout=1.5,
            socket_connect_timeout=1.5,
            retry=Retry(NoBackoff(), 3),
            retry_on_error=[
                redis.exceptions.ConnectionError,
                redis.exceptions.TimeoutError,
                redis.exceptions.ReadOnlyError,
            ],
        )

    return redis.Redis(
        host=os.environ['WAL_REDIS_HOST'],
        port=int(os.environ['WAL_REDIS_PORT']),
        password=password,
        db=db_num,
    )


wal: redis.Redis = _build_wal_connection()


def persist_batch(batch: dict) -> None:
    """
    Append-write the current batch state to wal-redis.
    Also ensures batch_id is in the wal:orch:pending set.
    """
    batch_id = batch['batch_id']
    try:
        pipe = wal.pipeline(transaction=True)
        pipe.set(_batch_key(batch_id), json.dumps(batch))
        pipe.sadd(PENDING_KEY, batch_id)
        pipe.execute()
    except redis.exceptions.RedisError as exc:
        logger.error('WAL persist_batch %s failed: %s', batch_id, exc)
        raise


def load_batch(batch_id: str) -> dict | None:
    try:
        raw = wal.get(_batch_key(batch_id))
        return json.loads(raw) if raw else None
    except redis.exceptions.RedisError as exc:
        logger.error('WAL load_batch %s failed: %s', batch_id, exc)
        return None


def complete_batch(batch: dict) -> None:
    """
    Mark batch as complete in the WAL: update the JSON snapshot and remove
    from the pending set so recovery ignores it.
    """
    batch_id = batch['batch_id']
    try:
        pipe = wal.pipeline(transaction=True)
        pipe.set(_batch_key(batch_id), json.dumps(batch), ex=86400)  # 24h TTL
        pipe.srem(PENDING_KEY, batch_id)
        pipe.execute()
    except redis.exceptions.RedisError as exc:
        logger.error('WAL complete_batch %s failed: %s', batch_id, exc)


def acquire_lock(batch_id: str, ttl_s: int = 30) -> bool:
    try:
        return bool(wal.set(_lock_key(batch_id), 'locked', nx=True, ex=ttl_s))
    except redis.exceptions.RedisError:
        return False


def release_lock(batch_id: str) -> None:
    try:
        wal.delete(_lock_key(batch_id))
    except redis.exceptions.RedisError:
        pass


def pending_batch_ids() -> list[str]:
    try:
        return [b.decode() for b in wal.smembers(PENDING_KEY)]
    except redis.exceptions.RedisError as exc:
        logger.error('WAL pending_batch_ids failed: %s', exc)
        return []


def remove_from_pending(batch_id: str) -> None:
    try:
        wal.srem(PENDING_KEY, batch_id)
    except redis.exceptions.RedisError as exc:
        logger.warning('WAL remove_from_pending %s failed: %s', batch_id, exc)

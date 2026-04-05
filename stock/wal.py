"""
WAL entries stored here:
  wal:stock:2pc:{order_id}              → bytes  (prepared/committed/aborted)

Soft-reservation counters (reserved:stock:{item_id}) remain in the main
db-redis because they are *operational state* — they must be checked
atomically alongside the actual stock value in the same WATCH/MULTI/EXEC
pipeline. True WAL entries (transaction state machines) are separated.
"""

import os
import redis
from redis.retry import Retry
from redis.backoff import NoBackoff


def _stock_wal_key(order_id: str) -> str:
    return f"wal:stock:2pc:{order_id}"


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

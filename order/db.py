import os
import redis
from flask import abort
from msgspec import msgpack, Struct

DB_ERROR_STR = "DB error"

STATUS_PENDING = "pending"
STATUS_STARTED = "started"
STATUS_PAID    = "paid"
STATUS_FAILED  = "failed"

# WAL keys now live in wal-redis (imported from wal.py).
# These names are kept here as aliases so the rest of the codebase
# can import them from db without touching their call sites.
from wal import COORD_PENDING_KEY, SAGA_PENDING_KEY  # noqa: E402

# ── Connections ────────────────────────────────────────────────────────────────
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
        _peers, password=_REDIS_PASSWORD, db=_REDIS_DB,
        socket_timeout=1.5, socket_connect_timeout=1.5,
    ).master_for(
        _REDIS_MASTER_NAME,
        socket_timeout=1.5, socket_connect_timeout=1.5,
        retry=Retry(NoBackoff(), 3),
        retry_on_error=[redis.exceptions.ConnectionError,
                        redis.exceptions.TimeoutError,
                        redis.exceptions.ReadOnlyError],
    )
else:
    db: redis.Redis = redis.Redis(
        host=os.environ['REDIS_HOST'],
        port=int(os.environ['REDIS_PORT']),
        password=_REDIS_PASSWORD,
        db=_REDIS_DB,
    )

mq: redis.Redis = redis.Redis(
    host=os.environ['MQ_REDIS_HOST'],
    port=int(os.environ['MQ_REDIS_PORT']),
    password=os.environ['MQ_REDIS_PASSWORD'],
    db=int(os.environ['MQ_REDIS_DB']),
)

# ── Model ──────────────────────────────────────────────────────────────────────
class OrderValue(Struct, kw_only=True):
    paid: bool
    items: dict[str, int]
    user_id: str
    total_cost: int
    status: str = STATUS_PENDING

# ── Helpers ────────────────────────────────────────────────────────────────────
def get_order(order_id: str) -> OrderValue:
    try:
        raw = db.get(order_id)
    except redis.exceptions.RedisError:
        abort(500, DB_ERROR_STR)
    if not raw:
        abort(400, f"Order: {order_id} not found!")
    return msgpack.decode(raw, type=OrderValue)


def save_order(order_id: str, order: OrderValue, *,
               wal_add: str | None = None,
               wal_remove: str | None = None):
    """
    Persist order to business-data Redis with optional WAL set updates.

    WAL-before-act ordering:
      1. Write WAL entry to wal-redis first (if wal_add is set).
      2. Write order to db-redis.
      3. Remove WAL entry from wal-redis (if wal_remove is set, best-effort).
    """
    from wal import wal as _wal

    # Step 1: write WAL entry before touching db
    try:
        if wal_add:
            _wal.sadd(wal_add, order_id)
    except redis.exceptions.RedisError:
        raise  # cannot proceed without a WAL entry

    # Step 2: persist business data
    try:
        db.set(order_id, msgpack.encode(order))
    except redis.exceptions.RedisError:
        # db write failed — the WAL entry we just added is an orphan.
        # Recovery will find it, read STATUS_PENDING (unchanged) or a missing
        # order in db, and clean up safely. Re-raise so the caller aborts.
        raise

    # Step 3: best-effort WAL cleanup
    if wal_remove:
        try:
            _wal.srem(wal_remove, order_id)
        except redis.exceptions.RedisError:
            pass  # non-fatal: recovery will clean up on next startup

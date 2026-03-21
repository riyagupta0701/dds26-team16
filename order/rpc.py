import json
import logging
import uuid

from db import mq

log = logging.getLogger('order-service')


class RpcResponse:
    def __init__(self, status_code: int, json_data=None, error_msg: str = ""):
        self.status_code = status_code
        self._json = json_data
        self.text = error_msg

    def json(self):
        return self._json


def send_rpc(stream: str, action: str, data: dict, retries: int = 1) -> RpcResponse | None:
    """Synchronous RPC over a Redis Stream. Returns None on timeout."""
    reply_key = f"reply:{uuid.uuid4()}"
    message = {"type": action, "reply_to": reply_key,
               **{k: str(v) for k, v in data.items()}}

    for attempt in range(retries):
        try:
            mq.xadd(stream, message)
            resp = mq.blpop(reply_key, timeout=5)
            if resp:
                val = json.loads(resp[1])
                return RpcResponse(val.get('status_code', 500),
                                   json_data=val.get('body'),
                                   error_msg=val.get('error', ''))
            log.warning("RPC %s attempt %d timed out", action, attempt + 1)
        except Exception as e:
            log.error("RPC %s failed: %s", action, e)

    mq.delete(reply_key)
    return None


def submit_batch(batch_id: str, tasks: list, timeout: int = 60) -> dict | None:
    """Submit a task batch to the orchestrator. Returns result dict or None on timeout."""
    reply_to = f'reply:batch:{batch_id}'
    try:
        mq.xadd('events:orchestrator', {
            'type': 'submit_batch',
            'batch_id': batch_id,
            'reply_to': reply_to,
            'tasks': json.dumps(tasks),
        })
        resp = mq.blpop(reply_to, timeout=timeout)
        if resp:
            return json.loads(resp[1])
    except Exception as e:
        log.error("submit_batch %s failed: %s", batch_id, e)
    finally:
        mq.delete(reply_to)
    return None


def recovery_rpc(stream: str, action: str, data: dict) -> bool:
    """Best-effort RPC. Returns True only on a 200 response."""
    resp = send_rpc(stream, action, data)
    return bool(resp and resp.status_code == 200)


def task_ok(batch_result: dict, task_id: str) -> bool:
    return batch_result.get('tasks', {}).get(task_id, {}).get('state') == 'DELIVERED_OK'


def task_error(batch_result: dict, task_id: str) -> str:
    return batch_result.get('tasks', {}).get(task_id, {}).get('error', '')

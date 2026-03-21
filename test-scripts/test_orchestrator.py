"""
Comprehensive orchestrator integration tests.

Tests cover:
  - Saga success / stock failure / payment failure with compensation
  - 2PC success / prepare failures with reservation cleanup
  - Idempotent retry of a completed checkout
  - Concurrent checkouts on independent orders
  - Mode switching between saga and 2pc mid-run
  - Orchestrator pause → resume (checkout blocks, then succeeds)
  - Orchestrator kill → restart WAL recovery (pending set empties)

Requires docker-compose stack to be running at http://127.0.0.1:8000.
Docker socket access is needed for the resilience tests (skipped gracefully if absent).
"""

import json
import subprocess
import threading
import time
import unittest

import requests

BASE = "http://127.0.0.1:8000"
COMPOSE_PROJECT = "dds26-team16"
ORCHESTRATOR_CTR_1 = f"{COMPOSE_PROJECT}-orchestrator-1-1"
ORCHESTRATOR_CTR_2 = f"{COMPOSE_PROJECT}-orchestrator-2-1"
ORCHESTRATOR_CTR = ORCHESTRATOR_CTR_1  # primary for single-container ops
ORCH_REDIS_SENTINEL_CTR = f"{COMPOSE_PROJECT}-orch-redis-sentinel-1"


# ── helpers ────────────────────────────────────────────────────────────────────

def _get(path: str) -> requests.Response:
    return requests.get(f"{BASE}{path}", timeout=10)

def _post(path: str) -> requests.Response:
    return requests.post(f"{BASE}{path}", timeout=90)

def ok(r: requests.Response):
    return 200 <= r.status_code < 300

def fail(r: requests.Response):
    return r.status_code >= 400


def set_mode(mode: str):
    r = _post(f"/orders/mode/{mode}")
    assert ok(r), f"Could not set mode to {mode}: {r.text}"


def create_item(price: int) -> str:
    r = requests.post(f"{BASE}/stock/item/create/{price}", timeout=10)
    assert ok(r), r.text
    return r.json()["item_id"]


def add_stock(item_id: str, qty: int):
    r = _post(f"/stock/add/{item_id}/{qty}")
    assert ok(r), r.text


def create_user(credit: int = 0) -> str:
    r = requests.post(f"{BASE}/payment/create_user", timeout=10)
    assert ok(r), r.text
    user_id = r.json()["user_id"]
    if credit:
        r2 = _post(f"/payment/add_funds/{user_id}/{credit}")
        assert ok(r2), r2.text
    return user_id


def create_order(user_id: str) -> str:
    r = _post(f"/orders/create/{user_id}")
    assert ok(r), r.text
    return r.json()["order_id"]


def add_item_to_order(order_id: str, item_id: str, qty: int = 1):
    r = _post(f"/orders/addItem/{order_id}/{item_id}/{qty}")
    assert ok(r), r.text


def checkout(order_id: str) -> requests.Response:
    return requests.post(f"{BASE}/orders/checkout/{order_id}", timeout=90)


def find_order(order_id: str) -> dict:
    return _get(f"/orders/find/{order_id}").json()


def find_item(item_id: str) -> dict:
    return _get(f"/stock/find/{item_id}").json()


def find_user(user_id: str) -> dict:
    return _get(f"/payment/find_user/{user_id}").json()


def _docker(*args) -> subprocess.CompletedProcess:
    return subprocess.run(["docker"] + list(args),
                          capture_output=True, text=True, timeout=30)


def docker_available() -> bool:
    try:
        return _docker("info").returncode == 0
    except Exception:
        return False


def _orch_master_ip() -> str:
    """Resolve the current orch-redis master IP via Sentinel."""
    r = _docker("exec", ORCH_REDIS_SENTINEL_CTR,
                "redis-cli", "-p", "26379", "sentinel", "get-master-addr-by-name", "orch-master")
    return r.stdout.strip().split('\n')[0]

def orch_redis_cmd(*args) -> str:
    """Run a redis-cli command against the current orch-redis master (Sentinel-aware)."""
    master_ip = _orch_master_ip()
    r = _docker("exec", ORCH_REDIS_SENTINEL_CTR,
                "redis-cli", "-h", master_ip, "-p", "6379", "-a", "redis", *args)
    return r.stdout.strip()


def orch_pending_count() -> int:
    out = orch_redis_cmd("SCARD", "orch:pending")
    try:
        return int(out)
    except ValueError:
        return -1


def wait_for_orch_pending_empty(timeout: int = 30) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if orch_pending_count() == 0:
            return True
        time.sleep(0.5)
    return False


# ── fixtures ───────────────────────────────────────────────────────────────────

def make_shippable_order(price: int = 10, stock: int = 5, credit: int = 50,
                         qty: int = 1) -> tuple[str, str, str, str]:
    """Returns (order_id, item_id, user_id, item2_id=None)."""
    item_id  = create_item(price)
    add_stock(item_id, stock)
    user_id  = create_user(credit)
    order_id = create_order(user_id)
    add_item_to_order(order_id, item_id, qty)
    return order_id, item_id, user_id


# ── test classes ───────────────────────────────────────────────────────────────

class TestSagaMode(unittest.TestCase):

    def setUp(self):
        set_mode("saga")

    # ── happy path ─────────────────────────────────────────────────────────────

    def test_saga_success(self):
        """Stock deducted and credit charged on successful checkout."""
        order_id, item_id, user_id = make_shippable_order(price=10, stock=5, credit=50)
        stock_before  = find_item(item_id)["stock"]
        credit_before = find_user(user_id)["credit"]

        r = checkout(order_id)
        self.assertTrue(ok(r), f"Expected 200, got {r.status_code}: {r.text}")

        self.assertEqual(find_item(item_id)["stock"],    stock_before  - 1)
        self.assertEqual(find_user(user_id)["credit"],   credit_before - 10)
        self.assertEqual(find_order(order_id)["status"], "paid")

    # ── idempotency ────────────────────────────────────────────────────────────

    def test_saga_idempotent_second_checkout(self):
        """A second checkout on a paid order returns 200 without re-charging."""
        order_id, item_id, user_id = make_shippable_order(price=10, stock=5, credit=50)
        r1 = checkout(order_id)
        self.assertTrue(ok(r1), r1.text)

        credit_after_first = find_user(user_id)["credit"]
        r2 = checkout(order_id)
        self.assertTrue(ok(r2), r2.text)
        self.assertEqual(find_user(user_id)["credit"], credit_after_first, "Credit must not be charged twice")

    # ── stock failure ──────────────────────────────────────────────────────────

    def test_saga_out_of_stock(self):
        """Checkout fails when stock is 0; stock and credit unchanged."""
        item_id  = create_item(10)
        # intentionally no add_stock — stock stays 0
        user_id  = create_user(50)
        order_id = create_order(user_id)
        add_item_to_order(order_id, item_id, 1)

        r = checkout(order_id)
        self.assertTrue(fail(r), f"Expected 4xx, got {r.status_code}")

        self.assertEqual(find_item(item_id)["stock"],    0,  "Stock must not change")
        self.assertEqual(find_user(user_id)["credit"],  50,  "Credit must not change")
        self.assertEqual(find_order(order_id)["status"], "failed")

    # ── payment failure + compensation ────────────────────────────────────────

    def test_saga_insufficient_credit_rolls_back_stock(self):
        """
        Stock is deducted in Wave 1.  Payment fails in Wave 2.
        Orchestrator marks payment DELIVERED_ERR; order service compensates by
        calling add_stock_batch (rollback).  Stock must be restored.
        """
        order_id, item_id, user_id = make_shippable_order(
            price=100, stock=5, credit=5)   # credit too low
        stock_before = find_item(item_id)["stock"]

        r = checkout(order_id)
        self.assertTrue(fail(r), f"Expected 4xx, got {r.status_code}")

        self.assertEqual(find_item(item_id)["stock"], stock_before,
                         "Stock must be rolled back after payment failure")
        self.assertEqual(find_order(order_id)["status"], "failed")

    # ── multi-item order ───────────────────────────────────────────────────────

    def test_saga_multi_item_success(self):
        """Batch subtract across two items works end-to-end."""
        item1 = create_item(10); add_stock(item1, 10)
        item2 = create_item(20); add_stock(item2, 10)
        user_id  = create_user(100)
        order_id = create_order(user_id)
        add_item_to_order(order_id, item1, 2)   # 20
        add_item_to_order(order_id, item2, 1)   # 20

        r = checkout(order_id)
        self.assertTrue(ok(r), r.text)
        self.assertEqual(find_item(item1)["stock"], 8)
        self.assertEqual(find_item(item2)["stock"], 9)
        self.assertEqual(find_user(user_id)["credit"], 60)


class Test2PCMode(unittest.TestCase):

    def setUp(self):
        set_mode("2pc")

    def tearDown(self):
        set_mode("saga")  # restore default

    # ── happy path ─────────────────────────────────────────────────────────────

    def test_2pc_success(self):
        """Both prepare and commit succeed; stock and credit updated."""
        order_id, item_id, user_id = make_shippable_order(price=15, stock=10, credit=100)
        r = checkout(order_id)
        self.assertTrue(ok(r), f"Expected 200, got {r.status_code}: {r.text}")

        self.assertEqual(find_item(item_id)["stock"],   9)
        self.assertEqual(find_user(user_id)["credit"], 85)
        self.assertEqual(find_order(order_id)["status"], "paid")

    # ── idempotency ────────────────────────────────────────────────────────────

    def test_2pc_idempotent_second_checkout(self):
        order_id, _, user_id = make_shippable_order(price=10, stock=5, credit=50)
        r1 = checkout(order_id)
        self.assertTrue(ok(r1), r1.text)
        credit_after = find_user(user_id)["credit"]

        r2 = checkout(order_id)
        self.assertTrue(ok(r2), r2.text)
        self.assertEqual(find_user(user_id)["credit"], credit_after)

    # ── stock prepare failure ──────────────────────────────────────────────────

    def test_2pc_stock_prepare_failure_aborts(self):
        """Out-of-stock in prepare: abort sent to payment, reservation released."""
        item_id  = create_item(10)      # no stock
        user_id  = create_user(100)
        order_id = create_order(user_id)
        add_item_to_order(order_id, item_id, 1)

        r = checkout(order_id)
        self.assertTrue(fail(r), f"Expected 4xx, got {r.status_code}")

        # Payment reservation must have been released — user can still check out another order
        item2    = create_item(5); add_stock(item2, 5)
        order2   = create_order(user_id)
        add_item_to_order(order2, item2, 1)
        r2 = checkout(order2)
        self.assertTrue(ok(r2), f"Reservation not released; second checkout failed: {r2.text}")

    # ── payment prepare failure ────────────────────────────────────────────────

    def test_2pc_payment_prepare_failure_aborts(self):
        """Insufficient credit in prepare: abort sent to stock, reservation released."""
        order_id, item_id, user_id = make_shippable_order(
            price=200, stock=5, credit=10)   # credit too low

        r = checkout(order_id)
        self.assertTrue(fail(r), f"Expected 4xx, got {r.status_code}")

        # Stock reservation must have been released — stock can be used by another order
        order2   = create_order(create_user(500))
        add_item_to_order(order2, item_id, 1)
        r2 = checkout(order2)
        self.assertTrue(ok(r2), f"Stock reservation not released; second checkout: {r2.text}")


class TestModeSwitching(unittest.TestCase):

    def tearDown(self):
        set_mode("saga")

    def test_switch_saga_to_2pc(self):
        set_mode("saga")
        o1, _, _ = make_shippable_order(price=10, stock=5, credit=50)
        self.assertTrue(ok(checkout(o1)), "Saga checkout should succeed")

        set_mode("2pc")
        o2, _, _ = make_shippable_order(price=10, stock=5, credit=50)
        self.assertTrue(ok(checkout(o2)), "2PC checkout should succeed after mode switch")

    def test_switch_2pc_to_saga(self):
        set_mode("2pc")
        o1, _, _ = make_shippable_order(price=10, stock=5, credit=50)
        self.assertTrue(ok(checkout(o1)))

        set_mode("saga")
        o2, _, _ = make_shippable_order(price=10, stock=5, credit=50)
        self.assertTrue(ok(checkout(o2)))


class TestConcurrency(unittest.TestCase):

    def setUp(self):
        set_mode("saga")

    def _run_checkout(self, order_id: str, results: list, idx: int):
        results[idx] = checkout(order_id)

    def test_concurrent_independent_orders(self):
        """Two independent orders check out concurrently; both must succeed."""
        o1, _, _ = make_shippable_order(price=10, stock=5, credit=50)
        o2, _, _ = make_shippable_order(price=10, stock=5, credit=50)

        results = [None, None]
        t1 = threading.Thread(target=self._run_checkout, args=(o1, results, 0))
        t2 = threading.Thread(target=self._run_checkout, args=(o2, results, 1))
        t1.start(); t2.start()
        t1.join(timeout=90); t2.join(timeout=90)

        self.assertTrue(ok(results[0]), f"Order 1 failed: {results[0].text if results[0] else 'timeout'}")
        self.assertTrue(ok(results[1]), f"Order 2 failed: {results[1].text if results[1] else 'timeout'}")

    def test_concurrent_same_item_limited_stock(self):
        """
        Two orders compete for the last unit of stock.
        The key invariant: stock never goes negative and at most 1 order is paid.
        One checkout may get a 4xx or a connection error — both count as failure.
        """
        item_id  = create_item(10); add_stock(item_id, 1)  # only 1 unit
        user1    = create_user(100); o1 = create_order(user1); add_item_to_order(o1, item_id)
        user2    = create_user(100); o2 = create_order(user2); add_item_to_order(o2, item_id)

        results = [None, None]
        t1 = threading.Thread(target=self._run_checkout, args=(o1, results, 0))
        t2 = threading.Thread(target=self._run_checkout, args=(o2, results, 1))
        t1.start(); t2.start()
        t1.join(timeout=90); t2.join(timeout=90)

        # None (exception / timeout in thread) counts as a non-success
        successes = sum(1 for r in results if r is not None and 200 <= r.status_code < 300)
        self.assertLessEqual(successes, 1, f"At most 1 order should succeed for 1 unit of stock")
        self.assertGreaterEqual(find_item(item_id)["stock"], 0, "Stock must never go negative")

    def test_concurrent_2pc_independent_orders(self):
        set_mode("2pc")
        try:
            o1, _, _ = make_shippable_order(price=10, stock=5, credit=50)
            o2, _, _ = make_shippable_order(price=10, stock=5, credit=50)

            results = [None, None]
            t1 = threading.Thread(target=self._run_checkout, args=(o1, results, 0))
            t2 = threading.Thread(target=self._run_checkout, args=(o2, results, 1))
            t1.start(); t2.start()
            t1.join(timeout=90); t2.join(timeout=90)

            self.assertTrue(ok(results[0]), results[0].text if results[0] else "timeout")
            self.assertTrue(ok(results[1]), results[1].text if results[1] else "timeout")
        finally:
            set_mode("saga")


@unittest.skipUnless(docker_available(), "Docker not available")
class TestOrchestratorResilience(unittest.TestCase):
    """
    Tests that require docker control of the orchestrator container.
    These mirror real-world failure scenarios.
    """

    def setUp(self):
        set_mode("saga")

    def tearDown(self):
        # Ensure both orchestrator replicas are running and mode is restored
        _docker("unpause", ORCHESTRATOR_CTR_1)
        _docker("unpause", ORCHESTRATOR_CTR_2)
        _docker("compose", "start", "orchestrator-1", "orchestrator-2")
        set_mode("saga")
        time.sleep(1)

    def test_wal_empty_after_successful_checkout(self):
        """orch:pending must be empty after a successful checkout completes."""
        order_id, _, _ = make_shippable_order(price=10, stock=5, credit=50)
        r = checkout(order_id)
        self.assertTrue(ok(r), r.text)

        # Give orchestrator a moment to clean up
        time.sleep(1)
        count = orch_pending_count()
        self.assertEqual(count, 0, f"orch:pending should be empty, has {count} entries")

    def test_orchestrator_pause_resume_saga(self):
        """
        Pause orchestrator → checkout blocks (but doesn't fail immediately) →
        unpause → checkout completes successfully.

        This simulates a brief orchestrator freeze or GC pause.
        """
        order_id, _, _ = make_shippable_order(price=10, stock=5, credit=50)

        result: list[requests.Response | None] = [None]

        def do_checkout():
            result[0] = checkout(order_id)

        # Pause both orchestrator replicas then start checkout concurrently
        _docker("pause", ORCHESTRATOR_CTR_1)
        _docker("pause", ORCHESTRATOR_CTR_2)
        t = threading.Thread(target=do_checkout, daemon=True)
        t.start()

        time.sleep(2)  # checkout is now blocking in blpop

        # Unpause — orchestrator processes the buffered stream message
        _docker("unpause", ORCHESTRATOR_CTR_1)
        _docker("unpause", ORCHESTRATOR_CTR_2)
        t.join(timeout=30)

        self.assertIsNotNone(result[0], "Checkout thread did not return")
        self.assertTrue(ok(result[0]), f"Expected 200 after resume, got {result[0].status_code}: {result[0].text}")

    def test_orchestrator_pause_resume_2pc(self):
        set_mode("2pc")
        order_id, _, _ = make_shippable_order(price=10, stock=5, credit=50)
        result: list[requests.Response | None] = [None]

        def do_checkout():
            result[0] = checkout(order_id)

        _docker("pause", ORCHESTRATOR_CTR_1)
        _docker("pause", ORCHESTRATOR_CTR_2)
        t = threading.Thread(target=do_checkout, daemon=True)
        t.start()
        time.sleep(2)
        _docker("unpause", ORCHESTRATOR_CTR_1)
        _docker("unpause", ORCHESTRATOR_CTR_2)
        t.join(timeout=60)

        self.assertIsNotNone(result[0])
        self.assertTrue(ok(result[0]), f"2PC after resume: {result[0].status_code}: {result[0].text}")

    def test_orchestrator_restart_clears_wal(self):
        """
        Seed orch:pending with a fake completed batch (status=COMPLETED),
        restart the orchestrator, and verify it cleans up the stale WAL entry.
        This tests the 'already terminal → just srem' recovery path.
        """
        fake_batch_id = "test:recovery:completed"
        fake_blob = json.dumps({
            "batch_id": fake_batch_id,
            "reply_to": "reply:batch:test:recovery:completed",
            "status": "COMPLETED",
            "tasks": {},
        })

        # Seed WAL directly
        orch_redis_cmd("SADD", "orch:pending", fake_batch_id)
        orch_redis_cmd("SET", f"orch:batch:{fake_batch_id}", fake_blob)

        self.assertEqual(orch_pending_count(), 1, "WAL seed failed")

        # Restart both orchestrator replicas — recovery runs after 5s delay
        _docker("compose", "restart", "orchestrator-1", "orchestrator-2")
        time.sleep(1)  # let it start

        cleared = wait_for_orch_pending_empty(timeout=15)
        self.assertTrue(cleared, "Orchestrator recovery did not clear completed batch from orch:pending")

    def test_orchestrator_restart_reprocesses_pending_batch(self):
        """
        Seed orch:pending with a RUNNING batch whose task is PENDING (not yet dispatched).
        Restart the orchestrator.  Recovery must dispatch the task and push a reply.

        This simulates: orchestrator crashed after writing WAL but before dispatching.
        """
        # Set up real resources so the task can actually execute
        item_id  = create_item(10); add_stock(item_id, 5)
        user_id  = create_user(100)
        order_id = create_order(user_id)
        add_item_to_order(order_id, item_id, 1)

        # Manually mark the order STATUS_STARTED and add to saga WAL
        # (simulate the order service having started checkout but orchestrator crashed)
        # We can't do this via HTTP, so we seed the orchestrator WAL instead.
        batch_id = f"saga:{order_id}"
        reply_to = f"reply:batch:{batch_id}"
        batch_blob = json.dumps({
            "batch_id": batch_id,
            "reply_to": reply_to,
            "status": "RUNNING",
            "tasks": {
                f"{order_id}:saga:stock": {
                    "task_id": f"{order_id}:saga:stock",
                    "stream": "events:stock",
                    "action": "subtract_stock_batch",
                    "payload": {"items": json.dumps({item_id: 1}), "transaction_id": order_id},
                    "depends_on": [],
                    "state": "PENDING",
                    "status_code": None,
                    "body": None,
                    "error": None,
                },
            },
        })

        orch_redis_cmd("SADD", "orch:pending", batch_id)
        orch_redis_cmd("SET", f"orch:batch:{batch_id}", batch_blob)

        stock_before = find_item(item_id)["stock"]

        # Restart both orchestrator replicas — recovery picks up the RUNNING batch
        _docker("compose", "restart", "orchestrator-1", "orchestrator-2")
        time.sleep(1)

        cleared = wait_for_orch_pending_empty(timeout=20)
        self.assertTrue(cleared, "Orchestrator did not finish the recovered batch")

        # The stock task was dispatched and executed by the stock service
        self.assertLess(find_item(item_id)["stock"], stock_before,
                        "Stock should have been decremented by the recovered task")

    def test_multiple_restarts_idempotent(self):
        """
        Checkout, then restart orchestrator twice.  WAL stays empty;
        order state is unchanged.
        """
        order_id, item_id, user_id = make_shippable_order(price=10, stock=5, credit=50)
        r = checkout(order_id)
        self.assertTrue(ok(r), r.text)

        final_stock  = find_item(item_id)["stock"]
        final_credit = find_user(user_id)["credit"]

        # Restart 1
        _docker("compose", "restart", "orchestrator-1", "orchestrator-2")
        time.sleep(8)  # wait past recovery delay

        # Restart 2
        _docker("compose", "restart", "orchestrator-1", "orchestrator-2")
        time.sleep(8)

        self.assertEqual(find_item(item_id)["stock"],  final_stock,  "Stock changed after restart")
        self.assertEqual(find_user(user_id)["credit"], final_credit, "Credit changed after restart")
        self.assertEqual(orch_pending_count(), 0, "orch:pending not empty after restarts")


if __name__ == '__main__':
    unittest.main(verbosity=2)

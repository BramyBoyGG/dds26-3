"""
End-to-end integration tests for stock stream consumer.

Requires docker-compose services to be running:
    docker-compose up --build -d

Run with:
    python -m unittest test.integration_test_stock_consumer -v

Tests publish commands directly to the stock-commands stream in stock-db,
then verify:
  - correct response messages on tx-responses (all fields)
  - stock state changes via the REST API
  - atomicity of multi-item reservations (batch Lua script)
  - idempotency (duplicate tx_id produces no side-effects)
"""
import time
import json
import uuid
import unittest

import redis

from . import utils as tu

# Direct Redis connections (exposed ports from docker-compose)
STOCK_DB = redis.Redis(host="127.0.0.1", port=6381, password="redis", db=0)
ORDER_DB = redis.Redis(host="127.0.0.1", port=6380, password="redis", db=0)

STOCK_COMMANDS_STREAM = "stock-commands"
TX_RESPONSES_STREAM = "tx-responses"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _publish_command(tx_id: str, command: str, payload: dict):
    """Publish a command to the stock-commands stream in stock-db."""
    fields = {
        "tx_id": tx_id,
        "command": command,
        "payload": json.dumps(payload),
    }
    return STOCK_DB.xadd(STOCK_COMMANDS_STREAM, fields)


def _decode_fields(fields: dict) -> dict:
    return {
        (k.decode() if isinstance(k, bytes) else k):
        (v.decode() if isinstance(v, bytes) else v)
        for k, v in fields.items()
    }


def _wait_for_response(tx_id: str, timeout: float = 5.0) -> dict | None:
    """Poll tx-responses in order-db until a response for tx_id appears."""
    deadline = time.time() + timeout
    last_id = "0-0"
    while time.time() < deadline:
        results = ORDER_DB.xread({TX_RESPONSES_STREAM: last_id}, count=10, block=500)
        if results:
            for _stream, messages in results:
                for msg_id, fields in messages:
                    last_id = msg_id
                    decoded = _decode_fields(fields)
                    if decoded.get("tx_id") == tx_id:
                        return decoded
    return None


def _count_responses_for_tx(tx_id: str) -> int:
    """Count how many responses exist on tx-responses for a given tx_id."""
    messages = ORDER_DB.xrange(TX_RESPONSES_STREAM)
    return sum(
        1 for _, fields in messages
        if _decode_fields(fields).get("tx_id") == tx_id
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestStockConsumer(unittest.TestCase):

    def _assert_response(self, resp, tx_id, step, status, reason_contains=""):
        """Assert all fields of a tx-responses message."""
        self.assertIsNotNone(resp, f"No response published for tx_id={tx_id}")
        self.assertEqual(resp["tx_id"], tx_id)
        self.assertEqual(resp["step"], step)
        self.assertEqual(resp["status"], status)
        if reason_contains:
            self.assertIn(reason_contains, resp.get("reason", ""))

    # -- RESERVE_STOCK: success cases ------------------------------------

    def test_reserve_stock_success(self):
        """Single-item RESERVE_STOCK with sufficient stock."""
        item = tu.create_item(10)
        item_id = item["item_id"]
        tu.add_stock(item_id, 50)

        tx_id = str(uuid.uuid4())
        _publish_command(tx_id, "RESERVE_STOCK", {"items": [[item_id, 5]]})

        resp = _wait_for_response(tx_id)
        self._assert_response(resp, tx_id, "RESERVE_STOCK", "SUCCESS")
        self.assertEqual(tu.find_item(item_id)["stock"], 45)

    def test_reserve_stock_exact_amount(self):
        """Reserve exactly all available stock (boundary)."""
        item = tu.create_item(10)
        item_id = item["item_id"]
        tu.add_stock(item_id, 7)

        tx_id = str(uuid.uuid4())
        _publish_command(tx_id, "RESERVE_STOCK", {"items": [[item_id, 7]]})

        resp = _wait_for_response(tx_id)
        self._assert_response(resp, tx_id, "RESERVE_STOCK", "SUCCESS")
        self.assertEqual(tu.find_item(item_id)["stock"], 0)

    def test_reserve_stock_multiple_items_success(self):
        """Multi-item RESERVE_STOCK: all items succeed atomically."""
        item1 = tu.create_item(5)
        item2 = tu.create_item(5)
        item_id1, item_id2 = item1["item_id"], item2["item_id"]
        tu.add_stock(item_id1, 20)
        tu.add_stock(item_id2, 30)

        tx_id = str(uuid.uuid4())
        _publish_command(tx_id, "RESERVE_STOCK", {
            "items": [[item_id1, 3], [item_id2, 7]]
        })

        resp = _wait_for_response(tx_id)
        self._assert_response(resp, tx_id, "RESERVE_STOCK", "SUCCESS")
        self.assertEqual(tu.find_item(item_id1)["stock"], 17)
        self.assertEqual(tu.find_item(item_id2)["stock"], 23)

    # -- RESERVE_STOCK: failure cases ------------------------------------

    def test_reserve_stock_insufficient(self):
        """RESERVE_STOCK with insufficient stock fails, stock unchanged."""
        item = tu.create_item(10)
        item_id = item["item_id"]
        tu.add_stock(item_id, 5)

        tx_id = str(uuid.uuid4())
        _publish_command(tx_id, "RESERVE_STOCK", {"items": [[item_id, 100]]})

        resp = _wait_for_response(tx_id)
        self._assert_response(resp, tx_id, "RESERVE_STOCK", "FAILURE", "Insufficient stock")
        self.assertEqual(tu.find_item(item_id)["stock"], 5)

    def test_reserve_stock_item_not_found(self):
        """RESERVE_STOCK for a non-existent item fails."""
        tx_id = str(uuid.uuid4())
        fake_id = str(uuid.uuid4())
        _publish_command(tx_id, "RESERVE_STOCK", {"items": [[fake_id, 1]]})

        resp = _wait_for_response(tx_id)
        self._assert_response(resp, tx_id, "RESERVE_STOCK", "FAILURE", "not found")

    def test_reserve_stock_multiple_items_atomic_rollback(self):
        """If second item fails, first item's stock is untouched (atomic batch)."""
        item1 = tu.create_item(10)
        item2 = tu.create_item(10)
        item_id1, item_id2 = item1["item_id"], item2["item_id"]
        tu.add_stock(item_id1, 50)
        tu.add_stock(item_id2, 2)

        tx_id = str(uuid.uuid4())
        _publish_command(tx_id, "RESERVE_STOCK", {
            "items": [[item_id1, 5], [item_id2, 10]]
        })

        resp = _wait_for_response(tx_id)
        self._assert_response(resp, tx_id, "RESERVE_STOCK", "FAILURE", "Insufficient stock")
        self.assertIn(item_id2, resp["reason"])

        self.assertEqual(tu.find_item(item_id1)["stock"], 50)
        self.assertEqual(tu.find_item(item_id2)["stock"], 2)

    # -- COMPENSATE_STOCK ------------------------------------------------

    def test_compensate_stock(self):
        """COMPENSATE_STOCK adds stock back after a reservation."""
        item = tu.create_item(10)
        item_id = item["item_id"]
        tu.add_stock(item_id, 50)

        tx_reserve = str(uuid.uuid4())
        _publish_command(tx_reserve, "RESERVE_STOCK", {"items": [[item_id, 10]]})
        resp = _wait_for_response(tx_reserve)
        self._assert_response(resp, tx_reserve, "RESERVE_STOCK", "SUCCESS")
        self.assertEqual(tu.find_item(item_id)["stock"], 40)

        tx_comp = str(uuid.uuid4())
        _publish_command(tx_comp, "COMPENSATE_STOCK", {"items": [[item_id, 10]]})
        resp = _wait_for_response(tx_comp)
        self._assert_response(resp, tx_comp, "COMPENSATE_STOCK", "SUCCESS")
        self.assertEqual(tu.find_item(item_id)["stock"], 50)

    def test_compensate_stock_nonexistent_item(self):
        """COMPENSATE_STOCK for a non-existent item still returns SUCCESS."""
        tx_id = str(uuid.uuid4())
        fake_id = str(uuid.uuid4())
        _publish_command(tx_id, "COMPENSATE_STOCK", {"items": [[fake_id, 5]]})

        resp = _wait_for_response(tx_id)
        self._assert_response(resp, tx_id, "COMPENSATE_STOCK", "SUCCESS")

    # -- Idempotency -----------------------------------------------------

    def test_idempotency_reserve_success(self):
        """Duplicate RESERVE_STOCK with same tx_id: stock subtracted only once."""
        item = tu.create_item(10)
        item_id = item["item_id"]
        tu.add_stock(item_id, 50)
        tx_id = str(uuid.uuid4())

        _publish_command(tx_id, "RESERVE_STOCK", {"items": [[item_id, 5]]})
        resp1 = _wait_for_response(tx_id)
        self._assert_response(resp1, tx_id, "RESERVE_STOCK", "SUCCESS")
        self.assertEqual(tu.find_item(item_id)["stock"], 45)

        _publish_command(tx_id, "RESERVE_STOCK", {"items": [[item_id, 5]]})
        time.sleep(2)  # give consumer time to process duplicate

        self.assertEqual(tu.find_item(item_id)["stock"], 45)
        self.assertEqual(_count_responses_for_tx(tx_id), 2,
                         "Idempotent replay should still publish a response")

    def test_idempotency_reserve_failure(self):
        """Duplicate failed RESERVE_STOCK: same failure replayed, no side-effects."""
        tx_id = str(uuid.uuid4())
        fake_id = str(uuid.uuid4())

        _publish_command(tx_id, "RESERVE_STOCK", {"items": [[fake_id, 1]]})
        resp1 = _wait_for_response(tx_id)
        self._assert_response(resp1, tx_id, "RESERVE_STOCK", "FAILURE", "not found")

        _publish_command(tx_id, "RESERVE_STOCK", {"items": [[fake_id, 1]]})
        time.sleep(2)

        self.assertEqual(_count_responses_for_tx(tx_id), 2)

    def test_idempotency_compensate(self):
        """Duplicate COMPENSATE_STOCK with same tx_id: stock added back only once."""
        item = tu.create_item(10)
        item_id = item["item_id"]
        tu.add_stock(item_id, 50)

        tx_reserve = str(uuid.uuid4())
        _publish_command(tx_reserve, "RESERVE_STOCK", {"items": [[item_id, 10]]})
        _wait_for_response(tx_reserve)
        self.assertEqual(tu.find_item(item_id)["stock"], 40)

        tx_comp = str(uuid.uuid4())
        _publish_command(tx_comp, "COMPENSATE_STOCK", {"items": [[item_id, 10]]})
        resp1 = _wait_for_response(tx_comp)
        self._assert_response(resp1, tx_comp, "COMPENSATE_STOCK", "SUCCESS")
        self.assertEqual(tu.find_item(item_id)["stock"], 50)

        _publish_command(tx_comp, "COMPENSATE_STOCK", {"items": [[item_id, 10]]})
        time.sleep(2)

        self.assertEqual(tu.find_item(item_id)["stock"], 50)
        self.assertEqual(_count_responses_for_tx(tx_comp), 2)


if __name__ == "__main__":
    unittest.main()

"""
test_common.py — Unit tests for the common utilities package.

These tests verify that all shared utilities work correctly in isolation.
They require a running Redis instance.  You can start one with:

    docker run -d --name test-redis -p 6379:6379 redis:7.2-bookworm

Then run the tests with:

    cd /path/to/project
    python -m pytest common/test_common.py -v

Or without pytest:

    python -m unittest common.test_common -v

The tests use a dedicated Redis DB (db=15) to avoid conflicts with
production data.  Each test flushes this DB before running.
"""

import json
import time
import unittest

import redis

from common.protocol import (
    STOCK_COMMANDS_STREAM,
    TX_RESPONSES_STREAM,
    STOCK_CONSUMER_GROUP,
    CMD_RESERVE_STOCK,
    CMD_COMPENSATE_STOCK,
    CMD_DEDUCT_PAYMENT,
    STATUS_SUCCESS,
    STATUS_FAILURE,
    TX_STARTED,
    TX_RESERVING_STOCK,
    TX_COMPLETED,
    TX_ABORTED,
    TERMINAL_STATES,
    IDEMPOTENCY_PREFIX,
)
from common.serialization import (
    encode_command,
    decode_command,
    encode_response,
    decode_response,
)
from common.stream_helpers import (
    create_consumer_group,
    publish_message,
    read_messages,
    ack_message,
)
from common.idempotency import (
    is_processed,
    mark_processed,
    get_cached_result,
)
from common.logging_utils import (
    setup_logging,
    get_consumer_id,
    log_tx,
    log_tx_state_change,
)


# ---------------------------------------------------------------------------
# Redis connection for tests — uses DB 15 to avoid conflicts
# ---------------------------------------------------------------------------
# Change REDIS_HOST/REDIS_PORT if your test Redis is elsewhere.
# ---------------------------------------------------------------------------

REDIS_HOST = "localhost"
REDIS_PORT = 6379
REDIS_DB = 15  # Dedicated test database


def get_test_redis() -> redis.Redis:
    """Create a Redis connection for testing and flush the test DB."""
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)
    r.flushdb()  # Start each test with a clean slate
    return r


# ===========================================================================
# Test: protocol.py
# ===========================================================================

class TestProtocol(unittest.TestCase):
    """Verify protocol constants are defined correctly."""

    def test_terminal_states_contains_completed_and_aborted(self):
        """TERMINAL_STATES should contain exactly COMPLETED and ABORTED."""
        self.assertIn(TX_COMPLETED, TERMINAL_STATES)
        self.assertIn(TX_ABORTED, TERMINAL_STATES)
        self.assertEqual(len(TERMINAL_STATES), 2)

    def test_non_terminal_states_not_in_terminal(self):
        """Active states should NOT be in TERMINAL_STATES."""
        self.assertNotIn(TX_STARTED, TERMINAL_STATES)
        self.assertNotIn(TX_RESERVING_STOCK, TERMINAL_STATES)

    def test_stream_names_are_strings(self):
        """Stream names should be non-empty strings."""
        self.assertIsInstance(STOCK_COMMANDS_STREAM, str)
        self.assertIsInstance(TX_RESPONSES_STREAM, str)
        self.assertTrue(len(STOCK_COMMANDS_STREAM) > 0)

    def test_command_names_are_strings(self):
        """Command constants should be non-empty strings."""
        self.assertIsInstance(CMD_RESERVE_STOCK, str)
        self.assertIsInstance(CMD_COMPENSATE_STOCK, str)
        self.assertIsInstance(CMD_DEDUCT_PAYMENT, str)


# ===========================================================================
# Test: serialization.py
# ===========================================================================

class TestSerialization(unittest.TestCase):
    """Verify encode/decode round-trips for commands and responses."""

    def test_encode_command_returns_flat_dict(self):
        """encode_command should return a dict of str→str (ready for XADD)."""
        fields = encode_command(
            tx_id="tx-001",
            command=CMD_RESERVE_STOCK,
            payload={"items": [["item-1", 2], ["item-2", 1]]},
        )
        # All keys and values must be strings (Redis requirement)
        for k, v in fields.items():
            self.assertIsInstance(k, str)
            self.assertIsInstance(v, str)

        self.assertEqual(fields["tx_id"], "tx-001")
        self.assertEqual(fields["command"], CMD_RESERVE_STOCK)
        # payload should be a JSON string
        payload = json.loads(fields["payload"])
        self.assertEqual(len(payload["items"]), 2)

    def test_decode_command_round_trip(self):
        """encode then decode should give back the original data."""
        original_payload = {"items": [["item-1", 2], ["item-2", 1]]}
        fields = encode_command("tx-001", CMD_RESERVE_STOCK, original_payload)
        decoded = decode_command(fields)

        self.assertEqual(decoded["tx_id"], "tx-001")
        self.assertEqual(decoded["command"], CMD_RESERVE_STOCK)
        self.assertEqual(decoded["items"], [["item-1", 2], ["item-2", 1]])

    def test_decode_command_handles_bytes(self):
        """decode_command should work with bytes keys/values (decode_responses=False)."""
        raw = {
            b"tx_id": b"tx-002",
            b"command": b"RESERVE_STOCK",
            b"payload": b'{"items": [["item-1", 3]]}',
        }
        decoded = decode_command(raw)
        self.assertEqual(decoded["tx_id"], "tx-002")
        self.assertEqual(decoded["items"], [["item-1", 3]])

    def test_encode_response_returns_flat_dict(self):
        """encode_response should return a dict of str→str."""
        fields = encode_response(
            tx_id="tx-001",
            step=CMD_RESERVE_STOCK,
            status=STATUS_SUCCESS,
            reason="",
        )
        self.assertEqual(fields["tx_id"], "tx-001")
        self.assertEqual(fields["step"], CMD_RESERVE_STOCK)
        self.assertEqual(fields["status"], STATUS_SUCCESS)
        self.assertEqual(fields["reason"], "")

    def test_decode_response_round_trip(self):
        """encode then decode response should preserve all fields."""
        fields = encode_response("tx-003", CMD_DEDUCT_PAYMENT, STATUS_FAILURE, "insufficient credit")
        decoded = decode_response(fields)

        self.assertEqual(decoded["tx_id"], "tx-003")
        self.assertEqual(decoded["step"], CMD_DEDUCT_PAYMENT)
        self.assertEqual(decoded["status"], STATUS_FAILURE)
        self.assertEqual(decoded["reason"], "insufficient credit")

    def test_decode_response_default_reason(self):
        """If reason is missing from raw data, it should default to empty string."""
        raw = {"tx_id": "tx-004", "step": "RESERVE_STOCK", "status": "SUCCESS"}
        decoded = decode_response(raw)
        self.assertEqual(decoded["reason"], "")

    def test_encode_command_with_payment_payload(self):
        """Payment commands have different payload structure than stock commands."""
        fields = encode_command(
            tx_id="tx-005",
            command=CMD_DEDUCT_PAYMENT,
            payload={"user_id": "user-42", "amount": 150},
        )
        decoded = decode_command(fields)
        self.assertEqual(decoded["user_id"], "user-42")
        self.assertEqual(decoded["amount"], 150)


# ===========================================================================
# Test: stream_helpers.py  (requires running Redis)
# ===========================================================================

class TestStreamHelpers(unittest.TestCase):
    """Test Redis Stream operations.  Requires a running Redis on localhost:6379."""

    def setUp(self):
        """Get a clean Redis connection before each test."""
        try:
            self.r = get_test_redis()
            self.r.ping()
        except redis.exceptions.ConnectionError:
            self.skipTest("Redis not available on localhost:6379 — skipping stream tests")

    def tearDown(self):
        """Clean up after each test."""
        if hasattr(self, "r"):
            self.r.flushdb()
            self.r.close()

    def test_create_consumer_group_new(self):
        """Creating a new consumer group should return True."""
        result = create_consumer_group(self.r, "test-stream", "test-group")
        self.assertTrue(result)

    def test_create_consumer_group_idempotent(self):
        """Creating the same group twice should not raise, returns False."""
        create_consumer_group(self.r, "test-stream", "test-group")
        result = create_consumer_group(self.r, "test-stream", "test-group")
        self.assertFalse(result)  # Already exists

    def test_publish_and_read_message(self):
        """Publish a message and read it back via consumer group."""
        stream = "test-stream"
        group = "test-group"
        consumer = "consumer-1"

        # Setup
        create_consumer_group(self.r, stream, group)

        # Publish
        fields = encode_command("tx-100", CMD_RESERVE_STOCK, {"items": [["item-1", 5]]})
        msg_id = publish_message(self.r, stream, fields)
        self.assertIsNotNone(msg_id)

        # Read
        messages = read_messages(self.r, stream, group, consumer, count=1, block_ms=1000)
        self.assertEqual(len(messages), 1)

        received_id, received_fields = messages[0]
        self.assertEqual(received_fields["tx_id"], "tx-100")
        self.assertEqual(received_fields["command"], CMD_RESERVE_STOCK)

    def test_ack_message_removes_from_pending(self):
        """After ACK, the message should no longer be pending."""
        stream = "test-stream"
        group = "test-group"
        consumer = "consumer-1"

        create_consumer_group(self.r, stream, group)
        fields = encode_command("tx-200", CMD_RESERVE_STOCK, {"items": []})
        publish_message(self.r, stream, fields)

        # Read the message (now it's pending)
        messages = read_messages(self.r, stream, group, consumer, count=1, block_ms=1000)
        self.assertEqual(len(messages), 1)
        msg_id = messages[0][0]

        # Check pending count before ACK
        pending = self.r.xpending(stream, group)
        self.assertEqual(pending["pending"], 1)

        # ACK
        ack_message(self.r, stream, group, msg_id)

        # Check pending count after ACK — should be 0
        pending = self.r.xpending(stream, group)
        self.assertEqual(pending["pending"], 0)

    def test_read_empty_stream_returns_empty(self):
        """Reading from an empty stream should return [] after timeout."""
        stream = "test-stream"
        group = "test-group"
        consumer = "consumer-1"

        create_consumer_group(self.r, stream, group)

        # Block for only 100ms to keep the test fast
        messages = read_messages(self.r, stream, group, consumer, count=1, block_ms=100)
        self.assertEqual(messages, [])

    def test_multiple_messages_ordering(self):
        """Messages should be read in the order they were published."""
        stream = "test-stream"
        group = "test-group"
        consumer = "consumer-1"

        create_consumer_group(self.r, stream, group)

        # Publish 3 messages
        for i in range(3):
            fields = encode_command(f"tx-{i}", CMD_RESERVE_STOCK, {"seq": i})
            publish_message(self.r, stream, fields)

        # Read all 3
        messages = read_messages(self.r, stream, group, consumer, count=10, block_ms=1000)
        self.assertEqual(len(messages), 3)

        # Verify order
        for i, (msg_id, fields) in enumerate(messages):
            self.assertEqual(fields["tx_id"], f"tx-{i}")

    def test_consumer_group_load_balancing(self):
        """Two consumers in the same group should each get different messages."""
        stream = "test-stream"
        group = "test-group"

        create_consumer_group(self.r, stream, group)

        # Publish 2 messages
        publish_message(self.r, stream, encode_command("tx-A", CMD_RESERVE_STOCK, {}))
        publish_message(self.r, stream, encode_command("tx-B", CMD_RESERVE_STOCK, {}))

        # Consumer 1 reads first message
        msgs1 = read_messages(self.r, stream, group, "consumer-1", count=1, block_ms=1000)
        # Consumer 2 reads second message
        msgs2 = read_messages(self.r, stream, group, "consumer-2", count=1, block_ms=1000)

        self.assertEqual(len(msgs1), 1)
        self.assertEqual(len(msgs2), 1)

        # They should have received different transactions
        tx_ids = {msgs1[0][1]["tx_id"], msgs2[0][1]["tx_id"]}
        self.assertEqual(tx_ids, {"tx-A", "tx-B"})


# ===========================================================================
# Test: idempotency.py  (requires running Redis)
# ===========================================================================

class TestIdempotency(unittest.TestCase):
    """Test idempotency helpers.  Requires a running Redis on localhost:6379."""

    def setUp(self):
        try:
            self.r = get_test_redis()
            self.r.ping()
        except redis.exceptions.ConnectionError:
            self.skipTest("Redis not available on localhost:6379 — skipping idempotency tests")

    def tearDown(self):
        if hasattr(self, "r"):
            self.r.flushdb()
            self.r.close()

    def test_not_processed_initially(self):
        """A fresh tx_id should not be marked as processed."""
        self.assertFalse(is_processed(self.r, "tx-new", "RESERVE_STOCK"))

    def test_mark_then_check_processed(self):
        """After marking as processed, is_processed should return True."""
        result = {"status": STATUS_SUCCESS, "reason": ""}
        mark_processed(self.r, "tx-001", "RESERVE_STOCK", result)
        self.assertTrue(is_processed(self.r, "tx-001", "RESERVE_STOCK"))

    def test_get_cached_result(self):
        """get_cached_result should return the exact result that was stored."""
        original = {"status": STATUS_FAILURE, "reason": "out of stock"}
        mark_processed(self.r, "tx-002", "RESERVE_STOCK", original)
        cached = get_cached_result(self.r, "tx-002", "RESERVE_STOCK")
        self.assertEqual(cached, original)

    def test_different_steps_are_independent(self):
        """Processing RESERVE_STOCK should not affect DEDUCT_PAYMENT."""
        mark_processed(self.r, "tx-003", "RESERVE_STOCK", {"status": STATUS_SUCCESS, "reason": ""})
        self.assertTrue(is_processed(self.r, "tx-003", "RESERVE_STOCK"))
        self.assertFalse(is_processed(self.r, "tx-003", "DEDUCT_PAYMENT"))

    def test_different_tx_ids_are_independent(self):
        """Processing tx-A should not affect tx-B."""
        mark_processed(self.r, "tx-A", "RESERVE_STOCK", {"status": STATUS_SUCCESS, "reason": ""})
        self.assertTrue(is_processed(self.r, "tx-A", "RESERVE_STOCK"))
        self.assertFalse(is_processed(self.r, "tx-B", "RESERVE_STOCK"))

    def test_ttl_is_set(self):
        """The idempotency key should have a TTL (auto-cleanup)."""
        mark_processed(self.r, "tx-004", "RESERVE_STOCK", {"status": STATUS_SUCCESS, "reason": ""}, ttl=60)
        key = f"{IDEMPOTENCY_PREFIX}:RESERVE_STOCK:tx-004"
        ttl = self.r.ttl(key)
        # TTL should be set and positive (between 1 and 60 seconds)
        self.assertGreater(ttl, 0)
        self.assertLessEqual(ttl, 60)

    def test_get_cached_result_missing_key(self):
        """get_cached_result on a non-existent key should return None."""
        result = get_cached_result(self.r, "tx-nonexistent", "RESERVE_STOCK")
        self.assertIsNone(result)


# ===========================================================================
# Test: logging_utils.py
# ===========================================================================

class TestLoggingUtils(unittest.TestCase):
    """Test logging setup — no Redis needed."""

    def test_setup_logging_returns_logger(self):
        """setup_logging should return a configured Logger."""
        logger = setup_logging("test-service", level="DEBUG")
        self.assertIsNotNone(logger)
        self.assertEqual(logger.name, "test-service")
        self.assertEqual(logger.level, 10)  # DEBUG = 10

    def test_setup_logging_idempotent(self):
        """Calling setup_logging twice should not add duplicate handlers."""
        logger1 = setup_logging("test-service-2")
        handler_count_1 = len(logger1.handlers)
        logger2 = setup_logging("test-service-2")
        handler_count_2 = len(logger2.handlers)
        self.assertEqual(handler_count_1, handler_count_2)

    def test_get_consumer_id_returns_string(self):
        """get_consumer_id should return a non-empty string."""
        cid = get_consumer_id()
        self.assertIsInstance(cid, str)
        self.assertTrue(len(cid) > 0)

    def test_log_tx_does_not_crash(self):
        """log_tx should run without errors."""
        logger = setup_logging("test-service-3", level="DEBUG")
        # This should not raise
        log_tx(logger, "tx-001", "RESERVE_STOCK", "Processing 3 items")

    def test_log_tx_state_change_does_not_crash(self):
        """log_tx_state_change should run without errors."""
        logger = setup_logging("test-service-4", level="DEBUG")
        log_tx_state_change(logger, "tx-001", "RESERVING_STOCK", "DEDUCTING_PAYMENT", "stock reserved")


# ===========================================================================
# Integration test: full flow simulation
# ===========================================================================

class TestFullFlowSimulation(unittest.TestCase):
    """
    Simulate a simplified SAGA flow using all common utilities together.

    This test proves that the utilities work end-to-end:
    1. Order publishes RESERVE_STOCK command
    2. Stock reads the command
    3. Stock checks idempotency (not processed yet)
    4. Stock publishes SUCCESS response
    5. Stock marks command as processed (idempotent)
    6. Order reads the response

    No actual stock/payment logic — just the messaging infrastructure.
    """

    def setUp(self):
        try:
            self.r = get_test_redis()
            self.r.ping()
        except redis.exceptions.ConnectionError:
            self.skipTest("Redis not available on localhost:6379")

    def tearDown(self):
        if hasattr(self, "r"):
            self.r.flushdb()
            self.r.close()

    def test_saga_message_round_trip(self):
        """Simulate Order → Stock → Order message flow."""
        # --- SETUP ---
        # In real life, stock-commands lives in stock-db and tx-responses in order-db.
        # For testing, we use the same Redis instance for both.
        cmd_stream = STOCK_COMMANDS_STREAM      # "stock-commands"
        resp_stream = TX_RESPONSES_STREAM        # "tx-responses"
        stock_group = STOCK_CONSUMER_GROUP       # "stock-service-group"
        order_group = "order-service-group"

        create_consumer_group(self.r, cmd_stream, stock_group)
        create_consumer_group(self.r, resp_stream, order_group)

        tx_id = "tx-integration-001"

        # --- STEP 1: Order publishes a RESERVE_STOCK command ---
        cmd_fields = encode_command(
            tx_id=tx_id,
            command=CMD_RESERVE_STOCK,
            payload={"items": [["item-1", 2], ["item-2", 1]]},
        )
        publish_message(self.r, cmd_stream, cmd_fields)

        # --- STEP 2: Stock reads the command ---
        commands = read_messages(self.r, cmd_stream, stock_group, "stock-consumer-1", count=1, block_ms=1000)
        self.assertEqual(len(commands), 1)

        cmd_msg_id, raw_cmd = commands[0]
        parsed_cmd = decode_command(raw_cmd)
        self.assertEqual(parsed_cmd["tx_id"], tx_id)
        self.assertEqual(parsed_cmd["command"], CMD_RESERVE_STOCK)
        self.assertEqual(len(parsed_cmd["items"]), 2)

        # --- STEP 3: Stock checks idempotency ---
        self.assertFalse(is_processed(self.r, tx_id, CMD_RESERVE_STOCK))

        # --- STEP 4: Stock processes command and publishes response ---
        # (In real code, actual stock subtraction would happen here)
        resp_fields = encode_response(
            tx_id=tx_id,
            step=CMD_RESERVE_STOCK,
            status=STATUS_SUCCESS,
        )
        publish_message(self.r, resp_stream, resp_fields)

        # --- STEP 5: Stock marks command as processed ---
        mark_processed(self.r, tx_id, CMD_RESERVE_STOCK, {"status": STATUS_SUCCESS, "reason": ""})
        ack_message(self.r, cmd_stream, stock_group, cmd_msg_id)

        # --- STEP 6: Order reads the response ---
        responses = read_messages(self.r, resp_stream, order_group, "order-consumer-1", count=1, block_ms=1000)
        self.assertEqual(len(responses), 1)

        resp_msg_id, raw_resp = responses[0]
        parsed_resp = decode_response(raw_resp)
        self.assertEqual(parsed_resp["tx_id"], tx_id)
        self.assertEqual(parsed_resp["step"], CMD_RESERVE_STOCK)
        self.assertEqual(parsed_resp["status"], STATUS_SUCCESS)

        ack_message(self.r, resp_stream, order_group, resp_msg_id)

        # --- VERIFY: Retry is idempotent ---
        self.assertTrue(is_processed(self.r, tx_id, CMD_RESERVE_STOCK))
        cached = get_cached_result(self.r, tx_id, CMD_RESERVE_STOCK)
        self.assertEqual(cached["status"], STATUS_SUCCESS)


if __name__ == "__main__":
    unittest.main()

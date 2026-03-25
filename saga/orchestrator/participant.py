"""
participant.py - Base class for SAGA participants.

A SagaParticipant is a service that handles commands from the orchestrator.
This class provides:
1. Command handler registration
2. Stream consumption with consumer groups
3. Automatic idempotency checking
4. Response publishing back to the orchestrator
5. Crash recovery via pending message claiming

Example usage:

    participant = SagaParticipant(
        name="stock-service",
        own_db=stock_db,
        orchestrator_db=order_db,
        command_stream="stock-commands",
        consumer_group="stock-service-group",
    )

    @participant.handler("RESERVE_STOCK")
    def handle_reserve_stock(tx_id: str, payload: dict) -> dict:
        # Do the actual work
        return {"status": "SUCCESS", "reason": ""}

    @participant.handler("COMPENSATE_STOCK")
    def handle_compensate_stock(tx_id: str, payload: dict) -> dict:
        # Undo the work
        return {"status": "SUCCESS", "reason": ""}

    participant.start()
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Callable, Optional

import redis

logger = logging.getLogger(__name__)


# Handler type: (tx_id, payload) -> {"status": str, "reason": str}
HandlerFunc = Callable[[str, dict], dict]


def _get_consumer_id() -> str:
    """Generate a unique consumer ID for this instance."""
    hostname = os.environ.get("HOSTNAME", "local")
    pid = os.getpid()
    return f"{hostname}-{pid}-{uuid.uuid4().hex[:8]}"


# ---------------------------------------------------------------------------
# Stream Helpers
# ---------------------------------------------------------------------------

def _create_consumer_group(redis_conn: redis.Redis, stream: str, group: str) -> bool:
    """Create a consumer group (idempotent)."""
    try:
        redis_conn.xgroup_create(stream, group, id="0", mkstream=True)
        return True
    except redis.exceptions.ResponseError as e:
        if "BUSYGROUP" in str(e):
            return False
        raise


def _publish_message(redis_conn: redis.Redis, stream: str, fields: dict) -> str:
    """Publish a message to a Redis stream."""
    return redis_conn.xadd(stream, fields, maxlen=10000, approximate=True)


def _read_messages(
    redis_conn: redis.Redis,
    stream: str,
    group: str,
    consumer: str,
    count: int = 10,
    block_ms: int = 2000,
) -> list[tuple[str, dict]]:
    """Read messages from a stream with consumer group."""
    try:
        response = redis_conn.xreadgroup(
            groupname=group,
            consumername=consumer,
            streams={stream: ">"},
            count=count,
            block=block_ms,
        )
    except (redis.exceptions.ResponseError, redis.exceptions.TimeoutError, TimeoutError):
        # TimeoutError can happen if socket_timeout < block_ms
        return []

    if not response:
        return []

    return response[0][1]


def _ack_message(redis_conn: redis.Redis, stream: str, group: str, msg_id: str) -> None:
    """Acknowledge a message."""
    redis_conn.xack(stream, group, msg_id)


def _claim_stale_pending(
    redis_conn: redis.Redis,
    stream: str,
    group: str,
    consumer: str,
    min_idle_ms: int = 30000,
    count: int = 10,
) -> list[tuple[str, dict]]:
    """Claim stale pending messages from crashed consumers."""
    try:
        pending_info = redis_conn.xpending_range(
            stream, group, min="-", max="+", count=count
        )
        if not pending_info:
            return []

        stale_ids = [
            entry["message_id"]
            for entry in pending_info
            if entry["time_since_delivered"] >= min_idle_ms
        ]

        if not stale_ids:
            return []

        return redis_conn.xclaim(
            stream, group, consumer,
            min_idle_time=min_idle_ms,
            message_ids=stale_ids,
        )
    except redis.exceptions.ResponseError:
        return []


def _decode_command(raw: dict) -> dict:
    """Decode a command message from Redis streams."""
    def to_str(v):
        return v.decode("utf-8") if isinstance(v, bytes) else str(v)

    cleaned = {to_str(k): to_str(v) for k, v in raw.items()}
    result = {
        "tx_id": cleaned["tx_id"],
        "command": cleaned["command"],
    }
    payload_str = cleaned.get("payload", "{}")
    payload = json.loads(payload_str)
    result.update(payload)
    return result


def _encode_response(tx_id: str, step: str, status: str, reason: str = "") -> dict[str, str]:
    """Encode a response message for Redis streams."""
    return {
        "tx_id": tx_id,
        "step": step,
        "status": status,
        "reason": reason,
    }


# ---------------------------------------------------------------------------
# Idempotency
# ---------------------------------------------------------------------------

IDEMPOTENCY_PREFIX = "processed"
IDEMPOTENCY_TTL = 3600  # 1 hour


def _idempotency_key(tx_id: str, step: str) -> str:
    """Build the Redis key for an idempotency record."""
    return f"{IDEMPOTENCY_PREFIX}:{step}:{tx_id}"


def _is_processed(redis_conn: redis.Redis, tx_id: str, step: str) -> bool:
    """Check if a command was already processed."""
    key = _idempotency_key(tx_id, step)
    return bool(redis_conn.exists(key))


def _mark_processed(redis_conn: redis.Redis, tx_id: str, step: str, result: dict) -> None:
    """Record that a command was processed."""
    key = _idempotency_key(tx_id, step)
    redis_conn.setex(key, IDEMPOTENCY_TTL, json.dumps(result))


def _get_cached_result(redis_conn: redis.Redis, tx_id: str, step: str) -> Optional[dict]:
    """Get the cached result of a previously processed command."""
    key = _idempotency_key(tx_id, step)
    raw = redis_conn.get(key)
    if raw is None:
        return None
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8")
    return json.loads(raw)


# ---------------------------------------------------------------------------
# SagaParticipant
# ---------------------------------------------------------------------------

@dataclass
class SagaParticipant:
    """
    A service that participates in SAGA workflows.

    This class handles the boilerplate of:
    - Consuming commands from a Redis stream
    - Checking idempotency before processing
    - Calling registered handlers
    - Publishing responses back to the orchestrator
    - Recovering from crashes

    Attributes
    ----------
    name : str
        Human-readable name for this participant (for logging).

    own_db : redis.Redis
        Connection to the participant's own Redis database.
        Used for storing data and reading commands.

    orchestrator_db : redis.Redis
        Connection to the orchestrator's Redis database.
        Used for publishing responses.

    command_stream : str
        Name of the stream to consume commands from.

    response_stream : str
        Name of the stream to publish responses to.

    consumer_group : str
        Consumer group name for load balancing.

    claim_interval : float
        How often to check for stale pending messages (seconds).
    """
    name: str
    own_db: redis.Redis
    orchestrator_db: redis.Redis
    command_stream: str
    response_stream: str = "tx-responses"
    consumer_group: str = "participant-group"
    claim_interval: float = 30.0

    # Internal state
    _handlers: dict[str, HandlerFunc] = field(default_factory=dict, init=False)
    _consumer_id: str = field(default_factory=_get_consumer_id)
    _consumer_thread: Optional[threading.Thread] = field(default=None, init=False)
    _running: bool = field(default=False, init=False)

    def handler(self, command: str) -> Callable[[HandlerFunc], HandlerFunc]:
        """
        Decorator to register a command handler.

        Example:
            @participant.handler("RESERVE_STOCK")
            def handle_reserve_stock(tx_id: str, payload: dict) -> dict:
                return {"status": "SUCCESS", "reason": ""}
        """
        def decorator(func: HandlerFunc) -> HandlerFunc:
            self._handlers[command] = func
            logger.info(f"{self.name}: Registered handler for {command}")
            return func
        return decorator

    def register_handler(self, command: str, func: HandlerFunc) -> None:
        """Register a command handler (non-decorator version)."""
        self._handlers[command] = func
        logger.info(f"{self.name}: Registered handler for {command}")

    def _process_message(self, msg_id: str, raw_fields: dict) -> None:
        """Process a single command message."""
        try:
            parsed = _decode_command(raw_fields)
            tx_id = parsed["tx_id"]
            command = parsed["command"]

            logger.debug(f"{self.name}: Received {command} for tx {tx_id}")

            handler = self._handlers.get(command)
            if handler is None:
                logger.warning(f"{self.name}: No handler for command {command}")
                _ack_message(self.own_db, self.command_stream, self.consumer_group, msg_id)
                return

            # Check idempotency
            if _is_processed(self.own_db, tx_id, command):
                cached = _get_cached_result(self.own_db, tx_id, command)
                if cached:
                    logger.info(f"{self.name}: Idempotency hit for {command}:{tx_id}")
                    result = cached
                else:
                    # Cached result expired but idempotency key exists - re-execute
                    result = handler(tx_id, parsed)
                    _mark_processed(self.own_db, tx_id, command, result)
            else:
                # Execute the handler
                result = handler(tx_id, parsed)
                _mark_processed(self.own_db, tx_id, command, result)

            # Publish response (unless handler indicates to skip)
            status = result.get("status", "SUCCESS")
            if status != "SKIP_RESPONSE":
                response_fields = _encode_response(
                    tx_id=tx_id,
                    step=command,
                    status=status,
                    reason=result.get("reason", ""),
                )
                _publish_message(self.orchestrator_db, self.response_stream, response_fields)
                logger.info(f"{self.name}: {command}:{tx_id} -> {status}")
            else:
                logger.debug(f"{self.name}: {command}:{tx_id} -> response handled by handler")

            # ACK the message
            _ack_message(self.own_db, self.command_stream, self.consumer_group, msg_id)

        except Exception as e:
            logger.exception(f"{self.name}: Error processing message {msg_id}: {e}")
            # Don't ACK - leave for retry

    def _consumer_loop(self) -> None:
        """Background thread that consumes commands."""
        _create_consumer_group(self.own_db, self.command_stream, self.consumer_group)
        last_claim_check = time.monotonic()

        logger.info(f"{self.name}: Consumer started (id={self._consumer_id})")

        while self._running:
            try:
                # Periodically claim stale pending messages
                now = time.monotonic()
                if now - last_claim_check >= self.claim_interval:
                    stale = _claim_stale_pending(
                        self.own_db, self.command_stream, self.consumer_group, self._consumer_id
                    )
                    for msg_id, raw_fields in stale:
                        self._process_message(msg_id, raw_fields)
                    last_claim_check = now

                # Read new messages
                messages = _read_messages(
                    self.own_db, self.command_stream, self.consumer_group, self._consumer_id
                )
                for msg_id, raw_fields in messages:
                    self._process_message(msg_id, raw_fields)

            except Exception as e:
                logger.exception(f"{self.name}: Consumer loop error: {e}")

    def start(self) -> None:
        """Start the participant (consumer thread)."""
        if self._running:
            return

        self._running = True
        self._consumer_thread = threading.Thread(
            target=self._consumer_loop,
            daemon=True,
            name=f"participant-{self.name}",
        )
        self._consumer_thread.start()
        logger.info(f"{self.name}: Participant started")

    def stop(self) -> None:
        """Stop the participant."""
        self._running = False
        if self._consumer_thread and self._consumer_thread.is_alive():
            self._consumer_thread.join(timeout=5)
        logger.info(f"{self.name}: Participant stopped")

    @property
    def is_running(self) -> bool:
        """Check if the participant is running."""
        return self._running

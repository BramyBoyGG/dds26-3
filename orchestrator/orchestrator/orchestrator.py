"""
orchestrator.py - The main SAGA orchestrator.

The SagaOrchestrator is responsible for:
1. Starting new saga executions
2. Persisting saga state to Redis
3. Consuming responses from participants
4. Advancing the state machine based on responses
5. Triggering compensations when steps fail
6. Recovery of in-flight sagas after restarts

This abstracts all the saga coordination logic so that
services like Order only need to define the saga structure
and start executions.
"""

from __future__ import annotations

import json
import logging
import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

import redis
from msgspec import msgpack, Struct

from orchestrator.saga_definition import SagaDefinition, SagaStep

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Saga State Constants
# ---------------------------------------------------------------------------

STATE_STARTED = "STARTED"
STATE_EXECUTING = "EXECUTING"  # Prefix: EXECUTING_{step_name}
STATE_COMPENSATING = "COMPENSATING"  # Prefix: COMPENSATING_{step_name}
STATE_COMPLETED = "COMPLETED"
STATE_ABORTED = "ABORTED"

TERMINAL_STATES = {STATE_COMPLETED, STATE_ABORTED}


def make_executing_state(step_name: str) -> str:
    """Create the state name for executing a step."""
    return f"{STATE_EXECUTING}_{step_name}"


def make_compensating_state(step_name: str) -> str:
    """Create the state name for compensating a step."""
    return f"{STATE_COMPENSATING}_{step_name}"


def parse_state(state: str) -> tuple[str, Optional[str]]:
    """
    Parse a state string into (state_type, step_name).

    Examples:
        "STARTED" -> ("STARTED", None)
        "EXECUTING_RESERVE_STOCK" -> ("EXECUTING", "RESERVE_STOCK")
        "COMPENSATING_RESERVE_STOCK" -> ("COMPENSATING", "RESERVE_STOCK")
    """
    if state.startswith(STATE_EXECUTING + "_"):
        return STATE_EXECUTING, state[len(STATE_EXECUTING) + 1:]
    elif state.startswith(STATE_COMPENSATING + "_"):
        return STATE_COMPENSATING, state[len(STATE_COMPENSATING) + 1:]
    else:
        return state, None


# ---------------------------------------------------------------------------
# Saga Execution Record
# ---------------------------------------------------------------------------

class SagaExecution(Struct):
    """
    Persistent record of a saga execution.

    Stored in Redis under key `saga:{saga_id}`.
    """
    saga_id: str
    saga_name: str
    state: str
    context: dict  # User-provided transaction data
    created_at: float
    failure_reason: str = ""
    completed_steps: list[str] = []


# ---------------------------------------------------------------------------
# Stream Helpers (adapted from common module)
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


def _encode_command(tx_id: str, command: str, payload: dict) -> dict[str, str]:
    """Encode a command message for Redis streams."""
    return {
        "tx_id": tx_id,
        "command": command,
        "payload": json.dumps(payload),
    }


def _decode_response(raw: dict) -> dict:
    """Decode a response message from Redis streams."""
    def to_str(v):
        return v.decode("utf-8") if isinstance(v, bytes) else str(v)

    cleaned = {to_str(k): to_str(v) for k, v in raw.items()}
    return {
        "tx_id": cleaned["tx_id"],
        "step": cleaned["step"],
        "status": cleaned["status"],
        "reason": cleaned.get("reason", ""),
    }


# ---------------------------------------------------------------------------
# SagaOrchestrator
# ---------------------------------------------------------------------------

@dataclass
class SagaOrchestrator:
    """
    Orchestrates SAGA executions.

    This class manages the lifecycle of distributed transactions:
    - Starting new sagas
    - Processing responses from participants
    - Driving state transitions
    - Handling compensations
    - Recovery after crashes

    Attributes
    ----------
    saga_definition : SagaDefinition
        The saga workflow to orchestrate.

    own_db : redis.Redis
        Connection to the orchestrator's own Redis database.
        Used for storing saga state and reading responses.

    participant_dbs : dict[str, redis.Redis]
        Map from target_db_key to Redis connections for publishing
        commands to participant services.

    response_stream : str
        Name of the stream where participants publish responses.

    consumer_group : str
        Consumer group name for reading responses.

    saga_key_prefix : str
        Redis key prefix for saga execution records.

    response_timeout : float
        How long to wait for saga completion (seconds).
    """
    saga_definition: SagaDefinition
    own_db: redis.Redis
    participant_dbs: dict[str, redis.Redis]
    response_stream: str = "tx-responses"
    consumer_group: str = "orchestrator-group"
    saga_key_prefix: str = "saga"
    response_timeout: float = 30.0

    # Internal state
    _consumer_id: str = field(default_factory=lambda: f"orch-{uuid.uuid4().hex[:8]}")
    _consumer_thread: Optional[threading.Thread] = field(default=None, init=False)
    _running: bool = field(default=False, init=False)

    def __post_init__(self):
        """Initialize the orchestrator after dataclass init."""
        pass

    # ---------------------------------------------------------------------------
    # Saga state persistence
    # ---------------------------------------------------------------------------

    def _saga_key(self, saga_id: str) -> str:
        """Get the Redis key for a saga execution."""
        return f"{self.saga_key_prefix}:{saga_id}"

    def _save_saga(self, saga: SagaExecution) -> None:
        """Persist a saga execution to Redis."""
        self.own_db.set(self._saga_key(saga.saga_id), msgpack.encode(saga))

    def _load_saga(self, saga_id: str) -> Optional[SagaExecution]:
        """Load a saga execution from Redis."""
        raw = self.own_db.get(self._saga_key(saga_id))
        if raw is None:
            return None
        return msgpack.decode(raw, type=SagaExecution)

    # ---------------------------------------------------------------------------
    # Command publishing
    # ---------------------------------------------------------------------------

    def _publish_command(self, saga: SagaExecution, step: SagaStep, is_compensate: bool = False) -> None:
        """Publish a command to the appropriate participant."""
        target_db = self.participant_dbs.get(step.target_db_key)
        if target_db is None:
            logger.error(f"No database configured for target: {step.target_db_key}")
            return

        if is_compensate:
            command = step.compensate_command
            payload = step.build_compensate_payload(saga.context)
        else:
            command = step.forward_command
            payload = step.build_forward_payload(saga.context)

        fields = _encode_command(saga.saga_id, command, payload)
        _publish_message(target_db, step.target_stream, fields)

        action = "compensate" if is_compensate else "forward"
        logger.info(f"Saga {saga.saga_id}: Published {action} command {command} to {step.target_stream}")

    # ---------------------------------------------------------------------------
    # State machine
    # ---------------------------------------------------------------------------

    def _advance_state_machine(self, saga: SagaExecution, response: dict) -> None:
        """
        Process a response and advance the saga state machine.

        This is the core logic that decides what to do next based on
        the current state and the response received.
        """
        status = response.get("status", "")
        step_name = response.get("step", "")
        reason = response.get("reason", "")

        state_type, current_step = parse_state(saga.state)
        old_state = saga.state

        if state_type == STATE_EXECUTING:
            if status == "SUCCESS":
                # Mark this step as completed
                if current_step not in saga.completed_steps:
                    saga.completed_steps = saga.completed_steps + [current_step]

                # Check if there's a next step
                next_step = self.saga_definition.get_next_step(current_step)
                if next_step:
                    # Execute the next step
                    saga.state = make_executing_state(next_step.name)
                    self._save_saga(saga)
                    logger.info(f"Saga {saga.saga_id}: {old_state} -> {saga.state}")
                    self._publish_command(saga, next_step, is_compensate=False)
                else:
                    # All steps completed successfully
                    saga.state = STATE_COMPLETED
                    self._save_saga(saga)
                    logger.info(f"Saga {saga.saga_id}: {old_state} -> {saga.state}")
                    if self.saga_definition.on_complete:
                        try:
                            self.saga_definition.on_complete(saga.context)
                        except Exception as e:
                            logger.exception(f"Saga {saga.saga_id}: on_complete callback failed: {e}")
            else:
                # Step failed - start compensation
                saga.failure_reason = reason
                steps_to_compensate = self.saga_definition.get_steps_to_compensate(current_step)

                if steps_to_compensate:
                    # Start compensating from the most recent completed step
                    first_compensate = steps_to_compensate[0]
                    saga.state = make_compensating_state(first_compensate.name)
                    self._save_saga(saga)
                    logger.info(f"Saga {saga.saga_id}: {old_state} -> {saga.state} (step {current_step} failed: {reason})")
                    self._publish_command(saga, first_compensate, is_compensate=True)
                else:
                    # No steps to compensate (first step failed)
                    saga.state = STATE_ABORTED
                    self._save_saga(saga)
                    logger.info(f"Saga {saga.saga_id}: {old_state} -> {saga.state} (first step failed: {reason})")
                    if self.saga_definition.on_abort:
                        try:
                            self.saga_definition.on_abort(saga.context, reason)
                        except Exception as e:
                            logger.exception(f"Saga {saga.saga_id}: on_abort callback failed: {e}")

        elif state_type == STATE_COMPENSATING:
            # Compensation always "succeeds" (best effort)
            # Find the next step to compensate
            current_idx = self.saga_definition.get_step_index(current_step)
            steps_to_compensate = self.saga_definition.get_steps_to_compensate(
                self.saga_definition.steps[current_idx].name
            )

            # Find remaining steps that need compensation
            remaining = []
            found_current = False
            for step in self.saga_definition.get_steps_to_compensate(
                self.saga_definition.steps[len(saga.completed_steps)].name
                if len(saga.completed_steps) < len(self.saga_definition.steps)
                else self.saga_definition.steps[-1].name
            ):
                if step.name == current_step:
                    found_current = True
                    continue
                if found_current:
                    remaining.append(step)

            if remaining:
                next_compensate = remaining[0]
                saga.state = make_compensating_state(next_compensate.name)
                self._save_saga(saga)
                logger.info(f"Saga {saga.saga_id}: {old_state} -> {saga.state}")
                self._publish_command(saga, next_compensate, is_compensate=True)
            else:
                # All compensations done
                saga.state = STATE_ABORTED
                self._save_saga(saga)
                logger.info(f"Saga {saga.saga_id}: {old_state} -> {saga.state} (compensation complete)")
                if self.saga_definition.on_abort:
                    try:
                        self.saga_definition.on_abort(saga.context, saga.failure_reason)
                    except Exception as e:
                        logger.exception(f"Saga {saga.saga_id}: on_abort callback failed: {e}")

        else:
            logger.warning(f"Saga {saga.saga_id}: Unexpected response in state {saga.state}")

    # ---------------------------------------------------------------------------
    # Response consumer
    # ---------------------------------------------------------------------------

    def _process_response_message(self, msg_id: str, raw_fields: dict) -> None:
        """Process a single response message."""
        try:
            response = _decode_response(raw_fields)
            saga_id = response["tx_id"]

            saga = self._load_saga(saga_id)
            if saga is None:
                logger.warning(f"Received response for unknown saga: {saga_id}")
                _ack_message(self.own_db, self.response_stream, self.consumer_group, msg_id)
                return

            if saga.state in TERMINAL_STATES:
                # Duplicate response for finished saga - ignore
                _ack_message(self.own_db, self.response_stream, self.consumer_group, msg_id)
                return

            self._advance_state_machine(saga, response)
            _ack_message(self.own_db, self.response_stream, self.consumer_group, msg_id)

        except Exception as e:
            logger.exception(f"Error processing response {msg_id}: {e}")
            # Don't ACK - leave for retry

    def _consumer_loop(self) -> None:
        """Background thread that consumes responses."""
        _create_consumer_group(self.own_db, self.response_stream, self.consumer_group)
        last_claim_check = time.monotonic()

        while self._running:
            try:
                # Periodically claim stale pending messages
                now = time.monotonic()
                if now - last_claim_check >= 30:
                    stale = _claim_stale_pending(
                        self.own_db, self.response_stream, self.consumer_group, self._consumer_id
                    )
                    for msg_id, raw_fields in stale:
                        self._process_response_message(msg_id, raw_fields)
                    last_claim_check = now

                # Read new messages
                messages = _read_messages(
                    self.own_db, self.response_stream, self.consumer_group, self._consumer_id
                )
                for msg_id, raw_fields in messages:
                    self._process_response_message(msg_id, raw_fields)

            except Exception as e:
                logger.exception(f"Consumer loop error: {e}")

    # ---------------------------------------------------------------------------
    # Recovery
    # ---------------------------------------------------------------------------

    def recover_in_flight_sagas(self) -> int:
        """
        Recover sagas that were in-flight when the orchestrator crashed.

        Scans all saga keys and re-publishes commands for non-terminal sagas.
        Safe because participants are idempotent.

        Returns the number of sagas recovered.
        """
        try:
            keys = self.own_db.keys(f"{self.saga_key_prefix}:*")
        except redis.exceptions.RedisError as e:
            logger.error(f"Recovery: failed to scan saga keys: {e}")
            return 0

        recovered = 0
        for key in keys:
            try:
                raw = self.own_db.get(key)
                if not raw:
                    continue

                saga = msgpack.decode(raw, type=SagaExecution)
                if saga.state in TERMINAL_STATES:
                    continue

                state_type, step_name = parse_state(saga.state)

                if state_type == STATE_STARTED:
                    # Re-start from first step
                    first_step = self.saga_definition.first_step
                    if first_step:
                        saga.state = make_executing_state(first_step.name)
                        self._save_saga(saga)
                        self._publish_command(saga, first_step, is_compensate=False)
                        logger.info(f"Recovery: Re-started saga {saga.saga_id}")
                        recovered += 1

                elif state_type == STATE_EXECUTING:
                    # Re-publish the current step's command
                    step = self.saga_definition.get_step(step_name)
                    if step:
                        self._publish_command(saga, step, is_compensate=False)
                        logger.info(f"Recovery: Re-published forward command for saga {saga.saga_id}")
                        recovered += 1

                elif state_type == STATE_COMPENSATING:
                    # Re-publish the current compensation command
                    step = self.saga_definition.get_step(step_name)
                    if step:
                        self._publish_command(saga, step, is_compensate=True)
                        logger.info(f"Recovery: Re-published compensate command for saga {saga.saga_id}")
                        recovered += 1

            except Exception as e:
                logger.exception(f"Recovery failed for key {key}: {e}")

        if recovered:
            logger.info(f"Recovery: Re-published commands for {recovered} in-flight saga(s)")

        return recovered

    # ---------------------------------------------------------------------------
    # Lifecycle
    # ---------------------------------------------------------------------------

    def start(self) -> None:
        """Start the orchestrator (recovery + consumer thread)."""
        if self._running:
            return

        self._running = True

        # Recover any in-flight sagas
        self.recover_in_flight_sagas()

        # Start the consumer thread
        self._consumer_thread = threading.Thread(
            target=self._consumer_loop,
            daemon=True,
            name=f"saga-orchestrator-{self.saga_definition.name}",
        )
        self._consumer_thread.start()
        logger.info(f"Orchestrator started for saga: {self.saga_definition.name}")

    def stop(self) -> None:
        """Stop the orchestrator."""
        self._running = False
        if self._consumer_thread and self._consumer_thread.is_alive():
            self._consumer_thread.join(timeout=5)
        logger.info(f"Orchestrator stopped for saga: {self.saga_definition.name}")

    # ---------------------------------------------------------------------------
    # Saga execution
    # ---------------------------------------------------------------------------

    def start_saga(self, context: dict) -> str:
        """
        Start a new saga execution.

        Parameters
        ----------
        context : dict
            Transaction data (e.g., order_id, user_id, items, total_cost).
            This is passed to payload builders and callbacks.

        Returns
        -------
        str
            The saga ID (UUID).
        """
        saga_id = str(uuid.uuid4())

        saga = SagaExecution(
            saga_id=saga_id,
            saga_name=self.saga_definition.name,
            state=STATE_STARTED,
            context=context,
            created_at=time.time(),
            completed_steps=[],
        )
        self._save_saga(saga)

        # Start executing the first step
        first_step = self.saga_definition.first_step
        if first_step:
            saga.state = make_executing_state(first_step.name)
            self._save_saga(saga)
            self._publish_command(saga, first_step, is_compensate=False)
            logger.info(f"Started saga {saga_id} ({self.saga_definition.name})")
        else:
            # Empty saga - immediately complete
            saga.state = STATE_COMPLETED
            self._save_saga(saga)
            logger.warning(f"Started empty saga {saga_id}")

        return saga_id

    def wait_for_completion(self, saga_id: str, timeout: Optional[float] = None) -> SagaExecution:
        """
        Wait for a saga to reach a terminal state.

        Parameters
        ----------
        saga_id : str
            The saga ID to wait for.
        timeout : float or None
            Max time to wait (seconds). Uses response_timeout if None.

        Returns
        -------
        SagaExecution
            The final saga state.

        Raises
        ------
        TimeoutError
            If the saga doesn't complete within the timeout.
        """
        if timeout is None:
            timeout = self.response_timeout

        deadline = time.monotonic() + timeout
        poll_interval = 0.05  # 50 ms

        while time.monotonic() < deadline:
            saga = self._load_saga(saga_id)
            if saga and saga.state in TERMINAL_STATES:
                return saga
            time.sleep(poll_interval)

        saga = self._load_saga(saga_id)
        if saga is None:
            raise TimeoutError(f"Saga {saga_id} not found")
        if saga.state not in TERMINAL_STATES:
            raise TimeoutError(f"Saga {saga_id} timed out in state {saga.state}")

        return saga

    def execute_saga(self, context: dict, timeout: Optional[float] = None) -> SagaExecution:
        """
        Start a saga and wait for it to complete.

        This is a convenience method that combines start_saga and wait_for_completion.

        Parameters
        ----------
        context : dict
            Transaction data.
        timeout : float or None
            Max time to wait for completion.

        Returns
        -------
        SagaExecution
            The final saga state.
        """
        saga_id = self.start_saga(context)
        return self.wait_for_completion(saga_id, timeout)

    def is_completed(self, saga_id: str) -> bool:
        """Check if a saga completed successfully."""
        saga = self._load_saga(saga_id)
        return saga is not None and saga.state == STATE_COMPLETED

    def is_aborted(self, saga_id: str) -> bool:
        """Check if a saga was aborted."""
        saga = self._load_saga(saga_id)
        return saga is not None and saga.state == STATE_ABORTED

    def get_saga_state(self, saga_id: str) -> Optional[str]:
        """Get the current state of a saga."""
        saga = self._load_saga(saga_id)
        return saga.state if saga else None

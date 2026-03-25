from __future__ import annotations

import logging
import threading
import time
import uuid
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional

import redis
from msgspec import msgpack

from common.protocol import (
    CMD_REFUND_PAYMENT,
    STATUS_FAILURE,
    STATUS_SUCCESS,
    TX_LOG_PREFIX,
    TX_RESPONSES_STREAM,
)
from common.serialization import decode_response, encode_command
from common.stream_helpers import (
    ack_message,
    claim_stale_pending,
    create_consumer_group,
    publish_message,
    read_messages,
)

logger = logging.getLogger("orchestrator")

SagaPayloadFn = Callable[[Dict[str, Any]], Dict[str, Any]]


@dataclass(frozen=True)
class SagaStep:
    name: str
    command: str
    target_stream: str
    payload_fn: SagaPayloadFn
    compensation_command: Optional[str] = None
    compensation_stream: Optional[str] = None
    compensation_payload_fn: Optional[SagaPayloadFn] = None


@dataclass(frozen=True)
class SagaDefinition:
    name: str
    steps: List[SagaStep]


@dataclass
class SagaResult:
    tx_id: str
    status: str
    reason: str
    failed_step: Optional[str] = None
    completed_steps: List[str] = None


class SagaOrchestrator:
    """Orchestrates SAGA flow through Redis Streams."""

    def __init__(
        self,
        stream_clients: Dict[str, redis.Redis],
        order_db: redis.Redis,
        consumer_group: str = "orchestrator-group",
        consumer_name: Optional[str] = None,
        read_block_ms: int = 5000,
        min_claim_idle_ms: int = 30000,
    ):
        self.stream_clients = stream_clients
        self.order_db = order_db
        self.consumer_group = consumer_group
        self.consumer_name = consumer_name or f"orchestrator-{uuid.uuid4()}"
        self.read_block_ms = read_block_ms
        self.min_claim_idle_ms = min_claim_idle_ms
        self._create_response_group()
        self._inflight: Dict[str, threading.Event] = {}

    def _tx_key(self, tx_id: str) -> str:
        return f"{TX_LOG_PREFIX}:{tx_id}"

    def _create_response_group(self) -> None:
        create_consumer_group(self.order_db, TX_RESPONSES_STREAM, self.consumer_group)

    def _persist_tx(self, tx_id: str, state: str, context: Dict[str, Any]) -> None:
        data = {
            "tx_id": tx_id,
            "state": state,
            "context": context,
            "updated_at": time.time(),
        }
        self.order_db.set(self._tx_key(tx_id), msgpack.encode(data))

    def _load_tx(self, tx_id: str) -> Optional[Dict[str, Any]]:
        raw = self.order_db.get(self._tx_key(tx_id))
        if not raw:
            return None
        return msgpack.decode(raw)

    def _publish_step(self, tx_id: str, step: SagaStep, context: Dict[str, Any]) -> None:
        # Pre-calculate payload for redo/compensation.
        payload = step.payload_fn(context)
        fields = encode_command(tx_id=tx_id, command=step.command, payload=payload)
        client = self.stream_clients.get(step.target_stream) or self.order_db
        publish_message(client, step.target_stream, fields)
        logger.info(f"Published command {step.command} for tx {tx_id} to {step.target_stream}")

    def _publish_compensation(self, tx_id: str, step: SagaStep, context: Dict[str, Any]) -> None:
        if not step.compensation_command or not step.compensation_stream:
            return

        payload_fn = step.compensation_payload_fn or step.payload_fn
        payload = payload_fn(context)
        fields = encode_command(tx_id=tx_id, command=step.compensation_command, payload=payload)
        client = self.stream_clients.get(step.compensation_stream) or self.order_db
        publish_message(client, step.compensation_stream, fields)
        logger.info(f"Published compensation command {step.compensation_command} for tx {tx_id}")

    def _wait_for_response(self, tx_id: str, step_name: str, timeout_s: float = 30.0) -> Dict[str, Any]:
        deadline = time.monotonic() + timeout_s

        while time.monotonic() < deadline:
            # claim stale message in case another instance died
            stale = claim_stale_pending(
                self.order_db,
                TX_RESPONSES_STREAM,
                self.consumer_group,
                self.consumer_name,
                min_idle_ms=self.min_claim_idle_ms,
            )
            for msg_id, fields in stale:
                response = decode_response(fields)
                if response["tx_id"] == tx_id and response["step"] == step_name:
                    ack_message(self.order_db, TX_RESPONSES_STREAM, self.consumer_group, msg_id)
                    return response
                ack_message(self.order_db, TX_RESPONSES_STREAM, self.consumer_group, msg_id)

            messages = read_messages(
                self.order_db,
                TX_RESPONSES_STREAM,
                self.consumer_group,
                self.consumer_name,
                block_ms=self.read_block_ms,
            )
            for msg_id, fields in messages:
                response = decode_response(fields)
                if response["tx_id"] == tx_id and response["step"] == step_name:
                    ack_message(self.order_db, TX_RESPONSES_STREAM, self.consumer_group, msg_id)
                    return response
                ack_message(self.order_db, TX_RESPONSES_STREAM, self.consumer_group, msg_id)

        raise TimeoutError(f"Timed out waiting for response for tx {tx_id} step {step_name}")

    def execute(self, saga: SagaDefinition, context: Dict[str, Any], timeout_s: float = 30.0) -> SagaResult:
        tx_id = str(uuid.uuid4())
        completed_steps: List[str] = []

        self._persist_tx(tx_id, "STARTED", context)

        try:
            for step in saga.steps:
                self._persist_tx(tx_id, step.name, context)
                self._publish_step(tx_id, step, context)

                response = self._wait_for_response(tx_id, step.command, timeout_s=timeout_s)
                if response["status"] != STATUS_SUCCESS:
                    # compensate all completed steps in reverse order
                    for completed in reversed(completed_steps):
                        matching_step = next((s for s in saga.steps if s.name == completed), None)
                        if matching_step:
                            self._publish_compensation(tx_id, matching_step, context)
                            try:
                                self._wait_for_response(tx_id, matching_step.compensation_command or "", timeout_s=timeout_s)
                            except Exception:
                                logger.warning(f"Compensation for {completed} timed out or failed")

                    self._persist_tx(tx_id, "ABORTED", context)
                    return SagaResult(
                        tx_id=tx_id,
                        status=STATUS_FAILURE,
                        reason=response.get("reason", "unknown"),
                        failed_step=step.name,
                        completed_steps=completed_steps,
                    )

                completed_steps.append(step.name)

            self._persist_tx(tx_id, "COMPLETED", context)
            return SagaResult(tx_id=tx_id, status=STATUS_SUCCESS, reason="", completed_steps=completed_steps)

        except TimeoutError as exc:
            # Attempt compensations
            for completed in reversed(completed_steps):
                matching_step = next((s for s in saga.steps if s.name == completed), None)
                if matching_step:
                    self._publish_compensation(tx_id, matching_step, context)
                    try:
                        self._wait_for_response(tx_id, matching_step.compensation_command or "", timeout_s=timeout_s)
                    except Exception:
                        logger.warning(f"Compensation for {completed} timed out or failed")

            self._persist_tx(tx_id, "ABORTED", context)
            return SagaResult(tx_id=tx_id, status=STATUS_FAILURE, reason=str(exc), failed_step=step.name if 'step' in locals() else None, completed_steps=completed_steps)

        except Exception as exc:
            self._persist_tx(tx_id, "ABORTED", context)
            return SagaResult(tx_id=tx_id, status=STATUS_FAILURE, reason=str(exc), failed_step=step.name if 'step' in locals() else None, completed_steps=completed_steps)

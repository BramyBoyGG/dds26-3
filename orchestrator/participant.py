from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional

import redis

from common.idempotency import get_cached_result, is_processed, mark_processed
from common.logging_utils import get_consumer_id, log_tx
from common.protocol import (
    CMD_LOOKUP_PRICE,
    PAYMENT_CONSUMER_GROUP,
    STOCK_CONSUMER_GROUP,
    STATUS_FAILURE,
    STATUS_SUCCESS,
    TX_RESPONSES_STREAM,
)
from common.serialization import decode_command, encode_response
from common.stream_helpers import (
    ack_message,
    claim_stale_pending,
    create_consumer_group,
    publish_message,
    read_messages,
)

logger = logging.getLogger("orchestrator.participant")

HandlerFn = Callable[[str, Dict[str, Any]], Dict[str, Any]]


@dataclass
class SagaParticipant:
    service_db: redis.Redis
    order_db: redis.Redis
    command_stream: str
    consumer_group: str
    consumer_name: Optional[str] = None
    pending_poll_period_s: float = 1.0

    def __post_init__(self):
        self.consumer_name = self.consumer_name or f"{get_consumer_id()}-{self.command_stream}"
        self.handlers: Dict[str, HandlerFn] = {}
        create_consumer_group(self.service_db, self.command_stream, self.consumer_group)
        self._running = False

    def register_handler(self, command: str, handler: HandlerFn):
        self.handlers[command] = handler

    def start(self) -> None:
        self._running = True
        thread = threading.Thread(target=self._run, name=f"participant-{self.command_stream}", daemon=True)
        thread.start()

    def stop(self) -> None:
        self._running = False

    def _send_response(self, tx_id: str, step: str, result: Dict[str, Any]) -> None:
        status = STATUS_SUCCESS if result.get("status") == STATUS_SUCCESS else STATUS_FAILURE
        fields = encode_response(tx_id=tx_id, step=step, status=status, reason=result.get("reason", ""))
        publish_message(self.order_db, TX_RESPONSES_STREAM, fields)
        log_tx(logger, tx_id, step, f"Responded with {status}")

    def _handle_cmd(self, tx_id: str, command: str, payload: Dict[str, Any]) -> None:
        if command == CMD_LOOKUP_PRICE:
            # pass through; most participants do not handle this command
            return

        if command not in self.handlers:
            logger.warning(f"No handler for command {command}")
            self._send_response(tx_id, command, {"status": STATUS_FAILURE, "reason": "unknown command"})
            return

        if is_processed(self.service_db, tx_id, command):
            cached = get_cached_result(self.service_db, tx_id, command)
            if cached is not None:
                log_tx(logger, tx_id, command, f"Idempotency replay: {cached['status']}")
                self._send_response(tx_id, command, cached)
                return

        try:
            result = self.handlers[command](tx_id, payload)
            if not isinstance(result, dict):
                raise ValueError("handler must return dict")
        except Exception as e:
            logger.exception(f"Handler error for {command}")
            result = {"status": STATUS_FAILURE, "reason": str(e)}

        mark_processed(self.service_db, tx_id, command, result)
        self._send_response(tx_id, command, result)

    def _run(self) -> None:
        while self._running:
            # Claim stale entries systematically.
            stale = claim_stale_pending(self.service_db, self.command_stream, self.consumer_group, self.consumer_name)
            for msg_id, fields in stale:
                data = decode_command(fields)
                self._handle_cmd(data["tx_id"], data["command"], {k: v for k, v in data.items() if k not in ["tx_id", "command"]})
                ack_message(self.service_db, self.command_stream, self.consumer_group, msg_id)

            messages = read_messages(self.service_db, self.command_stream, self.consumer_group, self.consumer_name)
            for msg_id, fields in messages:
                data = decode_command(fields)
                self._handle_cmd(data["tx_id"], data["command"], {k: v for k, v in data.items() if k not in ["tx_id", "command"]})
                ack_message(self.service_db, self.command_stream, self.consumer_group, msg_id)

            time.sleep(self.pending_poll_period_s)

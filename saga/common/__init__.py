"""
common — Shared utilities for the SAGA-based microservice architecture.

This package provides everything that individual services (Order, Stock,
Payment) need to participate in the orchestrated SAGA protocol:

  • protocol        — Constants: stream names, commands, states, timeouts
  • serialization   — Encode/decode messages for Redis Streams
  • stream_helpers  — Publish, consume, acknowledge, and claim stream messages
  • idempotency     — Prevent double-processing of retried commands
  • logging_utils   — Structured logging with transaction tracing

Quick import examples:

    # Import specific constants
    from common.protocol import (
        STOCK_COMMANDS_STREAM,
        CMD_RESERVE_STOCK,
        STATUS_SUCCESS,
    )

    # Import utility functions
    from common.stream_helpers import publish_message, read_messages, ack_message
    from common.serialization import encode_command, decode_command
    from common.idempotency import is_processed, mark_processed, get_cached_result
    from common.logging_utils import setup_logging, log_tx
"""

# Re-export the most commonly used items at the package level
# so services can do:  from common import publish_message, encode_command, ...

from common.protocol import (
    # Stream names
    STOCK_COMMANDS_STREAM,
    PAYMENT_COMMANDS_STREAM,
    TX_RESPONSES_STREAM,
    # Consumer groups
    STOCK_CONSUMER_GROUP,
    PAYMENT_CONSUMER_GROUP,
    TX_RESPONSES_CONSUMER_GROUP,
    # Commands
    CMD_RESERVE_STOCK,
    CMD_COMPENSATE_STOCK,
    CMD_DEDUCT_PAYMENT,
    CMD_REFUND_PAYMENT,
    # Statuses
    STATUS_SUCCESS,
    STATUS_FAILURE,
    # Transaction states
    TX_STARTED,
    TX_RESERVING_STOCK,
    TX_DEDUCTING_PAYMENT,
    TX_COMPENSATING_STOCK,
    TX_COMPLETED,
    TX_ABORTED,
    TERMINAL_STATES,
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
    claim_stale_pending,
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

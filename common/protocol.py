"""
protocol.py — The single source of truth for all shared constants.

Every service imports from here so that stream names, consumer group names,
command types, and transaction states are always consistent across the system.
If you need to rename a stream or add a new command, change it HERE and
every service picks it up automatically.
"""

# ---------------------------------------------------------------------------
# Redis Stream names
# ---------------------------------------------------------------------------
# Each stream lives inside the Redis instance of the *target* service.
#
#   stock-commands   → lives in stock-db   (Order writes, Stock reads)
#   payment-commands → lives in payment-db (Order writes, Payment reads)
#   tx-responses     → lives in order-db   (Stock/Payment write, Order reads)
#
# This means every service needs TWO Redis connections:
#   • its own DB (for data + consuming its command stream)
#   • order-db   (to publish responses back to the orchestrator)
# And the Order service additionally connects to stock-db and payment-db
# to publish commands.
# ---------------------------------------------------------------------------

STOCK_COMMANDS_STREAM = "stock-commands"
PAYMENT_COMMANDS_STREAM = "payment-commands"
TX_RESPONSES_STREAM = "tx-responses"

# ---------------------------------------------------------------------------
# Consumer group names
# ---------------------------------------------------------------------------
# Consumer groups allow multiple replicas of a service to load-balance
# message processing.  Each message is delivered to exactly ONE consumer
# in the group, which is what we want (no double-processing).
#
# We use a single group per stream.  Individual consumers within the group
# are identified by a unique consumer name (e.g., hostname or UUID).
# ---------------------------------------------------------------------------

STOCK_CONSUMER_GROUP = "stock-service-group"
PAYMENT_CONSUMER_GROUP = "payment-service-group"
TX_RESPONSES_CONSUMER_GROUP = "order-service-group"

# ---------------------------------------------------------------------------
# Command types  (Order → Stock / Payment)
# ---------------------------------------------------------------------------
# These are the values of the "command" field in messages published to
# stock-commands and payment-commands streams.
# ---------------------------------------------------------------------------

# Stock commands
CMD_RESERVE_STOCK = "RESERVE_STOCK"       # Subtract stock for all items in the order
CMD_COMPENSATE_STOCK = "COMPENSATE_STOCK" # Roll back a previous reservation (add stock back)

# Payment commands
CMD_DEDUCT_PAYMENT = "DEDUCT_PAYMENT"     # Subtract credit from user
CMD_REFUND_PAYMENT = "REFUND_PAYMENT"     # Roll back a previous deduction (add credit back)

# ---------------------------------------------------------------------------
# Response statuses  (Stock / Payment → Order)
# ---------------------------------------------------------------------------
# These are the values of the "status" field in messages published to
# the tx-responses stream.
# ---------------------------------------------------------------------------

STATUS_SUCCESS = "SUCCESS"
STATUS_FAILURE = "FAILURE"

# ---------------------------------------------------------------------------
# Transaction states  (stored in the Order service's transaction log)
# ---------------------------------------------------------------------------
# The checkout SAGA moves through these states.  The state is persisted in
# order-db under keys like  tx:{tx_id}  so that if the Order service crashes,
# it can resume the SAGA from the last persisted state on restart.
#
# State machine:
#
#   STARTED
#     │
#     ▼
#   RESERVING_STOCK ──(FAILURE)──► ABORTED
#     │ (SUCCESS)
#     ▼
#   DEDUCTING_PAYMENT ──(FAILURE)──► COMPENSATING_STOCK
#     │ (SUCCESS)                        │ (SUCCESS)
#     ▼                                  ▼
#   COMPLETED                         ABORTED
#
# Terminal states: COMPLETED, ABORTED
# ---------------------------------------------------------------------------

TX_STARTED = "STARTED"
TX_RESERVING_STOCK = "RESERVING_STOCK"
TX_DEDUCTING_PAYMENT = "DEDUCTING_PAYMENT"
TX_COMPENSATING_STOCK = "COMPENSATING_STOCK"
TX_COMPLETED = "COMPLETED"
TX_ABORTED = "ABORTED"

# Set of terminal states — used to check if a transaction is finished
TERMINAL_STATES = {TX_COMPLETED, TX_ABORTED}

# ---------------------------------------------------------------------------
# Transaction log key prefix
# ---------------------------------------------------------------------------
# Transaction log entries are stored in order-db with this prefix.
# Example key:  tx:550e8400-e29b-41d4-a716-446655440000
# ---------------------------------------------------------------------------

TX_LOG_PREFIX = "tx"

# ---------------------------------------------------------------------------
# Idempotency key prefix
# ---------------------------------------------------------------------------
# Used by Stock and Payment services to remember which tx_id+step combos
# they have already processed, so retries don't double-apply.
# Example key:  processed:RESERVE_STOCK:550e8400-e29b-41d4-a716-446655440000
# ---------------------------------------------------------------------------

IDEMPOTENCY_PREFIX = "processed"

# ---------------------------------------------------------------------------
# Default TTLs and timeouts (in seconds)
# ---------------------------------------------------------------------------

IDEMPOTENCY_TTL = 3600          # How long to remember a processed tx_id (1 hour)
STREAM_BLOCK_MS = 5000          # How long XREADGROUP blocks waiting for messages (5s)
STREAM_READ_COUNT = 10          # Max messages to read per XREADGROUP call
RESPONSE_TIMEOUT_S = 30         # How long the checkout endpoint waits for saga completion
RETRY_BACKOFF_BASE_S = 0.5      # Base delay for exponential backoff on retries
RETRY_MAX_ATTEMPTS = 10         # Max retry attempts before giving up
PENDING_CLAIM_MIN_IDLE_MS = 30000  # Claim pending messages idle for >30s (consumer crash recovery)

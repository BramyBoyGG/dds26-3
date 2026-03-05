# common/ — Shared Utilities for SAGA Microservices

This package provides all the shared infrastructure that the Order, Stock, and Payment services need to participate in the orchestrated SAGA protocol over Redis Streams.

## Modules

| Module | Purpose |
|--------|---------|
| `protocol.py` | Single source of truth for stream names, consumer groups, commands, transaction states, and timeouts |
| `serialization.py` | Encode/decode command and response messages for Redis Streams |
| `stream_helpers.py` | Wrappers around Redis Stream operations (publish, consume, ACK, claim) |
| `idempotency.py` | Prevent double-processing of retried commands |
| `logging_utils.py` | Structured logging with transaction ID tracing |

## Quick Start

```python
from common.protocol import (
    STOCK_COMMANDS_STREAM,
    STOCK_CONSUMER_GROUP,
    CMD_RESERVE_STOCK,
    STATUS_SUCCESS,
)
from common.serialization import encode_command, decode_command, encode_response
from common.stream_helpers import create_consumer_group, publish_message, read_messages, ack_message
from common.idempotency import is_processed, mark_processed, get_cached_result
from common.logging_utils import setup_logging, get_consumer_id, log_tx
```

Or import everything from the package root:

```python
from common import (
    STOCK_COMMANDS_STREAM,
    encode_command,
    publish_message,
    is_processed,
    setup_logging,
)
```

## Message Protocol

### Command Messages (Order → Stock / Payment)

Published to `stock-commands` or `payment-commands` streams.

| Field | Type | Description |
|-------|------|-------------|
| `tx_id` | string | Unique transaction ID (UUID) |
| `command` | string | One of: `RESERVE_STOCK`, `COMPENSATE_STOCK`, `DEDUCT_PAYMENT`, `REFUND_PAYMENT` |
| `payload` | JSON string | Command-specific data (see below) |

**Payload for `RESERVE_STOCK` / `COMPENSATE_STOCK`:**
```json
{ "items": [["item-id-1", 2], ["item-id-2", 1]] }
```

**Payload for `DEDUCT_PAYMENT` / `REFUND_PAYMENT`:**
```json
{ "user_id": "user-uuid", "amount": 150 }
```

### Response Messages (Stock / Payment → Order)

Published to `tx-responses` stream in `order-db`.

| Field | Type | Description |
|-------|------|-------------|
| `tx_id` | string | Transaction ID this response belongs to |
| `step` | string | Which saga step completed (e.g., `RESERVE_STOCK`) |
| `status` | string | `SUCCESS` or `FAILURE` |
| `reason` | string | Empty on success; human-readable error on failure |

## How Services Use This Package

### Stock Service (consumer example)

```python
import redis
from common import (
    STOCK_COMMANDS_STREAM, STOCK_CONSUMER_GROUP, TX_RESPONSES_STREAM,
    CMD_RESERVE_STOCK, CMD_COMPENSATE_STOCK, STATUS_SUCCESS, STATUS_FAILURE,
    create_consumer_group, read_messages, ack_message, publish_message,
    decode_command, encode_response,
    is_processed, mark_processed, get_cached_result,
    setup_logging, get_consumer_id, log_tx,
)

logger = setup_logging("stock-service")
consumer_name = get_consumer_id()

# Own DB (data + command stream)
db = redis.Redis(host="stock-db", port=6379, password="redis", db=0)
# Order DB (for publishing responses)
order_db = redis.Redis(host="order-db", port=6379, password="redis", db=0)

# Setup consumer group (safe to call on every startup)
create_consumer_group(db, STOCK_COMMANDS_STREAM, STOCK_CONSUMER_GROUP)

# Consumer loop
while True:
    messages = read_messages(db, STOCK_COMMANDS_STREAM, STOCK_CONSUMER_GROUP, consumer_name)
    for msg_id, raw in messages:
        cmd = decode_command(raw)
        tx_id = cmd["tx_id"]

        # Idempotency check
        if is_processed(db, tx_id, cmd["command"]):
            cached = get_cached_result(db, tx_id, cmd["command"])
            publish_message(order_db, TX_RESPONSES_STREAM,
                            encode_response(tx_id, cmd["command"], cached["status"], cached["reason"]))
            ack_message(db, STOCK_COMMANDS_STREAM, STOCK_CONSUMER_GROUP, msg_id)
            continue

        # Process the command
        if cmd["command"] == CMD_RESERVE_STOCK:
            # ... subtract stock ...
            result = {"status": STATUS_SUCCESS, "reason": ""}
        elif cmd["command"] == CMD_COMPENSATE_STOCK:
            # ... add stock back ...
            result = {"status": STATUS_SUCCESS, "reason": ""}

        # Publish response + mark processed + ACK
        publish_message(order_db, TX_RESPONSES_STREAM,
                        encode_response(tx_id, cmd["command"], result["status"], result["reason"]))
        mark_processed(db, tx_id, cmd["command"], result)
        ack_message(db, STOCK_COMMANDS_STREAM, STOCK_CONSUMER_GROUP, msg_id)
```

### Order Service (orchestrator example)

```python
from common import (
    STOCK_COMMANDS_STREAM, TX_RESPONSES_STREAM, TX_RESPONSES_CONSUMER_GROUP,
    CMD_RESERVE_STOCK, STATUS_SUCCESS,
    create_consumer_group, publish_message, read_messages, ack_message,
    encode_command, decode_response,
    setup_logging, log_tx,
)

logger = setup_logging("order-service")

# Publish a RESERVE_STOCK command to stock-db
stock_db = redis.Redis(host="stock-db", port=6379, password="redis", db=0)
fields = encode_command(
    tx_id="abc-123",
    command=CMD_RESERVE_STOCK,
    payload={"items": [["item-1", 2], ["item-2", 1]]}
)
publish_message(stock_db, STOCK_COMMANDS_STREAM, fields)
log_tx(logger, "abc-123", "RESERVE_STOCK", "Command published")

# Listen for responses on own DB
order_db = redis.Redis(host="order-db", port=6379, password="redis", db=0)
create_consumer_group(order_db, TX_RESPONSES_STREAM, TX_RESPONSES_CONSUMER_GROUP)

responses = read_messages(order_db, TX_RESPONSES_STREAM, TX_RESPONSES_CONSUMER_GROUP, "order-1")
for msg_id, raw in responses:
    resp = decode_response(raw)
    if resp["status"] == STATUS_SUCCESS:
        log_tx(logger, resp["tx_id"], resp["step"], "SUCCESS — advancing state machine")
    else:
        log_tx(logger, resp["tx_id"], resp["step"], f"FAILURE — {resp['reason']}")
    ack_message(order_db, TX_RESPONSES_STREAM, TX_RESPONSES_CONSUMER_GROUP, msg_id)
```

## Stream / Redis Topology

```
┌─────────────┐         ┌──────────────┐         ┌──────────────┐
│  order-db   │         │  stock-db    │         │  payment-db  │
│  (Redis)    │         │  (Redis)     │         │  (Redis)     │
│             │         │              │         │              │
│ tx-responses│◄────────│ Stock writes │         │ Payment      │
│  (stream)   │         │ responses    │         │ writes here  │──►│ tx-responses │
│             │         │              │         │              │
│ orders +    │         │ stock-       │         │ payment-     │
│ tx log      │         │ commands     │         │ commands     │
│             │         │  (stream)    │         │  (stream)    │
└─────────────┘         └──────────────┘         └──────────────┘
       │                       ▲                        ▲
       │                       │                        │
       └───── Order publishes commands to both ─────────┘
```

## Running Tests

Tests require a running Redis on `localhost:6379`. Start one with:

```bash
docker run -d --name test-redis -p 6379:6379 redis:7.2-bookworm
```

Then run:

```bash
cd /path/to/project
python3 -m unittest common.test_common -v
```

Without Redis, the protocol, serialization, and logging tests still pass. The stream and idempotency tests are skipped gracefully.

## Constants Reference

### Stream Names
- `STOCK_COMMANDS_STREAM` = `"stock-commands"` (lives in stock-db)
- `PAYMENT_COMMANDS_STREAM` = `"payment-commands"` (lives in payment-db)
- `TX_RESPONSES_STREAM` = `"tx-responses"` (lives in order-db)

### Consumer Groups
- `STOCK_CONSUMER_GROUP` = `"stock-service-group"`
- `PAYMENT_CONSUMER_GROUP` = `"payment-service-group"`
- `TX_RESPONSES_CONSUMER_GROUP` = `"order-service-group"`

### Commands
- `CMD_RESERVE_STOCK` = `"RESERVE_STOCK"`
- `CMD_COMPENSATE_STOCK` = `"COMPENSATE_STOCK"`
- `CMD_DEDUCT_PAYMENT` = `"DEDUCT_PAYMENT"`
- `CMD_REFUND_PAYMENT` = `"REFUND_PAYMENT"`

### Transaction States
- `TX_STARTED`, `TX_RESERVING_STOCK`, `TX_DEDUCTING_PAYMENT`, `TX_COMPENSATING_STOCK`, `TX_COMPLETED`, `TX_ABORTED`
- `TERMINAL_STATES` = `{TX_COMPLETED, TX_ABORTED}`

### Timeouts / Tuning
- `IDEMPOTENCY_TTL` = 3600s (1 hour)
- `STREAM_BLOCK_MS` = 5000ms (read timeout)
- `STREAM_READ_COUNT` = 10 (messages per read)
- `RESPONSE_TIMEOUT_S` = 30s (checkout wait)
- `PENDING_CLAIM_MIN_IDLE_MS` = 30000ms (crash recovery threshold)

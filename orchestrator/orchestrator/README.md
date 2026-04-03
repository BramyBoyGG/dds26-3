# Saga Orchestrator module

This folder contains a reusable orchestrator implementation for the SAGA pattern.

- `SagaStep` / `SagaDefinition`: declare business flow steps for ordering and compensation.
- `SagaOrchestrator`: drives the state machine, persists tx state in Redis, publishes commands to service command streams, consumes tx-responses from the order stream, and performs compensations on failures.
- `SagaParticipant`: helper base for participants (stock/payment services) to register step handlers and publish responses.

## Basics

1. Define Saga steps:
```python
from orchestrator import SagaStep, SagaDefinition
from common.protocol import CMD_RESERVE_STOCK, CMD_DEDUCT_PAYMENT, CMD_COMPENSATE_STOCK
from common.protocol import STOCK_COMMANDS_STREAM, PAYMENT_COMMANDS_STREAM

checkout_saga = SagaDefinition(
    name="checkout",
    steps=[
        SagaStep(
            name="reserve_stock",
            command=CMD_RESERVE_STOCK,
            target_stream=STOCK_COMMANDS_STREAM,
            compensation_command=CMD_COMPENSATE_STOCK,
            compensation_stream=STOCK_COMMANDS_STREAM,
            payload_fn=lambda ctx: {"items": ctx["items"]},
        ),
        SagaStep(
            name="deduct_payment",
            command=CMD_DEDUCT_PAYMENT,
            target_stream=PAYMENT_COMMANDS_STREAM,
            payload_fn=lambda ctx: {"user_id": ctx["user_id"], "amount": ctx["amount"]},
        ),
    ],
)
```

2. Execute saga:
```python
from orchestrator import SagaOrchestrator

orchestrator = SagaOrchestrator(order_db, stock_db, payment_db)
result = orchestrator.execute(checkout_saga, context)
```

3. Participant services can expose handlers via `SagaParticipant`.

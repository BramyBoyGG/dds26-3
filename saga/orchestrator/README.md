# Orchestrator Library

A reusable SAGA orchestration framework for distributed transactions using Redis Streams.

## Overview

This library provides a clean abstraction for implementing distributed transactions using the SAGA pattern. It separates the workflow definition from the execution logic, making it easy to:

1. Define saga workflows declaratively
2. Execute sagas with automatic state management
3. Handle compensations when steps fail
4. Recover from crashes

## Components

### SagaDefinition

Defines the structure of a saga workflow with ordered steps.

```python
from orchestrator import SagaDefinition, SagaStep

checkout_saga = SagaDefinition(
    name="checkout",
    on_complete=lambda ctx: print(f"Order {ctx['order_id']} completed!"),
)

checkout_saga.add_step(SagaStep(
    name="RESERVE_STOCK",
    forward_command="RESERVE_STOCK",
    compensate_command="COMPENSATE_STOCK",
    target_stream="stock-commands",
    target_db_key="STOCK",
    payload_builder=lambda ctx: {"items": ctx["items"]},
))

checkout_saga.add_step(SagaStep(
    name="DEDUCT_PAYMENT",
    forward_command="DEDUCT_PAYMENT",
    compensate_command=None,
    target_stream="payment-commands",
    target_db_key="PAYMENT",
    payload_builder=lambda ctx: {"user_id": ctx["user_id"], "amount": ctx["total"]},
))
```

### SagaOrchestrator

Executes sagas and manages the state machine.

```python
from orchestrator import SagaOrchestrator

orchestrator = SagaOrchestrator(
    saga_definition=checkout_saga,
    own_db=order_db,
    participant_dbs={"STOCK": stock_db, "PAYMENT": payment_db},
    response_stream="tx-responses",
    consumer_group="order-service-group",
)

# Start the orchestrator (runs recovery + consumer thread)
orchestrator.start()

# Execute a saga
result = orchestrator.execute_saga({
    "order_id": "order-123",
    "user_id": "user-456",
    "items": [("item-1", 2), ("item-2", 1)],
    "total": 100,
})

if result.state == "COMPLETED":
    print("Success!")
else:
    print(f"Failed: {result.failure_reason}")
```

### SagaParticipant

Base class for services that participate in sagas.

```python
from orchestrator import SagaParticipant

participant = SagaParticipant(
    name="stock-service",
    own_db=stock_db,
    orchestrator_db=order_db,
    command_stream="stock-commands",
    consumer_group="stock-service-group",
)

@participant.handler("RESERVE_STOCK")
def handle_reserve_stock(tx_id: str, payload: dict) -> dict:
    items = payload["items"]
    # ... reserve stock ...
    return {"status": "SUCCESS", "reason": ""}

@participant.handler("COMPENSATE_STOCK")
def handle_compensate_stock(tx_id: str, payload: dict) -> dict:
    items = payload["items"]
    # ... add stock back ...
    return {"status": "SUCCESS", "reason": ""}

# Start the participant
participant.start()
```

## State Machine

The orchestrator manages the following state transitions:

```
STARTED
    │
    ▼
EXECUTING_{step1} ──(FAILURE)──► ABORTED
    │ (SUCCESS)
    ▼
EXECUTING_{step2} ──(FAILURE)──► COMPENSATING_{step1} ──► ABORTED
    │ (SUCCESS)
    ▼
COMPLETED
```

## Features

- **Declarative Workflow Definition**: Define saga steps with commands and payloads
- **Automatic State Management**: State is persisted to Redis for recovery
- **Compensation**: Automatic rollback of completed steps when a step fails
- **Idempotency**: Handlers are idempotent - safe to retry
- **Crash Recovery**: Sagas resume from their last state after restart
- **Consumer Groups**: Load-balanced message consumption across replicas

## Error Handling

Handlers return a result dict with `status` and `reason`:

```python
# Success
return {"status": "SUCCESS", "reason": ""}

# Failure
return {"status": "FAILURE", "reason": "Insufficient stock"}

# Skip response (for non-saga commands like price lookup)
return {"status": "SKIP_RESPONSE", "reason": ""}
```

## Recovery

The orchestrator automatically recovers in-flight sagas on startup:

1. Scans all `saga:*` keys in Redis
2. Re-publishes commands for non-terminal sagas
3. Participants process commands idempotently

This ensures no saga gets stuck even if the orchestrator crashes mid-execution.

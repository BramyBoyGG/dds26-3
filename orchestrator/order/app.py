"""
Order Service - Refactored to use the Orchestrator library.

This service demonstrates how the Orchestrator abstracts away all the
SAGA coordination complexity. The service only needs to:
1. Define the saga workflow (steps)
2. Start saga executions
3. Wait for completion

All state machine logic, response handling, compensation, and recovery
is handled by the Orchestrator library.
"""

import json
import logging
import atexit
import random
import time
import uuid
from collections import defaultdict

import redis

from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response

# Import from orchestrator library
from orchestrator import SagaDefinition, SagaStep, SagaOrchestrator
from orchestrator.orchestrator import STATE_COMPLETED, STATE_ABORTED

from common.redis_client import get_redis_connection
from common.protocol import (
    CMD_RESERVE_STOCK, CMD_COMPENSATE_STOCK, CMD_DEDUCT_PAYMENT,
    CMD_LOOKUP_PRICE,
    RESPONSE_TIMEOUT_S,
    PRICE_RESPONSE_PREFIX, PRICE_CACHE_PREFIX, PRICE_CACHE_TTL, PRICE_RESPONSE_TTL,
    STOCK_COMMANDS_STREAM, PAYMENT_COMMANDS_STREAM,
)
from common.serialization import encode_command
from common.stream_helpers import publish_message
from common.logging_utils import setup_logging


DB_ERROR_STR = "DB error"

app = Flask("order-service")
logger = setup_logging("order-service")

# ── Redis connections ────────────────────────────────────────────────────────
# order-db: stores orders, saga state, and the tx-responses stream
db: redis.Redis = get_redis_connection(
    "REDIS_MASTER_NAME", "REDIS_PASSWORD",
    "REDIS_HOST", "REDIS_PORT", "REDIS_DB",
)

# stock-db: Order publishes RESERVE_STOCK / COMPENSATE_STOCK here
stock_db: redis.Redis = get_redis_connection(
    "STOCK_REDIS_MASTER_NAME", "STOCK_REDIS_PASSWORD",
    "STOCK_REDIS_HOST", "STOCK_REDIS_PORT", "STOCK_REDIS_DB",
)

# payment-db: Order publishes DEDUCT_PAYMENT here
payment_db: redis.Redis = get_redis_connection(
    "PAYMENT_REDIS_MASTER_NAME", "PAYMENT_REDIS_PASSWORD",
    "PAYMENT_REDIS_HOST", "PAYMENT_REDIS_PORT", "PAYMENT_REDIS_DB",
)


def close_db_connections():
    db.close()
    stock_db.close()
    payment_db.close()


atexit.register(close_db_connections)


# ── Data models ──────────────────────────────────────────────────────────────

class OrderValue(Struct):
    paid: bool
    items: list[tuple[str, int]]
    user_id: str
    total_cost: int


# ── Saga Definition ──────────────────────────────────────────────────────────
# Define the checkout saga using the orchestrator library.
# This declarative approach separates workflow definition from execution logic.

def _on_checkout_complete(context: dict) -> None:
    """Callback when checkout saga completes successfully."""
    order_id = context.get("order_id")
    if order_id:
        _mark_order_paid(order_id)
        logger.info(f"Order {order_id} marked as paid")


def _mark_order_paid(order_id: str) -> None:
    """Update the order record to paid=True."""
    try:
        raw = db.get(order_id)
        if not raw:
            logger.warning(f"Order {order_id} not found when marking paid")
            return
        order = msgpack.decode(raw, type=OrderValue)
        paid_order = OrderValue(
            paid=True,
            items=order.items,
            user_id=order.user_id,
            total_cost=order.total_cost,
        )
        db.set(order_id, msgpack.encode(paid_order))
    except Exception as e:
        logger.exception(f"Failed to mark order {order_id} as paid: {e}")


checkout_saga = SagaDefinition(
    name="checkout",
    on_complete=_on_checkout_complete,
)

# Step 1: Reserve stock
checkout_saga.add_step(SagaStep(
    name="RESERVE_STOCK",
    forward_command=CMD_RESERVE_STOCK,
    compensate_command=CMD_COMPENSATE_STOCK,
    target_stream=STOCK_COMMANDS_STREAM,
    target_db_key="STOCK",
    payload_builder=lambda ctx: {"items": ctx["items"]},
))

# Step 2: Deduct payment
checkout_saga.add_step(SagaStep(
    name="DEDUCT_PAYMENT",
    forward_command=CMD_DEDUCT_PAYMENT,
    compensate_command=None,  # No compensation needed for payment (we compensate stock)
    target_stream=PAYMENT_COMMANDS_STREAM,
    target_db_key="PAYMENT",
    payload_builder=lambda ctx: {"user_id": ctx["user_id"], "amount": ctx["total_cost"]},
))

# ── Orchestrator Instance ────────────────────────────────────────────────────

orchestrator = SagaOrchestrator(
    saga_definition=checkout_saga,
    own_db=db,
    participant_dbs={
        "STOCK": stock_db,
        "PAYMENT": payment_db,
    },
    response_stream="tx-responses",
    consumer_group="order-service-group",
    saga_key_prefix="saga",
    response_timeout=RESPONSE_TIMEOUT_S,
)


# ── Flask helpers ─────────────────────────────────────────────────────────────

def get_order_from_db(order_id: str) -> OrderValue | None:
    try:
        entry: bytes = db.get(order_id)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    entry: OrderValue | None = msgpack.decode(entry, type=OrderValue) if entry else None
    if entry is None:
        abort(400, f"Order: {order_id} not found!")
    return entry


# ── Flask endpoints ───────────────────────────────────────────────────────────

@app.post('/create/<user_id>')
def create_order(user_id: str):
    key = str(uuid.uuid4())
    value = msgpack.encode(OrderValue(paid=False, items=[], user_id=user_id, total_cost=0))
    try:
        db.set(key, value)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({'order_id': key})


@app.post('/batch_init/<n>/<n_items>/<n_users>/<item_price>')
def batch_init_users(n: int, n_items: int, n_users: int, item_price: int):
    n = int(n)
    n_items = int(n_items)
    n_users = int(n_users)
    item_price = int(item_price)

    def generate_entry() -> OrderValue:
        user_id = random.randint(0, n_users - 1)
        item1_id = random.randint(0, n_items - 1)
        item2_id = random.randint(0, n_items - 1)
        return OrderValue(
            paid=False,
            items=[(f"{item1_id}", 1), (f"{item2_id}", 1)],
            user_id=f"{user_id}",
            total_cost=2 * item_price,
        )

    kv_pairs: dict[str, bytes] = {f"{i}": msgpack.encode(generate_entry()) for i in range(n)}
    try:
        db.mset(kv_pairs)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({"msg": "Batch init for orders successful"})


@app.get('/find/<order_id>')
def find_order(order_id: str):
    order_entry: OrderValue = get_order_from_db(order_id)
    return jsonify({
        "order_id": order_id,
        "paid": order_entry.paid,
        "items": order_entry.items,
        "user_id": order_entry.user_id,
        "total_cost": order_entry.total_cost,
    })


@app.post('/addItem/<order_id>/<item_id>/<quantity>')
def add_item(order_id: str, item_id: str, quantity: int):
    quantity = int(quantity)

    # Validate order exists and is not yet paid
    order_entry: OrderValue = get_order_from_db(order_id)
    if order_entry.paid:
        abort(400, f"Order: {order_id} is already paid!")

    # --- Get price (cache → stream fallback) ---
    cache_key = f"{PRICE_CACHE_PREFIX}:{item_id}"
    cached = db.get(cache_key)
    if cached is not None:
        price_data = json.loads(cached)
    else:
        # Publish LOOKUP_PRICE command and poll for the response
        request_id = str(uuid.uuid4())
        fields = encode_command(
            tx_id=request_id,
            command=CMD_LOOKUP_PRICE,
            payload={"item_id": item_id},
        )
        publish_message(stock_db, STOCK_COMMANDS_STREAM, fields)

        resp_key = f"{PRICE_RESPONSE_PREFIX}:{request_id}"
        deadline = time.monotonic() + RESPONSE_TIMEOUT_S
        price_data = None
        while time.monotonic() < deadline:
            raw = db.get(resp_key)
            if raw is not None:
                price_data = json.loads(raw)
                db.delete(resp_key)
                break
            time.sleep(0.05)

        if price_data is None:
            abort(400, f"Timeout looking up price for item {item_id}")

    if not price_data.get("found"):
        abort(400, f"Item: {item_id} does not exist!")

    item_price = price_data["price"]

    # --- Atomic read-modify-write with WATCH/MULTI ---
    max_retries = 10
    for attempt in range(max_retries):
        try:
            with db.pipeline() as pipe:
                pipe.watch(order_id)
                raw = pipe.get(order_id)
                if raw is None:
                    abort(400, f"Order: {order_id} not found!")
                order = msgpack.decode(raw, type=OrderValue)
                if order.paid:
                    abort(400, f"Order: {order_id} is already paid!")

                updated = OrderValue(
                    paid=order.paid,
                    items=order.items + [(item_id, quantity)],
                    user_id=order.user_id,
                    total_cost=order.total_cost + quantity * item_price,
                )
                pipe.multi()
                pipe.set(order_id, msgpack.encode(updated))
                pipe.execute()

            return Response(
                f"Item: {item_id} added to: {order_id} price updated to: {updated.total_cost}",
                status=200,
            )
        except redis.WatchError:
            continue

    abort(400, f"Could not update order {order_id}: too much contention")


@app.post('/checkout/<order_id>')
def checkout(order_id: str):
    """
    Orchestrated checkout using the SAGA pattern via the Orchestrator library.

    This endpoint demonstrates how clean the application code becomes when
    using the orchestrator. All the complexity of state management, response
    handling, and compensation is abstracted away.
    """
    app.logger.debug(f"Checking out {order_id}")
    order_entry: OrderValue = get_order_from_db(order_id)

    if order_entry.paid:
        return Response(f"Order paid", status=200)

    # Aggregate item quantities (in case the same item was added multiple times)
    items_quantities: dict[str, int] = defaultdict(int)
    for item_id, quantity in order_entry.items:
        items_quantities[item_id] += quantity
    items = list(items_quantities.items())

    # Build the saga context with all the data needed by the steps
    context = {
        "order_id": order_id,
        "user_id": order_entry.user_id,
        "items": items,
        "total_cost": order_entry.total_cost,
    }

    # Execute the checkout saga and wait for completion
    try:
        saga_result = orchestrator.execute_saga(context, timeout=RESPONSE_TIMEOUT_S)

        if saga_result.state == STATE_COMPLETED:
            app.logger.debug(f"Checkout successful for order {order_id}")
            return Response("Checkout successful", status=200)
        else:
            reason = saga_result.failure_reason or "insufficient stock or credit"
            return abort(400, f"Checkout failed: {reason}")

    except TimeoutError:
        app.logger.warning(f"Checkout timed out for order {order_id}")
        return abort(400, "Checkout timed out")


# ── Application startup ───────────────────────────────────────────────────────

def _start_orchestrator() -> None:
    """Start the saga orchestrator."""
    orchestrator.start()
    logger.info("Checkout saga orchestrator started")


_start_orchestrator()


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)

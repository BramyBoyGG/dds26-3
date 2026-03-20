"""
Stock Service - Refactored to use the Orchestrator library.

This service demonstrates how the SagaParticipant class simplifies
participating in SAGA workflows. The service only needs to:
1. Register handlers for each command it handles
2. Start the participant

All stream consumption, idempotency, and response publishing is handled
by the SagaParticipant.
"""

import logging
import atexit
import uuid
import json

import redis

from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response

# Import from orchestrator library
from orchestrator import SagaParticipant

from lua_scripts import LUA_SUBTRACT_STOCK, LUA_SUBTRACT_STOCK_BATCH, LUA_ADD_STOCK
from common.redis_client import get_redis_connection
from common.protocol import (
    STOCK_COMMANDS_STREAM,
    STOCK_CONSUMER_GROUP,
    CMD_RESERVE_STOCK,
    CMD_COMPENSATE_STOCK,
    CMD_LOOKUP_PRICE,
    STATUS_SUCCESS,
    STATUS_FAILURE,
    PRICE_RESPONSE_PREFIX,
    PRICE_CACHE_PREFIX,
    PRICE_CACHE_TTL,
    PRICE_RESPONSE_TTL,
)
from common.logging_utils import setup_logging


DB_ERROR_STR = "DB error"

app = Flask("stock-service")
logger = setup_logging("stock-service")

# ── Redis connections ────────────────────────────────────────────────────────
db: redis.Redis = get_redis_connection(
    "REDIS_MASTER_NAME", "REDIS_PASSWORD",
    "REDIS_HOST", "REDIS_PORT", "REDIS_DB",
)

order_db: redis.Redis = get_redis_connection(
    "ORDER_REDIS_MASTER_NAME", "ORDER_REDIS_PASSWORD",
    "ORDER_REDIS_HOST", "ORDER_REDIS_PORT", "ORDER_REDIS_DB",
)


def close_db_connection():
    db.close()
    order_db.close()


atexit.register(close_db_connection)

# Register Lua scripts with Redis
subtract_script = db.register_script(LUA_SUBTRACT_STOCK)
subtract_batch_script = db.register_script(LUA_SUBTRACT_STOCK_BATCH)
add_script = db.register_script(LUA_ADD_STOCK)


class StockValue(Struct):
    stock: int
    price: int


def get_item_from_db(item_id: str) -> StockValue | None:
    try:
        entry: bytes = db.get(item_id)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    entry: StockValue | None = msgpack.decode(entry, type=StockValue) if entry else None
    if entry is None:
        abort(400, f"Item: {item_id} not found!")
    return entry


# ── REST Endpoints ───────────────────────────────────────────────────────────

@app.post('/item/create/<price>')
def create_item(price: int):
    key = str(uuid.uuid4())
    app.logger.debug(f"Item: {key} created")
    value = msgpack.encode(StockValue(stock=0, price=int(price)))
    try:
        db.set(key, value)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({'item_id': key})


@app.post('/batch_init/<n>/<starting_stock>/<item_price>')
def batch_init_users(n: int, starting_stock: int, item_price: int):
    n = int(n)
    starting_stock = int(starting_stock)
    item_price = int(item_price)
    kv_pairs: dict[str, bytes] = {f"{i}": msgpack.encode(StockValue(stock=starting_stock, price=item_price))
                                  for i in range(n)}
    try:
        db.mset(kv_pairs)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({"msg": "Batch init for stock successful"})


@app.get('/find/<item_id>')
def find_item(item_id: str):
    item_entry: StockValue = get_item_from_db(item_id)
    return jsonify(
        {
            "stock": item_entry.stock,
            "price": item_entry.price
        }
    )


@app.post('/add/<item_id>/<amount>')
def add_stock(item_id: str, amount: int):
    amount = int(amount)
    try:
        new_stock = add_script(keys=[item_id], args=[amount])
        if new_stock == -1:
            abort(400, f"Item: {item_id} not found!")
        return Response(f"Item: {item_id} stock updated to: {new_stock}", status=200)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)


@app.post('/subtract/<item_id>/<amount>')
def remove_stock(item_id: str, amount: int):
    amount = int(amount)
    try:
        new_stock = subtract_script(keys=[item_id], args=[amount])
        if new_stock == -2:
            abort(400, f"Item: {item_id} not found!")
        elif new_stock == -1:
            abort(400, f"Item: {item_id} stock cannot get reduced below zero!")
        app.logger.debug(f"Item: {item_id} stock updated to: {new_stock}")
        return Response(f"Item: {item_id} stock updated to: {new_stock}", status=200)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)


# ── SAGA Participant ─────────────────────────────────────────────────────────
# Create a SagaParticipant that handles SAGA commands

participant = SagaParticipant(
    name="stock-service",
    own_db=db,
    orchestrator_db=order_db,
    command_stream=STOCK_COMMANDS_STREAM,
    response_stream="tx-responses",
    consumer_group=STOCK_CONSUMER_GROUP,
)


@participant.handler(CMD_RESERVE_STOCK)
def handle_reserve_stock(tx_id: str, payload: dict) -> dict:
    """Reserve stock for items in an order (atomic batch operation)."""
    items = payload.get("items", [])

    keys = [str(item_id) for item_id, _ in items]
    args = [int(quantity) for _, quantity in items]

    try:
        # Returns [status, index]:  1=success, 0=not found, -1=insufficient stock
        status, idx = subtract_batch_script(keys=keys, args=args)
    except redis.exceptions.RedisError as e:
        logger.error(f"[{tx_id}] Redis error during RESERVE_STOCK: {e}")
        return {"status": STATUS_FAILURE, "reason": str(e)}

    if status == 0:
        failed_item = keys[idx - 1]
        return {"status": STATUS_FAILURE, "reason": f"Item {failed_item} not found"}
    elif status == -1:
        failed_item = keys[idx - 1]
        return {"status": STATUS_FAILURE, "reason": f"Insufficient stock for item {failed_item}"}
    else:
        logger.info(f"[{tx_id}] Reserved {len(items)} item(s)")
        return {"status": STATUS_SUCCESS, "reason": ""}


@participant.handler(CMD_COMPENSATE_STOCK)
def handle_compensate_stock(tx_id: str, payload: dict) -> dict:
    """Add stock back (compensation for a failed checkout)."""
    items = payload.get("items", [])

    for item_id, quantity in items:
        item_id = str(item_id)
        quantity = int(quantity)
        try:
            add_script(keys=[item_id], args=[quantity])
            logger.debug(f"[{tx_id}] Restored {quantity} of item {item_id}")
        except redis.exceptions.RedisError as e:
            logger.error(f"[{tx_id}] Error restoring item {item_id}: {e}")

    logger.info(f"[{tx_id}] Compensated {len(items)} item(s)")
    return {"status": STATUS_SUCCESS, "reason": ""}


@participant.handler(CMD_LOOKUP_PRICE)
def handle_lookup_price(tx_id: str, payload: dict) -> dict:
    """
    Look up an item's price.

    Note: This is a special non-saga command that writes results
    directly to order-db keys instead of returning via the response stream.
    """
    item_id = payload.get("item_id")
    request_id = tx_id  # tx_id carries the request_id for price lookups

    resp_key = f"{PRICE_RESPONSE_PREFIX}:{request_id}"
    cache_key = f"{PRICE_CACHE_PREFIX}:{item_id}"

    try:
        raw = db.get(item_id)
    except redis.exceptions.RedisError as e:
        logger.error(f"LOOKUP_PRICE {request_id}: Redis error reading item {item_id}: {e}")
        result = json.dumps({"found": False, "reason": str(e)})
        order_db.set(resp_key, result, ex=PRICE_RESPONSE_TTL)
        # Return a special result that tells participant not to publish response
        return {"status": "SKIP_RESPONSE", "reason": ""}

    if raw is None:
        result = json.dumps({"found": False, "reason": f"Item {item_id} not found"})
        order_db.set(resp_key, result, ex=PRICE_RESPONSE_TTL)
        return {"status": "SKIP_RESPONSE", "reason": ""}

    item = msgpack.decode(raw, type=StockValue)
    result = json.dumps({"found": True, "price": item.price})

    # Write to both response key and cache
    pipe = order_db.pipeline(transaction=False)
    pipe.set(resp_key, result, ex=PRICE_RESPONSE_TTL)
    pipe.set(cache_key, result, ex=PRICE_CACHE_TTL)
    pipe.execute()

    logger.info(f"LOOKUP_PRICE {request_id}: item {item_id} price={item.price}")

    # Return SKIP_RESPONSE since we handled the response ourselves
    return {"status": "SKIP_RESPONSE", "reason": ""}


# ── Application startup ───────────────────────────────────────────────────────

def _start_participant() -> None:
    """Start the saga participant."""
    participant.start()
    logger.info("Stock service SAGA participant started")


_start_participant()


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)

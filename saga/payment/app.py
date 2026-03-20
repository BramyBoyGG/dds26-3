"""
Payment Service - Refactored to use the Orchestrator library.

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

import redis

from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response

# Import from orchestrator library
from orchestrator import SagaParticipant

from common.redis_client import get_redis_connection
from common.protocol import (
    PAYMENT_COMMANDS_STREAM,
    PAYMENT_CONSUMER_GROUP,
    CMD_DEDUCT_PAYMENT,
    CMD_REFUND_PAYMENT,
    STATUS_SUCCESS,
    STATUS_FAILURE,
)
from common.logging_utils import setup_logging


DB_ERROR_STR = "DB error"

app = Flask("payment-service")
logger = setup_logging("payment-service")

# ── Redis connections ────────────────────────────────────────────────────────
# Own database (payment-db): stores user data + payment-commands stream
db: redis.Redis = get_redis_connection(
    "REDIS_MASTER_NAME", "REDIS_PASSWORD",
    "REDIS_HOST", "REDIS_PORT", "REDIS_DB",
)

# Order database (order-db): for publishing tx-responses back to the orchestrator
order_db: redis.Redis = get_redis_connection(
    "ORDER_REDIS_MASTER_NAME", "ORDER_REDIS_PASSWORD",
    "ORDER_REDIS_HOST", "ORDER_REDIS_PORT", "ORDER_REDIS_DB",
)


def close_db_connection():
    db.close()
    order_db.close()


atexit.register(close_db_connection)


# ── Data model ───────────────────────────────────────────────────────────────

class UserValue(Struct):
    credit: int


# ── Lua scripts for atomic credit operations ─────────────────────────────────

# Atomic subtract: GET -> decode -> check credit >= amount -> subtract -> SET
# Returns 1 on success, 0 on insufficient credit, -1 if user not found
LUA_SUBTRACT_CREDIT = """
local key = KEYS[1]
local amount = tonumber(ARGV[1])

local data = redis.call('GET', key)
if not data then
    return -1
end

local decoded = cmsgpack.unpack(data)
if decoded.credit == nil then
    decoded.credit = 0
end

if decoded.credit < amount then
    return 0
end

decoded.credit = decoded.credit - amount
redis.call('SET', key, cmsgpack.pack(decoded))
return 1
"""

# Atomic add: GET -> decode -> add -> SET
# Returns 1 on success, -1 if user not found
LUA_ADD_CREDIT = """
local key = KEYS[1]
local amount = tonumber(ARGV[1])

local data = redis.call('GET', key)
local decoded
if not data then
    return -1
else
    decoded = cmsgpack.unpack(data)
    if decoded.credit == nil then
        decoded.credit = 0
    end
end
decoded.credit = decoded.credit + amount

redis.call('SET', key, cmsgpack.pack(decoded))
return 1
"""

# Register Lua scripts
subtract_credit_script = db.register_script(LUA_SUBTRACT_CREDIT)
add_credit_script = db.register_script(LUA_ADD_CREDIT)


# ── Helper: read user from DB ────────────────────────────────────────────────

def get_user_from_db(user_id: str) -> UserValue | None:
    try:
        entry: bytes = db.get(user_id)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    entry: UserValue | None = msgpack.decode(entry, type=UserValue) if entry else None
    if entry is None:
        abort(400, f"User: {user_id} not found!")
    return entry


# ── REST endpoints ───────────────────────────────────────────────────────────

@app.post('/create_user')
def create_user():
    key = str(uuid.uuid4())
    value = msgpack.encode(UserValue(credit=0))
    try:
        db.set(key, value)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({'user_id': key})


@app.post('/batch_init/<n>/<starting_money>')
def batch_init_users(n: int, starting_money: int):
    n = int(n)
    starting_money = int(starting_money)
    kv_pairs: dict[str, bytes] = {
        f"{i}": msgpack.encode(UserValue(credit=starting_money))
        for i in range(n)
    }
    try:
        db.mset(kv_pairs)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({"msg": "Batch init for users successful"})


@app.get('/find_user/<user_id>')
def find_user(user_id: str):
    user_entry: UserValue = get_user_from_db(user_id)
    return jsonify(
        {
            "user_id": user_id,
            "credit": user_entry.credit
        }
    )


@app.post('/add_funds/<user_id>/<amount>')
def add_credit(user_id: str, amount: int):
    amount = int(amount)
    result = add_credit_script(keys=[user_id], args=[amount])
    if result == -1:
        abort(400, f"User: {user_id} not found!")
    user_entry = get_user_from_db(user_id)
    return Response(f"User: {user_id} credit updated to: {user_entry.credit}", status=200)


@app.post('/pay/<user_id>/<amount>')
def remove_credit(user_id: str, amount: int):
    amount = int(amount)
    app.logger.debug(f"Removing {amount} credit from user: {user_id}")
    result = subtract_credit_script(keys=[user_id], args=[amount])
    if result == -1:
        abort(400, f"User: {user_id} not found!")
    if result == 0:
        abort(400, f"User: {user_id} credit cannot get reduced below zero!")
    user_entry = get_user_from_db(user_id)
    return Response(f"User: {user_id} credit updated to: {user_entry.credit}", status=200)


# ── SAGA Participant ─────────────────────────────────────────────────────────
# Create a SagaParticipant that handles SAGA commands

participant = SagaParticipant(
    name="payment-service",
    own_db=db,
    orchestrator_db=order_db,
    command_stream=PAYMENT_COMMANDS_STREAM,
    response_stream="tx-responses",
    consumer_group=PAYMENT_CONSUMER_GROUP,
)


@participant.handler(CMD_DEDUCT_PAYMENT)
def handle_deduct_payment(tx_id: str, payload: dict) -> dict:
    """Atomically deduct credit from a user."""
    user_id = payload.get("user_id")
    amount = int(payload.get("amount", 0))

    result_code = subtract_credit_script(keys=[user_id], args=[amount])

    if result_code == -1:
        return {"status": STATUS_FAILURE, "reason": f"User {user_id} not found"}
    elif result_code == 0:
        return {"status": STATUS_FAILURE, "reason": f"Insufficient credit for user {user_id}"}
    else:
        logger.info(f"[{tx_id}] Deducted {amount} from user {user_id}")
        return {"status": STATUS_SUCCESS, "reason": ""}


@participant.handler(CMD_REFUND_PAYMENT)
def handle_refund_payment(tx_id: str, payload: dict) -> dict:
    """Add credit back to a user (compensation). Always succeeds."""
    user_id = payload.get("user_id")
    amount = int(payload.get("amount", 0))

    add_credit_script(keys=[user_id], args=[amount])
    logger.info(f"[{tx_id}] Refunded {amount} to user {user_id}")
    return {"status": STATUS_SUCCESS, "reason": ""}


# ── Application startup ───────────────────────────────────────────────────────

def _start_participant() -> None:
    """Start the saga participant."""
    participant.start()
    logger.info("Payment service SAGA participant started")


_start_participant()


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)

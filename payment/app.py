import logging
import os
import atexit
import uuid
import threading

import redis

from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response

# Import common SAGA utilities
from common.protocol import (
    PAYMENT_COMMANDS_STREAM,
    PAYMENT_CONSUMER_GROUP,
    TX_RESPONSES_STREAM,
    CMD_DEDUCT_PAYMENT,
    CMD_REFUND_PAYMENT,
    STATUS_SUCCESS,
    STATUS_FAILURE,
)
from common.stream_helpers import (
    create_consumer_group,
    publish_message,
    read_messages,
    ack_message,
    claim_stale_pending,
)
from common.serialization import decode_command, encode_response
from common.idempotency import is_processed, mark_processed, get_cached_result
from common.logging_utils import setup_logging, get_consumer_id, log_tx

DB_ERROR_STR = "DB error"

app = Flask("payment-service")

# --- Redis connections ---
# Own database (payment-db): stores user data + payment-commands stream
db: redis.Redis = redis.Redis(
    host=os.environ['REDIS_HOST'],
    port=int(os.environ['REDIS_PORT']),
    password=os.environ['REDIS_PASSWORD'],
    db=int(os.environ['REDIS_DB']),
)

# Order database (order-db): for publishing tx-responses back to the orchestrator
order_db: redis.Redis = redis.Redis(
    host=os.environ.get('ORDER_REDIS_HOST', 'order-db'),
    port=int(os.environ.get('ORDER_REDIS_PORT', '6379')),
    password=os.environ.get('ORDER_REDIS_PASSWORD', 'redis'),
    db=int(os.environ.get('ORDER_REDIS_DB', '0')),
)


def close_db_connection():
    db.close()
    order_db.close()


atexit.register(close_db_connection)

# --- Structured logger ---
saga_logger = setup_logging("payment-service")

# --- Data model ---


class UserValue(Struct):
    credit: int


# ---------------------------------------------------------------------------
# Lua scripts for atomic credit operations
# ---------------------------------------------------------------------------

# Atomic subtract: GET -> decode -> check credit >= amount -> subtract -> SET
# Returns 1 on success, 0 on insufficient credit, -1 if user not found
LUA_SUBTRACT_CREDIT = """
local key = KEYS[1]
local amount = tonumber(ARGV[1])

local data = redis.call('GET', key)
if not data then
    return -1
end

-- msgpack: UserValue has one field (credit), encoded as a fixarray of 1 element
-- msgspec encodes Struct as a msgpack array, so [credit_value]
-- We use cmsgpack to decode/encode
local decoded = cmsgpack.unpack(data)
local credit = decoded[1]

if credit < amount then
    return 0
end

decoded[1] = credit - amount
redis.call('SET', key, cmsgpack.pack(decoded))
return 1
"""

# Atomic add: GET -> decode -> add -> SET
# Returns 1 on success, -1 if user not found
LUA_ADD_CREDIT = """
local key = KEYS[1]
local amount = tonumber(ARGV[1])

local data = redis.call('GET', key)
if not data then
    return -1
end

local decoded = cmsgpack.unpack(data)
decoded[1] = decoded[1] + amount
redis.call('SET', key, cmsgpack.pack(decoded))
return 1
"""

# Register Lua scripts
subtract_credit_script = db.register_script(LUA_SUBTRACT_CREDIT)
add_credit_script = db.register_script(LUA_ADD_CREDIT)


# ---------------------------------------------------------------------------
# Helper: read user from DB
# ---------------------------------------------------------------------------

def get_user_from_db(user_id: str) -> UserValue | None:
    try:
        entry: bytes = db.get(user_id)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    entry: UserValue | None = msgpack.decode(entry, type=UserValue) if entry else None
    if entry is None:
        abort(400, f"User: {user_id} not found!")
    return entry


# ---------------------------------------------------------------------------
# REST endpoints (kept working for benchmark compatibility)
# ---------------------------------------------------------------------------

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
    # Read back the updated value for the response
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


# ---------------------------------------------------------------------------
# SAGA stream handlers
# ---------------------------------------------------------------------------

def handle_deduct_payment(tx_id: str, user_id: str, amount: int) -> dict:
    """Atomically deduct credit from a user. Returns result dict."""
    # Idempotency check
    if is_processed(db, tx_id, CMD_DEDUCT_PAYMENT):
        cached = get_cached_result(db, tx_id, CMD_DEDUCT_PAYMENT)
        if cached:
            return cached

    result_code = subtract_credit_script(keys=[user_id], args=[amount])

    if result_code == -1:
        result = {"status": STATUS_FAILURE, "reason": f"User {user_id} not found"}
    elif result_code == 0:
        result = {"status": STATUS_FAILURE, "reason": f"Insufficient credit for user {user_id}"}
    else:
        result = {"status": STATUS_SUCCESS, "reason": ""}

    mark_processed(db, tx_id, CMD_DEDUCT_PAYMENT, result)
    return result


def handle_refund_payment(tx_id: str, user_id: str, amount: int) -> dict:
    """Add credit back to a user (compensation). Always succeeds."""
    # Idempotency check
    if is_processed(db, tx_id, CMD_REFUND_PAYMENT):
        cached = get_cached_result(db, tx_id, CMD_REFUND_PAYMENT)
        if cached:
            return cached

    add_credit_script(keys=[user_id], args=[amount])
    result = {"status": STATUS_SUCCESS, "reason": ""}

    mark_processed(db, tx_id, CMD_REFUND_PAYMENT, result)
    return result


# ---------------------------------------------------------------------------
# Stream consumer thread
# ---------------------------------------------------------------------------

def stream_consumer_loop():
    """Background thread that consumes payment-commands from payment-db."""
    consumer_id = get_consumer_id()
    saga_logger.info(f"Starting stream consumer: {consumer_id}")

    # Create consumer group (idempotent)
    create_consumer_group(db, PAYMENT_COMMANDS_STREAM, PAYMENT_CONSUMER_GROUP)

    claim_counter = 0

    while True:
        try:
            # Periodically claim stale pending messages (crash recovery)
            claim_counter += 1
            if claim_counter % 6 == 0:  # Every ~30s (6 * 5s block)
                stale = claim_stale_pending(
                    db, PAYMENT_COMMANDS_STREAM, PAYMENT_CONSUMER_GROUP, consumer_id
                )
                for msg_id, fields in stale:
                    _process_message(msg_id, fields, consumer_id)

            # Read new messages (blocks up to 5 seconds)
            messages = read_messages(
                db, PAYMENT_COMMANDS_STREAM, PAYMENT_CONSUMER_GROUP, consumer_id
            )

            for msg_id, fields in messages:
                _process_message(msg_id, fields, consumer_id)

        except Exception:
            saga_logger.exception("Error in stream consumer loop")


def _process_message(msg_id, fields, consumer_id):
    """Process a single command message from the payment-commands stream."""
    try:
        parsed = decode_command(fields)
        tx_id = parsed["tx_id"]
        command = parsed["command"]

        log_tx(saga_logger, tx_id, command, f"Received command")

        if command == CMD_DEDUCT_PAYMENT:
            user_id = parsed["user_id"]
            amount = int(parsed["amount"])
            result = handle_deduct_payment(tx_id, user_id, amount)
        elif command == CMD_REFUND_PAYMENT:
            user_id = parsed["user_id"]
            amount = int(parsed["amount"])
            result = handle_refund_payment(tx_id, user_id, amount)
        else:
            saga_logger.warning(f"Unknown command: {command}")
            ack_message(db, PAYMENT_COMMANDS_STREAM, PAYMENT_CONSUMER_GROUP, msg_id)
            return

        # Publish response to tx-responses stream in order-db
        response_fields = encode_response(
            tx_id=tx_id,
            step=command,
            status=result["status"],
            reason=result.get("reason", ""),
        )
        publish_message(order_db, TX_RESPONSES_STREAM, response_fields)

        log_tx(saga_logger, tx_id, command, f"Result: {result['status']}")

        # Acknowledge the message
        ack_message(db, PAYMENT_COMMANDS_STREAM, PAYMENT_CONSUMER_GROUP, msg_id)

    except Exception:
        saga_logger.exception(f"Error processing message {msg_id}")


# ---------------------------------------------------------------------------
# Start the consumer thread when the app loads
# ---------------------------------------------------------------------------

_consumer_started = False


def start_consumer():
    global _consumer_started
    if _consumer_started:
        return
    _consumer_started = True
    t = threading.Thread(target=stream_consumer_loop, daemon=True)
    t.start()
    saga_logger.info("Payment stream consumer thread started")


# Start consumer on import (works with gunicorn workers)
start_consumer()


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)

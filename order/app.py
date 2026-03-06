import logging
import os
import atexit
import random
import threading
import time
import uuid
from collections import defaultdict

import redis
import requests

from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response

from common.protocol import (
    STOCK_COMMANDS_STREAM, PAYMENT_COMMANDS_STREAM, TX_RESPONSES_STREAM,
    TX_RESPONSES_CONSUMER_GROUP,
    CMD_RESERVE_STOCK, CMD_COMPENSATE_STOCK, CMD_DEDUCT_PAYMENT,
    TX_STARTED, TX_RESERVING_STOCK, TX_DEDUCTING_PAYMENT,
    TX_COMPENSATING_STOCK, TX_COMPLETED, TX_ABORTED, TERMINAL_STATES,
    TX_LOG_PREFIX, RESPONSE_TIMEOUT_S, STATUS_SUCCESS,
    PENDING_CLAIM_MIN_IDLE_MS,
)
from common.stream_helpers import (
    create_consumer_group, publish_message, read_messages,
    ack_message, claim_stale_pending,
)
from common.serialization import encode_command, decode_response
from common.logging_utils import setup_logging, get_consumer_id, log_tx, log_tx_state_change


DB_ERROR_STR = "DB error"
REQ_ERROR_STR = "Requests error"

GATEWAY_URL = os.environ['GATEWAY_URL']

app = Flask("order-service")
logger = setup_logging("order-service")

# ── Redis connections ────────────────────────────────────────────────────────
# order-db: stores orders, the transaction log, and the tx-responses stream
db: redis.Redis = redis.Redis(
    host=os.environ['REDIS_HOST'],
    port=int(os.environ['REDIS_PORT']),
    password=os.environ['REDIS_PASSWORD'],
    db=int(os.environ['REDIS_DB']),
)

# stock-db: Order publishes RESERVE_STOCK / COMPENSATE_STOCK here
stock_db: redis.Redis = redis.Redis(
    host=os.environ['STOCK_REDIS_HOST'],
    port=int(os.environ.get('STOCK_REDIS_PORT', '6379')),
    password=os.environ.get('STOCK_REDIS_PASSWORD', 'redis'),
    db=int(os.environ.get('STOCK_REDIS_DB', '0')),
)

# payment-db: Order publishes DEDUCT_PAYMENT here
payment_db: redis.Redis = redis.Redis(
    host=os.environ['PAYMENT_REDIS_HOST'],
    port=int(os.environ.get('PAYMENT_REDIS_PORT', '6379')),
    password=os.environ.get('PAYMENT_REDIS_PASSWORD', 'redis'),
    db=int(os.environ.get('PAYMENT_REDIS_DB', '0')),
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


class TransactionLog(Struct):
    """
    Persisted record of a checkout SAGA in progress (or completed).

    Stored in order-db under the key  tx:{tx_id}.
    The `state` field drives the state machine; it must always be written
    to Redis before any command is published so that recovery on restart
    can re-publish the right command.
    """
    tx_id: str
    order_id: str
    state: str          # One of the TX_* constants from protocol.py
    items: list[tuple[str, int]]   # Aggregated (item_id, quantity) pairs
    user_id: str
    total_cost: int
    created_at: float   # Unix timestamp (time.time())


# ── Transaction log helpers ──────────────────────────────────────────────────

def _tx_key(tx_id: str) -> str:
    return f"{TX_LOG_PREFIX}:{tx_id}"


def save_tx_log(tx: TransactionLog) -> None:
    db.set(_tx_key(tx.tx_id), msgpack.encode(tx))


def load_tx_log(tx_id: str) -> TransactionLog | None:
    raw = db.get(_tx_key(tx_id))
    if raw is None:
        return None
    return msgpack.decode(raw, type=TransactionLog)


# ── Stream publishing helpers ─────────────────────────────────────────────────

def _publish_reserve_stock(tx: TransactionLog) -> None:
    fields = encode_command(
        tx_id=tx.tx_id,
        command=CMD_RESERVE_STOCK,
        payload={"items": tx.items},
    )
    publish_message(stock_db, STOCK_COMMANDS_STREAM, fields)
    log_tx(logger, tx.tx_id, CMD_RESERVE_STOCK, "Command published to stock-db")


def _publish_deduct_payment(tx: TransactionLog) -> None:
    fields = encode_command(
        tx_id=tx.tx_id,
        command=CMD_DEDUCT_PAYMENT,
        payload={"user_id": tx.user_id, "amount": tx.total_cost},
    )
    publish_message(payment_db, PAYMENT_COMMANDS_STREAM, fields)
    log_tx(logger, tx.tx_id, CMD_DEDUCT_PAYMENT, "Command published to payment-db")


def _publish_compensate_stock(tx: TransactionLog) -> None:
    fields = encode_command(
        tx_id=tx.tx_id,
        command=CMD_COMPENSATE_STOCK,
        payload={"items": tx.items},
    )
    publish_message(stock_db, STOCK_COMMANDS_STREAM, fields)
    log_tx(logger, tx.tx_id, CMD_COMPENSATE_STOCK, "Command published to stock-db")


# ── State machine ─────────────────────────────────────────────────────────────

def _advance_state_machine(tx: TransactionLog, response: dict) -> None:
    """
    Given an incoming tx-response, transition the saga to its next state and
    publish the next command (or signal completion if terminal).

    State transitions:
      RESERVING_STOCK  + SUCCESS  -> DEDUCTING_PAYMENT  (publish DEDUCT_PAYMENT)
      RESERVING_STOCK  + FAILURE  -> ABORTED            (done)
      DEDUCTING_PAYMENT + SUCCESS -> COMPLETED           (mark order paid, done)
      DEDUCTING_PAYMENT + FAILURE -> COMPENSATING_STOCK  (publish COMPENSATE_STOCK)
      COMPENSATING_STOCK + *      -> ABORTED             (done)
    """
    status = response.get("status", "")
    old_state = tx.state

    if tx.state == TX_RESERVING_STOCK:
        if status == STATUS_SUCCESS:
            tx.state = TX_DEDUCTING_PAYMENT
            save_tx_log(tx)
            log_tx_state_change(logger, tx.tx_id, old_state, tx.state)
            _publish_deduct_payment(tx)
        else:
            tx.state = TX_ABORTED
            save_tx_log(tx)
            log_tx_state_change(logger, tx.tx_id, old_state, tx.state)

    elif tx.state == TX_DEDUCTING_PAYMENT:
        if status == STATUS_SUCCESS:
            # Mark the order as paid in the orders data store
            _mark_order_paid(tx.order_id)
            tx.state = TX_COMPLETED
            save_tx_log(tx)
            log_tx_state_change(logger, tx.tx_id, old_state, tx.state)
        else:
            tx.state = TX_COMPENSATING_STOCK
            save_tx_log(tx)
            log_tx_state_change(logger, tx.tx_id, old_state, tx.state)
            _publish_compensate_stock(tx)

    elif tx.state == TX_COMPENSATING_STOCK:
        # COMPENSATE_STOCK always succeeds; abort the transaction
        tx.state = TX_ABORTED
        save_tx_log(tx)
        log_tx_state_change(logger, tx.tx_id, old_state, tx.state)

    else:
        logger.warning(
            f"Unexpected response for tx {tx.tx_id} in state {tx.state}: {response}"
        )


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


# ── Consumer thread ───────────────────────────────────────────────────────────

CONSUMER_NAME = get_consumer_id()


def _process_response_message(msg_id: str, raw_fields: dict) -> None:
    """Parse one tx-response message and drive the state machine."""
    try:
        response = decode_response(raw_fields)
        tx_id = response["tx_id"]
        log_tx(logger, tx_id, response.get("step", "?"), f"Response received: {response['status']}")

        tx = load_tx_log(tx_id)
        if tx is None:
            logger.warning(f"Received response for unknown tx_id: {tx_id} — ACKing and skipping")
            ack_message(db, TX_RESPONSES_STREAM, TX_RESPONSES_CONSUMER_GROUP, msg_id)
            return

        if tx.state in TERMINAL_STATES:
            # Duplicate response for an already-finished tx — safe to ignore
            ack_message(db, TX_RESPONSES_STREAM, TX_RESPONSES_CONSUMER_GROUP, msg_id)
            return

        _advance_state_machine(tx, response)
        ack_message(db, TX_RESPONSES_STREAM, TX_RESPONSES_CONSUMER_GROUP, msg_id)

    except Exception as e:
        logger.exception(f"Error processing response message {msg_id}: {e}")
        # Do NOT ACK — leave it in the PEL so it can be re-claimed and retried


def _response_consumer_loop() -> None:
    """
    Background thread: reads tx-responses from order-db and drives the state machine.

    Runs forever as a daemon thread.  Creates the consumer group on startup
    (idempotent), then loops reading messages and periodically claiming stale
    pending messages from crashed sibling consumers.
    """
    create_consumer_group(db, TX_RESPONSES_STREAM, TX_RESPONSES_CONSUMER_GROUP)
    last_claim_check = time.monotonic()

    while True:
        # Periodically claim stale pending messages (handles consumer crashes)
        now = time.monotonic()
        if now - last_claim_check >= 30:
            stale = claim_stale_pending(
                db, TX_RESPONSES_STREAM, TX_RESPONSES_CONSUMER_GROUP, CONSUMER_NAME,
                min_idle_ms=PENDING_CLAIM_MIN_IDLE_MS,
            )
            for msg_id, raw_fields in stale:
                _process_response_message(msg_id, raw_fields)
            last_claim_check = now

        messages = read_messages(
            db, TX_RESPONSES_STREAM, TX_RESPONSES_CONSUMER_GROUP, CONSUMER_NAME
        )
        for msg_id, raw_fields in messages:
            _process_response_message(msg_id, raw_fields)


# ── Startup recovery ──────────────────────────────────────────────────────────

def _recover_in_flight_transactions() -> None:
    """
    On startup, scan for any transactions that were left in a non-terminal
    state (e.g., because the Order service crashed mid-saga).  Re-publish
    the appropriate command so the saga can continue.

    This is safe because Stock and Payment services are idempotent: they
    won't double-apply a command for the same tx_id.
    """
    try:
        keys = db.keys(f"{TX_LOG_PREFIX}:*")
    except redis.exceptions.RedisError as e:
        logger.error(f"Startup recovery: failed to scan tx keys: {e}")
        return

    recovered = 0
    for key in keys:
        try:
            raw = db.get(key)
            if not raw:
                continue
            tx = msgpack.decode(raw, type=TransactionLog)
            if tx.state in TERMINAL_STATES:
                continue

            logger.info(f"Recovery: tx {tx.tx_id} stuck in state {tx.state} — re-publishing command")

            if tx.state in (TX_STARTED, TX_RESERVING_STOCK):
                # Treat STARTED as if we already wrote RESERVING_STOCK
                tx.state = TX_RESERVING_STOCK
                save_tx_log(tx)
                _publish_reserve_stock(tx)
            elif tx.state == TX_DEDUCTING_PAYMENT:
                _publish_deduct_payment(tx)
            elif tx.state == TX_COMPENSATING_STOCK:
                _publish_compensate_stock(tx)

            recovered += 1
        except Exception as e:
            logger.exception(f"Recovery failed for key {key}: {e}")

    if recovered:
        logger.info(f"Startup recovery: re-published commands for {recovered} in-flight transaction(s)")


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


def send_post_request(url: str):
    try:
        response = requests.post(url)
    except requests.exceptions.RequestException:
        abort(400, REQ_ERROR_STR)
    else:
        return response


def send_get_request(url: str):
    try:
        response = requests.get(url)
    except requests.exceptions.RequestException:
        abort(400, REQ_ERROR_STR)
    else:
        return response


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
    order_entry: OrderValue = get_order_from_db(order_id)
    item_reply = send_get_request(f"{GATEWAY_URL}/stock/find/{item_id}")
    if item_reply.status_code != 200:
        abort(400, f"Item: {item_id} does not exist!")
    item_json: dict = item_reply.json()
    order_entry.items.append((item_id, int(quantity)))
    order_entry.total_cost += int(quantity) * item_json["price"]
    try:
        db.set(order_id, msgpack.encode(order_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(
        f"Item: {item_id} added to: {order_id} price updated to: {order_entry.total_cost}",
        status=200,
    )


@app.post('/checkout/<order_id>')
def checkout(order_id: str):
    """
    Orchestrated checkout using the SAGA pattern.

    Instead of making synchronous REST calls to stock and payment, we:
      1. Create a TransactionLog entry in order-db.
      2. Publish a RESERVE_STOCK command to stock-db.
      3. Poll the transaction log until it reaches a terminal state.
      4. Return 200 (COMPLETED) or 400 (ABORTED / timeout).

    The consumer thread (running in the background) drives the state machine
    by reading tx-responses and publishing follow-up commands.

    Polling is used instead of threading.Event so that multiple gunicorn
    workers can all safely wait for the same transaction — each worker polls
    Redis independently and the consumer thread (also one per worker) updates
    the shared tx log in Redis.
    """
    app.logger.debug(f"Checking out {order_id}")
    order_entry: OrderValue = get_order_from_db(order_id)

    if order_entry.paid:
        return abort(400, "Order already paid")

    # Aggregate item quantities (in case the same item was added multiple times)
    items_quantities: dict[str, int] = defaultdict(int)
    for item_id, quantity in order_entry.items:
        items_quantities[item_id] += quantity
    items = list(items_quantities.items())

    # Create transaction log entry
    tx_id = str(uuid.uuid4())
    tx = TransactionLog(
        tx_id=tx_id,
        order_id=order_id,
        state=TX_STARTED,
        items=items,
        user_id=order_entry.user_id,
        total_cost=order_entry.total_cost,
        created_at=time.time(),
    )
    save_tx_log(tx)
    log_tx(logger, tx_id, TX_STARTED, f"Checkout started for order {order_id}")

    # Transition to RESERVING_STOCK and publish the first command.
    # We MUST save the new state to Redis BEFORE publishing the command so
    # that, if we crash right after publishing, recovery will re-publish it.
    tx.state = TX_RESERVING_STOCK
    save_tx_log(tx)
    _publish_reserve_stock(tx)

    # Poll the transaction log until the saga reaches a terminal state.
    # We poll Redis (not a threading.Event) so this works correctly with
    # multiple gunicorn worker processes.
    deadline = time.monotonic() + RESPONSE_TIMEOUT_S
    poll_interval = 0.05  # 50 ms

    tx = None
    while time.monotonic() < deadline:
        tx = load_tx_log(tx_id)
        if tx and tx.state in TERMINAL_STATES:
            break
        time.sleep(poll_interval)

    if tx is None or tx.state not in TERMINAL_STATES:
        app.logger.warning(f"Checkout timed out for tx {tx_id} / order {order_id}")
        return abort(400, "Checkout timed out")

    if tx.state == TX_COMPLETED:
        app.logger.debug(f"Checkout successful for order {order_id}")
        return Response("Checkout successful", status=200)
    else:
        return abort(400, "Checkout failed: insufficient stock or credit")


# ── Application startup ───────────────────────────────────────────────────────

def _start_background_consumer() -> None:
    """Run startup recovery then launch the consumer thread."""
    _recover_in_flight_transactions()
    t = threading.Thread(
        target=_response_consumer_loop,
        daemon=True,
        name="tx-response-consumer",
    )
    t.start()
    logger.info(f"Consumer thread started (consumer_name={CONSUMER_NAME})")


_start_background_consumer()


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)

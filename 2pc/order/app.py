import logging
import os
import atexit
import random
import uuid
from collections import defaultdict

import redis
import requests

from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response


DB_ERROR_STR = "DB error"
REQ_ERROR_STR = "Requests error"

GATEWAY_URL = os.environ['GATEWAY_URL']

app = Flask("order-service")

db: redis.Redis = redis.Redis(host=os.environ['REDIS_HOST'],
                              port=int(os.environ['REDIS_PORT']),
                              password=os.environ['REDIS_PASSWORD'],
                              db=int(os.environ['REDIS_DB']))


def close_db_connection():
    db.close()


atexit.register(close_db_connection)


# ═══════════════════════════════════════════════════════════════════════════
# Data Model
# ═══════════════════════════════════════════════════════════════════════════

class OrderValue(Struct):
    paid: bool
    items: list[tuple[str, int]]
    user_id: str
    total_cost: int


# ═══════════════════════════════════════════════════════════════════════════
# Helper Functions
# ═══════════════════════════════════════════════════════════════════════════

def get_order_from_db(order_id: str) -> OrderValue | None:
    """Retrieve an order from Redis, or abort with 400 if not found."""
    try:
        entry: bytes = db.get(order_id)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    entry: OrderValue | None = msgpack.decode(entry, type=OrderValue) if entry else None
    if entry is None:
        abort(400, f"Order: {order_id} not found!")
    return entry


def send_post_request(url: str):
    """Send a POST request without a body. Used for simple endpoints."""
    try:
        response = requests.post(url)
    except requests.exceptions.RequestException:
        abort(400, REQ_ERROR_STR)
    else:
        return response


def send_get_request(url: str):
    """Send a GET request. Used to look up item prices."""
    try:
        response = requests.get(url)
    except requests.exceptions.RequestException:
        abort(400, REQ_ERROR_STR)
    else:
        return response


def send_post_request_json(url: str, data: dict):
    """Send a POST request with a JSON body. Used for 2PC prepare requests."""
    try:
        response = requests.post(url, json=data, timeout=10)
    except requests.exceptions.RequestException:
        return None  # Return None so the caller can handle the failure
    return response


# ═══════════════════════════════════════════════════════════════════════════
# Existing REST Endpoints (unchanged)
# ═══════════════════════════════════════════════════════════════════════════

@app.post('/create/<user_id>')
def create_order(user_id: str):
    """Create a new empty order for a user."""
    key = str(uuid.uuid4())
    value = msgpack.encode(OrderValue(paid=False, items=[], user_id=user_id, total_cost=0))
    try:
        db.set(key, value)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({'order_id': key})


@app.post('/batch_init/<n>/<n_items>/<n_users>/<item_price>')
def batch_init_users(n: int, n_items: int, n_users: int, item_price: int):
    """Batch-create n orders with random items and users."""
    n = int(n)
    n_items = int(n_items)
    n_users = int(n_users)
    item_price = int(item_price)

    def generate_entry() -> OrderValue:
        user_id = random.randint(0, n_users - 1)
        item1_id = random.randint(0, n_items - 1)
        item2_id = random.randint(0, n_items - 1)
        value = OrderValue(paid=False,
                           items=[(f"{item1_id}", 1), (f"{item2_id}", 1)],
                           user_id=f"{user_id}",
                           total_cost=2*item_price)
        return value

    kv_pairs: dict[str, bytes] = {f"{i}": msgpack.encode(generate_entry())
                                  for i in range(n)}
    try:
        db.mset(kv_pairs)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({"msg": "Batch init for orders successful"})


@app.get('/find/<order_id>')
def find_order(order_id: str):
    """Look up an order's details."""
    order_entry: OrderValue = get_order_from_db(order_id)
    return jsonify(
        {
            "order_id": order_id,
            "paid": order_entry.paid,
            "items": order_entry.items,
            "user_id": order_entry.user_id,
            "total_cost": order_entry.total_cost
        }
    )


@app.post('/addItem/<order_id>/<item_id>/<quantity>')
def add_item(order_id: str, item_id: str, quantity: int):
    """Add an item to an order (looks up price from the stock service)."""
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
    return Response(f"Item: {item_id} added to: {order_id} price updated to: {order_entry.total_cost}",
                    status=200)


# ═══════════════════════════════════════════════════════════════════════════
# 2PC Coordinator — Checkout
# ═══════════════════════════════════════════════════════════════════════════
#
# The Order Service acts as the COORDINATOR in the 2PC protocol.
# It orchestrates the distributed transaction across two participants:
#   - Stock Service  (reserve/release items)
#   - Payment Service (deduct/refund credit)
#
# The checkout flow:
#
#   ┌─────────────────────────────────────────────────────────────────┐
#   │                                                                 │
#   │  1. Generate a unique transaction ID (tx_id)                    │
#   │                                                                 │
#   │  ═══ PHASE 1: PREPARE (Voting) ═══                              │
#   │                                                                 │
#   │  2. Send PREPARE to Stock Service                               │
#   │     → "Can you reserve these items?"                            │
#   │     → Stock subtracts tentatively, votes YES or NO              │
#   │                                                                 │
#   │  3. Send PREPARE to Payment Service                             │
#   │     → "Can you deduct this amount from the user?"               │
#   │     → Payment subtracts tentatively, votes YES or NO            │
#   │                                                                 │
#   │  ═══ DECISION ═══                                               │
#   │                                                                 │
#   │  4. If ALL voted YES → go to COMMIT                             │
#   │     If ANY voted NO  → go to ABORT                              │
#   │                                                                 │
#   │  ═══ PHASE 2: COMMIT ═══                                        │
#   │                                                                 │
#   │  5. Send COMMIT to Stock Service                                │
#   │  6. Send COMMIT to Payment Service                              │
#   │  7. Mark order as paid → return 200                             │
#   │                                                                 │
#   │  ═══ PHASE 2: ABORT ═══                                         │
#   │                                                                 │
#   │  5. Send ABORT to all participants that were prepared            │
#   │  6. Return 400 (checkout failed)                                │
#   │                                                                 │
#   └─────────────────────────────────────────────────────────────────┘
#
# ═══════════════════════════════════════════════════════════════════════════

@app.post('/checkout/<order_id>')
def checkout(order_id: str):
    """
    2PC Coordinator: orchestrate a distributed checkout transaction.

    This replaces the old simple-rollback approach with a proper
    Two-Phase Commit protocol.
    """
    app.logger.debug(f"Checking out {order_id}")
    order_entry: OrderValue = get_order_from_db(order_id)

    # Don't allow double-checkout
    if order_entry.paid:
        return Response("Order already paid", status=200)

    # ── Aggregate item quantities ──
    # If the same item was added multiple times, combine the quantities.
    # e.g., [(A, 1), (A, 2), (B, 1)] → {A: 3, B: 1}
    items_quantities: dict[str, int] = defaultdict(int)
    for item_id, quantity in order_entry.items:
        items_quantities[item_id] += quantity
    items = list(items_quantities.items())

    if not items:
        abort(400, "Order has no items")

    # ── Generate a unique transaction ID ──
    # This tx_id ties together all the prepare/commit/abort calls
    # across both participants.
    tx_id = str(uuid.uuid4())
    app.logger.info(f"2PC CHECKOUT {tx_id}: Starting for order {order_id}")

    # Track which participants have been successfully prepared.
    # We need this so that if Phase 1 fails, we know who to abort.
    stock_prepared = False
    payment_prepared = False

    # ═══════════════════════════════════════════════════════════════════
    # PHASE 1: PREPARE (Voting)
    # Ask each participant: "Can you commit?"
    # ═══════════════════════════════════════════════════════════════════

    # ── Step 1: Prepare Stock Service ──
    # Ask the stock service to reserve all items in the order.
    app.logger.info(f"2PC CHECKOUT {tx_id}: Phase 1 — Preparing stock...")
    stock_resp = send_post_request_json(
        f"{GATEWAY_URL}/stock/2pc/prepare/{tx_id}",
        {"items": items}
    )

    if stock_resp is None:
        # Stock service is unreachable — abort (nothing to rollback yet)
        app.logger.error(f"2PC CHECKOUT {tx_id}: Stock service unreachable")
        abort(400, "Stock service unavailable")

    if stock_resp.status_code == 200:
        stock_prepared = True
        app.logger.info(f"2PC CHECKOUT {tx_id}: Stock voted YES")
    else:
        # Stock voted NO — no need to abort stock (it already said no).
        # Nothing was prepared, so nothing to clean up.
        app.logger.info(f"2PC CHECKOUT {tx_id}: Stock voted NO — aborting")
        abort(400, f"Checkout failed: insufficient stock")

    # ── Step 2: Prepare Payment Service ──
    # Ask the payment service to reserve the credit.
    app.logger.info(f"2PC CHECKOUT {tx_id}: Phase 1 — Preparing payment...")
    payment_resp = send_post_request_json(
        f"{GATEWAY_URL}/payment/2pc/prepare/{tx_id}",
        {"user_id": order_entry.user_id, "amount": order_entry.total_cost}
    )

    if payment_resp is None:
        # Payment service is unreachable — must abort stock (it was prepared)
        app.logger.error(f"2PC CHECKOUT {tx_id}: Payment service unreachable — aborting stock")
        send_post_request(f"{GATEWAY_URL}/stock/2pc/abort/{tx_id}")
        abort(400, "Payment service unavailable")

    if payment_resp.status_code == 200:
        payment_prepared = True
        app.logger.info(f"2PC CHECKOUT {tx_id}: Payment voted YES")
    else:
        # Payment voted NO — must abort stock (it was prepared)
        app.logger.info(f"2PC CHECKOUT {tx_id}: Payment voted NO — aborting stock")
        send_post_request(f"{GATEWAY_URL}/stock/2pc/abort/{tx_id}")
        abort(400, "Checkout failed: insufficient credit")

    # ═══════════════════════════════════════════════════════════════════
    # DECISION: Both participants voted YES
    # ═══════════════════════════════════════════════════════════════════
    app.logger.info(f"2PC CHECKOUT {tx_id}: All participants voted YES — committing...")

    # ═══════════════════════════════════════════════════════════════════
    # PHASE 2: COMMIT
    # Tell all participants to finalize their changes.
    # ═══════════════════════════════════════════════════════════════════

    # ── Step 3: Commit Stock Service ──
    stock_commit_resp = send_post_request(f"{GATEWAY_URL}/stock/2pc/commit/{tx_id}")
    if stock_commit_resp is None or stock_commit_resp.status_code != 200:
        app.logger.error(f"2PC CHECKOUT {tx_id}: Stock commit failed!")
        # In strict 2PC, we should retry commits indefinitely.
        # For simplicity, we log the error. The TTL on the lock will
        # eventually clean it up.

    # ── Step 4: Commit Payment Service ──
    payment_commit_resp = send_post_request(f"{GATEWAY_URL}/payment/2pc/commit/{tx_id}")
    if payment_commit_resp is None or payment_commit_resp.status_code != 200:
        app.logger.error(f"2PC CHECKOUT {tx_id}: Payment commit failed!")

    # ── Step 5: Mark order as paid ──
    order_entry.paid = True
    try:
        db.set(order_id, msgpack.encode(order_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)

    app.logger.info(f"2PC CHECKOUT {tx_id}: Checkout successful for order {order_id}")
    return Response("Checkout successful", status=200)


# ═══════════════════════════════════════════════════════════════════════════
# App Startup
# ═══════════════════════════════════════════════════════════════════════════

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)

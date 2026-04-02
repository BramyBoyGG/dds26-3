import logging
import os
import atexit
import random
import uuid
from collections import defaultdict

import redis
import requests

from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response, request


DB_ERROR_STR = "DB error"
REQ_ERROR_STR = "Requests error"

GATEWAY_URL = os.environ['GATEWAY_URL']
ORCHESTRATOR_URL = os.environ['ORCHESTRATOR_URL']

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

# ── 2PC Configuration ─────────────────────────────────────────────────────
# Time-to-live for 2PC reservation records in Redis.
# If a coordinator crashes and never sends commit/abort, the lock expires
# automatically so credit isn't locked forever.
LOCK_TTL_SECONDS = 60

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


@app.post('/checkout/<order_id>')
def checkout(order_id: str):
    """
    endpoint kept because specs need it
    """
    resp = requests.post(f"{ORCHESTRATOR_URL}/checkout/{order_id}", data=request.data)

    return (resp.content, resp.status_code)

@app.post('/2pc/prepare/<tx_id>')
def tpc_prepare(tx_id: str):
    """
    """
    lock_key = f"2pc:{tx_id}"

    # ── Idempotency check ──
    # If this transaction was already prepared, don't do it again.
    if db.exists(lock_key):
        app.logger.info(f"2PC PREPARE {tx_id}: Already prepared (duplicate request)")
        return Response("Already prepared", status=409)

    # ── Parse the request body ──
    data = request.get_json()
    if not data or "order_id" not in data:
        return abort(400, "Missing 'order_id' in request body")

    order_id = int(data["order_id"])

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

    # ── Mark order as paid tentatively ──
    order_entry.paid = True
    try:
        db.set(order_id, msgpack.encode(order_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)

    # ── Store the reservation ──
    db.set(lock_key, "PAID", ex=LOCK_TTL_SECONDS)

    app.logger.info(f"2PC PREPARE {tx_id}: VOTE YES — reserved order: {order_id}")
    return Response("Prepared", status=200)


@app.post('/2pc/commit/<tx_id>')
def tpc_commit(tx_id: str):
    """
    ┌──────────────────────────────────────────────────────────────────┐
    │  PHASE 2: COMMIT                                                 │
    │                                                                  │
    │  The coordinator says: "All participants voted YES — commit!"     │
    │                                                                  │
    │                                                                  │
    │  This endpoint is idempotent: calling it twice is safe.          │
    │                                                                  │
    │  Returns:                                                        │
    │    200 = Committed successfully (or already committed)            │
    └──────────────────────────────────────────────────────────────────┘
    """
    lock_key = f"2pc:{tx_id}"

    if not db.exists(lock_key):
        # Already committed or lock expired — that's fine (idempotent)
        app.logger.info(f"2PC COMMIT {tx_id}: No reservation found (already committed)")
        return Response("Already committed", status=200)

    # Credit was already deducted during prepare.
    # Just delete the reservation record to finalize.
    db.delete(lock_key)

    app.logger.info(f"2PC COMMIT {tx_id}: Committed — reservation cleared")
    return Response("Committed", status=200)

@app.post('/2pc/abort/<tx_id>')
def tpc_abort(tx_id: str):
    """
    ┌──────────────────────────────────────────────────────────────────┐
    │  PHASE 2: ABORT                                                  │
    │                                                                  │
    │  The coordinator says: "A participant voted NO — roll back!"      │
    │                                                                  │
    │                                                                  │
    │  This endpoint is idempotent: calling it twice is safe.          │
    │                                                                  │
    │  Returns:                                                        │
    │    200 = Aborted and credit restored (or nothing to abort)        │
    └──────────────────────────────────────────────────────────────────┘
    """
    lock_key = f"2pc:{tx_id}"

    # Read the reservation to know what to restore
    reservation_raw = db.get(lock_key)
    if not reservation_raw:
        # Nothing to abort — already aborted or never prepared (idempotent)
        app.logger.info(f"2PC ABORT {tx_id}: No reservation found (already aborted)")
        return Response("Nothing to abort", status=200)

    data = request.get_json()
    if not data or "order_id" not in data:
        return abort(400, "Missing 'order_id' in request body")

    order_id = int(data["order_id"])

    try:
        # set order.paid in database to false
        order_entry: OrderValue = get_order_from_db(order_id)
        if order_entry:
            order_entry.paid = False
            db.set(order_id, msgpack.encode(order_entry))
    except redis.exceptions.RedisError:
        app.logger.error(f"2PC ABORT {tx_id}: Failed to restore order to unpaid")

    # Delete the reservation record
    db.delete(lock_key)

    app.logger.info(f"2PC ABORT {tx_id}: Aborted — restored order to unpaid")
    return Response("Aborted", status=200)


# ═══════════════════════════════════════════════════════════════════════════
# App Startup
# ═══════════════════════════════════════════════════════════════════════════

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)

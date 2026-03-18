import json
import logging
import os
import atexit
import time
import uuid

import redis

from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response, request

DB_ERROR_STR = "DB error"

app = Flask("stock-service")

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

class StockValue(Struct):
    stock: int
    price: int


# ── 2PC Configuration ─────────────────────────────────────────────────────
# Time-to-live for 2PC reservation records in Redis.
# If a coordinator crashes and never sends commit/abort, the lock expires
# automatically so resources aren't locked forever.
LOCK_TTL_SECONDS = 60


# ═══════════════════════════════════════════════════════════════════════════
# Helper Functions
# ═══════════════════════════════════════════════════════════════════════════

def get_item_from_db(item_id: str) -> StockValue | None:
    """Retrieve a stock item from Redis, or abort with 400 if not found."""
    try:
        entry: bytes = db.get(item_id)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    entry: StockValue | None = msgpack.decode(entry, type=StockValue) if entry else None
    if entry is None:
        abort(400, f"Item: {item_id} not found!")
    return entry


def _rollback_prepared_items(subtracted_items: list[tuple[str, int]]):
    """
    Internal helper: restore stock for items that were subtracted during
    a FAILED prepare phase.

    Called when one item in the batch fails validation (e.g., insufficient
    stock). We need to undo what we already subtracted for earlier items
    in the same prepare call.
    """
    for item_id, quantity in subtracted_items:
        try:
            raw = db.get(item_id)
            if raw:
                item = msgpack.decode(raw, type=StockValue)
                item.stock += int(quantity)
                db.set(item_id, msgpack.encode(item))
        except redis.exceptions.RedisError:
            app.logger.error(f"Rollback failed for item {item_id}")


# ═══════════════════════════════════════════════════════════════════════════
# Existing REST Endpoints (unchanged — used by tests and direct API calls)
# ═══════════════════════════════════════════════════════════════════════════

@app.post('/item/create/<price>')
def create_item(price: int):
    """Create a new stock item with the given price and 0 initial stock."""
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
    """Batch-create n items with the given starting stock and price."""
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
    """Look up an item's current stock and price."""
    item_entry: StockValue = get_item_from_db(item_id)
    return jsonify(
        {
            "stock": item_entry.stock,
            "price": item_entry.price
        }
    )


@app.post('/add/<item_id>/<amount>')
def add_stock(item_id: str, amount: int):
    """Add stock to an existing item."""
    item_entry: StockValue = get_item_from_db(item_id)
    item_entry.stock += int(amount)
    try:
        db.set(item_id, msgpack.encode(item_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"Item: {item_id} stock updated to: {item_entry.stock}", status=200)


@app.post('/subtract/<item_id>/<amount>')
def remove_stock(item_id: str, amount: int):
    """Subtract stock from an existing item (fails if stock would go below 0)."""
    item_entry: StockValue = get_item_from_db(item_id)
    item_entry.stock -= int(amount)
    app.logger.debug(f"Item: {item_id} stock updated to: {item_entry.stock}")
    if item_entry.stock < 0:
        abort(400, f"Item: {item_id} stock cannot get reduced below zero!")
    try:
        db.set(item_id, msgpack.encode(item_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"Item: {item_id} stock updated to: {item_entry.stock}", status=200)


# ═══════════════════════════════════════════════════════════════════════════
# 2PC (Two-Phase Commit) Endpoints
# ═══════════════════════════════════════════════════════════════════════════
#
# These endpoints allow the Stock Service to participate as a PARTICIPANT
# in a distributed transaction coordinated by the Order Service.
#
# The 2PC protocol has two phases:
#
#   PHASE 1 — PREPARE (Voting):
#     The coordinator asks: "Can you reserve these items?"
#     We check stock availability, subtract it tentatively, and store
#     a reservation record in Redis. If everything checks out, we
#     vote YES (200). Otherwise, we vote NO (400).
#
#   PHASE 2 — COMMIT or ABORT (Decision):
#     COMMIT: All participants voted YES. Stock was already subtracted
#             during prepare, so we just delete the reservation record.
#     ABORT:  A participant voted NO. We read the reservation record,
#             add the stock back, and delete the record.
#
# Reservation records are stored in Redis under keys like "2pc:<tx_id>"
# with a TTL so they auto-expire if the coordinator crashes.
#
# ═══════════════════════════════════════════════════════════════════════════

@app.post('/2pc/prepare/<tx_id>')
def tpc_prepare(tx_id: str):
    """
    ┌──────────────────────────────────────────────────────────────────┐
    │  PHASE 1: PREPARE (Vote)                                        │
    │                                                                  │
    │  The coordinator asks: "Can you reserve these items?"            │
    │                                                                  │
    │  For each item in the request:                                   │
    │    1. Check the item exists                                      │
    │    2. Check there's enough stock                                 │
    │    3. Subtract the stock (tentatively)                           │
    │                                                                  │
    │  If ALL items pass → store reservation → vote YES (200)          │
    │  If ANY item fails → rollback all → vote NO (400)               │
    │                                                                  │
    │  Request body (JSON):                                            │
    │    { "items": [["item-id-1", 2], ["item-id-2", 1]] }            │
    │                                                                  │
    │  Returns:                                                        │
    │    200 = "YES, I can commit"  (stock has been reserved)          │
    │    400 = "NO, I cannot"       (insufficient stock / not found)   │
    │    409 = already prepared     (duplicate prepare for same tx_id) │
    └──────────────────────────────────────────────────────────────────┘
    """
    lock_key = f"2pc:{tx_id}"

    # ── Idempotency check ──
    # If this transaction was already prepared, don't do it again.
    # This protects against duplicate prepare requests.
    if db.exists(lock_key):
        app.logger.info(f"2PC PREPARE {tx_id}: Already prepared (duplicate request)")
        return Response("Already prepared", status=409)

    # ── Parse the request body ──
    data = request.get_json()
    if not data or "items" not in data:
        return abort(400, "Missing 'items' in request body")

    items = data["items"]  # e.g., [["item-uuid-1", 2], ["item-uuid-2", 1]]

    # ── Try to subtract stock for each item ──
    # If any item fails, we rollback everything we've already subtracted.
    subtracted = []  # Track what we've done so far (for rollback if needed)

    for item_id, quantity in items:
        item_id = str(item_id)
        quantity = int(quantity)

        try:
            raw = db.get(item_id)
        except redis.exceptions.RedisError:
            _rollback_prepared_items(subtracted)
            return abort(400, DB_ERROR_STR)

        # Check 1: Does the item exist?
        if raw is None:
            _rollback_prepared_items(subtracted)
            app.logger.info(f"2PC PREPARE {tx_id}: VOTE NO — item {item_id} not found")
            return abort(400, f"Item {item_id} not found")

        item = msgpack.decode(raw, type=StockValue)

        # Check 2: Is there enough stock?
        if item.stock < quantity:
            _rollback_prepared_items(subtracted)
            app.logger.info(
                f"2PC PREPARE {tx_id}: VOTE NO — insufficient stock for {item_id} "
                f"(have {item.stock}, need {quantity})"
            )
            return abort(400, f"Insufficient stock for item {item_id}")

        # All checks passed for this item — subtract stock tentatively
        item.stock -= quantity
        try:
            db.set(item_id, msgpack.encode(item))
        except redis.exceptions.RedisError:
            _rollback_prepared_items(subtracted)
            return abort(400, DB_ERROR_STR)

        subtracted.append((item_id, quantity))

    # ── All items validated and subtracted ──
    # Store the reservation so that:
    #   - commit() knows to just clean up
    #   - abort() knows what to restore
    reservation = json.dumps({
        "items": items,
        "prepared_at": time.time(),
    })
    db.set(lock_key, reservation, ex=LOCK_TTL_SECONDS)

    app.logger.info(f"2PC PREPARE {tx_id}: VOTE YES — reserved {len(items)} item(s)")
    return Response("Prepared", status=200)


@app.post('/2pc/commit/<tx_id>')
def tpc_commit(tx_id: str):
    """
    ┌──────────────────────────────────────────────────────────────────┐
    │  PHASE 2: COMMIT                                                 │
    │                                                                  │
    │  The coordinator says: "All participants voted YES — commit!"     │
    │                                                                  │
    │  Since stock was already subtracted during prepare, committing    │
    │  just means deleting the reservation record (cleanup).           │
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

    # Stock was already subtracted during prepare.
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
    │  We read the reservation record to find out which items were      │
    │  subtracted during prepare, then add the stock back.             │
    │  Finally, we delete the reservation record.                      │
    │                                                                  │
    │  This endpoint is idempotent: calling it twice is safe.          │
    │                                                                  │
    │  Returns:                                                        │
    │    200 = Aborted and stock restored (or nothing to abort)         │
    └──────────────────────────────────────────────────────────────────┘
    """
    lock_key = f"2pc:{tx_id}"

    # Read the reservation to know what to restore
    reservation_raw = db.get(lock_key)
    if not reservation_raw:
        # Nothing to abort — already aborted or never prepared (idempotent)
        app.logger.info(f"2PC ABORT {tx_id}: No reservation found (already aborted)")
        return Response("Nothing to abort", status=200)

    # Parse the reservation and restore stock for each item
    reservation = json.loads(reservation_raw)
    items = reservation["items"]

    for item_id, quantity in items:
        item_id = str(item_id)
        quantity = int(quantity)
        try:
            raw = db.get(item_id)
            if raw:
                item = msgpack.decode(raw, type=StockValue)
                item.stock += quantity
                db.set(item_id, msgpack.encode(item))
        except redis.exceptions.RedisError:
            app.logger.error(f"2PC ABORT {tx_id}: Failed to restore item {item_id}")

    # Delete the reservation record
    db.delete(lock_key)

    app.logger.info(f"2PC ABORT {tx_id}: Aborted — restored {len(items)} item(s)")
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

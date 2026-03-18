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

app = Flask("payment-service")

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

class UserValue(Struct):
    credit: int


# ── 2PC Configuration ─────────────────────────────────────────────────────
# Time-to-live for 2PC reservation records in Redis.
# If a coordinator crashes and never sends commit/abort, the lock expires
# automatically so credit isn't locked forever.
LOCK_TTL_SECONDS = 60


# ═══════════════════════════════════════════════════════════════════════════
# Helper Functions
# ═══════════════════════════════════════════════════════════════════════════

def get_user_from_db(user_id: str) -> UserValue | None:
    """Retrieve a user from Redis, or abort with 400 if not found."""
    try:
        entry: bytes = db.get(user_id)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    entry: UserValue | None = msgpack.decode(entry, type=UserValue) if entry else None
    if entry is None:
        abort(400, f"User: {user_id} not found!")
    return entry


# ═══════════════════════════════════════════════════════════════════════════
# Existing REST Endpoints (unchanged — used by tests and direct API calls)
# ═══════════════════════════════════════════════════════════════════════════

@app.post('/create_user')
def create_user():
    """Create a new user with 0 initial credit."""
    key = str(uuid.uuid4())
    value = msgpack.encode(UserValue(credit=0))
    try:
        db.set(key, value)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({'user_id': key})


@app.post('/batch_init/<n>/<starting_money>')
def batch_init_users(n: int, starting_money: int):
    """Batch-create n users with the given starting credit."""
    n = int(n)
    starting_money = int(starting_money)
    kv_pairs: dict[str, bytes] = {f"{i}": msgpack.encode(UserValue(credit=starting_money))
                                  for i in range(n)}
    try:
        db.mset(kv_pairs)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({"msg": "Batch init for users successful"})


@app.get('/find_user/<user_id>')
def find_user(user_id: str):
    """Look up a user's current credit."""
    user_entry: UserValue = get_user_from_db(user_id)
    return jsonify(
        {
            "user_id": user_id,
            "credit": user_entry.credit
        }
    )


@app.post('/add_funds/<user_id>/<amount>')
def add_credit(user_id: str, amount: int):
    """Add credit to a user's account."""
    user_entry: UserValue = get_user_from_db(user_id)
    user_entry.credit += int(amount)
    try:
        db.set(user_id, msgpack.encode(user_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"User: {user_id} credit updated to: {user_entry.credit}", status=200)


@app.post('/pay/<user_id>/<amount>')
def remove_credit(user_id: str, amount: int):
    """Deduct credit from a user (fails if credit would go below 0)."""
    app.logger.debug(f"Removing {amount} credit from user: {user_id}")
    user_entry: UserValue = get_user_from_db(user_id)
    user_entry.credit -= int(amount)
    if user_entry.credit < 0:
        abort(400, f"User: {user_id} credit cannot get reduced below zero!")
    try:
        db.set(user_id, msgpack.encode(user_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"User: {user_id} credit updated to: {user_entry.credit}", status=200)


# ═══════════════════════════════════════════════════════════════════════════
# 2PC (Two-Phase Commit) Endpoints
# ═══════════════════════════════════════════════════════════════════════════
#
# These endpoints allow the Payment Service to participate as a PARTICIPANT
# in a distributed transaction coordinated by the Order Service.
#
# The 2PC protocol has two phases:
#
#   PHASE 1 — PREPARE (Voting):
#     The coordinator asks: "Can you deduct this amount from the user?"
#     We check the user's credit is sufficient, subtract it tentatively,
#     and store a reservation record. If OK, vote YES (200). Otherwise NO (400).
#
#   PHASE 2 — COMMIT or ABORT (Decision):
#     COMMIT: All participants voted YES. Credit was already deducted
#             during prepare, so we just delete the reservation record.
#     ABORT:  A participant voted NO. We read the reservation record,
#             add the credit back, and delete the record.
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
    │  The coordinator asks: "Can you deduct payment for this user?"   │
    │                                                                  │
    │  Steps:                                                          │
    │    1. Check the user exists                                      │
    │    2. Check the user has enough credit                           │
    │    3. Subtract the credit (tentatively)                          │
    │    4. Store the reservation (so we can undo on abort)            │
    │                                                                  │
    │  Request body (JSON):                                            │
    │    { "user_id": "user-uuid", "amount": 100 }                    │
    │                                                                  │
    │  Returns:                                                        │
    │    200 = "YES, I can commit"  (credit has been reserved)         │
    │    400 = "NO, I cannot"       (insufficient credit / not found)  │
    │    409 = already prepared     (duplicate prepare for same tx_id) │
    └──────────────────────────────────────────────────────────────────┘
    """
    lock_key = f"2pc:{tx_id}"

    # ── Idempotency check ──
    # If this transaction was already prepared, don't do it again.
    if db.exists(lock_key):
        app.logger.info(f"2PC PREPARE {tx_id}: Already prepared (duplicate request)")
        return Response("Already prepared", status=409)

    # ── Parse the request body ──
    data = request.get_json()
    if not data or "user_id" not in data or "amount" not in data:
        return abort(400, "Missing 'user_id' or 'amount' in request body")

    user_id = str(data["user_id"])
    amount = int(data["amount"])

    # ── Check the user exists ──
    try:
        raw = db.get(user_id)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)

    if raw is None:
        app.logger.info(f"2PC PREPARE {tx_id}: VOTE NO — user {user_id} not found")
        return abort(400, f"User {user_id} not found")

    user = msgpack.decode(raw, type=UserValue)

    # ── Check sufficient credit ──
    if user.credit < amount:
        app.logger.info(
            f"2PC PREPARE {tx_id}: VOTE NO — insufficient credit for user {user_id} "
            f"(have {user.credit}, need {amount})"
        )
        return abort(400, f"Insufficient credit for user {user_id}")

    # ── Subtract credit tentatively ──
    user.credit -= amount
    try:
        db.set(user_id, msgpack.encode(user))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)

    # ── Store the reservation ──
    # This record is needed so abort() knows what to restore
    reservation = json.dumps({
        "user_id": user_id,
        "amount": amount,
        "prepared_at": time.time(),
    })
    db.set(lock_key, reservation, ex=LOCK_TTL_SECONDS)

    app.logger.info(f"2PC PREPARE {tx_id}: VOTE YES — reserved {amount} from user {user_id}")
    return Response("Prepared", status=200)


@app.post('/2pc/commit/<tx_id>')
def tpc_commit(tx_id: str):
    """
    ┌──────────────────────────────────────────────────────────────────┐
    │  PHASE 2: COMMIT                                                 │
    │                                                                  │
    │  The coordinator says: "All participants voted YES — commit!"     │
    │                                                                  │
    │  Credit was already deducted during prepare, so committing       │
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
    │  We read the reservation record to find out the user_id and      │
    │  amount that were deducted during prepare, then add the credit   │
    │  back. Finally, we delete the reservation record.                │
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

    # Parse the reservation and restore credit
    reservation = json.loads(reservation_raw)
    user_id = reservation["user_id"]
    amount = int(reservation["amount"])

    try:
        raw = db.get(user_id)
        if raw:
            user = msgpack.decode(raw, type=UserValue)
            user.credit += amount
            db.set(user_id, msgpack.encode(user))
    except redis.exceptions.RedisError:
        app.logger.error(f"2PC ABORT {tx_id}: Failed to restore credit for user {user_id}")

    # Delete the reservation record
    db.delete(lock_key)

    app.logger.info(f"2PC ABORT {tx_id}: Aborted — restored {amount} credit to user {user_id}")
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

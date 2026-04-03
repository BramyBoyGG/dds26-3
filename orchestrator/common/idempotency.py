from __future__ import annotations

"""
idempotency.py — Prevent double-processing of SAGA commands.

Problem:
  In a distributed system with retries, the same command can be delivered
  more than once (at-least-once delivery).  Without idempotency, a
  RESERVE_STOCK command could subtract stock TWICE for the same checkout.

Solution:
  Before processing a command, check if we've already processed it.
  If yes, return the previously stored result (replay).
  If no, process it, store the result, then return it.

Storage:
  We use Redis keys with a TTL:
    Key:   processed:{step}:{tx_id}
    Value: JSON-encoded result (status + reason)
    TTL:   1 hour (configurable)

  Using the service's own Redis instance — no cross-service dependency.

Usage example (in the Stock service's RESERVE_STOCK handler):

    from common.idempotency import is_processed, mark_processed, get_cached_result

    def handle_reserve_stock(db, tx_id, items):
        # Check if we already processed this exact command
        if is_processed(db, tx_id, "RESERVE_STOCK"):
            # Return the same result we returned last time (replay)
            return get_cached_result(db, tx_id, "RESERVE_STOCK")

        # ... do the actual stock reservation ...

        result = {"status": "SUCCESS", "reason": ""}

        # Remember that we processed this, so retries are safe
        mark_processed(db, tx_id, "RESERVE_STOCK", result)

        return result
"""

import json
import logging
from typing import Optional

import redis

from common.protocol import IDEMPOTENCY_PREFIX, IDEMPOTENCY_TTL

logger = logging.getLogger(__name__)


def _make_key(tx_id: str, step: str) -> str:
    """
    Build the Redis key for an idempotency record.

    Format: processed:{step}:{tx_id}

    We put the step first so that all RESERVE_STOCK records are grouped
    together in Redis key space (useful for debugging with redis-cli KEYS).

    Parameters
    ----------
    tx_id : str
        The transaction ID.
    step : str
        The saga step (e.g., "RESERVE_STOCK", "DEDUCT_PAYMENT").

    Returns
    -------
    str
        The Redis key, e.g. "processed:RESERVE_STOCK:abc-123"
    """
    return f"{IDEMPOTENCY_PREFIX}:{step}:{tx_id}"


def is_processed(redis_conn: redis.Redis, tx_id: str, step: str) -> bool:
    """
    Check whether a (tx_id, step) combination has already been processed.

    This should be the FIRST thing a command handler checks before doing
    any real work.  If True, the handler should call get_cached_result()
    and replay the previous response instead of re-executing.

    Parameters
    ----------
    redis_conn : redis.Redis
        Connection to the service's own Redis instance.
    tx_id : str
        The transaction ID from the incoming command.
    step : str
        The saga step being checked (e.g., "RESERVE_STOCK").

    Returns
    -------
    bool
        True if this (tx_id, step) was already processed.
    """
    key = _make_key(tx_id, step)
    exists = redis_conn.exists(key)
    if exists:
        logger.info(f"Idempotency hit: {step}:{tx_id} already processed")
    return bool(exists)


def mark_processed(
    redis_conn: redis.Redis,
    tx_id: str,
    step: str,
    result: dict,
    ttl: int = IDEMPOTENCY_TTL,
) -> None:
    """
    Record that a (tx_id, step) has been processed, along with its result.

    Call this AFTER successfully processing a command AND writing the
    response to the tx-responses stream.  The stored result allows us to
    replay the exact same response if the command is retried.

    The TTL ensures old records are automatically cleaned up.  1 hour is
    more than enough — if a saga hasn't completed in 1 hour, something
    else is very wrong.

    Parameters
    ----------
    redis_conn : redis.Redis
        Connection to the service's own Redis instance.
    tx_id : str
        The transaction ID.
    step : str
        The saga step (e.g., "RESERVE_STOCK").
    result : dict
        The result to cache, typically {"status": "SUCCESS", "reason": ""}
        or {"status": "FAILURE", "reason": "insufficient stock"}.
    ttl : int
        Time-to-live in seconds.  After this, the key is auto-deleted.
    """
    key = _make_key(tx_id, step)
    # Store the result as a JSON string so we can reconstruct it later
    redis_conn.setex(key, ttl, json.dumps(result))
    logger.debug(f"Marked processed: {step}:{tx_id} -> {result}")


def get_cached_result(redis_conn: redis.Redis, tx_id: str, step: str) -> Optional[dict]:
    """
    Retrieve the cached result for a previously processed (tx_id, step).

    Use this when is_processed() returns True.  The returned dict contains
    the same status and reason that was originally produced, allowing the
    handler to replay the exact same response to the orchestrator.

    Parameters
    ----------
    redis_conn : redis.Redis
        Connection to the service's own Redis instance.
    tx_id : str
        The transaction ID.
    step : str
        The saga step.

    Returns
    -------
    dict or None
        The cached result dict (e.g., {"status": "SUCCESS", "reason": ""}),
        or None if the key expired or doesn't exist (shouldn't happen if
        is_processed() just returned True, but we handle it defensively).
    """
    key = _make_key(tx_id, step)
    raw = redis_conn.get(key)
    if raw is None:
        logger.warning(f"Idempotency key missing (expired?): {step}:{tx_id}")
        return None
    # raw is bytes if decode_responses=False, str otherwise
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8")
    return json.loads(raw)

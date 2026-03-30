from __future__ import annotations

"""
serialization.py — Encode and decode messages for Redis Streams.

Redis Streams store each message as a flat dictionary of string keys → string
values.  Our messages carry structured data (lists of items, nested fields),
so we JSON-serialize the complex "payload" field while keeping simple fields
(tx_id, command, status) as plain strings for readability in redis-cli.

Two message types:
  1. COMMAND  — sent by Order to Stock/Payment  (via stock-commands / payment-commands)
  2. RESPONSE — sent by Stock/Payment back to Order  (via tx-responses)

Usage examples:

    # --- Encoding a command (Order side) ---
    fields = encode_command(
        tx_id="abc-123",
        command="RESERVE_STOCK",
        payload={"items": [("item-1", 2), ("item-2", 1)]}
    )
    redis_conn.xadd("stock-commands", fields)

    # --- Decoding a command (Stock side) ---
    raw = {"tx_id": "abc-123", "command": "RESERVE_STOCK", "payload": '{"items": ...}'}
    parsed = decode_command(raw)
    # parsed = {"tx_id": "abc-123", "command": "RESERVE_STOCK", "items": [...]}

    # --- Encoding a response (Stock side) ---
    fields = encode_response(
        tx_id="abc-123",
        step="RESERVE_STOCK",
        status="SUCCESS"
    )
    order_redis_conn.xadd("tx-responses", fields)

    # --- Decoding a response (Order side) ---
    parsed = decode_response(raw)
    # parsed = {"tx_id": "abc-123", "step": "RESERVE_STOCK", "status": "SUCCESS", "reason": ""}
"""

import json


def encode_command(tx_id: str, command: str, payload: dict) -> dict[str, str]:
    """
    Build the field-value dict to pass to XADD for a command message.

    Parameters
    ----------
    tx_id : str
        Unique transaction identifier (UUID).
    command : str
        One of the CMD_* constants from protocol.py
        (e.g., "RESERVE_STOCK", "DEDUCT_PAYMENT").
    payload : dict
        Arbitrary data for the command.  For RESERVE_STOCK this would be
        {"items": [["item-id-1", 2], ["item-id-2", 1]]}.
        For DEDUCT_PAYMENT: {"user_id": "u-123", "amount": 50}.
        This dict is JSON-serialized into a single string field.

    Returns
    -------
    dict[str, str]
        A flat dict ready for redis_conn.xadd(stream, fields).
        Example: {"tx_id": "abc", "command": "RESERVE_STOCK", "payload": '{"items": ...}'}
    """
    return {
        "tx_id": tx_id,
        "command": command,
        "payload": json.dumps(payload),
    }


def decode_command(raw: dict) -> dict:
    """
    Parse a raw Redis Stream message (from XREADGROUP) into a usable dict.

    The raw message comes from Redis as {b"tx_id": b"abc", b"command": b"...", ...}
    when decode_responses=False, or {"tx_id": "abc", ...} when decode_responses=True.

    We handle both cases by converting keys/values to str, then JSON-parsing
    the "payload" field back into a Python dict.

    Parameters
    ----------
    raw : dict
        The field-value dict from a Redis Stream message.

    Returns
    -------
    dict
        Parsed message with keys: "tx_id", "command", and all payload fields
        merged in at the top level for convenience.
        Example: {"tx_id": "abc", "command": "RESERVE_STOCK", "items": [[...], ...]}
    """
    # Convert bytes to str if needed (depends on decode_responses setting)
    cleaned = {
        _to_str(k): _to_str(v) for k, v in raw.items()
    }

    # Parse the JSON payload and merge its fields into the top-level dict
    result = {
        "tx_id": cleaned["tx_id"],
        "command": cleaned["command"],
    }
    payload_str = cleaned.get("payload", "{}")
    payload = json.loads(payload_str)
    result.update(payload)  # e.g., adds "items", "user_id", "amount", etc.

    return result


def encode_response(tx_id: str, step: str, status: str, reason: str = "") -> dict[str, str]:
    """
    Build the field-value dict to pass to XADD for a response message.

    Parameters
    ----------
    tx_id : str
        The transaction ID this response belongs to.
    step : str
        Which saga step this response is for (e.g., "RESERVE_STOCK").
        This lets the orchestrator know which step just completed.
    status : str
        Either STATUS_SUCCESS or STATUS_FAILURE from protocol.py.
    reason : str, optional
        Human-readable reason for failure (e.g., "insufficient stock for item X").
        Empty string on success.

    Returns
    -------
    dict[str, str]
        A flat dict ready for redis_conn.xadd(stream, fields).
    """
    return {
        "tx_id": tx_id,
        "step": step,
        "status": status,
        "reason": reason,
    }


def decode_response(raw: dict) -> dict:
    """
    Parse a raw Redis Stream response message into a usable dict.

    Parameters
    ----------
    raw : dict
        The field-value dict from a Redis Stream message.

    Returns
    -------
    dict
        Parsed message with keys: "tx_id", "step", "status", "reason".
    """
    cleaned = {
        _to_str(k): _to_str(v) for k, v in raw.items()
    }
    return {
        "tx_id": cleaned["tx_id"],
        "step": cleaned["step"],
        "status": cleaned["status"],
        "reason": cleaned.get("reason", ""),
    }


# ---------------------------------------------------------------------------
# Internal helper
# ---------------------------------------------------------------------------

def _to_str(value) -> str:
    """
    Convert bytes to str.  Redis returns bytes when decode_responses=False.
    If already a str, return as-is.  This lets our code work regardless of
    the Redis client's decode_responses setting.
    """
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return str(value)

from __future__ import annotations

"""
stream_helpers.py — Wrappers around Redis Stream commands.

Redis Streams are the messaging backbone of our SAGA architecture.
This module provides clean, reusable functions so that individual services
don't need to deal with raw Redis Stream API details.

Key Redis Stream concepts used here:
  • XADD     — Append a message to a stream (like "publish").
  • XREADGROUP — Read new messages as part of a consumer group.
                 Each message is delivered to exactly ONE consumer in the group.
  • XACK     — Acknowledge that a message was processed successfully.
               Until ACK'd, the message stays in the Pending Entries List (PEL).
  • XCLAIM   — Take ownership of a pending message from a dead consumer.
               Used for crash recovery: if a consumer dies mid-processing,
               another consumer can claim and re-process the message.
  • XPENDING — Inspect messages that were delivered but not yet ACK'd.

Flow for a service consuming messages:
  1. On startup: create_consumer_group() — idempotent, safe to call every time.
  2. In a loop:  read_messages()         — blocks until messages arrive.
  3. Per message: process it, then ack_message().
  4. Periodically: claim_stale_pending() — pick up messages from crashed consumers.
"""

import logging
from typing import Optional

import redis

from common.protocol import STREAM_BLOCK_MS, STREAM_READ_COUNT, PENDING_CLAIM_MIN_IDLE_MS

logger = logging.getLogger(__name__)


def create_consumer_group(
    redis_conn: redis.Redis,
    stream: str,
    group: str,
    start_id: str = "0",
) -> bool:
    """
    Create a consumer group for a stream.  Safe to call on every startup.

    A consumer group tracks which messages have been delivered to which
    consumer, and which have been acknowledged.  We set mkstream=True so
    that if the stream doesn't exist yet, Redis creates it automatically.

    Parameters
    ----------
    redis_conn : redis.Redis
        Connection to the Redis instance where the stream lives.
    stream : str
        Name of the stream (e.g., "stock-commands").
    group : str
        Name of the consumer group (e.g., "stock-service-group").
    start_id : str
        The message ID from which the group should start reading.
        "0" means read ALL existing messages (useful for recovery).
        "$" means only read NEW messages arriving after group creation.
        We default to "0" so that on restart we don't miss anything.

    Returns
    -------
    bool
        True if the group was created, False if it already existed.
    """
    try:
        # mkstream=True: create the stream if it doesn't exist yet
        redis_conn.xgroup_create(stream, group, id=start_id, mkstream=True)
        logger.info(f"Created consumer group '{group}' on stream '{stream}'")
        return True
    except redis.exceptions.ResponseError as e:
        # "BUSYGROUP Consumer Group name already exists" is expected on restart
        if "BUSYGROUP" in str(e):
            logger.debug(f"Consumer group '{group}' already exists on stream '{stream}'")
            return False
        raise  # Re-raise unexpected errors


def publish_message(
    redis_conn: redis.Redis,
    stream: str,
    fields: dict[str, str],
    max_len: Optional[int] = 10000,
) -> str:
    """
    Publish a message to a Redis Stream (wrapper around XADD).

    Parameters
    ----------
    redis_conn : redis.Redis
        Connection to the Redis instance where the stream lives.
        IMPORTANT: This may be a DIFFERENT Redis than the service's own DB.
        For example, the Stock service publishes to tx-responses in order-db.
    stream : str
        Name of the stream to publish to.
    fields : dict[str, str]
        The message fields.  Use encode_command() or encode_response() from
        serialization.py to build this dict.
    max_len : int or None
        Approximate max length of the stream.  Redis trims old messages to
        keep memory bounded.  Set to None for no trimming.
        We use approximate trimming (~) for performance — Redis may keep
        slightly more than max_len entries.

    Returns
    -------
    str
        The message ID assigned by Redis (e.g., "1678886400000-0").
        This ID is auto-generated and monotonically increasing.
    """
    # maxlen with approximate=True lets Redis trim efficiently
    # without scanning the entire stream on every XADD.
    message_id = redis_conn.xadd(stream, fields, maxlen=max_len, approximate=True)
    logger.debug(f"Published to '{stream}': id={message_id}, fields={fields}")
    return message_id


def read_messages(
    redis_conn: redis.Redis,
    stream: str,
    group: str,
    consumer: str,
    count: int = STREAM_READ_COUNT,
    block_ms: int = STREAM_BLOCK_MS,
) -> list[tuple[str, dict]]:
    """
    Read new messages from a stream as part of a consumer group.

    Uses XREADGROUP with the special ID ">" which means "give me messages
    that have never been delivered to any consumer in this group."

    This call BLOCKS for up to block_ms milliseconds if no messages are
    available, which is efficient (no busy-waiting / polling).

    Parameters
    ----------
    redis_conn : redis.Redis
        Connection to the Redis instance where the stream lives.
    stream : str
        Name of the stream to read from.
    group : str
        Consumer group name.
    consumer : str
        Unique name for THIS consumer instance (e.g., hostname or UUID).
        Multiple consumers in the same group load-balance the messages.
    count : int
        Maximum number of messages to read per call.
    block_ms : int
        How long to block (in milliseconds) waiting for new messages.
        After this timeout, returns an empty list.

    Returns
    -------
    list[tuple[str, dict]]
        List of (message_id, fields_dict) tuples.
        Empty list if no messages arrived within block_ms.
        Example: [("1678886400000-0", {"tx_id": "abc", "command": "RESERVE_STOCK", ...})]
    """
    try:
        # XREADGROUP GROUP <group> <consumer> COUNT <n> BLOCK <ms> STREAMS <stream> >
        # The ">" means: only deliver messages not yet seen by this group.
        response = redis_conn.xreadgroup(
            groupname=group,
            consumername=consumer,
            streams={stream: ">"},
            count=count,
            block=block_ms,
        )
    except redis.exceptions.ResponseError as e:
        # Handle edge case where stream or group was deleted
        logger.error(f"Error reading from stream '{stream}': {e}")
        return []

    if not response:
        # Timeout — no messages arrived within block_ms
        return []

    # response format: [(stream_name, [(msg_id, fields_dict), ...])]
    # We only read from one stream, so take the first (and only) entry.
    messages = response[0][1]  # list of (message_id, fields_dict)

    logger.debug(f"Read {len(messages)} message(s) from '{stream}'")
    return messages


def ack_message(
    redis_conn: redis.Redis,
    stream: str,
    group: str,
    message_id: str,
) -> None:
    """
    Acknowledge that a message has been successfully processed.

    Until a message is ACK'd, it stays in the group's Pending Entries List
    (PEL).  If the consumer crashes before ACK'ing, the message can be
    re-delivered to another consumer via XCLAIM (see claim_stale_pending).

    This is what gives us at-least-once delivery:
      • Message is delivered → consumer processes it → ACK
      • If consumer crashes before ACK → message stays pending → re-claimed

    Combined with idempotency (see idempotency.py), we get exactly-once
    *processing* semantics (at-least-once delivery + idempotent handlers).

    Parameters
    ----------
    redis_conn : redis.Redis
        Connection to the Redis instance where the stream lives.
    stream : str
        Name of the stream.
    group : str
        Consumer group name.
    message_id : str
        The ID of the message to acknowledge.
    """
    redis_conn.xack(stream, group, message_id)
    logger.debug(f"ACK'd message {message_id} on '{stream}'")


def claim_stale_pending(
    redis_conn: redis.Redis,
    stream: str,
    group: str,
    consumer: str,
    min_idle_ms: int = PENDING_CLAIM_MIN_IDLE_MS,
    count: int = STREAM_READ_COUNT,
) -> list[tuple[str, dict]]:
    """
    Claim pending messages that have been idle for too long.

    When a consumer crashes, its unacknowledged messages remain in the
    Pending Entries List (PEL).  This function lets a LIVING consumer
    take ownership of those "orphaned" messages and re-process them.

    This is the key mechanism for fault tolerance:
      1. Consumer A picks up a message but crashes before ACK'ing
      2. The message sits in the PEL, idle
      3. Consumer B calls claim_stale_pending() and takes it over
      4. Consumer B processes the message (idempotency ensures safety)
      5. Consumer B ACK's it

    Should be called periodically (e.g., every 30 seconds) by each consumer.

    Parameters
    ----------
    redis_conn : redis.Redis
        Connection to the Redis instance where the stream lives.
    stream : str
        Name of the stream.
    group : str
        Consumer group name.
    consumer : str
        The consumer claiming the messages (this instance).
    min_idle_ms : int
        Only claim messages that have been pending for at least this long.
        This prevents claiming messages that another consumer is still
        actively processing.  Default: 30 seconds.
    count : int
        Maximum number of messages to claim per call.

    Returns
    -------
    list[tuple[str, dict]]
        List of (message_id, fields_dict) tuples for the claimed messages.
        Empty list if no stale messages found.
    """
    try:
        # First, check what's pending in the group
        pending_info = redis_conn.xpending_range(
            stream, group,
            min="-",           # From the earliest pending message
            max="+",           # To the latest pending message
            count=count,
        )

        if not pending_info:
            return []

        # Filter to only messages that have been idle long enough
        stale_ids = [
            entry["message_id"]
            for entry in pending_info
            if entry["time_since_delivered"] >= min_idle_ms
        ]

        if not stale_ids:
            return []

        # XCLAIM: take ownership of these messages
        # This atomically transfers them from the old (possibly dead) consumer
        # to this consumer, and resets the idle timer.
        claimed = redis_conn.xclaim(
            stream, group, consumer,
            min_idle_time=min_idle_ms,
            message_ids=stale_ids,
        )

        logger.info(f"Claimed {len(claimed)} stale message(s) from '{stream}'")

        # claimed format: [(msg_id, fields_dict), ...]
        return claimed

    except redis.exceptions.ResponseError as e:
        logger.error(f"Error claiming pending messages from '{stream}': {e}")
        return []

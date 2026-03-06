import logging
import os
import threading

import redis

from common.protocol import (
    STOCK_COMMANDS_STREAM,
    STOCK_CONSUMER_GROUP,
    TX_RESPONSES_STREAM,
    CMD_RESERVE_STOCK,
    CMD_COMPENSATE_STOCK,
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

import app as stock_app


logger = setup_logging("stock-service")
consumer_id = f"{get_consumer_id()}-{os.getpid()}"
_consumer_running = False


# ---------------------------------------------------------------------------
# Command handlers
# ---------------------------------------------------------------------------

def handle_reserve_stock(tx_id: str, items: list) -> dict:
    if is_processed(stock_app.db, tx_id, CMD_RESERVE_STOCK):
        cached = get_cached_result(stock_app.db, tx_id, CMD_RESERVE_STOCK)
        if cached is not None:
            log_tx(logger, tx_id, CMD_RESERVE_STOCK, f"Idempotency replay: {cached['status']}")
            return cached

    keys = [str(item_id) for item_id, _ in items]
    args = [int(quantity) for _, quantity in items]

    try:
        # Returns [status, index]:  1=success, 0=not found, -1=insufficient stock
        status, idx = stock_app.subtract_batch_script(keys=keys, args=args)
    except redis.exceptions.RedisError as e:
        log_tx(logger, tx_id, CMD_RESERVE_STOCK,
               f"Redis error: {e}", level=logging.ERROR)
        resp = {"status": STATUS_FAILURE, "reason": str(e)}
        mark_processed(stock_app.db, tx_id, CMD_RESERVE_STOCK, resp)
        return resp

    if status == 0:
        failed_item = keys[idx - 1]
        resp = {"status": STATUS_FAILURE, "reason": f"Item {failed_item} not found"}
    elif status == -1:
        failed_item = keys[idx - 1]
        resp = {"status": STATUS_FAILURE, "reason": f"Insufficient stock for item {failed_item}"}
    else:
        resp = {"status": STATUS_SUCCESS, "reason": ""}
        log_tx(logger, tx_id, CMD_RESERVE_STOCK, f"Reserved {len(items)} item(s)")

    mark_processed(stock_app.db, tx_id, CMD_RESERVE_STOCK, resp)
    return resp


def handle_compensate_stock(tx_id: str, items: list) -> dict:
    if is_processed(stock_app.db, tx_id, CMD_COMPENSATE_STOCK):
        cached = get_cached_result(stock_app.db, tx_id, CMD_COMPENSATE_STOCK)
        if cached is not None:
            log_tx(logger, tx_id, CMD_COMPENSATE_STOCK, f"Idempotency replay: {cached['status']}")
            return cached

    for item_id, quantity in items:
        item_id = str(item_id)
        quantity = int(quantity)
        try:
            stock_app.add_script(keys=[item_id], args=[quantity])
            log_tx(logger, tx_id, CMD_COMPENSATE_STOCK,
                   f"Restored {quantity} of item {item_id}")
        except redis.exceptions.RedisError as e:
            log_tx(logger, tx_id, CMD_COMPENSATE_STOCK,
                   f"ERROR restoring item {item_id}: {e}", level=logging.ERROR)

    resp = {"status": STATUS_SUCCESS, "reason": ""}
    mark_processed(stock_app.db, tx_id, CMD_COMPENSATE_STOCK, resp)
    log_tx(logger, tx_id, CMD_COMPENSATE_STOCK, f"Compensated {len(items)} item(s)")
    return resp


# ---------------------------------------------------------------------------
# Dispatch + consumer loop
# ---------------------------------------------------------------------------

def _dispatch_command(msg_id: str, data: dict):
    """Route an incoming stream command to the appropriate handler."""
    tx_id = data["tx_id"]
    command = data["command"]
    items = data.get("items", [])

    if command == CMD_RESERVE_STOCK:
        result = handle_reserve_stock(tx_id, items)
    elif command == CMD_COMPENSATE_STOCK:
        result = handle_compensate_stock(tx_id, items)
    else:
        log_tx(logger, tx_id, command, f"Unknown command: {command}", level=logging.WARNING)
        return

    response_fields = encode_response(
        tx_id=tx_id,
        step=command,
        status=result["status"],
        reason=result.get("reason", ""),
    )
    publish_message(stock_app.order_db, TX_RESPONSES_STREAM, response_fields)
    log_tx(logger, tx_id, command, f"Published response: {result['status']}")


def _consumer_loop():
    global _consumer_running
    _consumer_running = True

    create_consumer_group(stock_app.db, STOCK_COMMANDS_STREAM, STOCK_CONSUMER_GROUP)

    logger.info(f"Stock consumer '{consumer_id}' starting on stream '{STOCK_COMMANDS_STREAM}'")

    claim_counter = 0
    while _consumer_running:
        try:
            messages = read_messages(
                stock_app.db, STOCK_COMMANDS_STREAM, STOCK_CONSUMER_GROUP, consumer_id,
            )

            for msg_id, raw_fields in messages:
                try:
                    data = decode_command(raw_fields)
                    _dispatch_command(msg_id, data)
                except Exception as e:
                    logger.error(f"Error processing message {msg_id}: {e}", exc_info=True)
                finally:
                    ack_message(stock_app.db, STOCK_COMMANDS_STREAM, STOCK_CONSUMER_GROUP, msg_id)

            claim_counter += 1
            if claim_counter >= 6:
                claim_counter = 0
                stale = claim_stale_pending(
                    stock_app.db, STOCK_COMMANDS_STREAM, STOCK_CONSUMER_GROUP, consumer_id,
                )
                for msg_id, raw_fields in stale:
                    try:
                        data = decode_command(raw_fields)
                        _dispatch_command(msg_id, data)
                    except Exception as e:
                        logger.error(f"Error processing claimed message {msg_id}: {e}", exc_info=True)
                    finally:
                        ack_message(stock_app.db, STOCK_COMMANDS_STREAM, STOCK_CONSUMER_GROUP, msg_id)

        except Exception as e:
            logger.error(f"Consumer loop error: {e}", exc_info=True)


def start_consumer():
    t = threading.Thread(
        target=_consumer_loop,
        daemon=True,
        name="stock-consumer",
    )
    t.start()
    logger.info("Stock stream consumer thread started")

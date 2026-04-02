import logging
import os
import uuid
import requests

from flask import Flask, abort, Response


REQ_ERROR_STR = "Requests error"

GATEWAY_URL = os.environ['GATEWAY_URL']

app = Flask("order-service")

# ═══════════════════════════════════════════════════════════════════════════
# Helper Functions
# ═══════════════════════════════════════════════════════════════════════════

def send_post_request(url: str):
    """Send a POST request without a body. Used for simple endpoints."""
    try:
        response = requests.post(url)
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
# 2PC orchestration
# ═══════════════════════════════════════════════════════════════════════════

@app.post('/checkout/<order_id>')
def checkout(order_id: str):
    # ── Generate a unique transaction ID ──
    # This tx_id ties together all the prepare/commit/abort calls
    # across both participants.
    tx_id = str(uuid.uuid4())
    app.logger.info(f"2PC CHECKOUT {tx_id}: Starting for order {order_id}")

    # ═══════════════════════════════════════════════════════════════════
    # PHASE 1: PREPARE (Voting)
    # Ask each participant: "Can you commit?"
    # ═══════════════════════════════════════════════════════════════════
    # ── Step 1: Prepare Order Service ──
    app.logger.info(f"2PC CHECKOUT {tx_id}: Phase 1 — Preparing order...")
    order_resp = send_post_request_json(
        f"{GATEWAY_URL}/order/2pc/prepare/{tx_id}",
        {"order_id": order_id}
    )

    if order_resp is None:
        app.logger.error(f"2PC CHECKOUT {tx_id}: Order service unreachable")
        abort(400, "Order service unavailable")

    if order_resp.status_code == 200:
        app.logger.info(f"2PC CHECKOUT {tx_id}: Order voted YES")
    else:
        # Order voted NO
        app.logger.info(f"2PC CHECKOUT {tx_id}: Order voted NO — aborting")
        abort(400, "Order cannot be prepared")

    # parse order
    order_data = order_resp.json()

    items = order_data.get("items")
    user_id = order_data.get("user_id")
    total_cost = order_data.get("total_cost")

    if not items:
        app.logger.error(f"2PC CHECKOUT {tx_id}: Order items could not be parsed")
        abort(400, "Order items could not be parsed")

    # ── Step 2: Prepare Stock Service ──
    # Ask the stock service to reserve all items in the order.
    app.logger.info(f"2PC CHECKOUT {tx_id}: Phase 1 — Preparing stock...")
    stock_resp = send_post_request_json(
        f"{GATEWAY_URL}/stock/2pc/prepare/{tx_id}",
        {"items": items}
    )

    if stock_resp is None:
        # Stock service is unreachable
        app.logger.error(f"2PC CHECKOUT {tx_id}: Stock service unreachable - aborting order")
        send_post_request(f"{GATEWAY_URL}/order/2pc/abort/{tx_id}")
        abort(400, "Stock service unavailable")

    if stock_resp.status_code == 200:
        app.logger.info(f"2PC CHECKOUT {tx_id}: Stock voted YES")
    else:
        # Stock voted NO
        app.logger.info(f"2PC CHECKOUT {tx_id}: Stock voted NO — aborting order")
        send_post_request(f"{GATEWAY_URL}/order/2pc/abort/{tx_id}")
        abort(400, f"Checkout failed: insufficient stock")

    # ── Step 3: Prepare Payment Service ──
    # Ask the payment service to reserve the credit.
    app.logger.info(f"2PC CHECKOUT {tx_id}: Phase 1 — Preparing payment...")
    payment_resp = send_post_request_json(
        f"{GATEWAY_URL}/payment/2pc/prepare/{tx_id}",
        {"user_id": user_id, "amount": total_cost}
    )

    if payment_resp is None:
        # Payment service is unreachable — must abort stock (it was prepared), abort order service
        app.logger.error(f"2PC CHECKOUT {tx_id}: Payment service unreachable — aborting stock and order")
        send_post_request(f"{GATEWAY_URL}/stock/2pc/abort/{tx_id}")
        send_post_request(f"{GATEWAY_URL}/order/2pc/abort/{tx_id}")
        abort(400, "Payment service unavailable")

    if payment_resp.status_code == 200:
        app.logger.info(f"2PC CHECKOUT {tx_id}: Payment voted YES")
    else:
        # Payment voted NO — must abort stock (it was prepared), abort order service
        app.logger.info(f"2PC CHECKOUT {tx_id}: Payment voted NO — aborting stock and order")
        send_post_request(f"{GATEWAY_URL}/stock/2pc/abort/{tx_id}")
        send_post_request(f"{GATEWAY_URL}/order/2pc/abort/{tx_id}")
        abort(400, "Checkout failed: insufficient credit")

    # ═══════════════════════════════════════════════════════════════════
    # DECISION: All participants voted YES
    # ═══════════════════════════════════════════════════════════════════
    app.logger.info(f"2PC CHECKOUT {tx_id}: All participants voted YES — committing...")

    # ═══════════════════════════════════════════════════════════════════
    # PHASE 2: COMMIT
    # Tell all participants to finalize their changes.
    # ═══════════════════════════════════════════════════════════════════

    # ── Step 4: Commit Order Service ──
    order_commit_resp = send_post_request(f"{GATEWAY_URL}/order/2pc/commit/{tx_id}")
    if order_commit_resp is None or order_commit_resp.status_code != 200:
        app.logger.error(f"2PC CHECKOUT {tx_id}: Order commit failed!")
        # In strict 2PC, we should retry commits indefinitely.
        # For simplicity, we log the error. The TTL on the lock will
        # eventually clean it up.
    
    # ── Step 5: Commit Stock Service ──
    stock_commit_resp = send_post_request(f"{GATEWAY_URL}/stock/2pc/commit/{tx_id}")
    if stock_commit_resp is None or stock_commit_resp.status_code != 200:
        app.logger.error(f"2PC CHECKOUT {tx_id}: Stock commit failed!")

    # ── Step 6: Commit Payment Service ──
    payment_commit_resp = send_post_request(f"{GATEWAY_URL}/payment/2pc/commit/{tx_id}")
    if payment_commit_resp is None or payment_commit_resp.status_code != 200:
        app.logger.error(f"2PC CHECKOUT {tx_id}: Payment commit failed!")


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

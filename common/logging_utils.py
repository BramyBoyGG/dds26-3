from __future__ import annotations

"""
logging_utils.py — Structured logging helpers for SAGA transactions.

Why structured logging?
  When debugging a distributed checkout that spans 3 services, you need to
  correlate log lines by tx_id.  Structured logs make this easy:

    [ORDER]   tx=abc-123  state=RESERVING_STOCK  Publishing RESERVE_STOCK
    [STOCK]   tx=abc-123  step=RESERVE_STOCK     Processing 3 items
    [STOCK]   tx=abc-123  step=RESERVE_STOCK     SUCCESS
    [ORDER]   tx=abc-123  state=DEDUCTING_PAYMENT Publishing DEDUCT_PAYMENT
    [PAYMENT] tx=abc-123  step=DEDUCT_PAYMENT    Insufficient credit
    [ORDER]   tx=abc-123  state=COMPENSATING_STOCK Compensating...

  You can grep for "tx=abc-123" to see the full lifecycle of one checkout
  across all services.

Usage:

    from common.logging_utils import setup_logging, log_tx

    logger = setup_logging("stock-service")

    # Regular logging still works
    logger.info("Service started")

    # Transaction-specific logging — always includes tx_id for tracing
    log_tx(logger, "abc-123", "RESERVE_STOCK", "Processing 3 items")
    log_tx(logger, "abc-123", "RESERVE_STOCK", "SUCCESS")
"""

import logging
import os
import socket
from typing import Optional


def setup_logging(service_name: str, level: Optional[str] = None) -> logging.Logger:
    """
    Configure and return a logger for a service.

    Creates a logger with a consistent format across all services.
    The format includes:
      • Timestamp (for ordering events across services)
      • Service name (to know which service emitted the log)
      • Consumer ID (hostname — to distinguish replicas)
      • Log level
      • Message

    Parameters
    ----------
    service_name : str
        Human-readable name for the service (e.g., "order-service",
        "stock-service", "payment-service").
    level : str or None
        Log level override.  If None, reads from LOG_LEVEL env var,
        defaulting to "INFO".  Valid values: DEBUG, INFO, WARNING, ERROR.

    Returns
    -------
    logging.Logger
        Configured logger instance.
    """
    if level is None:
        level = os.environ.get("LOG_LEVEL", "INFO").upper()

    # Get a unique consumer identifier — useful when running multiple replicas
    consumer_id = get_consumer_id()

    logger = logging.getLogger(service_name)
    logger.setLevel(getattr(logging, level, logging.INFO))

    # Avoid adding duplicate handlers if setup_logging is called multiple times
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            f"%(asctime)s [{service_name}] [{consumer_id}] %(levelname)s  %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger


def get_consumer_id() -> str:
    """
    Generate a unique consumer name for this service instance.

    Used as the consumer name in XREADGROUP.  Each replica of a service
    needs a unique name so that Redis distributes messages correctly
    within the consumer group.

    We use the hostname, which is unique per Docker container.
    Falls back to "unknown" if hostname resolution fails.

    Returns
    -------
    str
        A unique identifier for this consumer instance.
    """
    try:
        return socket.gethostname()
    except Exception:
        return f"consumer-{os.getpid()}"


def log_tx(
    logger: logging.Logger,
    tx_id: str,
    step: str,
    message: str,
    level: int = logging.INFO,
) -> None:
    """
    Log a transaction-related event with structured fields.

    Always includes tx_id and step so you can grep/filter logs by
    transaction:  grep "tx=abc-123" to see the full checkout lifecycle.

    Parameters
    ----------
    logger : logging.Logger
        The service's logger.
    tx_id : str
        Transaction ID.
    step : str
        Current saga step or state (e.g., "RESERVE_STOCK", "DEDUCTING_PAYMENT").
    message : str
        Human-readable description of what happened.
    level : int
        Logging level (default: INFO).  Use logging.DEBUG for verbose,
        logging.ERROR for failures.
    """
    logger.log(level, f"tx={tx_id}  step={step}  {message}")


def log_tx_state_change(
    logger: logging.Logger,
    tx_id: str,
    old_state: str,
    new_state: str,
    reason: str = "",
) -> None:
    """
    Log a transaction state transition (used by the Order/orchestrator).

    Example output:
      tx=abc-123  RESERVING_STOCK -> DEDUCTING_PAYMENT  (stock reserved)

    Parameters
    ----------
    logger : logging.Logger
        The service's logger.
    tx_id : str
        Transaction ID.
    old_state : str
        Previous state (e.g., "RESERVING_STOCK").
    new_state : str
        New state (e.g., "DEDUCTING_PAYMENT").
    reason : str, optional
        Why the transition happened (e.g., "stock reserved", "insufficient credit").
    """
    suffix = f"  ({reason})" if reason else ""
    logger.info(f"tx={tx_id}  {old_state} -> {new_state}{suffix}")

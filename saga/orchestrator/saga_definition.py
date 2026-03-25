"""
saga_definition.py - Declarative SAGA step definitions.

A SagaDefinition describes the sequence of steps in a distributed transaction,
including the forward action for each step and the compensating action to run
if a later step fails.

Example usage:

    checkout_saga = SagaDefinition("checkout")
    checkout_saga.add_step(
        SagaStep(
            name="RESERVE_STOCK",
            forward_command="RESERVE_STOCK",
            compensate_command="COMPENSATE_STOCK",
            target_stream="stock-commands",
            payload_builder=lambda ctx: {"items": ctx["items"]},
        )
    )
    checkout_saga.add_step(
        SagaStep(
            name="DEDUCT_PAYMENT",
            forward_command="DEDUCT_PAYMENT",
            compensate_command="REFUND_PAYMENT",
            target_stream="payment-commands",
            payload_builder=lambda ctx: {"user_id": ctx["user_id"], "amount": ctx["total_cost"]},
        )
    )
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Optional, Any


@dataclass
class SagaStep:
    """
    A single step in a SAGA workflow.

    Attributes
    ----------
    name : str
        Unique identifier for this step (e.g., "RESERVE_STOCK").
        Used as the state name and for response routing.

    forward_command : str
        The command type to publish to execute the forward action.

    compensate_command : str or None
        The command type to publish if compensation is needed.
        None if this step doesn't need compensation (rare).

    target_stream : str
        The Redis stream to publish the forward command to.

    target_db_key : str
        Environment variable prefix for the target Redis connection.
        E.g., "STOCK" will look for STOCK_REDIS_HOST, etc.

    payload_builder : Callable[[dict], dict]
        Function that takes the saga context and returns the payload
        for the forward command. Context contains transaction data.

    compensate_payload_builder : Callable[[dict], dict] or None
        Function that builds the payload for compensation.
        If None, uses the same payload_builder.
    """
    name: str
    forward_command: str
    compensate_command: Optional[str]
    target_stream: str
    target_db_key: str
    payload_builder: Callable[[dict], dict]
    compensate_payload_builder: Optional[Callable[[dict], dict]] = None

    def build_forward_payload(self, context: dict) -> dict:
        """Build the payload for the forward command."""
        return self.payload_builder(context)

    def build_compensate_payload(self, context: dict) -> dict:
        """Build the payload for the compensating command."""
        if self.compensate_payload_builder:
            return self.compensate_payload_builder(context)
        return self.payload_builder(context)


@dataclass
class SagaDefinition:
    """
    A complete SAGA workflow definition.

    A saga is a sequence of steps that must all succeed for the transaction
    to complete. If any step fails, previous steps are compensated in reverse
    order.

    Attributes
    ----------
    name : str
        Human-readable name for this saga (e.g., "checkout").

    steps : list[SagaStep]
        Ordered list of steps to execute.

    on_complete : Callable[[dict], None] or None
        Optional callback when saga completes successfully.
        Receives the saga context.

    on_abort : Callable[[dict, str], None] or None
        Optional callback when saga aborts.
        Receives the saga context and failure reason.
    """
    name: str
    steps: list[SagaStep] = field(default_factory=list)
    on_complete: Optional[Callable[[dict], None]] = None
    on_abort: Optional[Callable[[dict, str], None]] = None

    def add_step(self, step: SagaStep) -> "SagaDefinition":
        """Add a step to the saga. Returns self for chaining."""
        self.steps.append(step)
        return self

    def get_step(self, name: str) -> Optional[SagaStep]:
        """Get a step by name."""
        for step in self.steps:
            if step.name == name:
                return step
        return None

    def get_step_index(self, name: str) -> int:
        """Get the index of a step by name. Returns -1 if not found."""
        for i, step in enumerate(self.steps):
            if step.name == name:
                return i
        return -1

    def get_next_step(self, current_step_name: str) -> Optional[SagaStep]:
        """Get the next step after the given step name."""
        idx = self.get_step_index(current_step_name)
        if idx == -1 or idx >= len(self.steps) - 1:
            return None
        return self.steps[idx + 1]

    def get_steps_to_compensate(self, failed_step_name: str) -> list[SagaStep]:
        """
        Get the steps that need compensation when a step fails.

        Returns steps in reverse order (most recent first).
        Only includes steps that were completed before the failed step.
        """
        failed_idx = self.get_step_index(failed_step_name)
        if failed_idx <= 0:
            return []
        # Steps 0 to failed_idx-1 were completed and need compensation
        return list(reversed(self.steps[:failed_idx]))

    @property
    def first_step(self) -> Optional[SagaStep]:
        """Get the first step of the saga."""
        return self.steps[0] if self.steps else None

    @property
    def last_step(self) -> Optional[SagaStep]:
        """Get the last step of the saga."""
        return self.steps[-1] if self.steps else None

    def __repr__(self) -> str:
        step_names = [s.name for s in self.steps]
        return f"SagaDefinition(name={self.name!r}, steps={step_names})"

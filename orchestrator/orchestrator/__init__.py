"""Saga orchestrator library package.

This package provides a reusable orchestrator implementation for the SAGA pattern
(with full state logging, retry-safe spans, and compensation) and a participant
helper for microservices.

Usage:
    from orchestrator import SagaOrchestrator, SagaDefinition, SagaStep, SagaParticipant
"""

from orchestrator.saga_definition import SagaStep, SagaDefinition
from orchestrator.orchestrator import SagaOrchestrator
from orchestrator.participant import SagaParticipant

__all__ = [
    "SagaStep",
    "SagaDefinition",
    "SagaOrchestrator",
    "SagaParticipant",
]

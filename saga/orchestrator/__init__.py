"""
Orchestrator Library - A reusable SAGA orchestration framework.

This library provides a clean abstraction for implementing distributed
transactions using the SAGA pattern with Redis Streams.

Key components:
- SagaDefinition: Declarative saga step definitions
- SagaOrchestrator: Executes sagas and handles state transitions
- SagaParticipant: Base class for services participating in sagas
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

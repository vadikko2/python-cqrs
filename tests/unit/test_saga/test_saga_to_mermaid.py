"""Tests for Mermaid diagram generator."""

import typing


from cqrs.saga.mermaid import SagaMermaid
from cqrs.saga.saga import Saga
from cqrs.saga.step import SagaStepHandler

from .conftest import (
    OrderContext,
    ProcessPaymentStep,
    ReserveInventoryStep,
    ShipOrderStep,
    SagaContainer,
)


def test_to_mermaid_empty_steps(saga_container: SagaContainer) -> None:
    """Test that Mermaid handles empty steps list correctly."""
    steps: list[type[SagaStepHandler[OrderContext, typing.Any]]] = []
    saga = Saga(steps=steps, container=saga_container)  # type: ignore
    generator = SagaMermaid(saga)

    diagram = generator.sequence()

    assert "sequenceDiagram" in diagram
    assert "participant S as Saga" in diagram
    assert "No steps configured" in diagram


def test_to_mermaid_single_step(saga_container: SagaContainer) -> None:
    """Test that Mermaid generates correct diagram for single step."""
    steps = [ReserveInventoryStep]
    saga = Saga(steps=steps, container=saga_container)  # type: ignore
    generator = SagaMermaid(saga)

    diagram = generator.sequence()

    # Check basic structure
    assert "sequenceDiagram" in diagram
    assert "participant S as Saga" in diagram
    assert "participant S1 as ReserveInventoryStep" in diagram

    # Check successful flow
    assert "Successful Execution Flow" in diagram
    assert "S->>S1: act()" in diagram
    assert "S1-->>S: success" in diagram
    assert "Saga Completed" in diagram

    # Check failure flow
    assert "Failure & Compensation Flow" in diagram
    assert "No compensation needed" in diagram


def test_to_mermaid_multiple_steps(saga_container: SagaContainer) -> None:
    """Test that Mermaid generates correct diagram for multiple steps."""
    steps = [ReserveInventoryStep, ProcessPaymentStep, ShipOrderStep]
    saga = Saga(steps=steps, container=saga_container)  # type: ignore
    generator = SagaMermaid(saga)

    diagram = generator.sequence()

    # Check participants
    assert "participant S as Saga" in diagram
    assert "participant S1 as ReserveInventoryStep" in diagram
    assert "participant S2 as ProcessPaymentStep" in diagram
    assert "participant S3 as ShipOrderStep" in diagram

    # Check successful execution flow
    assert "Successful Execution Flow" in diagram
    assert "S->>S1: act()" in diagram
    assert "S1-->>S: success" in diagram
    assert "S->>S2: act()" in diagram
    assert "S2-->>S: success" in diagram
    assert "S->>S3: act()" in diagram
    assert "S3-->>S: success" in diagram
    assert "Saga Completed" in diagram

    # Check failure and compensation flow
    assert "Failure & Compensation Flow" in diagram
    failure_section_start = diagram.find("Failure & Compensation Flow")
    failure_section = diagram[failure_section_start:]

    # First two steps should succeed before S3 fails
    # Extract act() calls in failure section
    act_calls_in_failure = []
    for line in failure_section.split("\n"):
        if "act()" in line and "S->>" in line:
            act_calls_in_failure.append(line.strip())

    # Should have S1, S2 succeed, then S3 fails
    assert len(act_calls_in_failure) >= 3
    assert "S->>S1: act()" in act_calls_in_failure[0]
    assert "S->>S2: act()" in act_calls_in_failure[1]
    assert "S->>S3: act()" in act_calls_in_failure[2]

    # Check that S3 returns error
    assert "S3-->>S: error" in failure_section

    # Check compensation in reverse order
    assert "Compensation (reverse order)" in diagram
    # Should compensate S2 and S1 (all steps before S3) in reverse order
    compensate_section_start = diagram.find("Compensation (reverse order)")
    compensate_section = diagram[compensate_section_start:]
    # S2 should be compensated first (reverse order), then S1
    assert "S->>S2: compensate()" in compensate_section
    assert "S->>S1: compensate()" in compensate_section
    # Check order: S2 should come before S1 in compensation section
    s2_pos = compensate_section.find("S->>S2: compensate()")
    s1_pos = compensate_section.find("S->>S1: compensate()")
    assert s2_pos < s1_pos, "S2 should be compensated before S1 (reverse order)"
    assert "Saga Failed" in diagram


def test_to_mermaid_steps_order(saga_container: SagaContainer) -> None:
    """Test that Mermaid preserves steps order in diagram."""
    steps = [ReserveInventoryStep, ProcessPaymentStep, ShipOrderStep]
    saga = Saga(steps=steps, container=saga_container)  # type: ignore
    generator = SagaMermaid(saga)

    diagram = generator.sequence()

    # Check that steps appear in correct order
    lines = diagram.split("\n")

    # Find participant declarations
    participant_lines = [line for line in lines if "participant" in line]

    # S should be first, then S1, S2, S3
    assert "participant S as Saga" in participant_lines[0]
    assert "participant S1 as ReserveInventoryStep" in participant_lines[1]
    assert "participant S2 as ProcessPaymentStep" in participant_lines[2]
    assert "participant S3 as ShipOrderStep" in participant_lines[3]

    # Check execution order in successful flow
    successful_flow_start = diagram.find("Successful Execution Flow")
    failure_flow_start = diagram.find("Failure & Compensation Flow")

    # Extract only the successful flow section (between the two sections)
    if failure_flow_start != -1:
        successful_flow = diagram[successful_flow_start:failure_flow_start]
    else:
        successful_flow = diagram[successful_flow_start:]

    # Find act() calls in order within successful flow only
    act_calls = []
    for line in successful_flow.split("\n"):
        if "act()" in line and "S->>" in line:
            act_calls.append(line.strip())

    assert len(act_calls) == 3
    assert "S->>S1: act()" in act_calls[0]
    assert "S->>S2: act()" in act_calls[1]
    assert "S->>S3: act()" in act_calls[2]


def test_to_mermaid_compensation_reverse_order(saga_container: SagaContainer) -> None:
    """Test that compensation is shown in reverse order."""
    steps = [ReserveInventoryStep, ProcessPaymentStep, ShipOrderStep]
    saga = Saga(steps=steps, container=saga_container)  # type: ignore
    generator = SagaMermaid(saga)

    diagram = generator.sequence()

    # Find compensation section
    compensation_start = diagram.find("Compensation (reverse order)")
    compensation_section = diagram[compensation_start:]

    # Extract compensate() calls
    compensate_calls = []
    for line in compensation_section.split("\n"):
        if "compensate()" in line and "S->>" in line:
            compensate_calls.append(line.strip())

    # Should compensate in reverse order: S2 first, then S1 (all steps before S3)
    assert len(compensate_calls) == 2
    assert "S->>S2: compensate()" in compensate_calls[0]
    assert "S->>S1: compensate()" in compensate_calls[1]


def test_to_mermaid_long_step_names(saga_container: SagaContainer) -> None:
    """Test that long step names are truncated in diagram."""

    # Create a step with a very long name
    class VeryLongStepNameThatShouldBeTruncatedInTheDiagram(
        SagaStepHandler[OrderContext, typing.Any],
    ):
        def __init__(self) -> None:
            self._events: list = []

        @property
        def events(self) -> list:
            return self._events.copy()

        async def act(
            self,
            context: OrderContext,
        ) -> typing.Any:
            return self._generate_step_result(None)

        async def compensate(self, context: OrderContext) -> None:
            pass

    steps = [VeryLongStepNameThatShouldBeTruncatedInTheDiagram]
    saga = Saga(steps=steps, container=saga_container)  # type: ignore
    generator = SagaMermaid(saga)

    diagram = generator.sequence()

    # Check that the name is truncated (should be max 30 chars + "...")
    assert "participant S1 as" in diagram
    # The full name should not appear, but truncated version should
    participant_line = [
        line for line in diagram.split("\n") if "participant S1" in line
    ][0]
    # Name should be truncated to 30 chars max
    assert len(participant_line.split("as")[1].strip()) <= 33  # 30 + "..."


def test_class_diagram_empty_steps(saga_container: SagaContainer) -> None:
    """Test that class_diagram() handles empty steps list correctly."""
    steps: list[type[SagaStepHandler[OrderContext, typing.Any]]] = []
    saga = Saga(steps=steps, container=saga_container)  # type: ignore
    generator = SagaMermaid(saga)

    diagram = generator.class_diagram()

    assert "classDiagram" in diagram
    assert "class Saga" in diagram
    assert "No steps configured" in diagram


def test_class_diagram_basic_structure(saga_container: SagaContainer) -> None:
    """Test that class_diagram() generates correct basic structure."""
    steps = [ReserveInventoryStep, ProcessPaymentStep, ShipOrderStep]
    saga = Saga(steps=steps, container=saga_container)  # type: ignore
    generator = SagaMermaid(saga)

    diagram = generator.class_diagram()

    # Check basic structure
    assert "classDiagram" in diagram
    assert "class Saga" in diagram
    assert "+steps: List[SagaStepHandler]" in diagram
    assert "+transaction(context) SagaTransaction" in diagram

    # Check step classes
    assert "class ReserveInventoryStep" in diagram
    assert "class ProcessPaymentStep" in diagram
    assert "class ShipOrderStep" in diagram

    # Check step methods
    assert "+act(context) SagaStepResult" in diagram
    assert "+compensate(context) void" in diagram
    assert "+events: List[Event]" in diagram


def test_class_diagram_context_types(saga_container: SagaContainer) -> None:
    """Test that class_diagram() includes context types."""
    steps = [ReserveInventoryStep, ProcessPaymentStep, ShipOrderStep]
    saga = Saga(steps=steps, container=saga_container)  # type: ignore
    generator = SagaMermaid(saga)

    diagram = generator.class_diagram()

    # Check that OrderContext is included
    assert "class OrderContext" in diagram
    # Check that context fields are included
    assert "+order_id:" in diagram or "order_id" in diagram
    assert "+user_id:" in diagram or "user_id" in diagram
    assert "+amount:" in diagram or "amount" in diagram


def test_class_diagram_response_types(saga_container: SagaContainer) -> None:
    """Test that class_diagram() includes response types."""
    steps = [ReserveInventoryStep, ProcessPaymentStep, ShipOrderStep]
    saga = Saga(steps=steps, container=saga_container)  # type: ignore
    generator = SagaMermaid(saga)

    diagram = generator.class_diagram()

    # Check that response types are included
    assert "class ReserveInventoryResponse" in diagram
    assert "class ProcessPaymentResponse" in diagram
    assert "class ShipOrderResponse" in diagram


def test_class_diagram_relationships(saga_container: SagaContainer) -> None:
    """Test that class_diagram() includes relationships between classes."""
    steps = [ReserveInventoryStep, ProcessPaymentStep, ShipOrderStep]
    saga = Saga(steps=steps, container=saga_container)  # type: ignore
    generator = SagaMermaid(saga)

    diagram = generator.class_diagram()

    # Check Saga to Step relationships
    assert (
        "Saga --> ReserveInventoryStep" in diagram
        or "Saga --> ReserveInventoryStep : contains" in diagram
    )
    assert (
        "Saga --> ProcessPaymentStep" in diagram
        or "Saga --> ProcessPaymentStep : contains" in diagram
    )
    assert (
        "Saga --> ShipOrderStep" in diagram
        or "Saga --> ShipOrderStep : contains" in diagram
    )

    # Check Step to Context relationships
    assert (
        "ReserveInventoryStep ..> OrderContext" in diagram
        or "ReserveInventoryStep ..> OrderContext : uses" in diagram
    )

    # Check Step to Response relationships
    assert (
        "ReserveInventoryStep ..> ReserveInventoryResponse" in diagram
        or "ReserveInventoryStep ..> ReserveInventoryResponse : returns" in diagram
    )
    assert (
        "ProcessPaymentStep ..> ProcessPaymentResponse" in diagram
        or "ProcessPaymentStep ..> ProcessPaymentResponse : returns" in diagram
    )
    assert (
        "ShipOrderStep ..> ShipOrderResponse" in diagram
        or "ShipOrderStep ..> ShipOrderResponse : returns" in diagram
    )


def test_class_diagram_single_step(saga_container: SagaContainer) -> None:
    """Test that class_diagram() works with single step."""
    steps = [ReserveInventoryStep]
    saga = Saga(steps=steps, container=saga_container)  # type: ignore
    generator = SagaMermaid(saga)

    diagram = generator.class_diagram()

    assert "classDiagram" in diagram
    assert "class Saga" in diagram
    assert "class ReserveInventoryStep" in diagram
    assert "class OrderContext" in diagram
    assert "class ReserveInventoryResponse" in diagram

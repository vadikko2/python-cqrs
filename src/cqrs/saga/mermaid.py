"""Mermaid diagram generator for Saga."""

import inspect
import typing

from cqrs.saga.saga import Saga
from cqrs.saga.step import SagaStepHandler


class SagaMermaid:
    """
    Generator for Mermaid diagrams from Saga instances.

    This class can generate:
    - Sequence diagrams showing execution flow and compensation
    - Class diagrams showing types, relationships, and structure

    Usage::

        class MySaga(Saga[MyContext]):
            steps = [Step1, Step2, Step3]

        saga = MySaga()
        generator = SagaMermaid(saga)

        # Generate sequence diagram
        sequence_diagram = generator.sequence()

        # Generate class diagram
        class_diagram = generator.class_diagram()
    """

    def __init__(self, saga: Saga[typing.Any]) -> None:
        """
        Initialize Mermaid diagram generator.

        Args:
            saga: The saga instance to generate diagram for.
        """
        self._saga = saga

    def sequence(self) -> str:
        """
        Generate a Mermaid Sequence diagram showing all saga steps and compensations.

        Returns:
            A string containing the Mermaid Sequence diagram code.
        """
        steps = self._saga.steps

        if not steps:
            return "sequenceDiagram\n    participant S as Saga\n    Note over S: No steps configured"

        # Generate participant aliases for better readability
        participants = ["S as Saga"]
        step_aliases: dict[str, str] = {}

        for idx, step_type in enumerate(steps, start=1):
            step_name = step_type.__name__
            # Create short alias (S1, S2, S3, etc.)
            alias = f"S{idx}"
            step_aliases[step_name] = alias
            # Truncate long step names for better diagram readability
            display_name = step_name if len(step_name) <= 30 else step_name[:27] + "..."
            participants.append(f"{alias} as {display_name}")

        lines = ["sequenceDiagram"]
        lines.extend(f"    participant {p}" for p in participants)
        lines.append("")

        # Generate successful execution flow
        lines.append("    Note over S: Successful Execution Flow")
        for idx, step_type in enumerate(steps):
            step_name = step_type.__name__
            alias = step_aliases[step_name]
            lines.append(f"    S->>{alias}: act()")
            lines.append(f"    {alias}-->>S: success")
        lines.append("    Note over S: Saga Completed")
        lines.append("")

        # Generate failure and compensation flow
        lines.append("    Note over S: Failure & Compensation Flow")
        # Execute steps until failure
        if len(steps) > 1:
            # Show all steps except the last one succeeding
            for step_type in steps[:-1]:
                step_name = step_type.__name__
                alias = step_aliases[step_name]
                lines.append(f"    S->>{alias}: act()")
                lines.append(f"    {alias}-->>S: success")

            # Show last step failing
            last_step = steps[-1]
            last_alias = step_aliases[last_step.__name__]
            lines.append(f"    S->>{last_alias}: act()")
            lines.append(f"    {last_alias}-->>S: error")
            lines.append("")

            # Compensate completed steps in reverse order
            lines.append("    Note over S: Compensation (reverse order)")
            # Compensate all steps before the failing one (in reverse order)
            for step_type in reversed(steps[:-1]):
                step_name = step_type.__name__
                alias = step_aliases[step_name]
                lines.append(f"    S->>{alias}: compensate()")
                lines.append(f"    {alias}-->>S: success")
        else:
            # Single step scenario
            single_step = steps[0]
            single_alias = step_aliases[single_step.__name__]
            lines.append(f"    S->>{single_alias}: act()")
            lines.append(f"    {single_alias}-->>S: error")
            lines.append(
                "    Note over S: No compensation needed (step failed before completion)",
            )

        lines.append("    Note over S: Saga Failed")

        return "\n".join(lines)

    def class_diagram(self) -> str:
        """
        Generate a Mermaid Class diagram showing saga structure, types, and relationships.

        The diagram includes:
        - Saga class with its steps
        - Step handler classes with their context and response types
        - Context classes
        - Response classes
        - Event classes produced by steps (if detectable)

        Returns:
            A string containing the Mermaid Class diagram code.
        """
        steps = self._saga.steps

        if not steps:
            return (
                "classDiagram\n    class Saga\n    Note for Saga: No steps configured"
            )

        # Collect all types
        context_types: set[type] = set()
        response_types: set[type] = set()
        event_types: set[type] = set()
        step_info: list[tuple[str, type | None, type | None, list[type]]] = []

        # Extract type information from each step
        for step_type in steps:
            step_name = step_type.__name__
            context_type: type | None = None
            response_type: type | None = None
            step_events: list[type] = []

            # Extract generic type parameters from __orig_bases__
            orig_bases = getattr(step_type, "__orig_bases__", ())
            for base in orig_bases:
                origin = typing.get_origin(base)
                # Check if this base is SagaStepHandler or a subclass
                if origin is SagaStepHandler:
                    args = typing.get_args(base)
                    if len(args) >= 1 and args[0] is not typing.Any:
                        context_type = args[0]
                        if inspect.isclass(context_type):
                            context_types.add(context_type)
                    if len(args) >= 2 and args[1] is not typing.Any:
                        response_type = args[1]
                        if inspect.isclass(response_type):
                            response_types.add(response_type)
                    break  # Found the right base, no need to continue

            # If not found in __orig_bases__, try __bases__
            if context_type is None and response_type is None:
                for base in step_type.__bases__:
                    if issubclass(base, SagaStepHandler):
                        # Try to get type hints from the class itself
                        if hasattr(step_type, "__annotations__"):
                            # Check if we can infer from class definition
                            pass
                        break

            step_info.append((step_name, context_type, response_type, step_events))

        # Build class diagram
        lines = ["classDiagram"]

        # Add Saga class
        lines.append("    class Saga {")
        lines.append("        +steps: List[SagaStepHandler]")
        lines.append("        +transaction(context) SagaTransaction")
        lines.append("    }")
        lines.append("")

        # Add step handler classes
        for step_name, context_type, response_type, _ in step_info:
            lines.append(f"    class {step_name} {{")
            lines.append("        +act(context) SagaStepResult")
            lines.append("        +compensate(context) void")
            lines.append("        +events: List[Event]")
            lines.append("    }")
            lines.append("")

        # Add context classes
        for context_type in sorted(context_types, key=lambda x: x.__name__):
            class_name = context_type.__name__
            lines.append(f"    class {class_name} {{")
            # Try to get fields if it's a dataclass
            if hasattr(context_type, "__dataclass_fields__"):
                fields = context_type.__dataclass_fields__
                for field_name, field_info in fields.items():
                    field_type = (
                        field_info.type.__name__
                        if hasattr(field_info.type, "__name__")
                        else str(field_info.type)
                    )
                    lines.append(f"        +{field_name}: {field_type}")
            lines.append("    }")
            lines.append("")

        # Add response classes
        for response_type in sorted(response_types, key=lambda x: x.__name__):
            class_name = response_type.__name__
            lines.append(f"    class {class_name} {{")
            # Try to get fields if it's a Pydantic model or dataclass
            if hasattr(response_type, "__dataclass_fields__"):
                fields = response_type.__dataclass_fields__
                for field_name, field_info in fields.items():
                    field_type = (
                        field_info.type.__name__
                        if hasattr(field_info.type, "__name__")
                        else str(field_info.type)
                    )
                    lines.append(f"        +{field_name}: {field_type}")
            elif hasattr(response_type, "model_fields"):  # Pydantic v2
                fields = response_type.model_fields
                for field_name, field_info in fields.items():
                    field_type = (
                        field_info.annotation.__name__
                        if hasattr(field_info.annotation, "__name__")
                        else str(field_info.annotation)
                    )
                    lines.append(f"        +{field_name}: {field_type}")
            elif hasattr(response_type, "__fields__"):  # Pydantic v1
                fields = response_type.__fields__
                for field_name, field_info in fields.items():
                    field_type = (
                        field_info.type_.__name__
                        if hasattr(field_info.type_, "__name__")
                        else str(field_info.type_)
                    )
                    lines.append(f"        +{field_name}: {field_type}")
            lines.append("    }")
            lines.append("")

        # Add relationships
        lines.append("    %% Saga relationships")
        for step_name, _, _, _ in step_info:
            lines.append(f"    Saga --> {step_name} : contains")

        lines.append("")
        lines.append("    %% Step to Context relationships")
        for step_name, context_type, _, _ in step_info:
            if context_type and inspect.isclass(context_type):
                lines.append(f"    {step_name} ..> {context_type.__name__} : uses")

        lines.append("")
        lines.append("    %% Step to Response relationships")
        for step_name, _, response_type, _ in step_info:
            if response_type and inspect.isclass(response_type):
                lines.append(f"    {step_name} ..> {response_type.__name__} : returns")

        # Note about events
        if event_types:
            lines.append("")
            lines.append("    %% Event classes")
            for event_type in sorted(event_types, key=lambda x: x.__name__):
                class_name = event_type.__name__
                lines.append(f"    class {class_name} {{")
                lines.append("        +Event fields")
                lines.append("    }")
                lines.append("")

            lines.append("    %% Step to Event relationships")
            for step_name, _, _, step_events in step_info:
                for event_type in step_events:
                    lines.append(
                        f"    {step_name} ..> {event_type.__name__} : produces",
                    )

        return "\n".join(lines)

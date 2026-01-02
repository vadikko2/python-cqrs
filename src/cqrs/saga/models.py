import dataclasses
import typing


@dataclasses.dataclass
class SagaContext:
    """
    Base class for saga context objects.

    Provides methods for serialization/deserialization to work with storage.
    All subclasses must also be decorated with @dataclass to work properly.

    Example::

        @dataclass
        class OrderContext(SagaContext):
            order_id: str
            user_id: str
            amount: float
    """

    def to_dict(self) -> dict[str, typing.Any]:
        """
        Serialize context to dictionary.

        Returns:
            Dictionary representation of the context.
        """
        return dataclasses.asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, typing.Any]) -> "SagaContext":
        """
        Deserialize context from dictionary.

        Args:
            data: Dictionary containing context data.

        Returns:
            Instance of the context class.
        """
        # Get field names from dataclass
        field_names = {f.name for f in dataclasses.fields(cls)}
        # Filter data to only include known fields
        filtered_data = {k: v for k, v in data.items() if k in field_names}
        return cls(**filtered_data)

    def model_dump(self) -> dict[str, typing.Any]:
        """
        Alias for to_dict() for consistency.

        Returns:
            Dictionary representation of the context.
        """
        return self.to_dict()


# Type variable for saga context, bound to SagaContext
ContextT = typing.TypeVar("ContextT", bound=SagaContext)


@dataclasses.dataclass
class SagaResult(typing.Generic[ContextT]):
    """
    Result of saga execution.

    Contains the context and error information if the saga failed,
    or success status if all steps completed successfully.
    """

    context: ContextT
    with_error: bool = False
    error_message: str | None = None
    error_traceback: list[str] | None = None
    error_type: typing.Type[Exception] | None = None

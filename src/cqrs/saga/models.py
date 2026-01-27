import dataclasses
import typing
from dataclass_wizard import asdict, fromdict
# Type variable for from_dict classmethod return type
_T = typing.TypeVar("_T", bound="SagaContext")


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
        return asdict(self)

    @classmethod
    def from_dict(cls: type[_T], data: dict[str, typing.Any]) -> _T:
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
        return fromdict(cls, filtered_data)

    def model_dump(self) -> dict[str, typing.Any]:
        """
        Alias for to_dict() for consistency.

        Returns:
            Dictionary representation of the context.
        """
        return self.to_dict()


# Type variable for saga context, bound to SagaContext
ContextT = typing.TypeVar("ContextT", bound=SagaContext)

import abc
import dataclasses
import typing

import pydantic


class IResponse(abc.ABC):
    """
    Interface for response-type objects.

    This abstract base class defines the contract that all response implementations
    must follow. Responses are result objects returned by request handlers and are
    typically used for defining the result of queries in the CQRS pattern.

    All response implementations must provide:
    - `to_dict()`: Convert the response instance to a dictionary representation
    - `from_dict()`: Create a response instance from a dictionary
    """

    @abc.abstractmethod
    def to_dict(self) -> dict:
        """
        Convert the response instance to a dictionary representation.

        Returns:
            A dictionary containing all fields of the response instance.
        """
        raise NotImplementedError

    @classmethod
    @abc.abstractmethod
    def from_dict(cls, **kwargs) -> typing.Self:
        """
        Create a response instance from keyword arguments.

        Args:
            **kwargs: Keyword arguments matching the response fields.

        Returns:
            A new instance of the response class.
        """
        raise NotImplementedError


@dataclasses.dataclass
class DCResponse(IResponse):
    """
    Dataclass-based implementation of the response interface.

    This class provides a response implementation using Python's dataclasses.
    It's useful when you want to avoid pydantic dependency or prefer dataclasses
    for response definitions.

    Example::

        @dataclasses.dataclass
        class UserResponse(DCResponse):
            user_id: str
            username: str
            email: str

        response = UserResponse(user_id="123", username="john", email="john@example.com")
        data = response.to_dict()  # {"user_id": "123", "username": "john", "email": "john@example.com"}
        restored = UserResponse.from_dict(**data)
    """

    @classmethod
    def from_dict(cls, **kwargs) -> typing.Self:
        """
        Create a response instance from keyword arguments.

        Args:
            **kwargs: Keyword arguments matching the dataclass fields.

        Returns:
            A new instance of the response class.
        """
        return cls(**kwargs)

    def to_dict(self) -> dict:
        """
        Convert the response instance to a dictionary representation.

        Returns:
            A dictionary containing all fields of the dataclass instance.
        """
        return dataclasses.asdict(self)


class PydanticResponse(pydantic.BaseModel, IResponse):
    """
    Pydantic-based implementation of the response interface.

    This class provides a response implementation using Pydantic models.
    It offers data validation, serialization, and other Pydantic features.
    This is the default response implementation used by the library.

    The response is a result of the request handling, which is held by RequestHandler.
    Often the response is used for defining the result of the query.

    Example::

        class UserResponse(PydanticResponse):
            user_id: str
            username: str
            email: str

        response = UserResponse(user_id="123", username="john", email="john@example.com")
        data = response.to_dict()  # {"user_id": "123", "username": "john", "email": "john@example.com"}
        restored = UserResponse.from_dict(**data)
    """

    @classmethod
    def from_dict(cls, **kwargs) -> typing.Self:
        """
        Create a response instance from keyword arguments.

        Validates and converts types, ensuring required fields are present.

        Args:
            **kwargs: Keyword arguments matching the response fields.

        Returns:
            A new instance of the response class.
        """
        return cls.model_validate(kwargs)

    def to_dict(self) -> dict:
        """
        Convert the response instance to a dictionary representation.

        Returns:
            A dictionary containing all fields of the response instance.
        """
        return self.model_dump(mode="python")


Response = PydanticResponse

__all__ = ("Response", "IResponse", "DCResponse", "PydanticResponse")

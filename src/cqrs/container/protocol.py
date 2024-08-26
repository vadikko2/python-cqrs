import typing

T = typing.TypeVar("T")
C = typing.TypeVar("C")


class Container(typing.Protocol[C]):
    """
    The container interface.
    """

    @property
    def external_container(self) -> C:
        raise NotImplementedError

    def attach_external_container(self, container: C) -> None:
        raise NotImplementedError

    async def resolve(self, type_: typing.Type[T]) -> T:
        raise NotImplementedError

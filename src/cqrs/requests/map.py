import typing

from cqrs.requests.cor_request_handler import CORRequestHandler
from cqrs.requests.request import Request
from cqrs.requests.request_handler import (
    RequestHandler,
    StreamingRequestHandler,
)

_KT = typing.TypeVar("_KT", bound=typing.Type[Request])
_VT = typing.TypeVar("_VT", bound=object)

# Type alias for handler types that can be bound to requests
HandlerType = (
    typing.Type[RequestHandler | StreamingRequestHandler]
    | typing.List[typing.Type[CORRequestHandler]]
)


class RequestMap(typing.Dict[_KT, HandlerType]):
    _registry: typing.Dict[_KT, HandlerType]

    def bind(self, request_type: _KT, handler_type: HandlerType) -> None:
        self[request_type] = handler_type

    def __setitem__(self, __key: _KT, __value: HandlerType) -> None:
        if __key in self:
            raise KeyError(f"{__key} already exists in registry")
        super().__setitem__(__key, __value)

    def __delitem__(self, __key: _KT) -> typing.NoReturn:
        raise TypeError(f"{self.__class__.__name__} has no delete method")

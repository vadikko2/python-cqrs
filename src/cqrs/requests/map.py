import typing

from cqrs.requests import request, request_handler

_KT = typing.TypeVar("_KT", bound=typing.Type[request.Request])
_VT = typing.TypeVar(
    "_VT",
    bound=typing.Type[
        request_handler.RequestHandler | request_handler.SyncRequestHandler
    ],
)


class RequestMap(typing.Dict[_KT, _VT]):
    _registry: typing.Dict[_KT, _VT]

    def bind(self, request_type: _KT, handler_type: _VT) -> None:
        self[request_type] = handler_type

    def __setitem__(self, __key: _KT, __value: _VT) -> None:
        if __key in self:
            raise KeyError(f"{__key} already exists in registry")
        super().__setitem__(__key, __value)

    def __delitem__(self, __key):
        raise TypeError(f"{self.__class__.__name__} has no delete method")

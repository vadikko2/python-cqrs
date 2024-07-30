import typing

from cqrs import registry
from cqrs.requests import request, request_handler

TReq = typing.TypeVar("TReq", bound=typing.Type[request.Request], contravariant=True)
TH = typing.TypeVar("TH", bound=typing.Type[request_handler.RequestHandler], contravariant=True)


class RequestMap(registry.InMemoryRegistry[TReq, TH]):
    def bind(
        self,
        request_type: TReq,
        handler_type: TH,
    ) -> None:
        self._registry[request_type] = handler_type

    def get(self, request_type: TReq) -> TH:
        handler_type = self._registry.get(request_type)
        if not handler_type:
            raise RequestHandlerDoesNotExist("RequestHandler not found matching Request type.")

        return handler_type

    def __str__(self) -> str:
        return str(self._registry)


class RequestHandlerDoesNotExist(Exception):
    ...

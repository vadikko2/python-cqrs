import typing

from cqrs.events import event, event_handler

_KT = typing.TypeVar("_KT", bound=typing.Type[event.Event])
_VT: typing.TypeAlias = typing.List[
    typing.Type[event_handler.EventHandler[event.Event]]
]


class EventMap(typing.Dict[_KT, _VT]):
    def bind(
        self,
        event_type: _KT,
        handler_type: typing.Type[event_handler.EventHandler],
    ) -> None:
        if event_type not in self:
            self[event_type] = [handler_type]
        else:
            if handler_type in self[event_type]:
                raise KeyError(f"{handler_type} already bind to {event_type}")
            self[event_type].append(handler_type)

    def __setitem__(self, __key: _KT, __value: _VT) -> None:
        if __key in self:
            raise KeyError(f"{__key} already exists in registry")
        super().__setitem__(__key, __value)

    def __delitem__(self, __key_: _KT):
        raise TypeError(f"{self.__class__.__name__} has no delete method")

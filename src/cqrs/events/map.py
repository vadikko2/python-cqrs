import typing

from cqrs.events.event import IEvent
from cqrs.events import event_handler

_KT = typing.TypeVar("_KT", bound=typing.Type[IEvent])
_VT: typing.TypeAlias = typing.List[typing.Type[event_handler.EventHandler]]


class EventMap(typing.Dict[_KT, _VT]):
    """
    Registry mapping event types to one or more handler types.

    Use :meth:`bind` to register handlers for an event type. Multiple handlers
    can be bound to the same event; all will be invoked when the event is emitted.
    Keys cannot be overwritten or deleted.

    Example::

        event_map = EventMap()
        event_map.bind(OrderCreatedEvent, OrderCreatedEventHandler)
        event_map.bind(OrderCreatedEvent, SendEmailHandler)  # second handler for same event
        # event_map[OrderCreatedEvent] -> [OrderCreatedEventHandler, SendEmailHandler]
    """

    def bind(
        self,
        event_type: _KT,
        handler_type: typing.Type[event_handler.EventHandler],
    ) -> None:
        """
        Register a handler type for an event type.

        If the event type is new, creates a list with this handler. If the event
        type already exists, appends the handler (duplicates are rejected).

        Args:
            event_type: Event class (e.g. :class:`OrderCreatedEvent`).
            handler_type: Handler class implementing :class:`~cqrs.events.event_handler.EventHandler`.

        Raises:
            KeyError: If the same handler type is already bound to this event type.

        Example::

            event_map.bind(OrderCreatedEvent, OrderCreatedEventHandler)
        """
        if event_type not in self:
            self[event_type] = [handler_type]
        else:
            if handler_type in self[event_type]:
                raise KeyError(f"{handler_type} already bind to {event_type}")
            self[event_type].append(handler_type)

    def __setitem__(self, __key: _KT, __value: _VT) -> None:
        """
        Set handler list for an event type (only if key is not already present).

        Raises:
            KeyError: If the event type is already in the registry.
        """
        if __key in self:
            raise KeyError(f"{__key} already exists in registry")
        super().__setitem__(__key, __value)

    def __delitem__(self, __key_: _KT) -> typing.NoReturn:
        """Deletion is not supported; raises TypeError."""
        raise TypeError(f"{self.__class__.__name__} has no delete method")

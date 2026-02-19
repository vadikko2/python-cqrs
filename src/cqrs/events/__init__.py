"""Event types, handlers, emitter, and event map for the CQRS events layer.

Public API:
- Event types: :class:`Event`, :class:`DomainEvent`, :class:`NotificationEvent`,
  and their interfaces/base classes.
- :class:`EventHandler` — handler interface; implement :meth:`EventHandler.handle`
  and optionally :attr:`EventHandler.events` for follow-up events.
- :class:`EventEmitter` — sends domain events to handlers and notification events
  to a message broker.
- :class:`EventMap` — registry of event type -> handler types; use :meth:`EventMap.bind`.
"""

from cqrs.events.event import (
    DCEvent,
    DCDomainEvent,
    DCNotificationEvent,
    DomainEvent,
    Event,
    IDomainEvent,
    IEvent,
    INotificationEvent,
    NotificationEvent,
    PydanticDomainEvent,
    PydanticEvent,
    PydanticNotificationEvent,
)
from cqrs.events.event_emitter import EventEmitter
from cqrs.events.event_handler import EventHandler
from cqrs.events.map import EventMap

__all__ = (
    "Event",
    "IEvent",
    "DCEvent",
    "PydanticEvent",
    "DomainEvent",
    "IDomainEvent",
    "DCDomainEvent",
    "PydanticDomainEvent",
    "NotificationEvent",
    "INotificationEvent",
    "DCNotificationEvent",
    "PydanticNotificationEvent",
    "EventEmitter",
    "EventHandler",
    "EventMap",
)

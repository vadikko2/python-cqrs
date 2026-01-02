import typing

from cqrs.events.event import NotificationEvent


class OutboxedEventMap:
    _registry: typing.Dict[typing.Text, typing.Type[NotificationEvent]] = {}

    @classmethod
    def register(
        cls,
        event_name: typing.Text,
        event_type: typing.Type[NotificationEvent],
    ) -> None:
        if event_name in cls._registry:
            raise KeyError(f"Event with {event_name} already registered")
        cls._registry[event_name] = event_type

    @classmethod
    def get(
        cls,
        event_name: typing.Text,
    ) -> typing.Type[NotificationEvent] | None:
        return cls._registry.get(event_name)

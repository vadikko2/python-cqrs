import typing

from cqrs.events import event


class OutboxedEventMap:
    _registry: typing.Dict[typing.Text, typing.Type[event.NotificationEvent]] = {}

    @classmethod
    def register(
        cls,
        event_name: typing.Text,
        event_type: typing.Type[event.NotificationEvent],
    ) -> None:
        if event_name in cls._registry:
            raise KeyError(f"Event with {event_name} already registered")
        cls._registry[event_name] = event_type

    @classmethod
    def get(
        cls,
        event_name: typing.Text,
    ) -> typing.Type[event.NotificationEvent] | None:
        return cls._registry.get(event_name)

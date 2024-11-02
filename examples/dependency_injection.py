import abc
import asyncio
import typing

import di
from di import dependent

import cqrs
from cqrs.requests import bootstrap

STACK = []


class AbstractDependency(abc.ABC):
    @abc.abstractmethod
    async def do_something(self):
        raise NotImplementedError


class ConcreteDependency(AbstractDependency):
    async def do_something(self):
        print("Do something")
        STACK.append(1)


class Command(cqrs.Request):
    pass


class CommandHandler(cqrs.RequestHandler[Command, None]):
    def __init__(self, dependency: AbstractDependency) -> None:
        self.dependency = dependency

    @property
    def events(self) -> typing.List[cqrs.Event]:
        return []

    async def handle(self, request: Command) -> None:
        await self.dependency.do_something()


def setup_di() -> di.Container:
    """
    Initialize DI container
    """
    container = di.Container()
    bind = di.bind_by_type(
        dependent.Dependent(ConcreteDependency, scope="request"),
        AbstractDependency,
    )
    container.bind(bind)
    return container


def commands_mapper(mapper: cqrs.RequestMap) -> None:
    """Maps queries to handlers."""
    mapper.bind(Command, CommandHandler)


async def main():
    mediator = bootstrap.bootstrap(
        di_container=setup_di(),
        commands_mapper=commands_mapper,
    )

    result = await mediator.send(Command())

    assert result is None
    assert len(STACK) == 1


if __name__ == "__main__":
    asyncio.run(main())

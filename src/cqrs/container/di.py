import typing

import di
from di import dependent, executors

from cqrs import container as cqrs_container

T = typing.TypeVar("T")


class DIContainer(cqrs_container.Container[di.Container]):
    def __init__(self, external_container: di.Container) -> None:
        self._external_container = external_container

    @property
    def external_container(self) -> di.Container:
        return self._external_container

    def attach_external_container(self, container: di.Container) -> None:
        self._external_container = container

    async def resolve(self, type_: typing.Type[T]) -> T:
        executor = executors.AsyncExecutor()
        solved = self._external_container.solve(
            dependent.Dependent(type_, scope="request"),
            scopes=["request"],
        )
        with self._external_container.enter_scope("request") as state:
            return await solved.execute_async(executor=executor, state=state)

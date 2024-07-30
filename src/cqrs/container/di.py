import typing

import di
from di import dependent, executors

from cqrs import container

T = typing.TypeVar("T")


class DIContainer(container.Container[di.Container]):
    def __init__(self, external_container: di.Container | None = None) -> None:
        self._external_container = external_container

    async def resolve(self, type_: typing.Type[T]) -> T:
        executor = executors.AsyncExecutor()
        solved = self._external_container.solve(dependent.Dependent(type_, scope="request"), scopes=["request"])
        with self._external_container.enter_scope("request") as state:
            return await solved.execute_async(executor=executor, state=state)

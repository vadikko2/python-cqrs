"""Benchmarks for request handling performance (Pydantic Request/Response)."""

import typing
from collections import defaultdict

import cqrs
import di
import pytest
from cqrs.requests import bootstrap

STORAGE = defaultdict[str, typing.List[str]](lambda: [])


class JoinMeetingCommand(cqrs.Request):
    user_id: str
    meeting_id: str


class ReadMeetingQuery(cqrs.Request):
    meeting_id: str


class ReadMeetingQueryResult(cqrs.Response):
    users: list[str]


class JoinMeetingCommandHandler(cqrs.RequestHandler[JoinMeetingCommand, None]):
    @property
    def events(self):
        """
        List events produced by this request handler.
        
        Returns:
            list: Event types or instances associated with the handler; an empty list if the handler does not produce events.
        """
        return []

    async def handle(self, request: JoinMeetingCommand) -> None:
        """
        Add the command's user to the participant list for the specified meeting in shared STORAGE.
        
        Parameters:
            request (JoinMeetingCommand): Command containing `user_id` to add and `meeting_id` which identifies the meeting.
        """
        STORAGE[request.meeting_id].append(request.user_id)


class ReadMeetingQueryHandler(
    cqrs.RequestHandler[ReadMeetingQuery, ReadMeetingQueryResult],
):
    @property
    def events(self):
        """
        List events produced by this request handler.
        
        Returns:
            list: Event types or instances associated with the handler; an empty list if the handler does not produce events.
        """
        return []

    async def handle(self, request: ReadMeetingQuery) -> ReadMeetingQueryResult:
        """
        Retrieve the users registered for the requested meeting.
        
        Parameters:
            request (ReadMeetingQuery): Query containing the `meeting_id` to read.
        
        Returns:
            result (ReadMeetingQueryResult): Result object whose `users` field is the list of user IDs for the specified meeting.
        """
        return ReadMeetingQueryResult(users=STORAGE[request.meeting_id])


def command_mapper(mapper: cqrs.RequestMap) -> None:
    """
    Register command-to-handler mappings on the provided request mapper.
    
    Binds `JoinMeetingCommand` to `JoinMeetingCommandHandler` so the mediator can resolve and dispatch that command.
    
    Parameters:
        mapper (cqrs.RequestMap): Mapper used to register request-to-handler bindings.
    """
    mapper.bind(JoinMeetingCommand, JoinMeetingCommandHandler)


def query_mapper(mapper: cqrs.RequestMap) -> None:
    """
    Register query request mappings on the provided request map.
    
    Binds ReadMeetingQuery to ReadMeetingQueryHandler so the mediator can resolve and dispatch that query type.
    
    Parameters:
        mapper (cqrs.RequestMap): The request map used by the mediator to register query-to-handler bindings.
    """
    mapper.bind(ReadMeetingQuery, ReadMeetingQueryHandler)


@pytest.fixture
def mediator():
    """
    Create a mediator configured with a dependency-injection container and the command/query mappers.
    
    Returns:
        mediator: A mediator instance wired with the DI container and the `query_mapper` and `command_mapper`.
    """
    return bootstrap.bootstrap(
        di_container=di.Container(),
        queries_mapper=query_mapper,
        commands_mapper=command_mapper,
    )


@pytest.mark.benchmark
def test_benchmark_command_handling(benchmark, mediator):
    """
    Measure latency of handling a JoinMeetingCommand by sending it through the mediator.
    
    Creates a JoinMeetingCommand for user "user_1" and meeting "meeting_1" and benchmarks invoking the mediator with that command.
    """
    command = JoinMeetingCommand(user_id="user_1", meeting_id="meeting_1")

    async def run():
        await mediator.send(command)

    benchmark(lambda: run())


@pytest.mark.benchmark
def test_benchmark_query_handling(benchmark, mediator):
    """Benchmark query handling performance."""
    STORAGE["meeting_1"] = ["user_1", "user_2", "user_3"]
    query = ReadMeetingQuery(meeting_id="meeting_1")

    async def run():
        """
        Send the prepared ReadMeetingQuery to the mediator and return the query result.
        
        Returns:
            ReadMeetingQueryResult: the response containing the list of users for the meeting.
        """
        return await mediator.send(query)

    benchmark(lambda: run())


@pytest.mark.benchmark
def test_benchmark_multiple_commands(benchmark, mediator):
    """
    Measure the performance of sending ten JoinMeetingCommand instances sequentially through the mediator.
    """
    commands = [JoinMeetingCommand(user_id=f"user_{i}", meeting_id="meeting_2") for i in range(10)]

    async def run():
        """
        Sequentially sends each command from `commands` to `mediator`, awaiting each send before proceeding to the next.
        
        This ensures each command's handler runs to completion in order; any side effects produced by those handlers (for example, mutations to in-memory storage) will be applied before the next command is sent.
        """
        for cmd in commands:
            await mediator.send(cmd)

    benchmark(lambda: run())
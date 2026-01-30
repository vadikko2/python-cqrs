"""Benchmarks for Chain of Responsibility (dataclass DCRequest/DCResponse)."""

import dataclasses
import typing

import cqrs
import di
import pytest
from cqrs.requests import bootstrap
from cqrs.requests.cor_request_handler import CORRequestHandler


@dataclasses.dataclass
class TRequest(cqrs.DCRequest):
    method: str
    user_id: str


@dataclasses.dataclass
class TResult(cqrs.DCResponse):
    success: bool
    handler_name: str
    message: str = ""


class HandlerA(CORRequestHandler[TRequest, TResult | None]):
    @property
    def events(self) -> typing.Sequence[cqrs.IEvent]:
        """
        Provide the sequence of CQRS events produced by this handler.
        
        Returns:
            typing.Sequence[cqrs.IEvent]: A sequence of `cqrs.IEvent` instances; empty for this handler.
        """
        return []

    async def handle(self, request: TRequest) -> TResult | None:
        """
        Process requests with method "method_a" or forward the request to the next handler.
        
        Returns:
            A `TResult` indicating successful handling by HandlerA when `request.method == "method_a"`; otherwise the result returned by the next handler (possibly `None`).
        """
        if request.method == "method_a":
            return TResult(
                success=True,
                handler_name="HandlerA",
                message=f"Processed method_a for {request.user_id}",
            )
        return await self.next(request)


class HandlerB(CORRequestHandler[TRequest, TResult | None]):
    @property
    def events(self) -> typing.Sequence[cqrs.IEvent]:
        """
        Provide the sequence of CQRS events produced by this handler.
        
        Returns:
            typing.Sequence[cqrs.IEvent]: A sequence of `cqrs.IEvent` instances; empty for this handler.
        """
        return []

    async def handle(self, request: TRequest) -> TResult | None:
        """
        Handle a TRequest by processing requests whose method is "method_b" and forwarding others.
        
        Parameters:
            request (TRequest): The incoming request; processed when `request.method == "method_b"`.
        
        Returns:
            TResult: A successful TResult with `handler_name` set to "HandlerB" when `request.method` is "method_b".
            Otherwise returns the result returned by the next handler in the chain, which may be `None`.
        """
        if request.method == "method_b":
            return TResult(
                success=True,
                handler_name="HandlerB",
                message=f"Processed method_b for {request.user_id}",
            )
        return await self.next(request)


class HandlerC(CORRequestHandler[TRequest, TResult | None]):
    @property
    def events(self) -> typing.Sequence[cqrs.IEvent]:
        """
        Provide the sequence of CQRS events produced by this handler.
        
        Returns:
            typing.Sequence[cqrs.IEvent]: A sequence of `cqrs.IEvent` instances; empty for this handler.
        """
        return []

    async def handle(self, request: TRequest) -> TResult | None:
        """
        Process the given request if its method is "method_c"; otherwise forward it.
        
        Parameters:
            request (TRequest): The request containing `method` and `user_id`.
        
        Returns:
            TResult: A response with `success=True`, `handler_name='HandlerC'`, and a message when `request.method == "method_c"`; otherwise the result produced by subsequent handlers (possibly `None`).
        """
        if request.method == "method_c":
            return TResult(
                success=True,
                handler_name="HandlerC",
                message=f"Processed method_c for {request.user_id}",
            )
        return await self.next(request)


class DefaultHandler(CORRequestHandler[TRequest, TResult | None]):
    @property
    def events(self) -> typing.Sequence[cqrs.IEvent]:
        """
        Provide the sequence of CQRS events produced by this handler.
        
        Returns:
            typing.Sequence[cqrs.IEvent]: A sequence of `cqrs.IEvent` instances; empty for this handler.
        """
        return []

    async def handle(self, request: TRequest) -> TResult | None:
        """
        Produce a failure TResult for requests with unsupported methods.
        
        Returns:
            TResult: success is False, handler_name is "DefaultHandler", and message describes the unsupported method.
        """
        return TResult(
            success=False,
            handler_name="DefaultHandler",
            message=f"Unsupported method: {request.method}",
        )


def cor_mapper(mapper: cqrs.RequestMap) -> None:
    """
    Register the Chain of Responsibility for TRequest on the provided request map.
    
    Parameters:
        mapper (cqrs.RequestMap): Request mapping registry used to bind TRequest to the handler sequence [HandlerA, HandlerB, HandlerC, DefaultHandler].
    """
    mapper.bind(
        TRequest,
        [HandlerA, HandlerB, HandlerC, DefaultHandler],
    )


@pytest.fixture
def cor_mediator():
    """
    Create and return a mediator configured for the Chain of Responsibility benchmark tests.
    
    Returns:
        mediator: A mediator instance configured with a fresh dependency-injection container and `cor_mapper` as the commands mapper.
    """
    return bootstrap.bootstrap(
        di_container=di.Container(),
        commands_mapper=cor_mapper,
    )


@pytest.mark.benchmark
def test_benchmark_cor_first_handler(cor_mediator, benchmark):
    """Benchmark CoR when first handler in chain handles the request."""

    async def run():
        """
        Send a TRequest with method "method_a" and user_id "user_1" to the configured mediator and return the resulting response.
        
        Returns:
            TResult | None: The response produced by the chain of responsibility indicating which handler handled the request and related message, or `None` if no handler produced a response.
        """
        return await cor_mediator.send(TRequest(method="method_a", user_id="user_1"))

    benchmark(lambda: run())


@pytest.mark.benchmark
def test_benchmark_cor_second_handler(cor_mediator, benchmark):
    """Benchmark CoR when second handler in chain handles the request."""

    async def run():
        """
        Send a benchmark request with method "method_b" to the configured mediator.
        
        Returns:
            TResult | None: A `TResult` instance if a handler processed the request, `None` otherwise.
        """
        return await cor_mediator.send(TRequest(method="method_b", user_id="user_1"))

    benchmark(lambda: run())


@pytest.mark.benchmark
def test_benchmark_cor_third_handler(cor_mediator, benchmark):
    """
    Benchmark that measures handling of a request by the third handler in the Chain of Responsibility.
    
    Sends a TRequest with method "method_c" to the mediator and measures the time to receive the TResult produced by HandlerC.
    """

    async def run():
        """
        Send a TRequest with method "method_c" to the configured mediator and return its response.
        
        Returns:
            TResult | None: A TResult indicating which handler processed the request, or `None` if no handler produced a response.
        """
        return await cor_mediator.send(TRequest(method="method_c", user_id="user_1"))

    benchmark(lambda: run())


@pytest.mark.benchmark
def test_benchmark_cor_default_handler(cor_mediator, benchmark):
    """Benchmark CoR when only default (last) handler handles the request."""

    async def run():
        """
        Send a default TRequest with method "other" to the configured mediator and return its response.
        
        Returns:
            TResult | None: The response produced by handling the request; typically a failure TResult for an unsupported method.
        """
        return await cor_mediator.send(TRequest(method="other", user_id="user_1"))

    benchmark(lambda: run())


@pytest.mark.benchmark
def test_benchmark_cor_ten_requests_first_handler(cor_mediator, benchmark):
    """
    Benchmark processing of ten requests that are handled by the first chain handler.
    
    Sends 10 TRequest instances with method "method_a" and user_id values "user_0" through "user_9" to the provided mediator and measures execution time.
    """

    async def run():
        """
        Send ten TRequest messages with method "method_a" to the cor_mediator.
        
        Each request has user_id set to "user_0" through "user_9" and is sent sequentially.
        """
        for i in range(10):
            await cor_mediator.send(TRequest(method="method_a", user_id=f"user_{i}"))

    benchmark(lambda: run())
"""Benchmarks for Chain of Responsibility (Pydantic Request/Response)."""

import typing

import cqrs
import di
import pytest
from cqrs.requests import bootstrap
from cqrs.requests.cor_request_handler import CORRequestHandler


class TRequest(cqrs.Request):
    method: str
    user_id: str


class TResult(cqrs.Response):
    success: bool
    handler_name: str
    message: str = ""


class HandlerA(CORRequestHandler[TRequest, TResult | None]):
    @property
    def events(self) -> typing.Sequence[cqrs.IEvent]:
        """
        Provide the sequence of domain events produced by this handler.
        
        Returns:
            Sequence[cqrs.IEvent]: Events emitted by the handler; an empty sequence if the handler produces no events.
        """
        return []

    async def handle(self, request: TRequest) -> TResult | None:
        """
        Handle requests with method "method_a" by returning a successful TResult identifying this handler; otherwise return whatever result is produced for the request (possibly None).
        
        Parameters:
        	request (TRequest): Request whose `method` is checked for "method_a"; its `user_id` is included in the success message.
        
        Returns:
        	TResult | None: A successful TResult with `handler_name` set to "HandlerA" and a message that includes `request.user_id` if handled; otherwise the result produced for the request (may be `None`).
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
        Provide the sequence of domain events produced by this handler.
        
        Returns:
            Sequence[cqrs.IEvent]: Events emitted by the handler; an empty sequence if the handler produces no events.
        """
        return []

    async def handle(self, request: TRequest) -> TResult | None:
        """
        Handle a TRequest by processing requests with method "method_b" or delegating to the next handler.
        
        If the request's `method` equals "method_b", returns a TResult indicating successful handling by HandlerB and a message including the request's `user_id`. Otherwise, forwards the request to the next handler in the chain.
        
        Parameters:
            request (TRequest): The request to process; inspected for its `method` and `user_id`.
        
        Returns:
            TResult if the request was handled by HandlerB, or the result returned by the next handler (which may be `None`).
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
        Provide the sequence of domain events produced by this handler.
        
        Returns:
            Sequence[cqrs.IEvent]: Events emitted by the handler; an empty sequence if the handler produces no events.
        """
        return []

    async def handle(self, request: TRequest) -> TResult | None:
        """
        Handle a TRequest by processing requests with method "method_c" or forwarding handling otherwise.
        
        Parameters:
            request (TRequest): The incoming request containing `method` and `user_id`.
        
        Returns:
            TResult: If `request.method` is "method_c", a TResult with `success=True`, `handler_name="HandlerC"`, and a message including `request.user_id`; otherwise the result returned by the next handler in the chain (which may be `None` or a TResult).
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
        Provide the sequence of domain events produced by this handler.
        
        Returns:
            Sequence[cqrs.IEvent]: Events emitted by the handler; an empty sequence if the handler produces no events.
        """
        return []

    async def handle(self, request: TRequest) -> TResult | None:
        """
        Return a failure TResult indicating the request's method is unsupported.
        
        Parameters:
            request (TRequest): The incoming request whose `method` value is reported in the returned message.
        
        Returns:
            TResult: A result with `success` set to `False`, `handler_name` set to `"DefaultHandler"`, and `message` set to `"Unsupported method: {request.method}"`.
        """
        return TResult(
            success=False,
            handler_name="DefaultHandler",
            message=f"Unsupported method: {request.method}",
        )


def cor_mapper(mapper: cqrs.RequestMap) -> None:
    """
    Bind the TRequest type to the Chain of Responsibility handler chain in the provided request mapper.
    
    Registers the handler sequence [HandlerA, HandlerB, HandlerC, DefaultHandler] for TRequest so the mapper will resolve and execute that chain when TRequest is dispatched.
    
    Parameters:
        mapper (cqrs.RequestMap): The request mapping registry where the TRequest → handlers binding will be added.
    """
    mapper.bind(
        TRequest,
        [HandlerA, HandlerB, HandlerC, DefaultHandler],
    )


@pytest.fixture
def cor_mediator():
    """
    Create and return a mediator configured with the Chain of Responsibility handler mappings used in the benchmarks.
    
    Returns:
        mediator: A bootstrap-configured mediator instance with a DI container and the `cor_mapper` request mappings applied.
    """
    return bootstrap.bootstrap(
        di_container=di.Container(),
        commands_mapper=cor_mapper,
    )


@pytest.mark.benchmark
def test_benchmark_cor_first_handler(cor_mediator, benchmark):
    """
    Benchmark the Chain of Responsibility where the first handler processes the request.
    
    Sends a TRequest with method "method_a" and user_id "user_1" to the configured mediator and measures the send operation.
    
    Parameters:
        cor_mediator: Mediator fixture configured with the CoR handler chain.
        benchmark: pytest-benchmark fixture used to time the operation.
    """

    async def run():
        """
        Send a TRequest with method "method_a" and user_id "user_1" through the configured cor_mediator and return the chain's response.
        
        Returns:
            TResult | None: The response produced by the Chain of Responsibility for the sent request, or `None` if no handler produced a result.
        """
        return await cor_mediator.send(TRequest(method="method_a", user_id="user_1"))

    benchmark(lambda: run())


@pytest.mark.benchmark
def test_benchmark_cor_second_handler(cor_mediator, benchmark):
    """Benchmark CoR when second handler in chain handles the request."""

    async def run():
        """
        Send a TRequest with method "method_b" through the configured mediator.
        
        @returns
        TResult if a handler produced a response, `None` otherwise.
        """
        return await cor_mediator.send(TRequest(method="method_b", user_id="user_1"))

    benchmark(lambda: run())


@pytest.mark.benchmark
def test_benchmark_cor_third_handler(cor_mediator, benchmark):
    """Benchmark CoR when third handler in chain handles the request."""

    async def run():
        """
        Send a TRequest with method "method_c" and user_id "user_1" through the configured mediator and return the handler response.
        
        Returns:
            TResult | None: The response produced by the handler chain for the request; a TResult when a handler handles it, otherwise `None`.
        """
        return await cor_mediator.send(TRequest(method="method_c", user_id="user_1"))

    benchmark(lambda: run())


@pytest.mark.benchmark
def test_benchmark_cor_default_handler(cor_mediator, benchmark):
    """Benchmark CoR when only default (last) handler handles the request."""

    async def run():
        """
        Send a TRequest with method "other" through the configured COR mediator and return the handler chain's result.
        
        Returns:
            TResult | None: The response produced by the handler chain when a handler handles the request, or `None` if no handler returned a response.
        """
        return await cor_mediator.send(TRequest(method="other", user_id="user_1"))

    benchmark(lambda: run())


@pytest.mark.benchmark
def test_benchmark_cor_ten_requests_first_handler(cor_mediator, benchmark):
    """
    Benchmark Chain of Responsibility handling ten sequential requests routed to the first handler.
    
    Sends ten TRequest instances with method "method_a" and user_id values "user_0" through "user_9" to the configured mediator for performance measurement.
    """

    async def run():
        """
        Send ten sequential TRequest messages with method "method_a" and user_ids "user_0" through "user_9" to the configured CoR mediator.
        """
        for i in range(10):
            await cor_mediator.send(TRequest(method="method_a", user_id=f"user_{i}"))

    benchmark(lambda: run())
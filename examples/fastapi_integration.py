import logging
import typing

import di
import fastapi
import uvicorn

import cqrs
from cqrs.requests import bootstrap

logging.basicConfig(level=logging.DEBUG)

api_router = fastapi.APIRouter(prefix="/api")


class Hello(cqrs.Request):
    msg: str


class World(cqrs.Response):
    msg: str


class HelloWorldQueryHandler(cqrs.RequestHandler[Hello, World]):
    @property
    def events(self) -> typing.List[cqrs.Event]:
        return []

    async def handle(self, request: Hello) -> World:
        return World(msg="Hello CQRS!")


def queries_mapper(mapper: cqrs.RequestMap) -> None:
    """Maps queries to handlers."""
    mapper.bind(Hello, HelloWorldQueryHandler)


def mediator_factory() -> cqrs.RequestMediator:
    return bootstrap.bootstrap(
        di_container=di.Container(),
        queries_mapper=queries_mapper,
    )


@api_router.get("/hello", status_code=fastapi.status.HTTP_200_OK)
async def hello_world(
    msg: typing.Text = fastapi.Query(default="Hello World"),
    mediator: cqrs.RequestMediator = fastapi.Depends(mediator_factory),
) -> World:
    return await mediator.send(Hello(msg=msg))


if __name__ == "__main__":
    app = fastapi.FastAPI()
    app.include_router(api_router)

    uvicorn.run(app, host="0.0.0.0", port=8000)

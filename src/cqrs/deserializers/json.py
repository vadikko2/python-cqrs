import logging
import typing

import pydantic

_T = typing.TypeVar("_T", bound=pydantic.BaseModel)

logger = logging.getLogger("cqrs")


class JsonDeserializer(typing.Generic[_T]):
    def __init__(self, model: typing.Type[_T]):
        self._model: typing.Type[_T] = model

    def deserialize(self, data: typing.AnyStr | None) -> _T | None:
        if data is None:
            return

        try:
            return self._model.model_validate_json(data)
        except pydantic.ValidationError as error:
            logger.error(
                f"Error while deserializing json message: {error}",
            )
            return

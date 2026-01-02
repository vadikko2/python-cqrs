import logging
import typing

import pydantic

from cqrs.deserializers.exceptions import DeserializeJsonError

_T = typing.TypeVar("_T", bound=pydantic.BaseModel)

logger = logging.getLogger("cqrs")


class JsonDeserializer(typing.Generic[_T]):
    def __init__(self, model: typing.Type[_T]):
        self._model: typing.Type[_T] = model

    def __call__(self, data: str | bytes | None) -> _T | None | DeserializeJsonError:
        if data is None:
            return

        try:
            return self._model.model_validate_json(data)
        except Exception as e:
            logger.error(
                f"Error while deserializing json message: {e}",
            )
            return DeserializeJsonError(
                error_message=str(e),
                error_type=type(e),
                message_data=data,
            )

import typing

import orjson
import pydantic


def default_serializer(message: pydantic.BaseModel) -> typing.ByteString:
    return orjson.dumps(message.model_dump(mode="json"))

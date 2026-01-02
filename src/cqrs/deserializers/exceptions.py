import typing

import pydantic


class DeserializeJsonError(pydantic.BaseModel):
    error_message: str
    error_type: typing.Type[Exception]
    message_data: str | bytes | None


class DeserializeProtobufError(pydantic.BaseModel):
    error_message: str
    error_type: typing.Type[Exception]
    message_data: bytes

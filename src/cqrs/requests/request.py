import pydantic


class Request(pydantic.BaseModel):
    """
    Base class for request-type objects.

    The request is an input of the request handler.
    Often Request is used for defining queries or commands.
    """

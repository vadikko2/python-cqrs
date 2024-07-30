import pydantic


class Response(pydantic.BaseModel):
    """
    Base class for response type objects.

    The response is a result of the request handling, which hold by RequestHandler.

    Often the response is used for defining the result of the query.

    """

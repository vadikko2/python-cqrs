"""
Type definitions for CQRS framework.

This module contains common type variables used throughout the framework.
It is placed at the bottom of the dependency hierarchy to avoid circular imports.
"""

import typing

from cqrs.requests.request import IRequest
from cqrs.response import IResponse

# Type variable for request types (contravariant - can accept subtypes)
ReqT = typing.TypeVar("ReqT", bound=IRequest, contravariant=True)

# Type variable for response types (covariant - can return subtypes)
# Can be IResponse or None
ResT = typing.TypeVar("ResT", bound=IResponse | None, covariant=True)

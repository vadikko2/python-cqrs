"""
Type definitions for CQRS framework.

This module re-exports common type variables (ReqT, ResT) and interfaces
from cqrs.requests.request and cqrs.response for backward compatibility.
Defining ReqT/ResT in request.py avoids circular import with request_handler.
"""

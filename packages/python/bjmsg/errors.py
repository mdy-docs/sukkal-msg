"""What a caller can catch.

The broker answers a success with binjson and a failure with text/plain,
so an error here always carries the broker's own sentence about what was
wrong rather than a status code alone.
"""
from __future__ import annotations

from typing import Optional

__all__ = ["BjmsgError", "BrokerUnreachable"]


class BjmsgError(Exception):
    def __init__(
        self,
        message: str,
        *,
        status: Optional[int] = None,
        method: Optional[str] = None,
        url: Optional[str] = None,
    ):
        super().__init__(message)
        self.status = status
        self.method = method
        self.url = url


class BrokerUnreachable(BjmsgError):
    """The broker could not be reached at all — nothing was sent."""

    def __init__(self, origin: str, cause: BaseException):
        super().__init__(f"cannot reach the broker at {origin}: {cause}")
        self.__cause__ = cause

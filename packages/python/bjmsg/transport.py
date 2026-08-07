"""Talking to a broker over one kept-alive connection.

http.client rather than requests, for one concrete reason: requests
percent-encodes the request path, and the broker matches it raw. A
wildcard subscription to `eu.>` would arrive as `eu.%3E` and be rejected
as a bad pattern. HTTPConnection sends exactly the path it is given, and
is a persistent connection by construction — which is the other thing
wanted here, since every request from this client to a given broker
should reuse one socket.
"""
from __future__ import annotations

import http.client
import socket
import threading
import time
from typing import Any, Mapping, Optional, Tuple
from urllib.parse import urlsplit, urlencode

from .binjson import decode
from .errors import BjmsgError, BrokerUnreachable

MEDIA_TYPE = "application/binjson"

#: Failures that prove the request never reached the broker, so repeating
#: one cannot repeat an effect.
_NEVER_ARRIVED = (ConnectionRefusedError, socket.gaierror)


class Transport:
    """One broker, one connection, guarded by a lock.

    Deliveries arrive on their own threads and a handler may well publish
    from one, so requests are serialised rather than each opening a
    socket — the same discipline the C client keeps.
    """

    def __init__(
        self,
        url: str,
        *,
        timeout: float = 30.0,
        retry_seconds: float = 5.0,
    ):
        parts = urlsplit(url)
        if parts.scheme not in ("http", "https"):
            raise BjmsgError(f"broker URL must be http:// or https://, got '{url}'")
        self.scheme = parts.scheme
        self.host = parts.hostname or "127.0.0.1"
        self.port = parts.port or (443 if parts.scheme == "https" else 80)
        self.prefix = parts.path.rstrip("/")
        self.origin = f"{parts.scheme}://{parts.netloc}"
        self.timeout = timeout
        self.retry_seconds = retry_seconds

        self._lock = threading.Lock()
        self._conn: Optional[http.client.HTTPConnection] = None
        self._closed = False

    # -- the local address a callback has to name --------------------------

    def local_address(self) -> str:
        """
        Which of this host's addresses reaches the broker.

        A UDP socket is connected to the broker's address and asked what
        local address it was given. Nothing is sent — connect() on a
        datagram socket only fixes the route — so this costs no traffic
        and, unlike picking from the interface list, gets the right
        answer on a host with several addresses.
        """
        for family, socktype, proto, _, sockaddr in socket.getaddrinfo(
            self.host, self.port, 0, socket.SOCK_DGRAM
        ):
            probe = socket.socket(family, socktype, proto)
            try:
                probe.connect(sockaddr)
                return probe.getsockname()[0]
            except OSError:
                continue
            finally:
                probe.close()
        return "127.0.0.1"

    # -- requests -----------------------------------------------------------

    def request(
        self,
        method: str,
        path: str,
        *,
        query: Optional[Mapping[str, Any]] = None,
        body: Optional[bytes] = None,
        headers: Optional[Mapping[str, str]] = None,
    ) -> Tuple[int, Mapping[str, str], Any]:
        """
        One request. Returns (status, headers, value) where `value` is the
        decoded binjson body, or None when there is none.

        A non-2xx is raised as a BjmsgError carrying the broker's
        plain-text explanation.
        """
        target = self.prefix + path
        if query:
            pairs = [(k, str(v)) for k, v in query.items() if v is not None]
            if pairs:
                target += "?" + urlencode(pairs)

        deadline_reported = False
        while True:
            try:
                return self._once(method, target, body, headers or {})
            except _NEVER_ARRIVED as exc:
                if self.retry_seconds <= 0 or self._closed:
                    raise BrokerUnreachable(self.origin, exc) from exc
                if not deadline_reported:
                    deadline_reported = True
                time.sleep(self.retry_seconds)

    def _connection(self) -> http.client.HTTPConnection:
        if self._conn is None:
            cls = (http.client.HTTPSConnection if self.scheme == "https"
                   else http.client.HTTPConnection)
            self._conn = cls(self.host, self.port, timeout=self.timeout)
        return self._conn

    def _drop(self) -> None:
        if self._conn is not None:
            try:
                self._conn.close()
            except OSError:
                pass
            self._conn = None

    def _once(self, method, target, body, extra_headers):
        headers = dict(extra_headers)
        if body is not None:
            headers["Content-Type"] = MEDIA_TYPE
            headers["Content-Length"] = str(len(body))
        elif method != "GET":
            headers["Content-Length"] = "0"

        with self._lock:
            # A kept-alive connection can be closed by the far end between
            # requests, which surfaces on the next send. That is not a
            # failure to report, it is a connection to remake — but only
            # once, or a genuinely broken broker would loop here.
            for attempt in (0, 1):
                try:
                    conn = self._connection()
                    conn.request(method, target, body=body, headers=headers)
                    response = conn.getresponse()
                    raw = response.read()
                    break
                except _NEVER_ARRIVED:
                    self._drop()
                    raise
                except (http.client.HTTPException, OSError):
                    self._drop()
                    if attempt:
                        raise

        if not (200 <= response.status < 300):
            raise BjmsgError(
                raw.decode("utf-8", "replace").strip() or f"HTTP {response.status}",
                status=response.status,
                method=method,
                url=self.origin + target,
            )

        value = None
        content_type = response.getheader("Content-Type") or ""
        if raw and content_type.startswith(MEDIA_TYPE):
            value = decode(raw)
        return response.status, dict(response.getheaders()), value

    def close(self) -> None:
        self._closed = True
        with self._lock:
            self._drop()

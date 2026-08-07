"""The Flask side: where the broker POSTs.

bjmsg pushes. A subscription names a callback URL, the broker POSTs each
batch to it, and the HTTP response is the acknowledgement — so a
subscriber is a server, and this is that server.

One Flask app serves every subscription a client has, each on its own
path under `mount_path`. Routes are not added and removed as
subscriptions come and go; a single parameterised route dispatches from a
table, which also makes an unknown consumer a clean 404.
"""
from __future__ import annotations

import threading
from socketserver import ThreadingMixIn
from typing import Callable, Dict, Optional, Tuple
from wsgiref.simple_server import WSGIRequestHandler, WSGIServer, make_server

from flask import Flask, Response, request

from .protocol import delivery_info

DEFAULT_BODY_LIMIT = 8 * 1024 * 1024


class _Handler(WSGIRequestHandler):
    """
    HTTP/1.1, so the connection survives a response.

    wsgiref speaks HTTP/1.0 by default and closes after every exchange,
    which would make the broker reconnect for each delivery — losing the
    one kept-alive connection per subscriber that the whole design is
    arranged around. Flask sets Content-Length on every response, which
    is what HTTP/1.1 needs to keep the framing unambiguous.
    """

    protocol_version = "HTTP/1.1"
    #: wsgiref writes the status line, the headers and the body in
    #: separate socket writes. With Nagle on, the kernel holds the later
    #: ones waiting for an ACK — which is milliseconds added to every
    #: acknowledgement, on a path whose whole point is not waiting.
    disable_nagle_algorithm = True

    def log_message(self, fmt, *args):
        """A delivery is not news; the library logs what matters."""


class _ThreadingWSGIServer(ThreadingMixIn, WSGIServer):
    """One thread per connection, so one subscription's handler cannot
    hold up another's delivery. Within a subscription the broker sends
    one batch at a time regardless, so message order is not at risk."""

    daemon_threads = True
    #: The broker parks a connection here between deliveries, which on a
    #: quiet subject may be a long time; nothing should reap it.
    allow_reuse_address = True


class Receiver:
    """
    The HTTP server deliveries arrive on.

    Pass `app` to mount into a Flask app you already have — the routes go
    on, and listening stays yours. Otherwise this starts its own server on
    a background thread.
    """

    def __init__(
        self,
        *,
        port: int = 0,
        host: Optional[str] = None,
        mount_path: str = "/bjmsg",
        app: Optional[Flask] = None,
        body_limit: int = DEFAULT_BODY_LIMIT,
    ):
        self.port = port
        self.host = host
        self.mount_path = mount_path.rstrip("/")
        self.own = app is None
        self.app = app or Flask("bjmsg.receiver")
        self.app.config.setdefault("MAX_CONTENT_LENGTH", body_limit)

        self._routes: Dict[str, Tuple[str, Callable]] = {}
        self._lock = threading.Lock()
        self._server = None
        self._thread: Optional[threading.Thread] = None

        self.app.add_url_rule(
            f"{self.mount_path}/<consumer>",
            endpoint=f"bjmsg_deliver_{id(self)}",
            view_func=self._deliver,
            methods=["POST"],
        )

    # -- lifecycle ---------------------------------------------------------

    @property
    def listening(self) -> bool:
        return self._server is not None

    def listen(self, host: Optional[str] = None) -> "Receiver":
        """Start listening. A no-op when an existing app was supplied."""
        if not self.own or self._server is not None:
            return self
        bind = host or self.host or "0.0.0.0"
        self._server = make_server(
            bind, self.port, self.app,
            server_class=_ThreadingWSGIServer, handler_class=_Handler,
        )
        self.port = self._server.server_port
        self._thread = threading.Thread(
            target=self._server.serve_forever,
            name="bjmsg-receiver",
            daemon=True,
        )
        self._thread.start()
        return self

    def close(self) -> None:
        with self._lock:
            self._routes.clear()
        if not self.own or self._server is None:
            return
        server, thread = self._server, self._thread
        self._server = self._thread = None
        server.shutdown()
        server.server_close()
        if thread is not None:
            thread.join(timeout=5)

    # -- subscriptions ------------------------------------------------------

    def callback_url(self, consumer: str, advertised_host: str) -> str:
        host = advertised_host
        if ":" in host and not host.startswith("["):
            host = f"[{host}]"          # an IPv6 literal needs brackets
        return f"http://{host}:{self.port}{self.mount_path}/{consumer}"

    def register(self, consumer: str, token: str, handle: Callable) -> None:
        with self._lock:
            self._routes[consumer] = (token, handle)

    def unregister(self, consumer: str) -> None:
        with self._lock:
            self._routes.pop(consumer, None)

    def __len__(self) -> int:
        return len(self._routes)

    # -- the route ----------------------------------------------------------

    def _deliver(self, consumer: str):
        with self._lock:
            route = self._routes.get(consumer)
        if route is None:
            return Response("no such subscription here\n", status=404,
                            mimetype="text/plain")

        token, handle = route
        # The token proves the POST came from the broker we registered
        # with. It is the only thing that does: anything that can reach
        # this port can connect to it, and a subscriber that took
        # whatever arrived would accept messages from anywhere.
        if token and request.headers.get("Authorization") != f"Bearer {token}":
            return Response("bad or missing bearer token\n", status=401,
                            mimetype="text/plain")

        body = request.get_data()
        if not body:
            return Response("empty delivery\n", status=400, mimetype="text/plain")

        info = delivery_info(dict(request.headers))
        return handle(body, info)

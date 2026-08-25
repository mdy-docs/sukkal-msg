"""The whole of sukkal from Python.

Publishing is a request. Subscribing is the reverse: the client tells the
broker a URL to POST to, and the reply to that POST is the
acknowledgement — so delivery and ack are one exchange, backpressure is
"we have not answered yet", and there is no polling anywhere.
"""
from __future__ import annotations

import logging
import secrets
import threading
from typing import Any, Callable, List, Mapping, Optional

from flask import Response

from .binjson import decode
from .errors import SukkalError
from .protocol import (
    Job, Message, encode_message, is_valid_name, is_valid_subject,
    is_valid_target, parse_delivery,
)
from .receiver import Receiver
from .transport import Transport

log = logging.getLogger("sukkal")

DEFAULT_URL = "http://127.0.0.1:8080"
DEFAULT_REPLY_SUBJECT = "_reply"
DEFAULT_REPLY_GROUP = "repliers"

#: How often a subscription is re-asserted. Not a poll: it carries no
#: cursor and asks for nothing, it only re-states where to deliver. What
#: it buys is self-healing — a broker whose store was rebuilt has no
#: record of the subscription, and silence is indistinguishable from
#: "nothing to send".
DEFAULT_HEARTBEAT_SECONDS = 30.0


class Subscription:
    """One registration, and the timer that keeps it registered.

    A subscription holds no cursor. How far it has read is its read
    receipt in the broker, which is why it survives a broker restart, is
    visible to `sukkal consumers`, and counts against retention exactly
    like any other subscription.
    """

    def __init__(self, client: "Client", *, consumer, target, group,
                 callback, token, params, ephemeral, always_unregister,
                 heartbeat_seconds):
        self._client = client
        self.consumer = consumer
        self.target = target
        self.group = group
        self.callback = callback
        self.token = token
        self.params = params
        self.ephemeral = ephemeral
        self.always_unregister = always_unregister
        self.closed = False

        self._timer: Optional[threading.Timer] = None
        self._heartbeat = heartbeat_seconds
        if heartbeat_seconds > 0:
            self._schedule()

    def _schedule(self) -> None:
        self._timer = threading.Timer(self._heartbeat, self._beat)
        self._timer.daemon = True
        self._timer.start()

    def _beat(self) -> None:
        if self.closed:
            return
        try:
            self._client._register(self)
        except Exception as exc:                       # noqa: BLE001
            log.warning("could not re-register '%s': %s", self.consumer, exc)
        if not self.closed:
            self._schedule()

    def close(self, *, keep: bool = False) -> None:
        """
        Stop deliveries.

        A named subscription keeps its receipt, so it resumes where it
        left off; a generated one purges it, because a receipt holds
        retention off everything below it and an abandoned one would pin
        the log for good. `keep` leaves it registered so the broker goes
        on queueing.
        """
        if self.closed:
            return
        self.closed = True
        if self._timer is not None:
            self._timer.cancel()
        self._client._unregister(self, keep=keep)

    def __enter__(self) -> "Subscription":
        return self

    def __exit__(self, *exc) -> None:
        self.close()


class Client:
    """
    A broker connection, and the server its deliveries arrive on.

    :param url: broker base URL
    :param retry_seconds: wait between attempts when the broker cannot be
        reached; 0 disables retrying
    :param receiver: an existing :class:`Receiver`, or keyword arguments
        to build one — ``port=``, ``app=`` to mount into a Flask app you
        already have
    :param advertise: host or IP to put in callback URLs, when it is not
        simply where we are listening: a port forward, a NAT, a proxy
    :param heartbeat_seconds: re-registration interval; 0 disables
    """

    def __init__(
        self,
        url: str = DEFAULT_URL,
        *,
        retry_seconds: float = 5.0,
        timeout: float = 30.0,
        receiver: Any = None,
        advertise: Optional[str] = None,
        heartbeat_seconds: float = DEFAULT_HEARTBEAT_SECONDS,
    ):
        self.url = url
        self.advertise = advertise
        self.heartbeat_seconds = heartbeat_seconds
        self._transport = Transport(url, timeout=timeout,
                                    retry_seconds=retry_seconds)
        if isinstance(receiver, Receiver):
            self.receiver, self._own_receiver = receiver, False
        else:
            self.receiver = Receiver(**(receiver or {}))
            self._own_receiver = True
        self._subs: List[Subscription] = []
        self._closed = False

    # ---- publishing -------------------------------------------------------

    def publish(
        self,
        subject: str,
        value: Any,
        *,
        id: Optional[str] = None,          # noqa: A002 - the wire name
        headers: Optional[Mapping[str, Any]] = None,
        ack: Optional[Mapping[str, Any]] = None,
    ) -> dict:
        """
        Publish one message.

        :param id: an idempotency key, which makes this publish safe to
            repeat: a repeat inside the broker's dedup window returns the
            original index instead of appending a second copy.
        :param headers: stored as an envelope alongside the message,
            opaque to the broker. ``reply_to`` and ``correlation`` are
            what request-reply uses; the rest is yours.
        :param ack: ``{"subject", "consumer", "index"}`` — advance a
            subscription's receipt on ANOTHER subject as part of this
            publish. That is what makes a pipeline effectively-once:
            output and input-ack happen before one response, in that
            order, so a crash between them replays the input and the
            republished output collapses onto the existing one by its id.
        """
        if not is_valid_subject(subject):
            raise SukkalError(f"invalid subject '{subject}'")
        body, enveloped = encode_message(value, headers)

        _, _, out = self._transport.request(
            "POST", f"/pub/{subject}",
            query={
                "id": id,
                "headers": 1 if enveloped else None,
                "ack_subject": (ack or {}).get("subject"),
                "ack_consumer": (ack or {}).get("consumer"),
                "ack_index": (ack or {}).get("index"),
            },
            body=body,
        )
        return out

    # ---- subscribing ------------------------------------------------------

    def subscribe(
        self,
        target: str,
        handler: Callable[[Message], Any],
        *,
        consumer: Optional[str] = None,
        tail: bool = False,
        from_index: Optional[int] = None,
        batch_bytes: Optional[int] = None,
    ) -> Subscription:
        """
        Receive messages from a subject or wildcard pattern.

        The handler is called one message at a time, in order. If it
        raises, that message and everything after it in the batch is
        refused and the broker sends them again — so a handler that fails
        is a handler whose messages are not lost.

        :param consumer: name it, and the subscription is durable: the
            broker keeps a receipt, so rejoining delivers only what was
            missed. Without one a generated name is used and the
            subscription — receipt and all — goes when it closes.
        :param tail: only messages published from now on
        """
        if not is_valid_target(target):
            raise SukkalError(f"invalid subject or pattern '{target}'")

        return self._start(
            target=target,
            consumer=consumer,
            prefix="sub",
            params={
                "start": "last" if tail else None,
                "from": from_index,
                "batch": batch_bytes,
            },
            handle=lambda body, info: self._handle_messages(body, info, handler),
        )

    def work(
        self,
        target: str,
        handler: Callable[[Job], Any],
        *,
        group: str,
        max_jobs: Optional[int] = None,
        consumer: Optional[str] = None,
    ) -> Subscription:
        """
        Take jobs from a queue group: each job goes to exactly one
        member, however many are running.

        A handler that returns finishes the job; one that raises returns
        it to the queue, due again after the group's backoff. A job whose
        worker dies is redelivered when its lease expires, so a handler
        must tolerate running twice — ``job.attempts`` above 1 says it is
        seeing one again.

        :param max_jobs: jobs per delivery. One is the default, and is
            what spreads a queue evenly across workers.
        """
        if not is_valid_target(target):
            raise SukkalError(f"invalid subject or pattern '{target}'")
        if not is_valid_name(group):
            raise SukkalError("a queue group name is required")

        return self._start(
            target=target,
            consumer=consumer,
            group=group,
            prefix="work",
            params={"group": group, "max": max_jobs},
            # A worker never keeps its registration on the way out: the
            # broker would go on leasing jobs to a callback that is not
            # there, and each would sit out its lease before anyone else
            # could have it. A subscriber left registered merely
            # accumulates a backlog; a worker left registered holds jobs
            # hostage.
            always_unregister=True,
            handle=lambda body, info: self._handle_jobs(body, info, handler),
        )

    # ---- request / reply --------------------------------------------------

    def reply(
        self,
        subject: str,
        handler: Callable[[Message], Any],
        *,
        group: str = DEFAULT_REPLY_GROUP,
    ) -> Subscription:
        """
        Serve requests: run the handler for each and publish what it
        returns to the request's reply_to, with the same correlation.
        Repliers share a queue group, so each request is answered once
        however many are running.

        Returning None replies nothing, which a fire-and-forget request
        is entitled to.
        """
        def respond(job: Job) -> None:
            answer = handler(job)
            headers = job.headers or {}
            reply_to, correlation = headers.get("reply_to"), headers.get("correlation")
            if answer is None or not reply_to or not correlation:
                return
            self.publish(
                reply_to, answer,
                headers={"correlation": correlation},
                # Derived from the correlation, so a redelivered request
                # cannot produce a second reply.
                id=f"reply.{correlation}",
            )

        return self.work(subject, respond, group=group)

    def request(
        self,
        subject: str,
        value: Any,
        *,
        timeout: float = 5.0,
        reply_to: str = DEFAULT_REPLY_SUBJECT,
    ) -> Any:
        """
        Publish a request and wait for its reply.

        Subscribes to the reply subject BEFORE publishing, so a reply that
        comes back instantly still has somewhere to land, then waits on a
        connection rather than asking repeatedly. The correlation does the
        matching, because a reply subject is shared — several requesters
        may be waiting on it at once.
        """
        if not is_valid_subject(subject):
            raise SukkalError(f"invalid subject '{subject}'")

        correlation = secrets.token_hex(12)
        answered = threading.Event()
        box: dict = {}

        def catch(msg: Message) -> None:
            if (msg.headers or {}).get("correlation") == correlation:
                box["value"] = msg.value
                answered.set()

        sub = self.subscribe(reply_to, catch, tail=True)
        try:
            self.publish(subject, value,
                         headers={"reply_to": reply_to, "correlation": correlation},
                         id=f"req.{correlation}")
            if not answered.wait(timeout):
                raise SukkalError(
                    f"no reply on '{subject}' within {timeout:g}s"
                )
            return box["value"]
        finally:
            sub.close()

    # ---- pipelines --------------------------------------------------------

    def pipe(
        self,
        from_subject: str,
        handler: Callable[[Message], Any],
        *,
        to: str,
        consumer: str,
    ) -> Subscription:
        """
        Read a subject, transform each message, publish the result to
        another — effectively-once.

        The guarantee is in the ORDER of two writes, not in the
        transport: the output is published with the input's
        acknowledgement riding along in the same broker call, and carries
        an idempotency key derived from the input's index. So a crash
        anywhere replays the input, and the rerun's output collapses onto
        the one already there. One input, one output, whatever fails.

        The key is derived from the INPUT, deliberately — a handler that
        is not perfectly deterministic must still collapse onto the
        message its first run produced.

        Returning None drops a message. It is still acknowledged, so the
        stage keeps moving.
        """
        if not is_valid_subject(to):
            raise SukkalError("pipe needs a valid `to` subject")
        if not is_valid_name(consumer):
            raise SukkalError("pipe needs a `consumer` name: it is where the "
                             "stage resumes")

        def stage(msg: Message) -> None:
            out = handler(msg)
            if out is None:
                return
            self.publish(
                to, out,
                id=f"{consumer}.{msg.subject}.{msg.index}",
                ack={"subject": msg.subject, "consumer": consumer,
                     "index": msg.index},
            )

        return self.subscribe(from_subject, stage, consumer=consumer)

    # ---- queries ----------------------------------------------------------

    def health(self) -> dict:
        return self._transport.request("GET", "/health")[2]

    def subjects(self, pattern: Optional[str] = None) -> list:
        """Every subject, or those matching a pattern."""
        return self._transport.request("GET", "/subjects",
                                       query={"pattern": pattern})[2]

    def info(self, subject: str) -> dict:
        return self._transport.request("GET", f"/info/{subject}")[2]

    def consumers(self, subject: str) -> list:
        """Every consumer of a subject, with how far behind each is."""
        return self._transport.request("GET", f"/consumers/{subject}")[2]

    def pushes(self) -> list:
        """Push subscriptions the broker is delivering to, and how each fares."""
        return self._transport.request("GET", "/push")[2]

    def queues(self, subject: str) -> list:
        return self._transport.request("GET", f"/queue/{subject}")[2]

    def dead(self, subject: str, *, from_index: int = 1,
             max_bytes: Optional[int] = None) -> list:
        """
        Messages that ran out of attempts. Empty when nothing has died:
        the channel belongs to the subject, so "nothing here" is an
        emptiness rather than an absence.

        They are stored as ``<subject>.dead``, an ordinary subject, so
        `subscribe`, `info`, a retention policy and even a queue group of
        its own all work on it directly.
        """
        return self._transport.request(
            "GET", f"/dead/{subject}",
            query={"from": from_index, "max": max_bytes},
        )[2]

    def requeue(self, subject: str, dead_index: int) -> dict:
        """Put a dead-lettered message back on the subject it came from."""
        return self._transport.request("POST", f"/requeue/{subject}",
                                       query={"index": dead_index})[2]

    # ---- administration ----------------------------------------------------

    def ack(self, subject: str, consumer: str, index: int) -> dict:
        """Move a consumer's receipt by hand. It never moves backwards."""
        return self._transport.request(
            "POST", f"/ack/{subject}",
            query={"consumer": consumer, "index": index})[2]

    def unsubscribe(self, subject: str, consumer: str) -> dict:
        """Forget a durable subscription's position entirely."""
        return self._transport.request(
            "DELETE", f"/consumers/{subject}", query={"consumer": consumer})[2]

    def set_policy(self, subject: str, *, max_age_seconds=None,
                   max_messages=None, max_bytes=None,
                   ignore_consumers: bool = False) -> dict:
        """
        Retention. Any dimension left out is unlimited, and this replaces
        the whole policy rather than patching it. Several may be set at
        once — whichever limit is reached first takes effect.

        By default retention will not discard a message a subscription
        has not read. `ignore_consumers` makes the policy authoritative,
        which is the point of retention being a bound that holds.
        """
        return self._transport.request("PUT", f"/policy/{subject}", query={
            "max_age_s": max_age_seconds,
            "max_messages": max_messages,
            "max_bytes": max_bytes,
            "ignore_consumers": 1 if ignore_consumers else None,
        })[2]

    def policy(self, subject: str) -> dict:
        return self._transport.request("GET", f"/policy/{subject}")[2]

    def clear_policy(self, subject: str) -> dict:
        return self._transport.request("DELETE", f"/policy/{subject}")[2]

    def policies(self) -> list:
        return self._transport.request("GET", "/policies")[2]

    def trim(self, subject: str, *, before=None, keep=None,
             force: bool = False) -> dict:
        """
        Discard messages below a boundary. `force` is required to go
        below a consumer's receipt, because that destroys messages it has
        not read.
        """
        return self._transport.request("POST", f"/trim/{subject}", query={
            "before": before, "keep": keep, "force": 1 if force else None,
        })[2]

    def configure_queue(self, subject: str, group: str, *, lease_ms=None,
                        max_attempts=None, backoff_ms=None,
                        max_backoff_ms=None) -> list:
        return self._transport.request("PUT", f"/queue/{subject}", query={
            "group": group,
            "lease_ms": lease_ms,
            "max_attempts": max_attempts,
            "backoff_ms": backoff_ms,
            "max_backoff_ms": max_backoff_ms,
        })[2]

    # ---- lifecycle ---------------------------------------------------------

    def close(self, *, keep: bool = False) -> None:
        """Stop every subscription and release the connection."""
        if self._closed:
            return
        self._closed = True
        for sub in list(self._subs):
            try:
                sub.close(keep=keep)
            except Exception as exc:                   # noqa: BLE001
                log.warning("closing '%s': %s", sub.consumer, exc)
        if self._own_receiver:
            self.receiver.close()
        self._transport.close()

    def __enter__(self) -> "Client":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    # ---- internals ----------------------------------------------------------

    def _start(self, *, target, consumer, prefix, params, handle,
               group=None, always_unregister=False) -> Subscription:
        if consumer is not None and not is_valid_name(consumer):
            raise SukkalError(f"invalid consumer '{consumer}'")
        ephemeral = consumer is None
        name = consumer or f"{prefix}-{secrets.token_hex(8)}"

        # Bind to the address this host reaches the broker from: both the
        # one most likely to work and a smaller thing to leave open than
        # every interface.
        advertised = self.advertise or self._transport.local_address()
        self.receiver.listen(None if self.advertise else advertised)

        token = secrets.token_hex(16)
        callback = self.receiver.callback_url(name, advertised)
        self.receiver.register(name, token, handle)

        sub = Subscription(
            self,
            consumer=name, target=target, group=group, callback=callback,
            token=token, params=params,
            ephemeral=ephemeral and not always_unregister,
            always_unregister=always_unregister,
            heartbeat_seconds=self.heartbeat_seconds,
        )
        try:
            self._register(sub)
        except Exception:
            self.receiver.unregister(name)
            sub.closed = True
            if sub._timer is not None:
                sub._timer.cancel()
            raise
        self._subs.append(sub)
        return sub

    def _register(self, sub: Subscription) -> None:
        """
        Register, or re-register. A PUT is idempotent here on purpose:
        repeating it moves the callback and leaves the receipt alone,
        which is what lets a client that restarted on a new port simply
        say so, and what makes the heartbeat safe to send at any time.
        """
        query = {"consumer": sub.consumer, "callback": sub.callback,
                 "token": sub.token}
        query.update({k: v for k, v in sub.params.items() if v is not None})
        self._transport.request("PUT", f"/push/{sub.target}", query=query)

    def _unregister(self, sub: Subscription, *, keep: bool) -> None:
        if sub in self._subs:
            self._subs.remove(sub)
        self.receiver.unregister(sub.consumer)
        if keep and not sub.always_unregister:
            return
        try:
            self._transport.request("DELETE", "/push", query={
                "consumer": sub.consumer,
                # A generated name is not coming back, so its position
                # goes with it — a receipt holds retention off everything
                # below it.
                "purge": 1 if sub.ephemeral else None,
            })
        except Exception as exc:                       # noqa: BLE001
            log.warning("could not unregister '%s': %s", sub.consumer, exc)

    def _handle_messages(self, body, info, handler):
        """
        A subscription batch. Messages are handled in order and one at a
        time, because that is the order they were published in and the
        only thing a receipt can express.

        X-Sukkal-Ack reports how far we got: the whole batch when every
        handler succeeded, less when one raised — and 0 when none did,
        which the broker reads as "not now" and retries with a backoff
        rather than immediately.
        """
        try:
            messages = parse_delivery(body, info)
        except Exception:                              # noqa: BLE001
            return Response("malformed delivery\n", status=400,
                            mimetype="text/plain")

        took = 0
        for msg in messages:
            try:
                handler(msg)
            except Exception:                          # noqa: BLE001
                log.exception("handler failed on %s #%d", msg.subject, msg.index)
                break
            took = msg.index

        return Response("", status=200, mimetype="text/plain",
                        headers={"X-Sukkal-Ack": str(took)})

    def _handle_jobs(self, body, info, handler):
        """
        A job batch. Jobs finish out of order, so a high-water mark
        cannot say which ones did: X-Sukkal-Done names them, and the
        broker returns whatever the list omits. A delivery where nothing
        succeeded is a 500, which returns all of them.
        """
        try:
            jobs = parse_delivery(body, info, as_job=True)
        except Exception:                              # noqa: BLE001
            return Response("malformed delivery\n", status=400,
                            mimetype="text/plain")

        done = []
        for job in jobs:
            try:
                handler(job)
                done.append(job.index)
            except Exception:                          # noqa: BLE001
                log.exception("handler failed on job %s #%d",
                              job.subject, job.index)

        if not done:
            return Response("handler failed\n", status=500, mimetype="text/plain")
        headers = {}
        if len(done) < len(jobs):
            headers["X-Sukkal-Done"] = ",".join(str(i) for i in done)
        return Response("", status=200, mimetype="text/plain", headers=headers)

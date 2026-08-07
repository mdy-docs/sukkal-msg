"""The shapes on the wire.

A delivery body is the entry log's own batch encoding, forwarded by the
broker without re-encoding: an ARRAY of {index, term, type, payload}
where `payload` is BINARY holding the message's encoded bytes. So a
delivery decodes in two steps, and the second one is the caller's message
exactly as it was published.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, List, Mapping, Optional

from .binjson import decode, encode

__all__ = [
    "ENTRY_PLAIN", "ENTRY_ENVELOPE", "Message", "Job", "DeliveryInfo",
    "parse_delivery", "encode_message", "delivery_info",
    "is_valid_subject", "is_valid_name", "is_valid_pattern",
    "is_pattern", "is_valid_target",
]

#: The entry log's type byte, which says what the payload is.
ENTRY_PLAIN = 0x01       # the payload is the message, as published
ENTRY_ENVELOPE = 0x10    # the payload is [ headers, message ]


@dataclass
class Message:
    """One message from a subscription."""

    index: int
    value: Any
    #: None unless the publisher sent any. Headers live behind the entry
    #: type byte rather than in a universal wrapper, so a headerless
    #: message costs nothing.
    headers: Optional[dict] = None
    #: The message's encoded bytes, for passing on untouched.
    raw: bytes = b""
    subject: str = ""
    #: How many messages are still waiting behind this batch.
    lag: int = 0
    type: int = ENTRY_PLAIN


@dataclass
class Job(Message):
    """One job leased from a queue group."""

    #: Above 1 means this job has been handed out before and its lease
    #: expired — the signal a handler needs to guard itself.
    attempts: int = 1
    expires_at: Optional[datetime] = None
    group: str = ""


@dataclass
class DeliveryInfo:
    """What the broker put in headers, so nothing has to be decoded."""

    subject: str = ""
    consumer: str = ""
    group: str = ""
    count: int = 0
    first_index: int = 0
    last_index: int = 0
    lag: int = 0


def encode_message(value: Any, headers: Optional[Mapping[str, Any]] = None):
    """Encode a message, with headers if there are any."""
    if headers is None:
        return encode(value), False
    return encode([dict(headers), value]), True


def _unwrap(entry: Mapping[str, Any]):
    value = decode(entry["payload"])
    if (entry.get("type") == ENTRY_ENVELOPE
            and isinstance(value, list) and len(value) == 2):
        return value[0], value[1]
    return None, value


def parse_delivery(body: bytes, info: "DeliveryInfo", as_job: bool = False):
    """
    Turn a delivery body into messages. Handles both shapes the broker
    sends — a subscription batch and a queue-group take — which differ
    only in carrying `term` versus `attempts` / `expires_ms`.
    """
    entries = decode(body)
    if not isinstance(entries, list):
        raise ValueError("delivery body is not a batch")

    out: List[Message] = []
    for entry in entries:
        headers, value = _unwrap(entry)
        common = dict(
            index=int(entry["index"]),
            value=value,
            headers=headers,
            raw=entry["payload"],
            subject=info.subject,
            type=int(entry.get("type", ENTRY_PLAIN)),
        )
        if as_job:
            expires = entry.get("expires_ms") or 0
            out.append(Job(
                **common,
                attempts=int(entry.get("attempts", 1)),
                expires_at=(datetime.fromtimestamp(expires / 1000, tz=timezone.utc)
                            if expires else None),
                group=info.group,
            ))
        else:
            out.append(Message(**common, lag=info.lag))
    return out


def delivery_info(headers: Mapping[str, str]) -> DeliveryInfo:
    lower = {k.lower(): v for k, v in headers.items()}

    def num(name: str) -> int:
        try:
            return int(lower.get(name, 0))
        except (TypeError, ValueError):
            return 0

    return DeliveryInfo(
        subject=lower.get("x-bjmsg-subject", ""),
        consumer=lower.get("x-bjmsg-consumer", ""),
        group=lower.get("x-bjmsg-group", ""),
        count=num("x-bjmsg-count"),
        first_index=num("x-bjmsg-first-index"),
        last_index=num("x-bjmsg-last-index"),
        lag=num("x-bjmsg-lag"),
    )


# ---- names ---------------------------------------------------------------

# Subject names are file names on the broker, so they are restricted.
_NAME = re.compile(r"^(?!\.)(?!.*\.\.)(?!.*\.$)[A-Za-z0-9_.-]{1,128}$")
_TOKEN = re.compile(r"^[A-Za-z0-9_-]+$")


def is_valid_subject(s: Any) -> bool:
    return isinstance(s, str) and bool(_NAME.match(s))


def is_valid_name(s: Any) -> bool:
    """Consumer and queue-group names follow the subject rules."""
    return is_valid_subject(s)


def is_pattern(s: Any) -> bool:
    return isinstance(s, str) and ("*" in s or ">" in s)


def is_valid_pattern(s: Any) -> bool:
    """
    Patterns match token-wise on '.': '*' is one token, '>' is this and
    everything below and may only be last. Neither character is legal in
    a subject, so a pattern can never be mistaken for one.
    """
    if not isinstance(s, str) or not 0 < len(s) <= 128:
        return False
    tokens = s.split(".")
    for i, token in enumerate(tokens):
        if token == ">":
            if i != len(tokens) - 1:
                return False
        elif token != "*" and not _TOKEN.match(token):
            return False
    return True


def is_valid_target(s: Any) -> bool:
    """Accepts either, which is what every subscribe route does."""
    return is_valid_pattern(s) if is_pattern(s) else is_valid_subject(s)

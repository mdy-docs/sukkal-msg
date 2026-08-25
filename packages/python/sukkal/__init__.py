"""sukkal — publish/subscribe over HTTP/1.1 with binjson payloads.

Messages are pushed, never polled: a subscription names a callback URL
the broker POSTs to, and the reply to that POST is the acknowledgement.
So a subscriber is an HTTP server, which is what Flask is here for.

    from sukkal import Client

    with Client("http://127.0.0.1:8080") as sukkal:
        sukkal.subscribe("orders.>", lambda m: print(m.subject, m.value))
        sukkal.publish("orders.new", {"id": 1})
"""
from .binjson import ObjectId, Pointer, decode, encode
from .client import (
    Client, Subscription, DEFAULT_HEARTBEAT_SECONDS, DEFAULT_URL,
)
from .errors import SukkalError, BrokerUnreachable
from .protocol import (
    ENTRY_ENVELOPE, ENTRY_PLAIN, DeliveryInfo, Job, Message,
    is_pattern, is_valid_name, is_valid_pattern, is_valid_subject,
    is_valid_target,
)
from .receiver import Receiver

__version__ = "0.2.0"

__all__ = [
    "Client", "Receiver", "Subscription",
    "Message", "Job", "DeliveryInfo",
    "SukkalError", "BrokerUnreachable",
    "encode", "decode", "ObjectId", "Pointer",
    "ENTRY_PLAIN", "ENTRY_ENVELOPE",
    "is_valid_subject", "is_valid_pattern", "is_valid_target",
    "is_valid_name", "is_pattern",
    "DEFAULT_URL", "DEFAULT_HEARTBEAT_SECONDS", "__version__",
]

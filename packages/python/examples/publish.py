"""A publisher. Sends a short series and exits.

    python examples/publish.py
    python examples/publish.py "any message you like"
"""
import os
import sys

from sukkal import Client

messages = sys.argv[1:] or [
    "Hello NATS!",
    "...except this one is binjson over HTTP/1.1",
    {"shape": "anything binjson encodes", "n": 3},
    "goodbye",
]

with Client(os.environ.get("SUKKAL_URL", "http://127.0.0.1:8080")) as client:
    for message in messages:
        result = client.publish("greet", message)
        print(f"#{result['index']} {message!r}")

    # An idempotency key makes a publish safe to retry: a repeat inside
    # the broker's dedup window returns the original index rather than
    # appending a second copy.
    a = client.publish("greet", "exactly once", id="demo-key")
    b = client.publish("greet", "exactly once", id="demo-key")
    print(f"same message twice -> index {a['index']} and {b['index']} "
          f"(duplicate: {b['duplicate']})")

"""A subscriber. Run the broker and the publisher alongside:

    ../../bin/bjmsg serve &
    python examples/subscribe.py &
    python examples/publish.py
"""
import logging
import os
import signal
import threading

from bjmsg import Client

logging.basicConfig(level=logging.INFO, format="%(message)s")

client = Client(os.environ.get("BJMSG_URL", "http://127.0.0.1:8080"))


def show(msg):
    print(f"#{msg.index} {msg.value!r} {msg.headers or ''}", flush=True)


# A named consumer makes it durable: stop this process, publish more,
# start it again, and the broker delivers exactly what was missed. Drop
# `consumer` for a throwaway subscription that forgets on exit.
sub = client.subscribe("greet", show, consumer="example-logger")

print(f"listening on {sub.callback}", flush=True)
print("Ctrl-C to stop.", flush=True)

stop = threading.Event()
for sig in (signal.SIGINT, signal.SIGTERM):
    signal.signal(sig, lambda *_: stop.set())
stop.wait()
client.close()

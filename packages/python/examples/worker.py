"""A queue-group worker. Each job goes to exactly one member of the
group, so run several of these and they compete:

    python examples/worker.py &
    python examples/worker.py &
    python -c "from bjmsg import Client
with Client() as c:
    for i in range(1, 11): c.publish('jobs', i)"
"""
import logging
import os
import signal
import threading
import time

from bjmsg import Client

logging.basicConfig(level=logging.INFO, format="%(message)s")

client = Client(os.environ.get("BJMSG_URL", "http://127.0.0.1:8080"))

# A job whose worker dies is redelivered when its lease expires, so a
# handler must tolerate running twice — `attempts` above 1 is the warning
# that it is seeing one again. After max_attempts it goes to the
# dead-letter channel rather than starving the queue.
client.configure_queue("jobs", "crew", lease_ms=30000, max_attempts=5,
                       backoff_ms=1000)


def handle(job):
    if job.attempts > 1:
        print(f"retrying #{job.index} (attempt {job.attempts})", flush=True)
    print(f"working #{job.index}: {job.value!r}", flush=True)
    time.sleep(0.25)

    # Raising returns the job to the queue, due again after the group's
    # backoff. Returning finishes it. The library logs the traceback —
    # so the one below, from the deliberately poisonous job, is the
    # example working rather than failing.
    if job.value == "poison":
        raise RuntimeError("cannot handle that")

    print(f"done #{job.index}", flush=True)


client.work("jobs", handle, group="crew")
print('taking jobs from the "crew" group. Ctrl-C to stop.', flush=True)

stop = threading.Event()
for sig in (signal.SIGINT, signal.SIGTERM):
    signal.signal(sig, lambda *_: stop.set())
stop.wait()
client.close()

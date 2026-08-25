# sukkal — Python client

Publish/subscribe over HTTP/1.1 with [binjson](https://github.com/mdy-docs/binjson)
payloads, against a [sukkal](../../README.md) broker.

Messages are **pushed, never polled**. A subscription names a callback URL
the broker POSTs to, and the reply to that POST is the acknowledgement —
so a subscriber is an HTTP server, which is what Flask is here for. An
idle client makes no requests at all.

```sh
pip install -e .
```

```python
from sukkal import Client

with Client("http://127.0.0.1:8080") as sukkal:
    sukkal.subscribe("orders.>", lambda msg: print(msg.subject, msg.value))
    sukkal.publish("orders.new", {"id": 1, "total": 9.99})
```

`subscribe` starts a Flask server, works out what address this host
reaches the broker from, binds there, registers itself as the callback,
and returns once the broker has accepted it. Nothing needs configuring on
one machine or one network.

The only dependency is Flask. binjson is implemented here rather than
depended on — the format is a short frozen spec, and a pure-Python codec
has no dependencies at all.

## Subscribing

```python
sub = sukkal.subscribe("greet", handle)
...
sub.close()          # or use it as a context manager
```

The handler is called **one message at a time, in order**, on the
receiver's thread. If it raises, that message and everything after it in
the batch is refused and the broker sends them again — a handler that
fails is a handler whose messages are not lost.

| argument | |
| --- | --- |
| `consumer` | name it, and the subscription is durable: the broker keeps a receipt, so rejoining delivers only what was missed. Without one a generated name is used and the subscription — receipt and all — goes on close. |
| `tail` | only messages published from now on |
| `from_index` | start at this index |
| `batch_bytes` | how much the broker may send per delivery |

A `Message` is a dataclass:

```python
Message(
    subject="orders.new",   # which subject it came from
    index=42,               # its position in that subject's log
    value={"id": 1},        # as published
    headers=None,           # None unless the publisher sent any
    raw=b"...",             # the encoded bytes, to pass on untouched
    lag=0,                  # how many are still waiting behind it
)
```

### Wildcards

`orders.*` takes one token, `orders.>` takes that and everything below.
Matching happens in the broker, so a subject created later is picked up
with nothing re-resolving — and `msg.subject` says which one it was.

There is **no order across subjects**: each is delivered in its own
order, and `orders.us` index 5 and `orders.eu` index 5 are unrelated. If
you need a total order over a set, they have to be one subject.

## Publishing

```python
sukkal.publish("orders.new", {"id": 1})
sukkal.publish("orders.new", "anything binjson encodes")
sukkal.publish("orders.new", value, headers={"trace": "abc"})
```

`id` makes a publish **safe to retry**: a repeat inside the broker's
dedup window returns the original index instead of appending a second
copy, and says so.

```python
a = sukkal.publish("orders.new", v, id="order-1")
b = sukkal.publish("orders.new", v, id="order-1")
# b["index"] == a["index"], b["duplicate"] is True
```

## Job queues

A queue group turns a subject into a work queue: each job goes to exactly
one member, however many are running.

```python
def handle(job):
    if job.attempts > 1:
        log.warning("seeing #%d again", job.index)
    do_work(job.value)

sukkal.work("jobs", handle, group="crew")
```

Returning finishes the job. **Raising returns it to the queue**, due again
after the group's backoff. A job whose worker dies is redelivered when its
lease expires, so a handler must tolerate running twice — `job.attempts`
above 1 is the warning that it is.

```python
sukkal.configure_queue("jobs", "crew", lease_ms=30_000, max_attempts=5,
                      backoff_ms=1_000, max_backoff_ms=300_000)
```

After `max_attempts` a job goes to the subject's dead-letter channel
rather than starving the queue. An empty one is an empty list — the
channel belongs to the subject, so "nothing has died" is not a missing
resource:

```python
dead = sukkal.dead("jobs")
sukkal.requeue("jobs", dead[0]["index"])
```

`max_jobs` asks for more than one job per delivery. One is the default
and is what spreads a queue evenly across workers; more trades that for
fewer round trips, and a worker that finishes only some of them says
which.

## Request-reply

```python
# the service
sukkal.reply("echo", lambda msg: str(msg.value).upper())

# the caller
sukkal.request("echo", "hello")        # 'HELLO'
```

Repliers share a queue group, so each request is answered once however
many are running. The requester **subscribes before it publishes**, so a
reply that comes back instantly still has somewhere to land, and matches
on a correlation — several requesters share one reply subject without
needing one each. `request` is thread-safe; concurrent callers each get
their own answer.

## Pipelines

```python
sukkal.pipe("orders.raw", lambda msg: normalise(msg.value),
           to="orders.clean", consumer="normaliser")
```

Effectively-once. The guarantee is in the **order of two writes**: the
output is published with the input's acknowledgement riding along in the
same broker call, and carries an idempotency key derived from the input's
index. So a crash anywhere replays the input, and the rerun's output
collapses onto the one already there. One input, one output, whatever
fails.

Returning `None` drops a message. It is still acknowledged, so the stage
keeps moving.

## Using your own Flask app

By default the client starts its own server on a background thread. To
mount into one you already have — so deliveries arrive on the port your
service already listens on:

```python
from sukkal import Client, Receiver

app = Flask(__name__)
receiver = Receiver(app=app, mount_path="/hooks/sukkal")
receiver.port = 3000                 # tell it where you are listening

sukkal = Client(BROKER, receiver=receiver, advertise="orders.internal")
```

`advertise` is what goes in the callback URL when the broker reaches this
service by a name rather than by the address it happens to bind — a port
forward, a NAT, a proxy in front.

One Flask app serves every subscription a client has, each on its own
path under `mount_path`, so `client.receiver.app` is yours to add routes
to.

Whatever serves it must **keep connections alive**: the broker parks one
open between deliveries. The built-in server sets `HTTP/1.1` for exactly
that reason, and a WSGI server in front should not be configured with a
short keep-alive timeout.

## Threading

Deliveries arrive on the receiver's threads, one per connection, so
handlers for different subscriptions run concurrently. Within one
subscription the broker sends a single batch at a time, so its messages
are strictly ordered and never overlap — a handler needs no locking
against itself, only against whatever else it touches.

Requests to the broker are serialised on one connection, so publishing
from inside a handler is safe and reuses the same socket.

## What the broker needs to reach

The connection runs **broker → subscriber**, which is the one thing to
know before deploying this. sukkal is built for server-to-server: both
ends reachable, both able to dial. A process the broker cannot dial
cannot be pushed to, and `advertise` only helps where something in front
forwards the connection on.

Each subscription registers a random bearer token that the broker sends
on every delivery, so a POST that did not come from the broker you
registered with is refused with a 401.

## Reliability

- **Delivery is at-least-once.** Handlers must tolerate seeing a message
  twice. `(subject, index)` is a stable, permanent key — indexes never
  shift, even when the log is trimmed — so it is what to deduplicate on.
- **The client re-registers every 30 s** (`heartbeat_seconds`, `0` to
  disable). Not a poll: it carries no cursor and asks for nothing. It
  exists so a broker whose store was rebuilt re-learns the subscription,
  since silence is otherwise indistinguishable from "nothing to send".
- **Requests are retried when the broker cannot be reached**
  (`retry_seconds`, default 5, `0` to disable). Only connection failures
  — an HTTP status is the broker answering.
- **A delivery the broker cannot make is retried** with a backoff, for as
  long as it takes. The receipt is durable, so an unreachable subscriber
  is merely behind. `sukkal.pushes()` shows the failure count and the last
  error.
- **Handler errors are logged, not raised**, on the `sukkal` logger — the
  delivery is being retried anyway, and an exception escaping into a
  server thread would help nobody.

### Latency

Publish-to-handler is around 3.5 ms on a loopback, against 0.5 ms for the
C client. The difference is not the network: Flask and `wsgiref` take
about 1.5 ms to turn a delivery around, which lands the response just
after the broker's 2 ms in-flight poll window, so it is collected on the
next tick. Fine for almost everything, worth knowing if you are chasing
sub-millisecond. Throughput is unaffected — batches amortise it, and a
thousand messages drain as fast as they are published.

## Inspecting a broker

```python
sukkal.health()             # {"ok", "backend", "subjects", "connections", ...}
sukkal.subjects("eu.>")     # matching subject names
sukkal.info("orders")       # {"base", "first", "last", "messages", "bytes", ...}
sukkal.consumers("orders")  # [{"consumer", "acked", "lag"}]
sukkal.pushes()             # subscriptions, and how each is faring
sukkal.queues("jobs")       # queue-group state
```

Retention, so a subject does not grow without bound. Any dimension left
out is unlimited, and whichever limit is reached first takes effect:

```python
sukkal.set_policy("orders", max_age_seconds=86_400, max_messages=10_000)
sukkal.trim("orders", keep=1_000)
```

By default retention will not discard a message a subscription has not
read. `ignore_consumers=True` makes the policy authoritative — the point
of retention being a bound that holds.

## binjson

`sukkal.encode` / `sukkal.decode` are a complete implementation of the
format, byte-for-byte compatible with the C broker and the JavaScript
client (verified against the worked examples in
[FORMAT.md](../../third_party/binjson/FORMAT.md) and against the
reference encoder's output).

```python
from sukkal import encode, decode, ObjectId

decode(encode({"id": ObjectId(), "when": datetime.now(timezone.utc)}))
```

One deliberate difference from the JavaScript producer, which has a
single numeric type: there, `1.0` is an integer and encodes as `INT`.
Here `int` encodes as `INT` and `float` as `FLOAT`, so a Python value
round-trips to itself. Both are valid encodings of the same number and
every decoder accepts either.

Integers beyond ±(2⁵³−1) are encoded as `FLOAT`, because the JavaScript
decoder refuses an `INT` that large rather than silently losing
precision. Precision is lost either way; the boundary is where it becomes
visible.

## Errors

Everything raises `SukkalError`, carrying the broker's own explanation
(success bodies are binjson, errors are `text/plain`) and its status.
`BrokerUnreachable` is the subclass for "nothing was sent".

```python
from sukkal import SukkalError

try:
    sukkal.info("nope")
except SukkalError as err:
    if err.status == 404:
        ...
```

## Tests

The client tests run against a real broker rather than a mock — the point
of this library is interoperating with the C broker's wire format, and a
mock would only assert that it agrees with itself. The codec tests assert
the byte sequences in FORMAT.md, which a round-trip test alone would
happily pass on a private format.

```sh
make -C ../..                # build bin/sukkal first
pip install -e ".[test]"
pytest
```

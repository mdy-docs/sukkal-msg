# bjmsg — Node client

Publish/subscribe over HTTP/1.1 with [binjson](https://github.com/mdy-docs/binjson)
payloads, against a [bjmsg](../../README.md) broker.

Messages are **pushed, never polled**. A subscription names a callback URL
the broker POSTs to, and the reply to that POST is the acknowledgement —
so a subscriber is an HTTP server, which is what Express is here for. A
message reaches a handler in well under a millisecond, and an idle client
makes no requests at all.

```sh
npm install
```

```js
import { Client } from 'bjmsg';

const bjmsg = new Client({ url: 'http://127.0.0.1:8080' });

await bjmsg.subscribe('orders.>', (msg) => {
  console.log(msg.subject, msg.index, msg.value);
});

await bjmsg.publish('orders.new', { id: 1, total: 9.99 });

await bjmsg.close();
```

`subscribe` starts an Express server, works out what address this host
reaches the broker from, registers itself as the callback, and returns
once the broker has accepted it. Nothing needs configuring on one machine
or one network.

## Subscribing

```js
const sub = await bjmsg.subscribe('greet', async (msg) => {
  await handle(msg.value);
});

await sub.close();
```

The handler is awaited **one message at a time, in order**. If it throws,
that message and everything after it in the batch is refused and the
broker sends them again — a handler that fails is a handler whose
messages are not lost.

| option | |
| --- | --- |
| `consumer` | name it, and the subscription is durable: the broker keeps a receipt, so rejoining delivers only what was missed. Without one a generated name is used and the subscription — receipt and all — goes on exit. |
| `tail` | only messages published from now on |
| `from` | start at this index |
| `batchBytes` | how much the broker may send per delivery |

A message is:

```js
{
  subject: 'orders.new',   // which subject it came from
  index: 42,               // its position in that subject's log
  value: { id: 1 },        // as published
  headers: null,           // null unless the publisher sent any
  raw: Uint8Array,         // the encoded bytes, to pass on untouched
  lag: 0,                  // how many are still waiting behind it
}
```

### Wildcards

`orders.*` takes one token, `orders.>` takes that and everything below.
Matching happens in the broker, so a subject created later is picked up
with nothing re-resolving — and `msg.subject` says which one it was.

```js
await bjmsg.subscribe('orders.*.created', (msg) => …);
```

There is **no order across subjects**: each is delivered in its own
order, and `orders.us` index 5 and `orders.eu` index 5 are unrelated. If
you need a total order over a set, they have to be one subject.

## Publishing

```js
await bjmsg.publish('orders.new', { id: 1 });
await bjmsg.publish('orders.new', 'anything binjson encodes');
await bjmsg.publish('orders.new', value, { headers: { trace: 'abc' } });
```

`id` makes a publish **safe to retry**: a repeat inside the broker's
dedup window returns the original index instead of appending a second
copy, and says so.

```js
const a = await bjmsg.publish('orders.new', v, { id: 'order-1' });
const b = await bjmsg.publish('orders.new', v, { id: 'order-1' });
// b.index === a.index, b.duplicate === true
```

## Job queues

A queue group turns a subject into a work queue: each job goes to exactly
one member, however many are running.

```js
await bjmsg.work('jobs', async (job) => {
  if (job.attempts > 1) console.warn('seeing this one again');
  await process(job.value);
}, { group: 'crew' });
```

Returning finishes the job. **Throwing returns it to the queue**, due
again after the group's backoff. A job whose worker dies is redelivered
when its lease expires, so a handler must tolerate running twice —
`job.attempts` above 1 is the warning that it is.

```js
await bjmsg.configureQueue('jobs', 'crew', {
  leaseMs: 30000, maxAttempts: 5, backoffMs: 1000, maxBackoffMs: 300000,
});
```

After `maxAttempts` a job goes to the subject's dead-letter channel
rather than starving the queue. An empty one is an empty list — the
channel belongs to the subject, so "nothing has died" is not a missing
resource:

```js
const dead = await bjmsg.dead('jobs');
await bjmsg.requeue('jobs', dead[0].index);
```

`max` asks for more than one job per delivery. One is the default and is
what spreads a queue evenly across workers; more trades that for fewer
round trips, and a worker that finishes only some of them says which.

## Request-reply

```js
// the service
await bjmsg.reply('echo', (msg) => String(msg.value).toUpperCase());

// the caller
const answer = await bjmsg.request('echo', 'hello');   // 'HELLO'
```

Repliers share a queue group, so each request is answered once however
many are running. The requester **subscribes before it publishes**, so a
reply that comes back instantly still has somewhere to land, and matches
on a correlation — several requesters share one reply subject without
needing one each.

## Pipelines

```js
await bjmsg.pipe('orders.raw', (msg) => normalise(msg.value), {
  to: 'orders.clean',
  consumer: 'normaliser',
});
```

Effectively-once. The guarantee is in the **order of two writes**: the
output is published with the input's acknowledgement riding along in the
same broker call, and carries an idempotency key derived from the input's
index. So a crash anywhere replays the input, and the rerun's output
collapses onto the one already there. One input, one output, whatever
fails.

Returning `undefined` drops a message. It is still acknowledged, so the
stage keeps moving.

## Using your own Express app

By default the client starts its own server. To mount into one you
already have — so deliveries arrive on the port your service already
listens on:

```js
import express from 'express';
import { Client, Receiver } from 'bjmsg';

const app = express();
const receiver = new Receiver({ app, mountPath: '/hooks/bjmsg' });

const server = app.listen(3000);
server.keepAliveTimeout = 5 * 60 * 1000;      // the broker parks here
server.headersTimeout = server.keepAliveTimeout + 1000;
receiver.port = 3000;

const bjmsg = new Client({ url: BROKER, receiver, advertise: 'orders.internal' });
```

`advertise` is what goes in the callback URL when the broker reaches this
service by a name rather than by the address it happens to bind — a port
forward, a NAT, a proxy in front.

One Express app serves every subscription a client has, each on its own
path under `mountPath`, so `client.receiver.app` is yours to add routes
to.

## What the broker needs to reach

The connection runs **broker → subscriber**, which is the one thing to
know before deploying this. bjmsg is built for server-to-server: both
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
- **The client re-registers every 30 s** (`heartbeatMs`, `0` to disable).
  Not a poll: it carries no cursor and asks for nothing. It exists so a
  broker whose store was rebuilt re-learns the subscription, since
  silence is otherwise indistinguishable from "nothing to send".
- **Requests are retried when the broker cannot be reached** (`retryMs`,
  default 5 s, `0` to disable). Only connection failures — an HTTP status
  is the broker answering.
- **A delivery the broker cannot make is retried** with a backoff, for as
  long as it takes. The receipt is durable, so an unreachable subscriber
  is merely behind. `await bjmsg.pushes()` shows the failure count and the
  last error.
- **Handler errors are emitted, not thrown.** `client.on('error', …)`, or
  they surface as process warnings.

## Inspecting a broker

```js
await bjmsg.health();          // { ok, backend, subjects, connections, … }
await bjmsg.subjects('eu.>');  // matching subject names
await bjmsg.info('orders');    // { base, first, last, messages, bytes, … }
await bjmsg.consumers('orders');  // [{ consumer, acked, lag }]
await bjmsg.pushes();          // subscriptions, and how each is faring
await bjmsg.queues('jobs');    // queue-group state
```

Retention, so a subject does not grow without bound. Any dimension left
out is unlimited, and whichever limit is reached first takes effect:

```js
await bjmsg.setPolicy('orders', { maxAgeSeconds: 86400, maxMessages: 10000 });
await bjmsg.trim('orders', { keep: 1000 });
```

By default retention will not discard a message a subscription has not
read. `ignoreConsumers: true` makes the policy authoritative — the point
of retention being a bound that holds.

## Errors

Everything throws `BjmsgError`, carrying the broker's own explanation
(success bodies are binjson, errors are `text/plain`) and its status.
`BrokerUnreachable` is the subclass for "nothing was sent".

```js
import { BjmsgError } from 'bjmsg';

try {
  await bjmsg.info('nope');
} catch (err) {
  if (err instanceof BjmsgError && err.status === 404) …
}
```

## Tests

The tests run against a real broker rather than a mock — the point of
this library is interoperating with the C broker's wire format, and a
mock would only assert that it agrees with itself.

```sh
make -C ../..      # build bin/bjmsg first
npm test
```

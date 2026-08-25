/*
 * client.js — the whole of sukkal from Node.
 *
 * Publishing is a request. Subscribing is the reverse: the client tells
 * the broker a URL to POST to, and the reply to that POST is the
 * acknowledgement — so delivery and ack are one exchange, backpressure
 * is "we have not answered yet", and there is no polling anywhere.
 */
import { EventEmitter } from 'node:events';

import { Transport } from './transport.js';
import { Receiver } from './receiver.js';
import { SukkalError } from './errors.js';
import {
  Subscription, DEFAULT_HEARTBEAT_MS, randomToken, generatedConsumer,
} from './subscription.js';
import {
  encodeMessage, isValidSubject, isValidName, isValidTarget,
} from './protocol.js';

const DEFAULT_URL = 'http://127.0.0.1:8080';
const DEFAULT_REPLY_SUBJECT = '_reply';
const DEFAULT_REPLY_GROUP = 'repliers';

/**
 * @typedef {object} Message
 * @property {string}  subject   which subject it came from
 * @property {number}  index     its position in that subject's log
 * @property {*}       value     the message as published
 * @property {?object} headers   null unless the publisher sent any
 * @property {Uint8Array} raw    the encoded bytes, to pass on untouched
 * @property {number}  lag       how many are still waiting behind it
 */

export class Client extends EventEmitter {
  #transport;
  #receiver;
  #ownReceiver;
  #subs = new Set();
  #closed = false;

  /**
   * @param {object}  [opts]
   * @param {string}  [opts.url]          broker base URL
   * @param {number}  [opts.retryMs]      wait between attempts when the
   *                                      broker cannot be reached; 0 off
   * @param {number}  [opts.timeoutMs]    per-request timeout
   * @param {Receiver|object} [opts.receiver]
   *        an existing Receiver, or options to build one — `{ port }`,
   *        `{ app }` to mount into your own Express app, etc.
   * @param {string}  [opts.advertise]    host or IP to put in callback
   *        URLs, when it is not simply where we are listening: a port
   *        forward, a NAT, a proxy in front
   * @param {number}  [opts.heartbeatMs]  re-registration interval; 0 off
   */
  constructor({
    url = DEFAULT_URL,
    retryMs = 5000,
    timeoutMs = 30000,
    receiver = null,
    advertise = null,
    heartbeatMs = DEFAULT_HEARTBEAT_MS,
  } = {}) {
    super();
    this.url = url;
    this.advertise = advertise;
    this.heartbeatMs = heartbeatMs;
    this.#transport = new Transport(url, { timeout: timeoutMs, retryMs });
    this.#ownReceiver = !(receiver instanceof Receiver);
    this.#receiver = receiver instanceof Receiver
      ? receiver
      : new Receiver(receiver ?? {});
  }

  /** The Express receiver, for mounting your own routes alongside. */
  get receiver() {
    return this.#receiver;
  }

  /* ---- publishing ----------------------------------------------------- */

  /**
   * Publish one message.
   *
   * @param {string} subject
   * @param {*} value  anything binjson encodes
   * @param {object} [opts]
   * @param {string} [opts.id]
   *        an idempotency key, which makes this publish safe to repeat:
   *        a repeat inside the broker's dedup window returns the original
   *        index instead of appending a second copy.
   * @param {object} [opts.headers]
   *        stored as an envelope alongside the message, opaque to the
   *        broker. `reply_to` and `correlation` are what request-reply
   *        uses; the rest is yours.
   * @param {{subject: string, consumer: string, index: number}} [opts.ack]
   *        advance a subscription's receipt on ANOTHER subject as part of
   *        this publish. That is what makes a pipeline effectively-once:
   *        output and input-ack happen before one response, in that
   *        order, so a crash between them replays the input and the
   *        republished output collapses onto the existing one by its id.
   * @returns {Promise<{subject: string, index: number, duplicate?: boolean}>}
   */
  async publish(subject, value, { id, headers, ack } = {}) {
    if (!isValidSubject(subject)) throw new SukkalError(`invalid subject '${subject}'`);
    const { body, enveloped } = encodeMessage(value, headers);

    const { value: out } = await this.#transport.request('POST', `/pub/${subject}`, {
      query: {
        id,
        headers: enveloped ? 1 : undefined,
        ack_subject: ack?.subject,
        ack_consumer: ack?.consumer,
        ack_index: ack?.index,
      },
      body,
    });
    return out;
  }

  /* ---- subscribing ---------------------------------------------------- */

  /**
   * Receive messages from a subject or wildcard pattern.
   *
   * The handler is awaited, one message at a time, in order. If it
   * throws, that message and everything after it in the batch is refused
   * and the broker sends them again — so a handler that fails is a
   * handler whose messages are not lost.
   *
   * @param {string} target  'orders.new', 'orders.*' or 'orders.>'
   * @param {(msg: Message) => void|Promise<void>} handler
   * @param {object}  [opts]
   * @param {string}  [opts.consumer]
   *        name it, and the subscription is durable: the broker keeps a
   *        receipt, so rejoining delivers only what was missed. Without
   *        one a generated name is used and the subscription — receipt
   *        and all — goes when this client does.
   * @param {boolean} [opts.tail]   only messages published from now on
   * @param {number}  [opts.from]   start at this index
   * @param {number}  [opts.batchBytes]  how much may be sent per delivery
   * @returns {Promise<Subscription>}
   */
  async subscribe(target, handler, {
    consumer, tail = false, from, batchBytes,
  } = {}) {
    if (!isValidTarget(target)) throw new SukkalError(`invalid subject or pattern '${target}'`);

    return this.#start({
      target,
      consumer,
      prefix: 'sub',
      params: { start: tail ? 'last' : undefined, from, batch: batchBytes },
      handle: (messages, info, res) => this.#handleMessages(
        messages, info, res, handler,
      ),
    });
  }

  /**
   * Take jobs from a queue group: each job goes to exactly one member,
   * however many are running.
   *
   * A handler that returns finishes the job; one that throws returns it
   * to the queue, due again after the group's backoff. A job whose worker
   * dies is redelivered when its lease expires, so a handler must
   * tolerate running twice — `job.attempts` above 1 says it is seeing one
   * again.
   *
   * @param {string} target
   * @param {(job: Message & {attempts: number, expiresAt: Date}) =>
   *          void|Promise<void>} handler
   * @param {object} opts
   * @param {string} opts.group
   * @param {number} [opts.max]  jobs per delivery. One is the default,
   *        and is what spreads a queue evenly across workers.
   * @param {string} [opts.consumer]  name it, so `sukkal push` says which
   *        worker is which
   * @returns {Promise<Subscription>}
   */
  async work(target, handler, { group, max, consumer } = {}) {
    if (!isValidTarget(target)) throw new SukkalError(`invalid subject or pattern '${target}'`);
    if (!isValidName(group)) throw new SukkalError('a queue group name is required');

    return this.#start({
      target,
      consumer,
      group,
      prefix: 'work',
      params: { group, max },
      /*
       * A worker never keeps its registration on the way out. The broker
       * would go on leasing jobs to a callback that is not there, and
       * each would sit out its lease before anyone else could have it —
       * where a subscriber left registered merely accumulates a backlog.
       */
      alwaysUnregister: true,
      handle: (jobs, info, res) => this.#handleJobs(jobs, info, res, handler),
    });
  }

  /* ---- request / reply ------------------------------------------------- */

  /**
   * Serve requests: run the handler for each and publish what it returns
   * to the request's reply_to, with the same correlation. Repliers share
   * a queue group, so each request is answered once however many run.
   *
   * @param {string} subject
   * @param {(msg: Message) => *|Promise<*>} handler
   *        what it returns is the reply; undefined replies nothing
   * @param {object} [opts]
   * @param {string} [opts.group]
   */
  async reply(subject, handler, { group = DEFAULT_REPLY_GROUP } = {}) {
    return this.work(subject, async (msg) => {
      const answer = await handler(msg);
      const replyTo = msg.headers?.reply_to;
      const correlation = msg.headers?.correlation;
      if (answer === undefined || !replyTo || !correlation) return;
      await this.publish(replyTo, answer, {
        headers: { correlation },
        /* Derived from the correlation, so a redelivered request cannot
         * produce a second reply. */
        id: `reply.${correlation}`,
      });
    }, { group });
  }

  /**
   * Publish a request and wait for its reply.
   *
   * Subscribes to the reply subject BEFORE publishing, so a reply that
   * comes back instantly still has somewhere to land, then waits on a
   * connection rather than asking repeatedly. The correlation does the
   * matching, because a reply subject is shared — several requesters may
   * be waiting on it at once.
   *
   * @param {string} subject
   * @param {*} value
   * @param {object} [opts]
   * @param {number} [opts.timeoutMs]  default 5000
   * @param {string} [opts.replyTo]    reply subject, shared by default
   * @returns {Promise<*>} the reply value
   */
  async request(subject, value, {
    timeoutMs = 5000, replyTo = DEFAULT_REPLY_SUBJECT,
  } = {}) {
    if (!isValidSubject(subject)) throw new SukkalError(`invalid subject '${subject}'`);
    const correlation = randomToken(12);

    let settle;
    const answered = new Promise((resolve) => { settle = resolve; });

    const sub = await this.subscribe(replyTo, (msg) => {
      if (msg.headers?.correlation === correlation) settle(msg.value);
    }, { tail: true });

    const timer = setTimeout(() => settle(TIMED_OUT), timeoutMs);
    try {
      await this.publish(subject, value, {
        headers: { reply_to: replyTo, correlation },
        id: `req.${correlation}`,
      });
      const answer = await answered;
      if (answer === TIMED_OUT) {
        throw new SukkalError(`no reply on '${subject}' within ${timeoutMs}ms`);
      }
      return answer;
    } finally {
      clearTimeout(timer);
      await sub.close();
    }
  }

  /* ---- pipelines -------------------------------------------------------- */

  /**
   * Read a subject, transform each message, publish the result to
   * another — effectively-once.
   *
   * The guarantee is in the ORDER of two writes, not in the transport:
   * the output is published with the input's acknowledgement riding
   * along in the same broker call, and carries an idempotency key
   * derived from the input's index. So a crash anywhere replays the
   * input, and the rerun's output collapses onto the one already there.
   * One input, one output, whatever fails.
   *
   * The key is derived from the INPUT, deliberately — a handler that is
   * not perfectly deterministic must still collapse onto the message its
   * first run produced.
   *
   * @param {string} from
   * @param {(msg: Message) => *|Promise<*>} handler
   *        returning undefined drops the message; it is still
   *        acknowledged, so the stage makes progress
   * @param {object} opts
   * @param {string} opts.to        output subject
   * @param {string} opts.consumer  names the stage; where it resumes
   */
  async pipe(from, handler, { to, consumer } = {}) {
    if (!isValidSubject(to)) throw new SukkalError('pipe needs a valid `to` subject');
    if (!isValidName(consumer)) throw new SukkalError('pipe needs a `consumer` name: it is where the stage resumes');

    return this.subscribe(from, async (msg) => {
      const out = await handler(msg);
      if (out === undefined) return;
      await this.publish(to, out, {
        id: `${consumer}.${msg.subject}.${msg.index}`,
        ack: { subject: msg.subject, consumer, index: msg.index },
      });
    }, { consumer });
  }

  /* ---- queries ---------------------------------------------------------- */

  async health() {
    return (await this.#transport.request('GET', '/health')).value;
  }

  /** Every subject, or those matching a pattern. */
  async subjects(pattern) {
    return (await this.#transport.request('GET', '/subjects', {
      query: { pattern },
    })).value;
  }

  async info(subject) {
    return (await this.#transport.request('GET', `/info/${subject}`)).value;
  }

  /** Every consumer of a subject, with how far behind each is. */
  async consumers(subject) {
    return (await this.#transport.request('GET', `/consumers/${subject}`)).value;
  }

  /** Push subscriptions the broker is delivering to, and how each fares. */
  async pushes() {
    return (await this.#transport.request('GET', '/push')).value;
  }

  async queues(subject) {
    return (await this.#transport.request('GET', `/queue/${subject}`)).value;
  }

  /**
   * Messages that ran out of attempts. Empty when nothing has died: the
   * channel belongs to the subject, so "nothing here" is an emptiness
   * rather than an absence.
   *
   * They are stored as `<subject>.dead`, an ordinary subject, so `sub`,
   * `info`, a retention policy and even a queue group of its own all
   * work on it directly.
   */
  async dead(subject, { from = 1, max } = {}) {
    return (await this.#transport.request('GET', `/dead/${subject}`, {
      query: { from, max },
    })).value;
  }

  /** Put a dead-lettered message back on the subject it came from. */
  async requeue(subject, deadIndex) {
    return (await this.#transport.request('POST', `/requeue/${subject}`, {
      query: { index: deadIndex },
    })).value;
  }

  /* ---- administration --------------------------------------------------- */

  /** Move a consumer's receipt by hand. It never moves backwards. */
  async ack(subject, consumer, index) {
    return (await this.#transport.request('POST', `/ack/${subject}`, {
      query: { consumer, index },
    })).value;
  }

  /** Forget a durable subscription's position entirely. */
  async unsubscribe(subject, consumer) {
    return (await this.#transport.request('DELETE', `/consumers/${subject}`, {
      query: { consumer },
    })).value;
  }

  /**
   * Retention. Any dimension left out is unlimited, and a PUT replaces
   * the whole policy rather than patching it. Several may be set at once
   * — whichever limit is reached first takes effect.
   */
  async setPolicy(subject, {
    maxAgeSeconds, maxMessages, maxBytes, ignoreConsumers = false,
  } = {}) {
    return (await this.#transport.request('PUT', `/policy/${subject}`, {
      query: {
        max_age_s: maxAgeSeconds,
        max_messages: maxMessages,
        max_bytes: maxBytes,
        ignore_consumers: ignoreConsumers ? 1 : undefined,
      },
    })).value;
  }

  async policy(subject) {
    return (await this.#transport.request('GET', `/policy/${subject}`)).value;
  }

  async clearPolicy(subject) {
    return (await this.#transport.request('DELETE', `/policy/${subject}`)).value;
  }

  async policies() {
    return (await this.#transport.request('GET', '/policies')).value;
  }

  /**
   * Discard messages below a boundary. `force` is required to go below a
   * consumer's receipt, because that destroys messages it has not read.
   */
  async trim(subject, { before, keep, force = false } = {}) {
    return (await this.#transport.request('POST', `/trim/${subject}`, {
      query: { before, keep, force: force ? 1 : undefined },
    })).value;
  }

  async configureQueue(subject, group, {
    leaseMs, maxAttempts, backoffMs, maxBackoffMs,
  } = {}) {
    return (await this.#transport.request('PUT', `/queue/${subject}`, {
      query: {
        group,
        lease_ms: leaseMs,
        max_attempts: maxAttempts,
        backoff_ms: backoffMs,
        max_backoff_ms: maxBackoffMs,
      },
    })).value;
  }

  /* ---- lifecycle --------------------------------------------------------- */

  /** Stop every subscription and release the connection. */
  async close({ keep = false } = {}) {
    if (this.#closed) return;
    this.#closed = true;
    await Promise.allSettled([...this.#subs].map((s) => s.close({ keep })));
    if (this.#ownReceiver) await this.#receiver.close();
    this.#transport.close();
  }

  /* ---- internals --------------------------------------------------------- */

  async #start({ target, consumer, group, prefix, params, handle, alwaysUnregister }) {
    if (consumer !== undefined && !isValidName(consumer)) {
      throw new SukkalError(`invalid consumer '${consumer}'`);
    }
    const ephemeral = consumer === undefined;
    const name = consumer ?? generatedConsumer(prefix);

    /*
     * Learn the local address before deciding where to listen: binding to
     * the address this host reaches the broker from is both the one most
     * likely to work and a smaller thing to leave open than 0.0.0.0.
     */
    if (!this.#transport.localAddress) await this.health();
    const advertised = this.advertise ?? this.#transport.localAddress ?? '127.0.0.1';

    await this.#receiver.listen(this.advertise ? undefined : advertised);

    const token = randomToken();
    const callback = this.#receiver.callbackUrl(name, advertised);

    this.#receiver.register(name, token, async (messages, info, res) => {
      try {
        await handle(messages, info, res);
      } catch (err) {
        this._emitError(err);
        if (!res.headersSent) {
          res.status(500).type('text/plain').send('handler failed\n');
        }
      }
    });

    const sub = new Subscription(this, {
      consumer: name,
      target,
      group,
      callback,
      token,
      params,
      ephemeral: ephemeral && !alwaysUnregister,
      heartbeatMs: this.heartbeatMs,
    });
    sub.alwaysUnregister = Boolean(alwaysUnregister);

    try {
      await this._register(sub);
    } catch (err) {
      this.#receiver.unregister(name);
      throw err;
    }
    this.#subs.add(sub);
    return sub;
  }

  /**
   * Register, or re-register. A PUT is idempotent here on purpose:
   * repeating it moves the callback and leaves the receipt alone, which
   * is what lets a client that restarted on a new port simply say so,
   * and what makes the heartbeat safe to send at any time.
   */
  async _register(sub) {
    await this.#transport.request('PUT', `/push/${sub.target}`, {
      query: {
        consumer: sub.consumer,
        callback: sub.callback,
        token: sub.token,
        ...sub.params,
      },
    });
  }

  async _unregister(sub, { keep }) {
    this.#subs.delete(sub);
    this.#receiver.unregister(sub.consumer);
    if (keep && !sub.alwaysUnregister) return;
    try {
      await this.#transport.request('DELETE', '/push', {
        query: {
          consumer: sub.consumer,
          /* A generated name is not coming back, so its position goes
           * with it — a receipt holds retention off everything below it. */
          purge: sub.ephemeral ? 1 : undefined,
        },
      });
    } catch (err) {
      this._emitError(err, sub);
    }
  }

  _emitError(err, sub) {
    if (this.listenerCount('error') > 0) this.emit('error', err, sub);
    else process.emitWarning(`sukkal: ${err.message}`);
  }

  /**
   * A subscription batch. Messages are handled in order and one at a
   * time, because that is the order they were published in and the only
   * thing a receipt can express.
   *
   * X-Sukkal-Ack reports how far we got: the whole batch when every
   * handler succeeded, less when one did not — and 0 when none did,
   * which the broker reads as "not now" and retries with a backoff
   * rather than immediately.
   */
  async #handleMessages(messages, info, res, handler) {
    let took = 0;
    for (const m of messages) {
      try {
        await handler({ ...m, subject: info.subject, lag: info.lag });
      } catch (err) {
        this._emitError(err);
        break;
      }
      took = m.index;
    }
    res.set('X-Sukkal-Ack', String(took)).status(200).type('text/plain').send('');
  }

  /**
   * A job batch. Jobs finish out of order, so a high-water mark cannot
   * say which ones did: X-Sukkal-Done names them, and the broker returns
   * whatever the list omits. A delivery where nothing succeeded is a 500,
   * which returns all of them.
   */
  async #handleJobs(jobs, info, res, handler) {
    const done = [];
    for (const j of jobs) {
      try {
        await handler({ ...j, subject: info.subject, group: info.group });
        done.push(j.index);
      } catch (err) {
        this._emitError(err);
      }
    }
    if (done.length === 0) {
      res.status(500).type('text/plain').send('handler failed\n');
      return;
    }
    if (done.length < jobs.length) res.set('X-Sukkal-Done', done.join(','));
    res.status(200).type('text/plain').send('');
  }
}

const TIMED_OUT = Symbol('timed-out');

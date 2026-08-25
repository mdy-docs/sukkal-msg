/*
 * subscription.js — one registration, and what keeps it registered.
 *
 * A subscription holds no cursor. How far it has read is its read
 * receipt in the broker, which is why it survives a broker restart, is
 * visible to `sukkal consumers`, and counts against retention exactly
 * like any other subscription.
 */
import { randomBytes } from 'node:crypto';

/**
 * How often the subscription is re-asserted. Not a poll: it carries no
 * cursor and asks for nothing, it only re-states where to deliver. What
 * it buys is self-healing — a broker whose store was rebuilt has no
 * record of this subscription, and silence is indistinguishable from
 * "nothing to send".
 */
export const DEFAULT_HEARTBEAT_MS = 30000;

export function randomToken(bytes = 16) {
  return randomBytes(bytes).toString('hex');
}

export function generatedConsumer(prefix) {
  return `${prefix}-${randomBytes(8).toString('hex')}`;
}

export class Subscription {
  #client;
  #timer = null;
  #closed = false;

  constructor(client, {
    consumer, target, group, callback, token, params, ephemeral,
    heartbeatMs = DEFAULT_HEARTBEAT_MS,
  }) {
    this.#client = client;
    this.consumer = consumer;
    this.target = target;
    this.group = group ?? null;
    this.callback = callback;
    this.token = token;
    this.params = params;
    this.ephemeral = ephemeral;

    if (heartbeatMs > 0) {
      this.#timer = setInterval(() => this.#beat(), heartbeatMs);
      /* Never the reason the process stays alive — the receiver's own
       * server is. */
      this.#timer.unref?.();
    }
  }

  async #beat() {
    if (this.#closed) return;
    try {
      await this.#client._register(this);
    } catch (err) {
      this.#client._emitError(err, this);
    }
  }

  /**
   * Stop deliveries. A named subscription keeps its receipt, so it
   * resumes where it left off; a generated one purges it, because a
   * receipt holds retention off everything below it and an abandoned one
   * would pin the log for good.
   *
   * @param {object}  [opts]
   * @param {boolean} [opts.keep]  leave it registered, so the broker
   *                               goes on queueing for it
   */
  async close({ keep = false } = {}) {
    if (this.#closed) return;
    this.#closed = true;
    if (this.#timer) clearInterval(this.#timer);
    await this.#client._unregister(this, { keep });
  }

  get closed() {
    return this.#closed;
  }
}

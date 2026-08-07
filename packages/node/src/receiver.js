/*
 * receiver.js — the Express side: where the broker POSTs.
 *
 * bjmsg pushes. A subscription names a callback URL, the broker POSTs
 * each batch to it, and the HTTP response is the acknowledgement — so a
 * subscriber is a server, and this is that server.
 *
 * One Express app serves every subscription this client has, each on its
 * own path under `mountPath`. Routes are not added and removed as
 * subscriptions come and go (Express has no good way to remove one);
 * instead a single parameterised route dispatches from a table, which
 * also makes an unknown consumer a clean 404 rather than a stack trace.
 */
import http from 'node:http';

import express from 'express';

import { MEDIA_TYPE } from './transport.js';
import { parseDelivery, deliveryInfo } from './protocol.js';

const DEFAULT_BODY_LIMIT = '8mb';

export class Receiver {
  #app;
  #server;
  #own;                 /* we created the app, so we own its lifecycle */
  #routes = new Map();  /* consumer -> { token, handle } */

  /**
   * @param {object}  [opts]
   * @param {number}  [opts.port]       0 picks a free one
   * @param {string}  [opts.host]       what to bind; defaults to the
   *                                    address this host reaches the
   *                                    broker from
   * @param {string}  [opts.mountPath]  path prefix for delivery routes
   * @param {import('express').Express} [opts.app]
   *        an existing app to mount into, instead of creating one. You
   *        keep responsibility for listening and closing it.
   * @param {string|number} [opts.bodyLimit]
   */
  constructor({
    port = 0,
    host,
    mountPath = '/bjmsg',
    app = null,
    bodyLimit = DEFAULT_BODY_LIMIT,
  } = {}) {
    this.port = port;
    this.host = host;
    this.mountPath = mountPath.replace(/\/$/, '');
    this.#own = app === null;
    this.#app = app ?? express();

    /*
     * Raw bytes, not JSON: the body is binjson and Express must not try
     * to interpret it. `type` is the media type the broker sends, so any
     * other body parser the host app has installed is left alone.
     */
    this.#app.post(
      `${this.mountPath}/:consumer`,
      express.raw({ type: MEDIA_TYPE, limit: bodyLimit }),
      (req, res) => this.#deliver(req, res),
    );
  }

  /** The Express app, for mounting your own routes alongside. */
  get app() {
    return this.#app;
  }

  get listening() {
    return Boolean(this.#server?.listening);
  }

  /** Start listening. No-op when an existing app was supplied. */
  async listen(host = this.host) {
    if (!this.#own || this.#server) return this;
    this.#server = http.createServer(this.#app);
    /*
     * The broker holds this connection open and uses it whenever there
     * is something to send, which on a quiet subject may be a long time.
     * A short idle timeout would close a connection that is working
     * exactly as intended.
     */
    this.#server.keepAliveTimeout = 5 * 60 * 1000;
    this.#server.headersTimeout = 5 * 60 * 1000 + 1000;

    await new Promise((resolve, reject) => {
      this.#server.once('error', reject);
      this.#server.listen(this.port, host ?? '0.0.0.0', () => {
        this.#server.removeListener('error', reject);
        this.port = this.#server.address().port;
        resolve();
      });
    });
    return this;
  }

  async close() {
    this.#routes.clear();
    if (!this.#own || !this.#server) return;
    const server = this.#server;
    this.#server = null;
    await new Promise((resolve) => {
      server.close(resolve);
      /*
       * The broker holds its delivery connection open — that is the
       * design — and server.close() waits for every open connection to
       * end of its own accord. Without this it waits forever.
       */
      server.closeAllConnections?.();
    });
  }

  /** Where the broker should POST for `consumer`. */
  callbackUrl(consumer, advertisedHost) {
    const host = advertisedHost.includes(':') && !advertisedHost.startsWith('[')
      ? `[${advertisedHost}]`   /* an IPv6 literal needs brackets to carry a port */
      : advertisedHost;
    return `http://${host}:${this.port}${this.mountPath}/${consumer}`;
  }

  register(consumer, token, handle) {
    this.#routes.set(consumer, { token, handle });
  }

  unregister(consumer) {
    this.#routes.delete(consumer);
  }

  get size() {
    return this.#routes.size;
  }

  async #deliver(req, res) {
    const route = this.#routes.get(req.params.consumer);
    if (!route) {
      res.status(404).type('text/plain').send('no such subscription here\n');
      return;
    }

    /*
     * The token proves the POST came from the broker we registered with.
     * It is the only thing that does: anything that can reach this port
     * can connect to it, and a subscriber that took whatever arrived
     * would accept messages from anywhere.
     */
    if (route.token && req.get('authorization') !== `Bearer ${route.token}`) {
      res.status(401).type('text/plain').send('bad or missing bearer token\n');
      return;
    }

    if (!Buffer.isBuffer(req.body) || req.body.length === 0) {
      res.status(400).type('text/plain').send('empty delivery\n');
      return;
    }

    let messages;
    const info = deliveryInfo(req.headers);
    try {
      messages = parseDelivery(req.body);
    } catch {
      res.status(400).type('text/plain').send('malformed delivery\n');
      return;
    }

    await route.handle(messages, info, res);
  }
}

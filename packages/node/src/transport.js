/*
 * transport.js — talking to a broker over one kept-alive connection.
 *
 * node:http rather than fetch, for two reasons. The agent is pinned to a
 * single socket, so every request from this client to a given broker
 * reuses one TCP connection the way the C client does; and a response
 * exposes its socket, which is how we find out what local address this
 * host reaches the broker from — the address a callback has to name.
 */
import http from 'node:http';
import https from 'node:https';

import { decode } from 'binjson';

import { SukkalError, BrokerUnreachable } from './errors.js';

export const MEDIA_TYPE = 'application/binjson';

/* Failures that prove the request never reached the broker, so repeating
 * one cannot repeat an effect. */
const NEVER_ARRIVED = new Set([
  'ECONNREFUSED', 'ENOTFOUND', 'EAI_AGAIN', 'EHOSTUNREACH', 'ENETUNREACH',
]);

export class Transport {
  #agent;
  #mod;

  /**
   * @param {string} url    broker base URL, e.g. http://127.0.0.1:8080
   * @param {object} [opts]
   * @param {number} [opts.timeout]   per-request timeout in ms
   * @param {number} [opts.retryMs]   wait between attempts when the broker
   *                                  cannot be reached; 0 disables retrying
   */
  constructor(url, { timeout = 30000, retryMs = 5000 } = {}) {
    this.base = new URL(url);
    this.timeout = timeout;
    this.retryMs = retryMs;
    /*
     * The local address the broker sees us at, learned from the socket of
     * any response. Not guessed from the interface list: a host with
     * several addresses reaches different brokers from different ones,
     * and only the connection knows which.
     */
    this.localAddress = null;

    this.#mod = this.base.protocol === 'https:' ? https : http;
    /* maxSockets: 1 is the point — requests queue on one connection
     * instead of opening a second. */
    this.#agent = new this.#mod.Agent({ keepAlive: true, maxSockets: 1 });
  }

  close() {
    this.#agent.destroy();
  }

  /**
   * One request. Resolves to { status, headers, value } where `value` is
   * the decoded binjson body, or undefined when there is none.
   *
   * A non-2xx is thrown as a SukkalError carrying the broker's plain-text
   * explanation — success bodies are binjson and errors are text, which
   * is the whole of the protocol's error convention.
   */
  async request(method, path, { query, body, headers = {} } = {}) {
    /*
     * Built by hand rather than through URL, because URL percent-encodes
     * the path and the broker matches it raw — a wildcard subscription to
     * `eu.>` would arrive as `eu.%3E` and be rejected as a bad pattern.
     * Subject and pattern characters are all URL-safe anyway; only query
     * values need escaping, and the broker decodes those.
     */
    const search = new URLSearchParams();
    for (const [k, v] of Object.entries(query ?? {})) {
      if (v === undefined || v === null) continue;
      search.set(k, String(v));
    }
    const prefix = this.base.pathname.replace(/\/$/, '');
    const target = prefix + path + (search.size ? `?${search}` : '');

    for (;;) {
      try {
        return await this.#once(method, target, body, headers);
      } catch (err) {
        const retryable = NEVER_ARRIVED.has(err.code) ||
                          err.code === 'ECONNRESET' && method === 'GET';
        if (!retryable || this.retryMs <= 0) throw err;
        await new Promise((r) => setTimeout(r, this.retryMs));
      }
    }
  }

  #once(method, target, body, extraHeaders) {
    return new Promise((resolve, reject) => {
      const headers = { ...extraHeaders };
      if (body !== undefined && body !== null) {
        headers['Content-Type'] = MEDIA_TYPE;
        headers['Content-Length'] = body.length;
      } else if (method !== 'GET') {
        headers['Content-Length'] = 0;
      }

      const req = this.#mod.request(
        {
          agent: this.#agent,
          protocol: this.base.protocol,
          hostname: this.base.hostname,
          port: this.base.port,
          path: target,
          method,
          headers,
        },
        (res) => {
          if (res.socket?.localAddress) {
            /* ::ffff:10.0.0.1 is an IPv4 address wearing an IPv6 hat; a
             * callback URL wants the plain form. */
            this.localAddress = res.socket.localAddress
              .replace(/^::ffff:/, '');
          }
          const chunks = [];
          res.on('data', (c) => chunks.push(c));
          res.on('error', reject);
          res.on('end', () => {
            const raw = Buffer.concat(chunks);
            if (res.statusCode < 200 || res.statusCode >= 300) {
              reject(new SukkalError(
                raw.toString('utf8').trim() || `HTTP ${res.statusCode}`,
                { status: res.statusCode, method, url: this.base.origin + target },
              ));
              return;
            }
            let value;
            const type = res.headers['content-type'] ?? '';
            if (raw.length && type.startsWith(MEDIA_TYPE)) {
              try {
                value = decode(new Uint8Array(raw));
              } catch (cause) {
                reject(new SukkalError('broker sent undecodable binjson',
                                      { cause }));
                return;
              }
            }
            resolve({ status: res.statusCode, headers: res.headers, value });
          });
        },
      );

      req.setTimeout(this.timeout, () => {
        req.destroy(Object.assign(new Error('request timed out'),
                                  { code: 'ETIMEDOUT' }));
      });
      req.on('error', (err) => {
        reject(NEVER_ARRIVED.has(err.code)
          ? new BrokerUnreachable(this.base.origin, err)
          : err);
      });

      if (body !== undefined && body !== null) req.write(body);
      req.end();
    });
  }
}

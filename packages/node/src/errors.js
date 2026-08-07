/*
 * errors.js — what a caller can catch.
 *
 * The broker answers a failure with text/plain and a success with
 * binjson, so an error here always carries the broker's own sentence
 * about what was wrong rather than a status code alone.
 */

export class BjmsgError extends Error {
  constructor(message, { status, method, url, cause } = {}) {
    super(message, { cause });
    this.name = 'BjmsgError';
    this.status = status;
    this.method = method;
    this.url = url;
  }
}

/** The broker could not be reached at all — nothing was sent. */
export class BrokerUnreachable extends BjmsgError {
  constructor(origin, cause) {
    super(`cannot reach the broker at ${origin}: ${cause.message}`, { cause });
    this.name = 'BrokerUnreachable';
    this.code = cause.code;
  }
}

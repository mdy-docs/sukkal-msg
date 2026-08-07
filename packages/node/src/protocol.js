/*
 * protocol.js — the shapes on the wire.
 *
 * A delivery body is the entry log's own batch encoding, forwarded by the
 * broker without re-encoding: an ARRAY of { index, term, type, payload }
 * where `payload` is BINARY holding the message's encoded bytes. So a
 * delivery decodes in two steps, and the second one is the caller's
 * message exactly as it was published.
 */
import { encode, decode } from 'binjson';

/** The entry log's type byte, which says what the payload is. */
export const ENTRY = {
  /** The payload is the message, byte-for-byte as published. */
  PLAIN: 0x01,
  /** The payload is [ headers, message ]. */
  ENVELOPE: 0x10,
};

/**
 * The message inside one batch entry. Headers are null unless the
 * publisher sent any — they live behind the type byte rather than in a
 * universal wrapper, so a headerless message costs nothing.
 */
export function unwrap(entry) {
  const value = decode(entry.payload);
  if (entry.type === ENTRY.ENVELOPE &&
      Array.isArray(value) && value.length === 2) {
    return { headers: value[0] ?? null, value: value[1] };
  }
  return { headers: null, value };
}

/** Encode a message, with headers if there are any. */
export function encodeMessage(value, headers) {
  if (headers === undefined || headers === null) {
    return { body: Buffer.from(encode(value)), enveloped: false };
  }
  return { body: Buffer.from(encode([headers, value])), enveloped: true };
}

/**
 * Turn a delivery body into messages. Tolerates both shapes the broker
 * sends: a subscription batch and a queue-group take, which differ only
 * in carrying `term` versus `attempts` / `expires_ms`.
 */
export function parseDelivery(buffer) {
  const entries = decode(new Uint8Array(buffer));
  if (!Array.isArray(entries)) {
    throw new Error('delivery body is not a batch');
  }
  return entries.map((e) => {
    const { headers, value } = unwrap(e);
    return {
      index: Number(e.index),
      type: e.type,
      value,
      headers,
      /** The message's encoded bytes, for passing on untouched. */
      raw: e.payload,
      ...(e.term !== undefined ? { term: Number(e.term) } : {}),
      ...(e.attempts !== undefined ? { attempts: Number(e.attempts) } : {}),
      ...(e.expires_ms ? { expiresAt: new Date(Number(e.expires_ms)) } : {}),
    };
  });
}

/** Delivery metadata the broker puts in headers so nothing has to be
 * decoded to read it. */
export function deliveryInfo(headers) {
  const num = (h) => (headers[h] === undefined ? undefined : Number(headers[h]));
  return {
    subject: headers['x-bjmsg-subject'],
    consumer: headers['x-bjmsg-consumer'],
    group: headers['x-bjmsg-group'],
    count: num('x-bjmsg-count'),
    firstIndex: num('x-bjmsg-first-index'),
    lastIndex: num('x-bjmsg-last-index'),
    /** How many messages are still waiting behind this batch. */
    lag: num('x-bjmsg-lag'),
  };
}

/** Names are file names on the broker, so they are restricted. */
const NAME = /^(?!\.)(?!.*\.\.)(?!.*\.$)[A-Za-z0-9_.-]{1,128}$/;

export function isValidSubject(s) {
  return typeof s === 'string' && NAME.test(s);
}

export function isValidName(s) {
  return typeof s === 'string' && NAME.test(s);
}

/**
 * Patterns match token-wise on '.': '*' is one token, '>' is this and
 * everything below and may only be last. Neither character is legal in a
 * subject, so a pattern can never be mistaken for one.
 */
export function isValidPattern(s) {
  if (typeof s !== 'string' || s.length === 0 || s.length > 128) return false;
  const tokens = s.split('.');
  return tokens.every((t, i) => {
    if (t === '>') return i === tokens.length - 1;
    if (t === '*') return true;
    return /^[A-Za-z0-9_-]+$/.test(t);
  });
}

export function isPattern(s) {
  return typeof s === 'string' && (s.includes('*') || s.includes('>'));
}

/** Accepts either, which is what every subscribe route does. */
export function isValidTarget(s) {
  return isPattern(s) ? isValidPattern(s) : isValidSubject(s);
}

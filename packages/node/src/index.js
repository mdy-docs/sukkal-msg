/*
 * bjmsg — publish/subscribe over HTTP/1.1 with binjson payloads.
 *
 * Messages are pushed, never polled: a subscription names a callback URL
 * the broker POSTs to, and the reply to that POST is the
 * acknowledgement. So a subscriber is an HTTP server, which is what
 * Express is here for.
 */
export { Client } from './client.js';
export { Receiver } from './receiver.js';
export { Subscription, DEFAULT_HEARTBEAT_MS } from './subscription.js';
export { BjmsgError, BrokerUnreachable } from './errors.js';
export {
  ENTRY, unwrap, encodeMessage, parseDelivery,
  isValidSubject, isValidPattern, isValidTarget, isPattern,
} from './protocol.js';

/** Re-exported so callers can encode a payload without a second binjson. */
export { encode, decode, ObjectId } from 'binjson';

import { Client } from './client.js';

/** Shorthand for `new Client({ url })`. */
export function connect(options) {
  return new Client(typeof options === 'string' ? { url: options } : options);
}

export default { Client, connect };

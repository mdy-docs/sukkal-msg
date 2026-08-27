import { test } from 'node:test';
import assert from 'node:assert/strict';

import { Broker } from '../wasm/broker.js';
import { MemoryStorageProvider, nodeStorageProvider, encode, decode } from '../wasm/sukkal-wasm.js';

/*
 * Phase 5 of docs/wasm-plan.md: the broker as a function call.
 *
 * Every request here goes through the same routing table `sukkal serve`
 * answers from — src/server.c, one definition — with no socket, no port,
 * nothing serialised and no HTTP anywhere. What the native binary does
 * over a connection, this does by calling it.
 */

const open = async (provider) => new Broker(provider ?? new MemoryStorageProvider()).open();
const publish = (b, subject, value) =>
  b.request('POST', `/pub/${subject}`, { body: encode(value), contentType: 'application/binjson' });

test('a broker answers /health, and says what is carrying it', async () => {
  const b = await open();
  try {
    const r = await b.request('GET', '/health');
    assert.equal(r.status, 200);
    const health = decode(r.body);
    assert.equal(health.ok, true);
    // Two facts only a transport knows. There is no transport, and it says
    // so rather than inventing them (Phase 2).
    assert.equal(health.backend, 'wasm');
    assert.equal(health.connections, 0);
  } finally { b.close(); }
});

test('publish and subscribe, in process', async () => {
  const b = await open();
  try {
    const published = [{ id: 'a-1001', who: 'Ada' }, { id: 'b-1002', who: 'Grace' }];
    for (const [i, v] of published.entries()) {
      const r = await publish(b, 'orders.eu', v);
      assert.equal(r.status, 200);
      // Indexes are the log's own — contiguous, assigned by it.
      assert.deepEqual(decode(r.body), { subject: 'orders.eu', index: i + 1 });
    }

    const sub = await b.request('GET', '/sub/orders.eu', { query: 'from=1' });
    assert.equal(sub.status, 200);
    assert.equal(sub.headers['x-sukkal-count'], '2');
    assert.deepEqual(decode(sub.body).map((e) => decode(e.payload)), published);
  } finally { b.close(); }
});

test('a subscribe to an unknown subject is 404, and does not conjure it', async () => {
  // The contract that made the plan carry a create flag: a host opening
  // every planned name with create:true would answer 200 here and bring
  // the subject into being.
  const b = await open();
  try {
    assert.equal((await b.request('GET', '/sub/never-published')).status, 404);
    assert.deepEqual(decode((await b.request('GET', '/subjects')).body), []);

    await publish(b, 'orders.eu', { id: 1 });
    assert.deepEqual(decode((await b.request('GET', '/subjects')).body), ['orders.eu']);
  } finally { b.close(); }
});

test('a dead-letter channel does not exist until something dies', async () => {
  const b = await open();
  try {
    await publish(b, 'jobs', { work: 1 });
    const subjects = decode((await b.request('GET', '/subjects')).body);
    assert.deepEqual(subjects, ['jobs'], 'no phantom jobs.dead');
    assert.deepEqual(decode((await b.request('GET', '/dead/jobs')).body), [],
      'empty, not 404, when nothing has died');
  } finally { b.close(); }
});

test('the query string is read without a transport to parse it', async () => {
  // http11c parses queries natively; a direct call has only the string,
  // so sukkal_query_from_string reads it — percent-decoding included.
  const b = await open();
  try {
    for (let i = 0; i < 3; i++) await publish(b, 'orders.eu', { n: i });
    const from2 = decode((await b.request('GET', '/sub/orders.eu', { query: 'from=2' })).body);
    assert.deepEqual(from2.map((e) => decode(e.payload).n), [1, 2]);

    const filtered = decode((await b.request('GET', '/subjects', { query: 'pattern=orders.%3E' })).body);
    assert.deepEqual(filtered, ['orders.eu'], 'the %3E decoded to > and matched');
  } finally { b.close(); }
});

test('the routing table is the one the native binary uses', async () => {
  const b = await open();
  try {
    assert.equal((await b.request('GET', '/nope')).status, 404);
    // 405 rather than 404 for a known path with the wrong verb — the
    // distinction http11c used to make for us (Phase 2).
    assert.equal((await b.request('DELETE', '/health')).status, 405);
  } finally { b.close(); }
});

test('trim refuses without an atomic replace, rather than risking the subject', async () => {
  // There is no rename here, and bjns will not pretend otherwise. Refusing
  // trades "the log stayed too long" for nothing; improvising would trade
  // it for "the subject is gone" (Phase 1).
  const b = await open();
  try {
    await publish(b, 'orders.eu', { id: 1 });
    const r = await b.request('POST', '/trim/orders.eu', { query: 'keep=1' });
    assert.notEqual(r.status, 200);
  } finally { b.close(); }
});

test('a broker on the node provider survives being closed and reopened', async () => {
  // The headline: durable messaging with no binary anywhere. Two of the
  // three providers keep their messages; which one is used is the
  // caller's choice, not the platform's.
  const { mkdtemp, rm } = await import('node:fs/promises');
  const { tmpdir } = await import('node:os');
  const { join } = await import('node:path');

  const dir = await mkdtemp(join(tmpdir(), 'sukkal-broker-'));
  try {
    const first = await open(await nodeStorageProvider(dir));
    await publish(first, 'orders.eu', { id: 'a-1001' });
    await publish(first, 'orders.eu', { id: 'b-1002' });
    first.close();

    const second = await open(await nodeStorageProvider(dir));
    try {
      const info = decode((await second.request('GET', '/info/orders.eu')).body);
      assert.equal(info.last, 2, 'the log knows where it got to');

      const sub = decode((await second.request('GET', '/sub/orders.eu', { query: 'from=1' })).body);
      assert.deepEqual(sub.map((e) => decode(e.payload).id), ['a-1001', 'b-1002']);

      // ...and it goes on from there rather than starting over.
      assert.equal(decode((await publish(second, 'orders.eu', { id: 'c-1003' })).body).index, 3);
    } finally { second.close(); }
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});

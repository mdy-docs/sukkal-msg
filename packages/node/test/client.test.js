import test from 'node:test';
import assert from 'node:assert/strict';

import { Client, SukkalError } from '../src/index.js';
import { startBroker, waitFor, sleep, hasBroker, BROKER } from './helpers.js';

if (!hasBroker) {
  test('broker binary is missing', { skip: `run \`make\` first — no ${BROKER}` }, () => {});
} else {

let broker;
let client;

test.before(async () => { broker = await startBroker(); });
test.after(async () => { await broker.stop(); });

test.beforeEach(() => { client = new Client({ url: broker.url, heartbeatMs: 0 }); });
test.afterEach(async () => { await client.close(); });

test('publish returns the assigned index', async () => {
  const a = await client.publish('t.pub', 'first');
  const b = await client.publish('t.pub', { n: 2 });
  assert.equal(a.subject, 't.pub');
  assert.equal(a.index, 1);
  assert.equal(b.index, 2);
});

test('round-trips every binjson shape', async () => {
  const got = [];
  await client.subscribe('t.shapes', (m) => { got.push(m.value); }, { tail: true });

  const sent = [
    'a string', 42, -1.5, true, false, null,
    { nested: { deep: [1, 2, 3] } },
    ['mixed', 1, null, { k: 'v' }],
  ];
  for (const v of sent) await client.publish('t.shapes', v);

  await waitFor(() => got.length === sent.length, 5000,
                () => `got ${got.length} of ${sent.length}: ${JSON.stringify(got)}`);
  assert.deepEqual(got, sent);
});

test('messages arrive pushed, in order, with metadata', async () => {
  const seen = [];
  await client.subscribe('t.order', (m) => { seen.push(m); }, { tail: true });

  for (let i = 1; i <= 20; i++) await client.publish('t.order', i);
  await waitFor(() => seen.length === 20);

  assert.deepEqual(seen.map((m) => m.value), Array.from({ length: 20 }, (_, i) => i + 1));
  assert.deepEqual(seen.map((m) => m.index), Array.from({ length: 20 }, (_, i) => i + 1));
  assert.equal(seen[0].subject, 't.order');
  assert.equal(seen[0].headers, null);
});

test('delivery is prompt, not polled', async () => {
  let at = 0;
  await client.subscribe('t.fast', () => { at = Date.now(); }, { tail: true });
  await sleep(100);

  const sent = Date.now();
  await client.publish('t.fast', 'now');
  await waitFor(() => at > 0);
  assert.ok(at - sent < 250, `took ${at - sent}ms — that smells like polling`);
});

test('headers survive as an envelope', async () => {
  const got = [];
  await client.subscribe('t.hdr', (m) => { got.push(m); }, { tail: true });

  await client.publish('t.hdr', 'body', { headers: { trace: 'abc', n: 7 } });
  await waitFor(() => got.length === 1);

  assert.equal(got[0].value, 'body');
  assert.deepEqual(got[0].headers, { trace: 'abc', n: 7 });
});

test('wildcards match token-wise and carry the subject', async () => {
  const got = [];
  await client.subscribe('eu.>', (m) => { got.push(`${m.subject}=${m.value}`); },
                         { tail: true });

  await client.publish('eu.de', 'hallo');
  await client.publish('eu.fr.paris', 'bonjour');
  await client.publish('us.en', 'hi');

  await waitFor(() => got.length === 2);
  await sleep(200);   // and no third
  assert.deepEqual(got.sort(), ['eu.de=hallo', 'eu.fr.paris=bonjour']);
});

test('a named consumer resumes where it left off', async () => {
  await client.publish('t.durable', 'one');
  await client.publish('t.durable', 'two');

  const first = [];
  const sub = await client.subscribe('t.durable', (m) => { first.push(m.value); },
                                     { consumer: 'reader' });
  await waitFor(() => first.length === 2);
  await sub.close();

  await client.publish('t.durable', 'three');

  const second = [];
  const sub2 = await client.subscribe('t.durable', (m) => { second.push(m.value); },
                                      { consumer: 'reader' });
  await waitFor(() => second.length === 1);
  assert.deepEqual(second, ['three']);
  await sub2.close();

  assert.deepEqual(await client.consumers('t.durable'),
                   [{ consumer: 'reader', acked: 3, lag: 0 }]);
});

test('a throwaway subscription takes its position with it', async () => {
  await client.publish('t.ephemeral', 'x');
  const sub = await client.subscribe('t.ephemeral', () => {}, { tail: true });
  await sub.close();
  assert.deepEqual(await client.consumers('t.ephemeral'), []);
  assert.deepEqual(await client.pushes(), []);
});

test('a throwing handler refuses the message and it comes back', async () => {
  client.on('error', () => {});   // expected

  let attempts = 0;
  let pass = false;
  await client.subscribe('t.retry', (m) => {
    attempts++;
    if (!pass) throw new Error('not yet');
    assert.equal(m.value, 'work');
  }, { consumer: 'flaky' });

  await client.publish('t.retry', 'work');
  await waitFor(() => attempts >= 2, 5000, () => `only ${attempts} attempt(s)`);
  /* Refused, so nothing is acknowledged and the message is still owed. */
  assert.equal((await client.consumers('t.retry'))[0]?.acked ?? 0, 0);

  pass = true;
  await waitFor(async () => (await client.consumers('t.retry'))[0]?.acked === 1,
                10000, () => `never acked after ${attempts} attempt(s)`);
});

test('idempotent publish collapses a repeat', async () => {
  const a = await client.publish('t.idem', 'once', { id: 'key-1' });
  const b = await client.publish('t.idem', 'once', { id: 'key-1' });
  assert.equal(a.duplicate, false);
  assert.equal(b.duplicate, true);
  assert.equal(b.index, a.index);
  assert.equal((await client.info('t.idem')).messages, 1);
});

test('queue groups: each job runs once across competing workers', async () => {
  const ran = [];
  const a = new Client({ url: broker.url, heartbeatMs: 0 });
  const b = new Client({ url: broker.url, heartbeatMs: 0 });
  try {
    await a.work('t.jobs', (j) => { ran.push(['a', j.value]); }, { group: 'crew' });
    await b.work('t.jobs', (j) => { ran.push(['b', j.value]); }, { group: 'crew' });

    for (let i = 1; i <= 12; i++) await client.publish('t.jobs', i);
    await waitFor(() => ran.length === 12, 8000,
                  () => `only ${ran.length} of 12 ran`);
    await sleep(200);

    assert.equal(ran.length, 12, 'no job ran twice');
    assert.deepEqual(ran.map((r) => r[1]).sort((x, y) => x - y),
                     Array.from({ length: 12 }, (_, i) => i + 1));
    assert.ok(ran.some((r) => r[0] === 'a') && ran.some((r) => r[0] === 'b'),
              'both workers took a share');
  } finally {
    await a.close();
    await b.close();
  }
});

test('a failing job comes back, and dead-letters at max attempts', async () => {
  client.on('error', () => {});
  await client.configureQueue('t.poison', 'p',
                              { maxAttempts: 2, backoffMs: 1, leaseMs: 2000 });
  await client.publish('t.poison', 'bad');

  let tries = 0;
  await client.work('t.poison', () => { tries++; throw new Error('nope'); },
                    { group: 'p' });

  await waitFor(async () => (await client.dead('t.poison')).length === 1, 20000,
                () => `${tries} attempt(s), nothing dead-lettered`);
  assert.equal(tries, 2);

  const [dead] = await client.dead('t.poison');
  const envelope = (await import('binjson')).decode(dead.payload);
  assert.equal(envelope.subject, 't.poison');
  assert.equal(envelope.group, 'p');
});

test('request-reply matches on correlation', async () => {
  const responder = new Client({ url: broker.url, heartbeatMs: 0 });
  try {
    await responder.reply('t.rpc', (m) => String(m.value).toUpperCase());

    assert.equal(await client.request('t.rpc', 'hello'), 'HELLO');

    /* Concurrent requesters share one reply subject; each must get its
     * own answer back. */
    const answers = await Promise.all(
      ['one', 'two', 'three', 'four'].map((w) => client.request('t.rpc', w)),
    );
    assert.deepEqual(answers, ['ONE', 'TWO', 'THREE', 'FOUR']);
  } finally {
    await responder.close();
  }
});

test('request times out when nobody answers', async () => {
  await assert.rejects(
    () => client.request('t.silent', 'anyone?', { timeoutMs: 300 }),
    (err) => err instanceof SukkalError && /within 300ms/.test(err.message),
  );
});

test('pipe transforms one subject into another, effectively-once', async () => {
  const stage = new Client({ url: broker.url, heartbeatMs: 0 });
  const out = [];
  try {
    for (const w of ['alpha', 'beta']) await client.publish('t.raw', w);

    await stage.pipe('t.raw', (m) => String(m.value).toUpperCase(),
                     { to: 't.upper', consumer: 'stage1' });
    await client.subscribe('t.upper', (m) => { out.push(m.value); });

    await client.publish('t.raw', 'gamma');
    await waitFor(() => out.length === 3, 5000, () => JSON.stringify(out));
    assert.deepEqual(out, ['ALPHA', 'BETA', 'GAMMA']);

    /* Restarting the stage must not republish what it already did. */
    await stage.close();
    const again = new Client({ url: broker.url, heartbeatMs: 0 });
    await again.pipe('t.raw', (m) => String(m.value).toUpperCase(),
                     { to: 't.upper', consumer: 'stage1' });
    await client.publish('t.raw', 'delta');
    await waitFor(() => out.length === 4);
    await sleep(300);
    assert.deepEqual(out, ['ALPHA', 'BETA', 'GAMMA', 'DELTA']);
    await again.close();
  } finally {
    await stage.close().catch(() => {});
  }
});

test('a dropped message still acknowledges the input', async () => {
  const stage = new Client({ url: broker.url, heartbeatMs: 0 });
  try {
    await client.publish('t.filter', 'keep');
    await client.publish('t.filter', 'drop');
    await client.publish('t.filter', 'keep2');

    await stage.pipe('t.filter', (m) => (m.value === 'drop' ? undefined : m.value),
                     { to: 't.kept', consumer: 'filt' });

    await waitFor(async () =>
      (await client.info('t.kept').catch(() => null))?.messages === 2);
    await waitFor(async () => (await client.consumers('t.filter'))[0]?.acked === 3);
  } finally {
    await stage.close();
  }
});

test('errors carry the broker\'s own explanation', async () => {
  await assert.rejects(
    () => client.info('no.such.subject'),
    (err) => err instanceof SukkalError && err.status === 404,
  );
  await assert.rejects(
    () => client.publish('not a valid subject', 'x'),
    (err) => err instanceof SukkalError && /invalid subject/.test(err.message),
  );
});

test('retention and trimming', async () => {
  for (let i = 1; i <= 10; i++) await client.publish('t.trim', i);
  const res = await client.trim('t.trim', { keep: 3 });
  assert.equal(res.removed, 7);
  assert.equal((await client.info('t.trim')).messages, 3);

  await client.setPolicy('t.trim', { maxMessages: 2 });
  const p = await client.policy('t.trim');
  assert.equal(p.max_messages, 2);
});

test('one client, many subscriptions, one Express app', async () => {
  const seen = { a: 0, b: 0, c: 0 };
  await client.subscribe('multi.a', () => { seen.a++; }, { tail: true });
  await client.subscribe('multi.b', () => { seen.b++; }, { tail: true });
  await client.subscribe('multi.c', () => { seen.c++; }, { tail: true });

  await client.publish('multi.a', 1);
  await client.publish('multi.b', 1);
  await client.publish('multi.c', 1);
  await waitFor(() => seen.a && seen.b && seen.c);

  assert.equal((await client.pushes()).length, 3);
  assert.equal(client.receiver.size, 3);
});

test('a delivery without the token is refused', async () => {
  await client.subscribe('t.auth', () => {}, { consumer: 'guarded' });
  const url = (await client.pushes()).find((p) => p.consumer === 'guarded').callback;

  const res = await fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/binjson' },
    body: new Uint8Array([5, 1, 0, 0, 0, 120]),
  });
  assert.equal(res.status, 401);
});

}

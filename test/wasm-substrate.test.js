import { test } from 'node:test';
import assert from 'node:assert/strict';

import {
  ready, EntryLog, MemoryHandle, encode, decode,
  MemoryStorageProvider, nodeStorageProvider,
} from '../wasm/sukkal-wasm.js';

/*
 * Phase 0 of docs/wasm-plan.md: prove the substrate before porting anything
 * onto it.
 *
 * The plan claims sukkal's own four C files are the whole job, because the
 * structures underneath it already run in WASM. That claim was made from a
 * file listing, which is not evidence. This is: a subject IS an entry log —
 * the README calls that the decision most of the broker follows from — so an
 * entry log that appends, syncs, reads back and survives a reopen, in WASM,
 * over a host-supplied handle, is a subject that does.
 *
 * No sukkal C is linked yet (wasm/sources.txt is empty on purpose). Nothing
 * here is the broker; everything here is what the broker is built out of.
 */

test('a subject is an entry log, and it runs in WASM', async () => {
  await ready();

  const log = new EntryLog(new MemoryHandle());
  await log.open({ create: true });

  const published = [
    { id: 'a-1001', customer: 'Ada Lovelace', total: 4500 },
    { id: 'b-1002', customer: 'Grace Hopper', total: 1200.5 },
    { id: 'c-1003', customer: 'Katherine Johnson', total: 900 },
  ];

  // Indexes are the log's own — contiguous, assigned by it, starting at 1.
  // That is where a message id comes from; the broker never invents one.
  const indexes = published.map((value) => log.append(0, encode(value)));
  assert.deepEqual(indexes, [1, 2, 3]);
  assert.equal(log.lastIndex, 3);

  log.sync();

  // Payloads are opaque bytes the log never interprets, so a binjson message
  // passes through unexamined and comes back byte-identical.
  const batch = log.getBatch(1);
  assert.equal(batch.length, 3);
  assert.deepEqual(batch.map((e) => decode(e.payload)), published);
  assert.deepEqual(batch.map((e) => e.index), [1, 2, 3]);

  log.close();
});

test('getBatch is bounded by bytes, not by count', async () => {
  // Worth pinning down, because it explains something that looks like a bug
  // from outside: a subscribe against the native broker answers
  // X-Sukkal-Count: 1 for a batch that has more waiting. The limit is a byte
  // budget and it always yields at least one entry.
  await ready();

  const log = new EntryLog(new MemoryHandle());
  await log.open({ create: true });
  for (let i = 0; i < 5; i++) log.append(0, encode({ n: i, filler: 'x'.repeat(64) }));
  log.sync();

  assert.equal(log.getBatch(1, 10).length, 1, 'a tiny budget still yields one');
  assert.equal(log.getBatch(1).length, 5, 'the default budget takes them all');

  log.close();
});

test('the log survives being closed and reopened on the same handle', async () => {
  // Durability, which is the whole difference between a broker and a queue
  // in a variable. MemoryHandle keeps its bytes for the process lifetime, so
  // this exercises the reopen path — reading the header back and recovering
  // lastIndex — without needing a filesystem.
  await ready();

  const handle = new MemoryHandle();
  const first = new EntryLog(handle);
  await first.open({ create: true });
  first.append(0, encode({ order: 'a-1001' }));
  first.append(0, encode({ order: 'b-1002' }));
  first.sync();
  first.close();

  const reopened = new EntryLog(handle);
  await reopened.open();
  assert.equal(reopened.lastIndex, 2, 'the log knows where it got to');

  const batch = reopened.getBatch(1);
  assert.deepEqual(batch.map((e) => decode(e.payload).order), ['a-1001', 'b-1002']);

  // ...and it goes on from there rather than starting over.
  assert.equal(reopened.append(0, encode({ order: 'c-1003' })), 3);
  reopened.close();
});

/*
 * Phase 4: storage providers, shared rather than written.
 *
 * A provider answers "where do the bytes live" in four methods that know
 * nothing about what is stored — which is why bjns and a JS provider turn
 * out to be the same abstraction from two sides, and why these live in
 * binjson-structures beside bjns and hostio rather than inside a database
 * or a broker.
 */

test('a subject stored through a provider, not a bare handle', async () => {
  await ready();

  const store = new MemoryStorageProvider();
  const log = new EntryLog(await store.openFile('orders.elog', { create: true }));
  await log.open({ create: true });
  log.append(0, encode({ id: 'a-1001' }));
  log.sync();

  assert.deepEqual(await store.listFiles(), ['orders.elog']);

  // Reopening by NAME is the thing a provider adds over a handle: the
  // broker knows subjects by name and has to find them again.
  log.close();
  const reopened = new EntryLog(await store.openFile('orders.elog'));
  await reopened.open();
  assert.equal(decode(reopened.getBatch(1)[0].payload).id, 'a-1001');
  reopened.close();
});

test('subjects are isolated by subProvider, as separate scopes', async () => {
  await ready();
  const root = new MemoryStorageProvider();
  const a = await root.subProvider('tenant-a');
  const b = await root.subProvider('tenant-b');

  await a.openFile('orders.elog', { create: true });
  assert.deepEqual(await a.listFiles(), ['orders.elog']);
  assert.deepEqual(await b.listFiles(), [], 'one scope cannot see another');
  // Cached: the same name is the same scope, not a new one each time.
  assert.equal(await root.subProvider('tenant-a'), a);
});

test('the node provider is durable — a subject survives the process', async () => {
  // The distinction that matters and is easy to state backwards: it is not
  // "browser means OPFS, node means memory". Two of the three providers
  // are durable, all three run wherever their platform does, and which one
  // is used is the caller's choice.
  await ready();
  const { mkdtemp, rm, readdir } = await import('node:fs/promises');
  const { tmpdir } = await import('node:os');
  const { join } = await import('node:path');

  const dir = await mkdtemp(join(tmpdir(), 'sukkal-provider-'));
  try {
    const store = await nodeStorageProvider(dir);
    const log = new EntryLog(await store.openFile('greet.elog', { create: true }));
    await log.open({ create: true });
    log.append(0, encode({ hello: 'world' }));
    log.sync();
    log.close();

    // On disk, under the name the broker would use.
    assert.deepEqual(await readdir(dir), ['greet.elog']);

    // ...and readable by a provider that never saw the first one.
    const fresh = await nodeStorageProvider(dir);
    const reread = new EntryLog(await fresh.openFile('greet.elog'));
    await reread.open();
    assert.deepEqual(decode(reread.getBatch(1)[0].payload), { hello: 'world' });
    reread.close();
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});

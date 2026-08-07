/*
 * A publisher. Sends a short series and exits.
 *
 *   node examples/publish.js
 *   node examples/publish.js "any message you like"
 */
import { Client } from '../src/index.js';

const client = new Client({ url: process.env.BJMSG_URL ?? 'http://127.0.0.1:8080' });

const messages = process.argv.slice(2).length
  ? process.argv.slice(2)
  : ['Hello NATS!', '...except this one is binjson over HTTP/1.1',
     { shape: 'anything binjson encodes', n: 3 }, 'goodbye'];

for (const m of messages) {
  const { index } = await client.publish('greet', m);
  console.log(`#${index}`, JSON.stringify(m));
}

/*
 * An idempotency key makes a publish safe to retry: a repeat inside the
 * broker's dedup window returns the original index rather than appending
 * a second copy.
 */
const a = await client.publish('greet', 'exactly once', { id: 'demo-key' });
const b = await client.publish('greet', 'exactly once', { id: 'demo-key' });
console.log(`same message twice -> index ${a.index} and ${b.index}`,
            `(duplicate: ${b.duplicate})`);

await client.close();

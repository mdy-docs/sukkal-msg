/*
 * A subscriber. Run the broker and the publisher alongside:
 *
 *   ../../bin/bjmsg serve &
 *   node examples/subscribe.js &
 *   node examples/publish.js
 */
import { Client } from '../src/index.js';

const client = new Client({ url: process.env.BJMSG_URL ?? 'http://127.0.0.1:8080' });

/* A named consumer makes it durable: stop this process, publish more,
 * start it again, and the broker delivers exactly what was missed. Drop
 * `consumer` for a throwaway subscription that forgets on exit. */
const sub = await client.subscribe('greet', (msg) => {
  console.log(`#${msg.index}`, msg.value, msg.headers ?? '');
}, { consumer: 'example-logger' });

console.log(`listening on ${sub.callback}`);
console.log('Ctrl-C to stop.');

/* Deliveries that fail land here rather than crashing the process. */
client.on('error', (err) => console.error('bjmsg:', err.message));

for (const sig of ['SIGINT', 'SIGTERM']) {
  process.on(sig, async () => {
    await client.close();
    process.exit(0);
  });
}

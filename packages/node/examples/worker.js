/*
 * A queue-group worker. Each job goes to exactly one member of the
 * group, so run several of these and they compete:
 *
 *   node examples/worker.js &
 *   node examples/worker.js &
 *   node -e "import('./src/index.js').then(async ({Client}) => {
 *     const c = new Client();
 *     for (let i = 1; i <= 10; i++) await c.publish('jobs', i);
 *     await c.close();
 *   })"
 */
import { Client } from '../src/index.js';

const client = new Client({ url: process.env.SUKKAL_URL ?? 'http://127.0.0.1:8080' });

/*
 * A job whose worker dies is redelivered when its lease expires, so a
 * handler must tolerate running twice — `attempts` above 1 is the warning
 * that it is seeing one again. After maxAttempts it goes to `jobs.dead`
 * rather than starving the queue.
 */
await client.configureQueue('jobs', 'crew', {
  leaseMs: 30000,
  maxAttempts: 5,
  backoffMs: 1000,
});

await client.work('jobs', async (job) => {
  if (job.attempts > 1) console.warn(`retrying #${job.index} (attempt ${job.attempts})`);
  console.log(`working #${job.index}:`, job.value);
  await new Promise((r) => setTimeout(r, 250));

  /* Throwing returns the job to the queue, due again after the group's
   * backoff. Returning finishes it. */
  if (job.value === 'poison') throw new Error('cannot handle that');

  console.log(`done #${job.index}`);
}, { group: 'crew' });

console.log('taking jobs from the "crew" group. Ctrl-C to stop.');
client.on('error', (err) => console.error('sukkal:', err.message));

for (const sig of ['SIGINT', 'SIGTERM']) {
  process.on(sig, async () => {
    await client.close();
    process.exit(0);
  });
}

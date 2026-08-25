/*
 * A real broker for the tests to talk to. There is no mock: the whole
 * point of this library is interoperating with the C broker's wire
 * format, and a mock would only assert that we agree with ourselves.
 */
import { spawn } from 'node:child_process';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { existsSync } from 'node:fs';

const here = dirname(fileURLToPath(import.meta.url));
export const BROKER = join(here, '..', '..', '..', 'bin', 'sukkal');

export const hasBroker = existsSync(BROKER);

export async function startBroker({ port = 0 } = {}) {
  const dir = await mkdtemp(join(tmpdir(), 'sukkal-node-'));
  /* Port 0 would leave us guessing, so pick one in the ephemeral range
   * and let a collision surface as a failure to start. */
  const chosen = port || 20000 + Math.floor(Math.random() * 20000);

  const proc = spawn(BROKER, ['serve', '--port', String(chosen), '--dir', join(dir, 'data')],
                     { stdio: ['ignore', 'pipe', 'pipe'] });
  const logs = [];
  proc.stdout.on('data', (d) => logs.push(String(d)));
  proc.stderr.on('data', (d) => logs.push(String(d)));

  const url = `http://127.0.0.1:${chosen}`;
  await waitFor(async () => {
    const res = await fetch(`${url}/health`).catch(() => null);
    return res?.ok;
  }, 5000, () => `broker did not start:\n${logs.join('')}`);

  return {
    url,
    port: chosen,
    logs,
    async stop() {
      proc.kill('SIGINT');
      await new Promise((r) => proc.once('exit', r));
      await rm(dir, { recursive: true, force: true });
    },
  };
}

export async function waitFor(predicate, timeoutMs = 5000, message = () => 'timed out') {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    if (await predicate()) return;
    if (Date.now() > deadline) throw new Error(message());
    await sleep(20);
  }
}

export const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

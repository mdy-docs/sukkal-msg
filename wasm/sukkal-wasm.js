/*
 * sukkal-wasm.js — the WASM module and the structures bound to it.
 *
 * Phase 0 of docs/wasm-plan.md, and deliberately thin: none of sukkal's own
 * C is linked yet. What this proves is the claim the whole plan rests on —
 * that a subject, which is an entry log, already runs in WASM over a
 * host-supplied file handle, so the work ahead is sukkal's four files and
 * not the substrate under them.
 *
 * Three pieces, none of them copied:
 *
 *   - the module        lib/sukkal.wasm.mjs, one combined build of binjson +
 *                       binjson-structures (+ sukkal's own sources as they
 *                       stop needing POSIX). See build-wasm.sh.
 *   - the structures    binjson-structures' own bindStructures(), which
 *                       returns EntryLog/BPlusTree/... bound to whichever
 *                       module it is given. Its header says a consumer
 *                       should call it "instead of copying the classes".
 *   - the codec         binjson's pure-JS encode/decode. nisaba keeps its
 *                       own WASM-bound copy for reasons of its own; there
 *                       is no reason to have a third.
 */
import { bindStructures } from '../third_party/binjson-structures/wasm/structures-core.js';
import { encode, decode, ObjectId, Pointer, TYPE, MemoryHandle } from '../third_party/binjson/js/binjson.js';

import createModule from '../lib/sukkal.wasm.mjs';

let Module = null;
let readyPromise = null;

/** Instantiate the module. Idempotent; await before touching anything else. */
export function ready() {
  if (!readyPromise) readyPromise = createModule().then((m) => (Module = m));
  return readyPromise;
}

export const isReady = () => Module !== null;

function requireModule() {
  if (!Module) throw new Error('sukkal: await ready() first');
  return Module;
}

/* BJ_ERR_* -> Error. The structure wrappers hand back a code and expect the
 * consumer to decide what an error looks like. */
const ERR = {
  [-1]: 'out of memory',
  [-2]: 'builder state error',
  [-3]: 'unexpected end of data',
  [-4]: 'unknown type byte',
  [-5]: 'decoded integer exceeds safe range',
  [-6]: 'pointer offset out of valid range',
  [-7]: 'maximum nesting depth exceeded',
  [-8]: 'structural invariant violated',
  [-9]: 'argument out of range',
};

function codeError(code, context) {
  const why = ERR[code] ?? `error ${code}`;
  return new Error(context ? `sukkal: ${context}: ${why}` : `sukkal: ${why}`);
}

function check(code) {
  if (code !== 0) throw codeError(code);
  return code;
}

/* Only the structures that read a header in place need this, and an entry
 * log is not one of them — its payloads are opaque bytes it never looks
 * inside. Left explicit rather than stubbed silently, so the day something
 * does need it, it says so. */
function valueSize() {
  throw new Error('sukkal: valueSize is not wired up (no structure in use needs it yet)');
}

const structures = bindStructures({
  ready, requireModule, codeError, check,
  encode, decode, valueSize,
  ObjectId, Pointer, TYPE,
});

export const { EntryLog, ENTRY_TYPE, BPlusTree, orderedKey } = structures;
export { encode, decode, ObjectId, Pointer, TYPE, MemoryHandle };

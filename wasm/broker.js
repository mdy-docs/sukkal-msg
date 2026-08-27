/*
 * broker.js — a sukkal broker in this process.
 *
 * The same routing table `sukkal serve` answers from, called directly.
 * There is no HTTP: no port to bind, no address to discover, nothing to
 * serialise. `sukkal_request(method, path, body)` is a function call, and
 * this is the JS side of it.
 *
 * What this file mostly does is the middle step of bjns' discipline —
 * *C plans, the host opens, C executes*. Opening a file in a browser is
 * asynchronous and WASM cannot block on a promise, so each request is:
 *
 *   1. ask C which files it may touch      (sukkal_plan)
 *   2. open them through the provider      (await, here)
 *   3. run the request                     (sukkal_request, synchronous)
 *   4. drain whatever C asked to unlink    (after the call)
 *
 * Step 1 is C's on purpose: the names are C's. Which file a subject lives
 * in, that a dead-letter channel is `<subject>.dead`, that a compaction
 * writes a `.tmp` — a host that knew those would be keeping a second copy
 * of a naming scheme that already exists in src/store.c.
 */
import { ready, module } from './sukkal-wasm.js';

const encoder = new TextEncoder();
const decoder = new TextDecoder();

/* Scope ids are per-broker and only have to be unique within a module. */
let nextScope = 1;

export class Broker {
  #w = 0;             /* struct sukkal_wasm * */
  #scope;
  #store;
  #fds = new Map();   /* name -> fd, for everything currently open */
  #nextFd = 1;
  #listing = 0;       /* the NUL-separated names, in WASM memory */
  #listingLen = 0;

  /**
   * @param {object} provider  a storage provider — memory, OPFS or node fs
   *   (binjson-structures' bindProviders). Durability is this choice: two
   *   of the three keep their messages, one does not.
   */
  constructor(provider) {
    this.#store = provider;
    this.#scope = nextScope++;
  }

  async open() {
    const M = await ready();
    M.bjnsScopes ||= {};
    M.bjnsScopes[this.#scope] = {};
    M.bjnsPending ||= {};
    M.bjioHandles ||= {};

    /* Opening the store is itself an operation that touches files: the
     * pusher reads its registrations as it is constructed. So the shared
     * files are planned and opened first — with a path that names no
     * subject, so `plan` answers with exactly those. Same source of truth
     * as every request, rather than a list of names kept here. */
    await this.#openPlanned(M, 'GET', '/health');

    this.#w = M._sukkal_wasm_open(this.#scope);
    if (!this.#w) throw new Error('sukkal: cannot open the broker');
    return this;
  }

  close() {
    const M = module();
    if (this.#w) M._sukkal_wasm_close(this.#w);
    this.#w = 0;
    for (const fd of this.#fds.values()) {
      try { M.bjioHandles[fd]?.close?.(); } catch { /* already gone */ }
      delete M.bjioHandles[fd];
    }
    this.#fds.clear();
    delete M.bjnsScopes[this.#scope];
    if (this.#listing) { M._free(this.#listing); this.#listing = 0; }
  }

  /**
   * One request against the broker.
   *
   * @param {string} method
   * @param {string} path    e.g. "/pub/orders.eu"
   * @param {object} [opts]  { query, body, contentType }
   * @returns {Promise<{ status: number, headers: object, body: Uint8Array }>}
   */
  async request(method, path, { query = '', body = null, contentType = null } = {}) {
    const M = module();
    if (!this.#w) throw new Error('sukkal: broker is closed');

    await this.#prepare(M, method, path);

    const args = [
      cstr(M, method), cstr(M, path), cstr(M, query),
      body ? bytes(M, body) : 0, body ? body.length : 0,
      contentType ? cstr(M, contentType) : 0,
    ];
    let status;
    try {
      status = M._sukkal_request(this.#w, ...args);
    } finally {
      for (const p of args) if (typeof p === 'number' && p) M._free(p);
    }

    const out = {
      status,
      headers: readHeaders(M, this.#w),
      body: readBody(M, this.#w),
    };
    this.#drain(M);
    return out;
  }

  /* --- the plan/open half ------------------------------------------- */

  async #prepare(M, method, path) {
    await this.#openPlanned(M, method, path);
    await this.#refreshListing(M);
  }

  async #openPlanned(M, method, path) {
    /* Every name this request may touch, from C. A superset: one needless
     * open costs an fd, where a missing one is the BJ_ERR_STATE the
     * discipline promises cannot happen. */
    const cap = 4096;
    const buf = M._malloc(cap);
    const mptr = cstr(M, method), pptr = cstr(M, path);
    let names;
    try {
      const len = M._sukkal_plan(mptr, pptr, buf, cap);
      names = splitNames(M.HEAPU8.subarray(buf, buf + len));
    } finally {
      M._free(buf); M._free(mptr); M._free(pptr);
    }

    const scope = M.bjnsScopes[this.#scope];
    for (const planned of names) {
      /* '+' may be created by this request, '-' must already exist. The
       * difference is a contract, not an optimisation: opening everything
       * with create:true would make a subscribe to an unknown subject
       * answer 200 and bring the subject into being, where it must 404. */
      const create = planned[0] === '+';
      const name = planned.slice(1);
      if (this.#fds.has(name)) continue;
      let handle;
      try {
        handle = await this.#store.openFile(name, { create });
      } catch {
        continue;                       /* absent, and C will see it so */
      }
      const fd = this.#nextFd++;
      M.bjioHandles[fd] = handle;
      scope[name] = fd;
      this.#fds.set(name, fd);
    }
  }

  /* The routes that enumerate — /subjects, /health — need the names, and
   * bjns has no list() because OPFS cannot enumerate synchronously. So it
   * is gathered here and handed over as a buffer. */
  async #refreshListing(M) {
    const names = this.#store.listFiles ? await this.#store.listFiles() : [...this.#fds.keys()];
    const joined = encoder.encode(names.map((n) => n + '\0').join(''));
    if (this.#listing) M._free(this.#listing);
    this.#listing = M._malloc(joined.length || 1);
    M.HEAPU8.set(joined, this.#listing);
    this.#listingLen = joined.length;
    M._sukkal_wasm_set_listing(this.#w, this.#listing, this.#listingLen);
  }

  /* Deletions C queued rather than performed: a host that cannot unlink
   * synchronously drains them once the call has returned. Safe because
   * every deletion here is a space optimisation after a commit — an
   * undeleted file is an orphan the next sweep collects, never a
   * correctness problem. */
  #drain(M) {
    const pending = M.bjnsPending?.[this.#scope];
    if (!pending || pending.length === 0) return;
    const names = pending.splice(0);
    for (const name of names) {
      const fd = this.#fds.get(name);
      if (fd !== undefined) {
        try { M.bjioHandles[fd]?.close?.(); } catch { /* already gone */ }
        delete M.bjioHandles[fd];
        delete M.bjnsScopes[this.#scope][name];
        this.#fds.delete(name);
      }
      this.#store.deleteFile(name).catch(() => { /* orphan; swept later */ });
    }
  }
}

/* ---- small helpers ----------------------------------------------------- */

function cstr(M, s) {
  const b = encoder.encode(s + '\0');
  const p = M._malloc(b.length);
  M.HEAPU8.set(b, p);
  return p;
}

function bytes(M, u8) {
  const p = M._malloc(u8.length || 1);
  M.HEAPU8.set(u8, p);
  return p;
}

function splitNames(u8) {
  return decoder.decode(u8).split('\0').filter(Boolean);
}

function readBody(M, w) {
  const ptr = M._sukkal_response_body(w);
  const len = M._sukkal_response_len(w);
  /* Copied, not a view: the next request reuses that buffer. */
  return len ? M.HEAPU8.slice(ptr, ptr + len) : new Uint8Array(0);
}

function readHeaders(M, w) {
  const ptr = M._sukkal_response_headers(w);
  const len = M._sukkal_response_headers_len(w);
  const out = {};
  for (const line of splitNames(M.HEAPU8.subarray(ptr, ptr + len))) {
    const at = line.indexOf(':');
    if (at > 0) out[line.slice(0, at).toLowerCase()] = line.slice(at + 1);
  }
  return out;
}

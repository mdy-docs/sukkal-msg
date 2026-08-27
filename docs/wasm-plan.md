# Running sukkal as WASM — implementation plan

Every other piece of this stack already runs in a browser.
[lamassu](https://github.com/mdy-docs/lamassu-js) is a JavaScript engine
compiled to WASM; [nisaba](https://github.com/mdy-docs/nisaba-db) is a
document database compiled to WASM with a pluggable storage provider. sukkal
is the one part that is a native binary, and that has costs which are not
about elegance:

- **A build step nothing else needs.** `mdy dev` can publish and deliver
  messages only if someone has cloned this repo and run `make`. Everything
  else in mdy-docs arrives through npm.
- **The browser editor cannot do messaging at all.** mdy's whole stack runs
  client-side — that is what lamassu and nisaba being WASM buys — and
  messaging is the one capability that stops at the network boundary.
- **Per-platform binaries**, for a project whose other three C dependencies
  ship exactly one artifact.

## What is already done

Most of the hard part, as it turns out. sukkal is built on
binjson-structures, and that repo has been carrying the WASM story for its
own reasons:

| piece | state |
| --- | --- |
| `bjio.h` | already a vtable — `{ size, read, write, truncate, sync, close }` over an opaque ctx. `bjio_posix` is *one implementation*, not the interface. |
| `hostio.h` | `bj_io bjio_host(int fd)` — a bjio backed by JS `FileSystemSyncAccessHandle` objects the host registers by fd. Reads and writes go straight into WASM memory via `HEAPU8.subarray`, no intermediate copies. |
| `entrylog_wasm.c` | 24 exported functions. **A subject is an entry log**, which the README calls the whole of the broker's design — and it already runs in WASM. |
| `bplustree_wasm.c` | likewise, for the catalog and index side. |
| `bjns.h` | one directory-scoped file namespace: open, unlink, and make the directory entry durable. |

`bjns.h` deserves its own paragraph, because it solves the problem that
would otherwise sink this. In a browser, *opening* a file is asynchronous —
`getFileHandle()` and `createSyncAccessHandle()` both return promises — and
WASM cannot block on a promise without Asyncify or JSPI, neither of which
exists in a native or WASI build. Relying on either would mean two different
control-flow models for browser and server, which is the exact thing this
effort exists to remove. Its answer is a discipline rather than a mechanism:

> **C PLANS, the host OPENS, C EXECUTES.**

Every file-touching operation splits into a pure call returning the names it
will need, and a synchronous call that does the work over the handles the
host opened in between.

And on the JS side, `wasm/structures-core.js` already owns the handle
registry the C glue reads through — `registerHandle` / `unregisterHandle`
over `Module.bjioHandles`, with an explicit hook for a consumer that wants
to supply its own.

So the substrate is not the work. sukkal's own four files are.

## sukkal needs no storage story: nisaba already wrote it

The providers nisaba runs on are not database code. The whole interface is
four methods, and not one of them knows what a collection is:

```js
openFile(name, { create })   // → a sync access handle
deleteFile(name)
listFiles()                  // optional; enables orphan sweeps
subProvider(name)            // a nested, isolated scope
```

That is *word for word* what `bjns.h` calls itself — "one directory-scoped
file namespace". The JS provider interface and the C `bjns` interface are the
same abstraction seen from opposite sides of the bridge, arrived at
independently, which is the strongest evidence available that it is the right
seam.

Three implementations already exist, all with identical shapes:

| provider | where | what sukkal would get |
| --- | --- | --- |
| `MemoryStorageProvider` | `nisaba/wasm/nisaba-wasm.js` | a broker for tests and for `mdy dev`, losing everything on exit |
| `OPFSStorageProvider` | same | a **durable broker in a browser tab** — real sync handles, real fsync |
| `NodeFSStorageProvider` | `nisaba/src/db-node.js` | a **durable local broker with no binary at all** |

`NodeFSSyncHandle` duck-types `FileSystemSyncAccessHandle` — `getSize`,
`read(buf, { at })`, `write(buf, { at })`, `truncate` — so the C side cannot
tell the three apart, which is the point.

**The async question answers itself.** A provider's `openFile` is `async`,
and `bjns` requires opening to be synchronous — which looks like a conflict
and is precisely the arrangement `bjns` was designed around. The host opens
(asynchronously, in JS, before the call); C executes (synchronously, over the
handle it was given). *C plans, the host opens, C executes.* The two designs
already agree, which is why this is reuse rather than a port.

### Where the providers should live

Not copied into sukkal. **Moved down into binjson-structures**, which already
owns everything on both sides of them: `bjio`, `bjns` and `hostio` in C, and
`structures-core.js`'s handle registry in JS. The providers are the one piece
of that layer sitting a storey too high. nisaba re-exports them and nothing
breaks for anyone using it today; sukkal imports the same objects, so "the
same OPFS adapter" is the same code rather than a copy that drifts.

One honest note against this: nisaba deliberately keeps *its own* copies of
the binjson codec and the tree wrappers, and says so in its README. That
choice is about not linking two binjson checkouts into one binary — a C
concern that does not apply to a JS class with no native code in it. Worth
knowing the project has chosen duplication before, for a reason that is not
this one.

## Where the line falls

```
        8,597 lines of C, and only about half of it is the broker

  src/store.c     2,439   subjects, receipts, groups, leases,        ← WASM
                          retention, dead letters, dedup
  src/server.c    1,332   routing and response shaping               ← WASM
                          ...over http11c                              (shell)
  src/push.c        864   outbound delivery, via libcurl             ← host
  src/bjtext.c      230   binjson <-> text                           ← WASM

  src/client.c    3,036   the CLI client                             ← neither
  src/main.c        152   subcommand dispatch                        ← neither
```

`client.c` is a third of the codebase and none of it is a broker. It stays
native, and the WASM build simply never compiles it.

## Three seams to cut

### 1. Naming — `store.c` through `bjns`

The only seam with real work in it, and the smallest surface anyone would
guess. `store.c` reaches POSIX in **40 places**:

```
21 close()   9 open()   2 fdopendir()   2 readdir()
 2 closedir()  2 dup()   1 renameat()   1 mkdir()
```

Nine opens and two directory scans is the whole of it; the twenty-one closes
follow from the opens. It already includes `bjns.h` and uses it three times,
so this is finishing a conversion rather than starting one — and `bjns` was
designed for precisely this seam, being described in its own header as *"the
seam that lets the catalog, the compaction generation flip and the orphan
sweep live in C, because a layer that cannot name a file cannot own a
catalog."*

The two directory scans are where the discipline bites: enumerating subjects
is `readdir` today and becomes plan-then-execute, because a browser cannot
enumerate synchronously either.

### 2. Request — a shim `server.c` compiles against either way

`server.c` names `http11c_` 124 times, which sounds fatal and is not. There
are 26 distinct symbols, and only **nine of them are per-request**:

```
http11c_req_path          http11c_res_status
http11c_req_query_get     http11c_res_header
http11c_req_body          http11c_res_write
http11c_req_content_type  http11c_res_text
http11c_req_ctx
```

The other seventeen are server lifecycle — `_new`, `_listen`, `_run`,
`_route`, `_stop`, `_poll`, `_free`, the `set_*` options — and the WASM build
has no server to run.

So: define `sukkal_req` / `sukkal_res` with those nine accessors, implement
them over http11c for the native build and over a plain struct for WASM.
Every route handler in `server.c` then compiles unchanged in both, and the
routing table — which *is* the protocol — has exactly one definition.

### 3. Push — delivery becomes a host callback

`push.c` calls libcurl in 34 places to POST a batch to a subscriber's
callback URL. In WASM there is no libcurl and no socket. It becomes one
function pointer the host installs — `fetch` in a browser, libcurl in the
native binary — leaving `push.c`'s actual content (who is registered, what
is in flight, attempts, backoff, the dead-letter transition) portable.

## The part worth noticing

Once seam 2 exists, **the WASM broker needs no HTTP at all.**

```c
sukkal_response *sukkal_request(sukkal *s, const char *method,
                                const char *path, const uint8_t *body, size_t len);
```

A direct call. In-process there is nothing to serialise, no port to bind, no
address to discover. `@mdy-docs/mdy-bus` stops speaking HTTP to a subprocess
and calls a function, exactly as mdy already calls nisaba.

And delivery inverts with it. Today the broker POSTs to a callback URL, which
is why the bus had to work out which local address reaches the broker, mint a
bearer token, run an HTTP server, and re-assert its registration on a
heartbeat in case the broker was restarted onto a rebuilt store. In-process,
"deliver this message" is a call into the host — for `mdy dev`, straight into
rendering the page. All of that machinery is transport, and none of it is
messaging: it exists only because the two halves were in different processes.

That is the real prize here, and it is larger than "no build step".

## Phases

### Phase 0 — prove the substrate ✅

A WASM build of entrylog + bplustree with a memory `bj_io`, appending to one
subject and reading it back. No sukkal code at all. Exit: the claim that the
structures already work in WASM is a demonstration rather than a reading of
the file listing.

**Done.** `build-wasm.sh` produces one combined 177KB module — binjson plus
binjson-structures, linked from *their* manifests (`wasm/sources.txt`,
`wasm/exports.txt`) rather than a hand-copied list, so a structure added
there arrives here without an edit. `wasm/sukkal-wasm.js` binds it, and
`test/wasm-substrate.test.js` appends three messages to a subject, syncs,
reads them back byte-identical, closes the log and reopens it on the same
handle to find `lastIndex` intact and index 3 next.

Three things it settled:

  - **Nothing was copied.** The structures come from binjson-structures'
    own `bindStructures()`, whose header says a consumer should call it
    "instead of copying the classes"; the codec is binjson's pure-JS
    `encode`/`decode`, already a dependency of the Node client; and
    `MemoryHandle` — a sync-access-handle-shaped file — turns out to live
    in binjson itself. Even the memory provider was already written.
  - **The nested binjson submodule stays uninitialised**, as the README
    asks. This is a combined build against the top-level checkout, which is
    what `sources.txt` exists to make possible, and it is also the shape
    sukkal's own sources will join.
  - **`getBatch` is bounded by bytes, not count** — which explains
    something that reads as a bug from outside: a subscribe against the
    native broker answers `X-Sukkal-Count: 1` while more is waiting. It is
    a byte budget that always yields at least one entry. Pinned in a test
    so it stops looking like a defect.

### Phase 1 — `store.c` through `bjns`

The nine opens, two directory scans and one `mkdir`. Exit: `store.c` compiles
with no POSIX header included, and the native build still passes its tests —
which is the point, since a conversion that only the WASM build exercises is
a conversion nobody trusts.

### Phase 2 — the request seam

`sukkal_req`/`sukkal_res`, http11c behind them, `server.c` untouched below the
accessors. Exit: the native binary behaves identically, and `server.c` no
longer includes `http11c.h`.

### Phase 3 — the push seam

Delivery through a host-installed callback. Exit: `push.c` no longer includes
`curl.h`; the native shell installs a libcurl delivery function.

### Phase 4 — move the storage providers down

`MemoryStorageProvider`, `OPFSStorageProvider` and `NodeFSStorageProvider`
from nisaba into binjson-structures, beside the `bjns`/`hostio`/registry
layer they belong to. nisaba re-exports them, so nothing that uses it today
notices. Exit: one definition of each, imported by both consumers.

Doable before or after Phases 1–3 — it touches no C — and worth doing early,
because it is the phase that decides sukkal has no storage code of its own.

### Phase 5 — the WASM build and its JS package

`sukkal-wasm.js` mirroring nisaba's shape: `ready()`, a provider, and a
`Broker` whose methods are the routes. Exit: publish and subscribe in Node
with no binary, and in a browser tab, against all three providers.

### Phase 6 — mdy-bus gains an in-process transport

`@mdy-docs/mdy-bus` picks the WASM broker when no URL is configured. Exit:
`mdy dev` publishes and delivers with nothing installed but npm packages, and
the callback server, token, local-address discovery and heartbeat are all
gone from that path.

## Open questions

- **Durability is a choice the caller makes, not a property of the target.**
  It is tempting to read this as "browser means OPFS, Node means memory", and
  that is not what the providers say: OPFS is durable, `NodeFSStorageProvider`
  is durable, memory is not, and all three run wherever their platform does.
  A broker on memory loses every message when the tab or the process goes,
  which is right for `mdy dev` and wrong for everything else — so it should be
  named at construction rather than defaulted to by environment.
- **Leases and backoff need a clock.** `push.c` uses `sys/time.h`; WASM has
  no timers of its own. The host supplies `now()`, which also makes the
  retry logic testable for the first time.
- **Does the native binary survive?** Yes, and it should: the same core with
  a different shell. Nothing here is a rewrite, and a broker several
  machines talk to still wants a socket.
- **Two brokers, one format.** A WASM broker in a browser tab and a native
  broker on a server share a wire format and a routing table but no store.
  Whether they should ever sync is a much larger question than this plan,
  and worth not answering by accident.

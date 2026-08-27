/*
 * sukkal_wasm.c — the broker as a function call.
 *
 * Everything the native binary does over a socket, done in process. There
 * is no HTTP here: src/server.c's routing table and every handler behind
 * it are already written against sukkal_req/sukkal_res (Phase 2), so what
 * remains is to fill one in from arguments instead of from a parsed
 * request, and collect the answer into a buffer instead of a connection.
 *
 * The plan/execute discipline
 * ---------------------------
 * bjns requires open() to be synchronous, and opening a file in a browser
 * is not. Its answer is a discipline rather than a mechanism — *C plans,
 * the host opens, C executes* — and this file is where sukkal takes part:
 *
 *   1. sukkal_plan(method, path)  names every file the request may touch.
 *   2. The host opens them (asynchronously, in JS) into the scope table.
 *   3. sukkal_request(...) runs, resolving names against that table.
 *
 * Step 1 is C's because the names are C's: which file a subject lives in,
 * that a dead-letter channel is `<subject>.dead`, that a compaction writes
 * a `.tmp` beside the live log. A host that had to know those would be a
 * host keeping a second, drifting copy of this file's naming scheme.
 */
#include "sukkal.h"
#include "bjns_bridge.h"

#include <stdlib.h>
#include <string.h>

#ifdef __EMSCRIPTEN__
#include <emscripten.h>
#else
#define EMSCRIPTEN_KEEPALIVE
#endif

/* The store's own shared files. Named here rather than exported from
 * store.c because this is the only caller that needs them, and a header
 * entry would invite someone else to depend on the layout. */
static int starts(const char *s, const char *prefix) {
    return strncmp(s, prefix, strlen(prefix)) == 0;
}

static const char *const SHARED_FILES[] = {
    "_cursors.bpt", "_policy.bpt", "_queues.bpt",
    "_dedup0.bpt", "_dedup1.bpt", "_push.bpt",
};

/* ---- one broker ------------------------------------------------------- */

struct sukkal_wasm {
    bj_ns       ns;
    bjm_store  *store;
    bjm_pusher *push;
    sukkal_app  app;
    int         scope;

    /* The response being composed. One at a time: a request is a call, and
     * a call has returned before the next one starts. */
    int      status;
    uint8_t *body;
    size_t   body_len, body_cap;
    char     headers[2048];
    size_t   headers_len;

    /* The current listing, set by the host before a request that needs
     * one. Borrowed: it is the host's buffer and outlives the call. */
    const char *listing;
    size_t      listing_len;
};

static int wasm_listing(void *ctx, char **out, size_t *out_len, int *owned) {
    struct sukkal_wasm *w = ctx;
    if (!w->listing) return BJ_ERR_STATE;
    *out = (char *)w->listing;
    *out_len = w->listing_len;
    *owned = 0;               /* the host's buffer, not ours to free */
    return BJ_OK;
}

/* ---- the response side of the seam ------------------------------------ */

static void r_status(void *impl, int code) {
    ((struct sukkal_wasm *)impl)->status = code;
}

static void r_header(void *impl, const char *name, const char *value) {
    struct sukkal_wasm *w = impl;
    size_t n = strlen(name), v = strlen(value);
    if (w->headers_len + n + v + 3 >= sizeof w->headers) return;
    memcpy(w->headers + w->headers_len, name, n);      w->headers_len += n;
    w->headers[w->headers_len++] = ':';
    memcpy(w->headers + w->headers_len, value, v);     w->headers_len += v;
    w->headers[w->headers_len++] = '\0';
}

static void r_write(void *impl, const uint8_t *data, size_t len) {
    struct sukkal_wasm *w = impl;
    if (w->body_len + len > w->body_cap) {
        size_t want = w->body_cap ? w->body_cap * 2 : 1024;
        while (want < w->body_len + len) want *= 2;
        uint8_t *grown = realloc(w->body, want);
        if (!grown) return;
        w->body = grown;
        w->body_cap = want;
    }
    memcpy(w->body + w->body_len, data, len);
    w->body_len += len;
}

/* ---- lifecycle -------------------------------------------------------- */

/*
 * `scope` is the host's table of pre-opened files (bjns_bridge.h). The
 * clock is passed in for the same reason the store's is: WASM has none,
 * and a broker whose sense of time is supplied is one whose leases and
 * backoffs can be tested without waiting for them.
 */
EMSCRIPTEN_KEEPALIVE
struct sukkal_wasm *sukkal_wasm_open(int scope) {
    struct sukkal_wasm *w = calloc(1, sizeof *w);
    if (!w) return NULL;
    w->scope = scope;

    if (bjns_bridge_open(scope, &w->ns) != BJ_OK) { free(w); return NULL; }

    w->store = bjm_store_open_ns(w->ns);
    if (!w->store) { bjns_bridge_free(&w->ns); free(w); return NULL; }

    /* The pusher reads the subject list as it is constructed, and nothing
     * has been opened yet — so the listing starts empty, which is true
     * rather than merely convenient. */
    bjm_store_set_listing(w->store, wasm_listing, w);
    w->listing = "";
    w->listing_len = 0;

    /* No adopt hook: there is no atomic replace here, so bjm_trim refuses
     * rather than risking a subject to a crash mid-compaction. Retention
     * by age and count still works; only trimming does not. */
    w->push = bjm_pusher_new(w->store, NULL, SUKKAL_DEFAULT_BATCH);
    if (!w->push) { bjm_store_free(w->store); bjns_bridge_free(&w->ns); free(w); return NULL; }

    w->app.store = w->store;
    w->app.push = w->push;
    w->app.bld = bj_builder_new();
    w->app.backend = "wasm";
    return w->app.bld ? w : (bjm_pusher_free(w->push), bjm_store_free(w->store),
                             bjns_bridge_free(&w->ns), free(w), NULL);
}

EMSCRIPTEN_KEEPALIVE
void sukkal_wasm_close(struct sukkal_wasm *w) {
    if (!w) return;
    bjm_pusher_free(w->push);
    bj_builder_free(w->app.bld);
    bjm_store_free(w->store);
    bjns_bridge_free(&w->ns);
    free(w->body);
    free(w);
}

EMSCRIPTEN_KEEPALIVE
void sukkal_wasm_set_clock(struct sukkal_wasm *w, uint64_t (*fn)(void *), void *ctx) {
    bjm_store_set_clock(w->store, fn, ctx);
}

/*
 * The names in this scope, NUL-separated, for the routes that enumerate.
 * Set before a request; bjns has no list() because OPFS cannot enumerate
 * synchronously, so the host gathers this when it opens the scope.
 */
EMSCRIPTEN_KEEPALIVE
void sukkal_wasm_set_listing(struct sukkal_wasm *w, const char *names, size_t len) {
    w->listing = names;
    w->listing_len = len;
}

/* ---- plan ------------------------------------------------------------- */

/*
 * One planned name, prefixed '+' when the request may CREATE it and '-'
 * when it must already exist.
 *
 * The distinction is not bookkeeping: a publish creates its subject and a
 * subscribe to an unknown one is a 404 rather than an implicit create. A
 * host that opened every planned name with create:true would answer 200
 * and conjure the subject — so which names may be created is part of the
 * plan, and stays in C with the rest of the naming.
 */
static int name_add(char *out, size_t cap, size_t *len, const char *name, int create) {
    size_t n = strlen(name) + 2;
    if (*len + n > cap) return 0;
    out[(*len)++] = create ? '+' : '-';
    memcpy(out + *len, name, n - 1);
    *len += n - 1;
    return 1;
}

/*
 * Every file `method path` may touch, NUL-separated. Deliberately a
 * superset: naming a file the request turns out not to want costs one
 * needless open, where missing one costs a BJ_ERR_STATE the discipline
 * promises can never happen.
 *
 * Returns the number of bytes written, or 0 if `out` is too small.
 */
EMSCRIPTEN_KEEPALIVE
size_t sukkal_plan(const char *method, const char *path, char *out, size_t cap) {
    (void)method;
    size_t len = 0;

    /* The store's own files are made on first use, whatever the route. */
    for (size_t i = 0; i < sizeof SHARED_FILES / sizeof *SHARED_FILES; i++)
        if (!name_add(out, cap, &len, SHARED_FILES[i], 1)) return 0;

    /* Every route but /subjects, /policies, /push and /health is
     * "/<verb>/<subject>", so the subject is whatever follows the second
     * slash. */
    const char *slash = strchr(path + 1, '/');
    if (!slash || !*(slash + 1)) return len;

    const char *subject = slash + 1;
    if (!bjm_subject_valid(subject)) return len;

    /* Which of the subject's files this route touches, and which of them
     * it is allowed to bring into being. */
    const int is_post   = strcmp(method, "POST") == 0;
    const int creates   = is_post && (starts(path, "/pub/") || starts(path, "/requeue/"));
    const int may_die   = is_post && (starts(path, "/fail/") || starts(path, "/done/"));
    const int reads_dead = starts(path, "/dead/") || starts(path, "/requeue/");
    const int compacts  = is_post && starts(path, "/trim/");

    char file[BJM_SUBJECT_MAX + 32];

    snprintf(file, sizeof file, "%s.elog", subject);
    if (!name_add(out, cap, &len, file, creates)) return 0;

    if (may_die || reads_dead) {
        snprintf(file, sizeof file, "%s%s.elog", subject, BJM_DEAD_SUFFIX);
        if (!name_add(out, cap, &len, file, may_die)) return 0;
    }

    if (compacts) {
        /* Compaction writes beside the live log before replacing it. */
        snprintf(file, sizeof file, "%s.elog.tmp", subject);
        if (!name_add(out, cap, &len, file, 1)) return 0;
    }

    return len;
}

/* ---- request ---------------------------------------------------------- */

/*
 * Route and run one request. The whole of what a transport does natively,
 * minus the transport: no socket, no parsing, no serialising, and the
 * routing table is the same one `sukkal serve` answers from.
 */
EMSCRIPTEN_KEEPALIVE
int sukkal_request(struct sukkal_wasm *w, const char *method, const char *path,
                   const char *query, const uint8_t *body, size_t body_len,
                   const char *content_type) {
    w->status = 200;
    w->body_len = 0;
    w->headers_len = 0;

    sukkal_req req = {
        .ctx = &w->app,
        .impl = (void *)query,
        .method = method,
        .path = path,
        .body = body,
        .body_len = body_len,
        .content_type = content_type,
        .query_get = sukkal_query_from_string,
    };
    sukkal_res res = { w, r_status, r_header, r_write };

    sukkal_dispatch(&req, &res);
    return w->status;
}

EMSCRIPTEN_KEEPALIVE uint8_t *sukkal_response_body(struct sukkal_wasm *w) { return w->body; }
EMSCRIPTEN_KEEPALIVE size_t   sukkal_response_len(struct sukkal_wasm *w)  { return w->body_len; }
EMSCRIPTEN_KEEPALIVE char    *sukkal_response_headers(struct sukkal_wasm *w) { return w->headers; }
EMSCRIPTEN_KEEPALIVE size_t   sukkal_response_headers_len(struct sukkal_wasm *w) { return w->headers_len; }

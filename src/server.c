/*
 * server.c — the broker: the routing table and every handler behind it.
 *
 * Written against sukkal_req/sukkal_res (see sukkal.h) and nothing else,
 * so it compiles wherever the store does. The table IS the protocol, and
 * it used to belong to http11c's router — which meant the transport
 * decided what this broker answered to. src/server_posix.c is the half
 * that owns a socket now; a WASM host calls sukkal_dispatch directly, with
 * no socket in the picture at all (docs/wasm-plan.md).
 *
 * Protocol (every success body is binjson; errors are text/plain so a
 * bare curl is readable):
 *
 *   POST /pub/<subject>              body: one binjson value (the message)
 *                                    -> { subject, index }
 *   PUT  /push/<pattern>?consumer=&callback=  register a push subscription
 *   GET  /sub/<subject>?from=&max=   -> ARRAY of { index, term, type, payload }
 *   GET  /subjects                   -> ARRAY of subject names
 *   GET  /health                     -> { ok, backend, subjects, conns }
 *
 * Subscribers are pushed to, not polled: a subscription names a callback
 * URL, and the broker POSTs each batch to it (see push.c) with the
 * response serving as the acknowledgement. So the broker is an HTTP
 * client as well as a server, and bjm_serve's loop interleaves the two.
 *
 * GET /sub survives as the way to *read* a subject — for browsing, for
 * one-shot tools, for `sukkal dead` — but nothing subscribes with it.
 *
 * Handlers run on the event loop, so nothing here may block. That is why
 * registering a subscription only records it: the first delivery happens
 * on the next pump, not inside the handler.
 */
#include "sukkal.h"

#include "binjson.h"

#include <stdlib.h>
#include <string.h>



/*
 * A query-string reader for transports that do not have one.
 *
 * http11c parses queries itself, which is why the seam takes a function
 * pointer rather than a string — but a caller with no HTTP in it has only
 * the raw "a=1&b=2", and somebody has to read it. Percent-decoding
 * included, because a callback URL arrives that way.
 *
 * Pass the raw query (or NULL) as sukkal_req.impl and this as query_get.
 */
static int unhex(int c) {
    if (c >= '0' && c <= '9') return c - '0';
    if (c >= 'a' && c <= 'f') return c - 'a' + 10;
    if (c >= 'A' && c <= 'F') return c - 'A' + 10;
    return -1;
}

int sukkal_query_from_string(void *impl, const char *key, char *buf, size_t buf_len) {
    const char *q = impl;
    if (!q || !*q) return 0;
    size_t klen = strlen(key);

    for (const char *p = q; *p; ) {
        const char *amp = strchr(p, '&');
        const char *end = amp ? amp : p + strlen(p);
        const char *eq = memchr(p, '=', (size_t)(end - p));
        if (eq && (size_t)(eq - p) == klen && memcmp(p, key, klen) == 0) {
            size_t out = 0;
            for (const char *v = eq + 1; v < end; v++) {
                int c = (unsigned char)*v;
                if (c == '+') c = ' ';
                else if (c == '%' && v + 2 < end) {
                    int hi = unhex(v[1]), lo = unhex(v[2]);
                    if (hi >= 0 && lo >= 0) { c = hi * 16 + lo; v += 2; }
                }
                if (out + 1 >= buf_len) return -1;   /* value does not fit */
                buf[out++] = (char)c;
            }
            buf[out] = '\0';
            return 1;
        }
        if (!amp) break;
        p = amp + 1;
    }
    return 0;
}

/* ---- response helpers ------------------------------------------------ */

static void res_bj(sukkal_res *res, int code, const uint8_t *data, size_t len) {
    res->status(res->impl, code);
    res->header(res->impl, "Content-Type", SUKKAL_MEDIA_TYPE);
    res->write(res->impl, data, len);
}

/* Errors are text/plain on purpose, so a bare curl against a broken
 * request is readable. That is the whole of this protocol's error
 * convention. */
static void res_err(sukkal_res *res, int code, const char *msg) {
    res->status(res->impl, code);
    res->header(res->impl, "Content-Type", "text/plain; charset=utf-8");
    res->write(res->impl, (const uint8_t *)msg, strlen(msg));
}

/* Map a BJ_ERR_* code onto the status that describes it to a client. */
static int status_for(int err) {
    switch (err) {
    case BJ_ERR_RANGE:  return 416;   /* cursor outside the log's bounds  */
    case BJ_ERR_STATE:  return 404;   /* no such subject / unopenable     */
    case BJ_ERR_OOM:    return 503;
    default:            return 500;
    }
}

/*
 * The subject named by a /pub/ or /sub/ path. Returns NULL and answers the
 * request itself when the path carries no valid subject.
 */
static const char *subject_of(sukkal_req *req, sukkal_res *res,
                              const char *prefix) {
    const char *subject = req->path + strlen(prefix);
    if (!bjm_subject_valid(subject)) {
        res_err(res, 400, "invalid subject: expected 1-128 chars of "
                          "[A-Za-z0-9_.-], no leading/trailing dot\n");
        return NULL;
    }
    return subject;
}

static uint64_t query_u64(sukkal_req *req, const char *key, uint64_t dflt) {
    char buf[32];
    if (req->query_get(req->impl, key, buf, sizeof buf) != 1) return dflt;
    char *end;
    unsigned long long v = strtoull(buf, &end, 10);
    return (end == buf || *end) ? dflt : (uint64_t)v;
}

/*
 * Read the `consumer` query parameter. Returns 1 when present and valid,
 * 0 when absent, -1 when present but malformed (having answered the
 * request itself).
 */
static int query_consumer(sukkal_req *req, sukkal_res *res,
                          char *out, size_t out_size) {
    int rc = req->query_get(req->impl, "consumer", out, out_size);
    if (rc == 0) return 0;
    if (rc != 1 || !bjm_consumer_valid(out)) {
        res_err(res, 400, "invalid consumer: expected 1-128 chars of "
                          "[A-Za-z0-9_.-], no leading/trailing dot\n");
        return -1;
    }
    return 1;
}

/* As query_consumer, for the `group` parameter. */
static int query_group(sukkal_req *req, sukkal_res *res,
                       char *out, size_t out_size) {
    int rc = req->query_get(req->impl, "group", out, out_size);
    if (rc == 0) return 0;
    if (rc != 1 || !bjm_group_valid(out)) {
        res_err(res, 400, "invalid group: expected 1-128 chars of "
                          "[A-Za-z0-9_.-], no leading/trailing dot\n");
        return -1;
    }
    return 1;
}

/* ---- handlers -------------------------------------------------------- */

static void h_publish(sukkal_req *req, sukkal_res *res) {
    sukkal_app *a = req->ctx;

    const char *subject = subject_of(req, res, "/pub/");
    if (!subject) return;

    if (!req->content_type || strcmp(req->content_type, SUKKAL_MEDIA_TYPE) != 0) {
        res_err(res, 415, "expected Content-Type: " SUKKAL_MEDIA_TYPE "\n");
        return;
    }

    size_t len = req->body_len;
    const uint8_t *body = req->body;
    if (!body || len == 0) {
        res_err(res, 400, "empty body\n");
        return;
    }
    /*
     * The log stores payloads opaquely, so nothing downstream would catch
     * a malformed message until a subscriber's decoder tripped over it —
     * by which point it is durable and undeliverable. Check the framing
     * here instead: exactly one complete binjson value, no trailing bytes.
     */
    size_t size = 0;
    if (bj_value_size(body, len, 0, &size) != BJ_OK || size != len) {
        res_err(res, 400, "body is not exactly one binjson value\n");
        return;
    }

    /*
     * ?headers=1 says the body is [ headers, message ] rather than the
     * message alone. The shape is checked here — a subscriber trusts the
     * entry type and would otherwise trip over a mislabelled payload —
     * but the contents of the headers object are never looked at.
     */
    int entry_type = BJM_ENTRY_PLAIN;
    if (query_u64(req, "headers", 0)) {
        if (!bjm_envelope_shape_ok(body, len)) {
            res_err(res, 400, "with ?headers=1 the body must be a 2-element "
                              "array: [ headers-object, message ]\n");
            return;
        }
        entry_type = BJM_ENTRY_ENVELOPE;
    }

    /*
     * An idempotency key makes this publish safe to repeat: if the id has
     * been seen inside the dedup window, the original index is returned
     * and nothing is appended. Look it up before the append, so a retry
     * of a request that already landed cannot add a second copy.
     */
    char id[BJM_DEDUP_ID_MAX + 1];
    int has_id = req->query_get(req->impl, "id", id, sizeof id);
    if (has_id == 1 && !bjm_dedup_id_valid(id)) {
        res_err(res, 400, "invalid id: 1-128 printable bytes, no '/'\n");
        return;
    }
    has_id = has_id == 1;

    uint64_t index = 0;
    int duplicate = 0;
    if (has_id) {
        int e = bjm_dedup_lookup(a->store, subject, id, &duplicate, &index);
        if (e) { res_err(res, status_for(e), "dedup lookup failed\n"); return; }
    }

    if (!duplicate) {
        int e = bjm_publish(a->store, subject, entry_type, body,
                            (uint32_t)len, &index);
        if (e) {
            res_err(res, status_for(e), "publish failed\n");
            return;
        }
        /* After the append, so a failed publish leaves no claim behind. */
        if (has_id) bjm_dedup_record(a->store, subject, id, index);
    }

    /*
     * Read-process-write in one call: advance a subscription's receipt on
     * the INPUT subject as part of publishing the OUTPUT. That is what
     * makes a pipeline effectively-once — the two writes the broker would
     * otherwise do separately, with a crash window between them, now
     * happen before one response.
     *
     * They still are not one atomic write, and cannot be: they are
     * different files. The order is what carries the guarantee. Publish
     * first, ack second, so a crash in between replays the input, the
     * handler reruns, and the republished output collapses onto the
     * existing one by its idempotency key. The alternative order would
     * ack an input whose output never landed, and lose the message.
     */
    char ack_subject[BJM_SUBJECT_MAX + 1];
    char ack_consumer[BJM_CONSUMER_MAX + 1];
    uint64_t ack_index = query_u64(req, "ack_index", 0);
    int acked = 0;
    if (ack_index > 0 &&
        req->query_get(req->impl, "ack_subject", ack_subject,
                              sizeof ack_subject) == 1 &&
        req->query_get(req->impl, "ack_consumer", ack_consumer,
                              sizeof ack_consumer) == 1) {
        if (!bjm_subject_valid(ack_subject) || !bjm_consumer_valid(ack_consumer)) {
            res_err(res, 400, "invalid ack_subject or ack_consumer\n");
            return;
        }
        int e = bjm_cursor_set(a->store, ack_subject, ack_consumer, ack_index);
        if (e) { res_err(res, status_for(e), "ack failed\n"); return; }
        /* The durability point of the pair: once this returns, the output
         * is in its log and the input will not be replayed. */
        bjm_cursor_sync(a->store);
        acked = 1;
    }

    bj_builder *b = a->bld;
    bj_builder_reset(b);
    bj_begin_object(b);
    bj_put_key(b, (const uint8_t *)"subject", 7);
    bj_put_string(b, (const uint8_t *)subject, (uint32_t)strlen(subject));
    bj_put_key(b, (const uint8_t *)"index", 5);
    bj_put_int(b, (int64_t)index);
    if (acked) {
        bj_put_key(b, (const uint8_t *)"acked", 5);
        bj_put_int(b, (int64_t)ack_index);
    }
    if (has_id) {
        /* True means "this id was already here" — the caller's retry did
         * not create a second message. */
        bj_put_key(b, (const uint8_t *)"duplicate", 9);
        bj_put_bool(b, duplicate);
    }
    bj_end_object(b);

    size_t out_len = 0;
    const uint8_t *out = bj_builder_data(b, &out_len);
    if (!out) { res_err(res, 500, "encode failed\n"); return; }
    res_bj(res, 200, out, out_len);
}

static void h_subscribe(sukkal_req *req, sukkal_res *res) {
    sukkal_app *a = req->ctx;

    const char *subject = subject_of(req, res, "/sub/");
    if (!subject) return;

    uint64_t from = query_u64(req, "from", 0);
    uint64_t max  = query_u64(req, "max", SUKKAL_DEFAULT_BATCH);
    if (max == 0 || max > SUKKAL_MAX_BODY_BYTES) max = SUKKAL_DEFAULT_BATCH;
    if (from == 0) from = 1;

    uint64_t base = 0, ignored_last = 0, ignored_bytes = 0;
    if (bjm_subject_info(a->store, subject, &base,
                         &ignored_last, &ignored_bytes) != BJ_OK) {
        res_err(res, 404, "no such subject\n");
        return;
    }

    /*
     * With ?consumer=, the cursor is the broker's to remember: the client
     * sends no `from` at all, and instead piggybacks `ack` for the batch
     * it finished processing. One request per cycle still, and the
     * receipt is persisted before the next batch is handed out.
     */
    char consumer[BJM_CONSUMER_MAX + 1];
    int has_consumer = query_consumer(req, res, consumer, sizeof consumer);
    if (has_consumer < 0) return;

    uint64_t acked = 0;
    if (has_consumer) {
        uint64_t ack = query_u64(req, "ack", 0);
        int e = 0;
        if (ack) e = bjm_cursor_set(a->store, subject, consumer, ack);
        if (e) { res_err(res, status_for(e), "ack failed\n"); return; }

        int found = 0;
        e = bjm_cursor_get(a->store, subject, consumer, &found, &acked);
        if (e) { res_err(res, status_for(e), "cursor lookup failed\n"); return; }

        if (!found) {
            /* First sight of this consumer. ?start=last joins at the end
             * of the log, skipping the backlog; the default replays it. */
            char start[8];
            if (req->query_get(req->impl, "start", start, sizeof start) == 1 &&
                strcmp(start, "last") == 0)
                acked = bjm_last_index(a->store, subject);
            e = bjm_cursor_set(a->store, subject, consumer, acked);
            if (e) { res_err(res, status_for(e), "cursor create failed\n"); return; }
        }
        from = acked + 1;
    }

    /*
     * A plain `from` cursor below the trim boundary is a browsing client
     * asking for messages that no longer exist — start it at the oldest
     * one there is and say how many it missed. A *consumer* cursor gets
     * no such courtesy: its receipt is a claim about what was delivered,
     * and quietly skipping past lost messages would hide the loss. That
     * case falls through to the 416 below.
     */
    uint64_t skipped = 0;
    if (!has_consumer && from <= base) {
        skipped = base + 1 - from;
        from = base + 1;
    }

    int count = 0;
    uint64_t last = 0;
    const uint8_t *out = NULL;
    size_t out_len = 0;
    int e = bjm_read(a->store, subject, from, (size_t)max,
                     &count, &out, &out_len, &last);
    if (e == BJ_ERR_RANGE) {
        /* The cursor points below the log's base: those messages were
         * trimmed away. Say so — "not found" would send a subscriber
         * looking for a subject that is right here. */
        res_err(res, 416, "those messages have been trimmed. Move on with "
                          "POST /ack/<subject>?consumer=&index= "
                          "(sukkal seek), or start over by deleting the "
                          "subscription\n");
        return;
    }
    if (e) {
        res_err(res, status_for(e), "no such subject\n");
        return;
    }

    /* The cursor to send next time, and how far behind it is — both
     * cheap enough to put in headers so a client can pace itself without
     * decoding the body. */
    char buf[32];
    snprintf(buf, sizeof buf, "%d", count);
    res->header(res->impl, "X-Sukkal-Count", buf);
    snprintf(buf, sizeof buf, "%llu", (unsigned long long)last);
    res->header(res->impl, "X-Sukkal-Last-Index", buf);
    if (skipped) {
        snprintf(buf, sizeof buf, "%llu", (unsigned long long)skipped);
        res->header(res->impl, "X-Sukkal-Skipped", buf);
    }
    if (has_consumer) {
        snprintf(buf, sizeof buf, "%llu", (unsigned long long)acked);
        res->header(res->impl, "X-Sukkal-Acked", buf);
    }

    res_bj(res, 200, out, out_len);
}

static void h_ack(sukkal_req *req, sukkal_res *res) {
    sukkal_app *a = req->ctx;

    const char *subject = subject_of(req, res, "/ack/");
    if (!subject) return;

    char consumer[BJM_CONSUMER_MAX + 1];
    int has_consumer = query_consumer(req, res, consumer, sizeof consumer);
    if (has_consumer < 0) return;
    if (!has_consumer) {
        res_err(res, 400, "?consumer=<name> is required\n");
        return;
    }

    uint64_t index = query_u64(req, "index", 0);
    if (index == 0) {
        res_err(res, 400, "?index=<n> is required and must be >= 1\n");
        return;
    }

    int e = bjm_cursor_set(a->store, subject, consumer, index);
    if (e) { res_err(res, status_for(e), "ack failed\n"); return; }

    /*
     * An explicit ack is the one a subscriber sends on its way out, so it
     * is also the point where paying for an fsync is worth it — losing it
     * would replay the batch the subscriber just finished.
     */
    bjm_cursor_sync(a->store);

    int found = 0;
    uint64_t acked = 0;
    bjm_cursor_get(a->store, subject, consumer, &found, &acked);

    bj_builder *b = a->bld;
    bj_builder_reset(b);
    bj_begin_object(b);
    bj_put_key(b, (const uint8_t *)"subject", 7);
    bj_put_string(b, (const uint8_t *)subject, (uint32_t)strlen(subject));
    bj_put_key(b, (const uint8_t *)"consumer", 8);
    bj_put_string(b, (const uint8_t *)consumer, (uint32_t)strlen(consumer));
    bj_put_key(b, (const uint8_t *)"acked", 5);
    bj_put_int(b, (int64_t)acked);
    bj_end_object(b);

    size_t out_len = 0;
    const uint8_t *out = bj_builder_data(b, &out_len);
    if (!out) { res_err(res, 500, "encode failed\n"); return; }
    res_bj(res, 200, out, out_len);
}

/* ---- push subscriptions ------------------------------------------------ */

/* As subject_of, for a path that names a subject *or* a wildcard pattern. */
static const char *pattern_of(sukkal_req *req, sukkal_res *res,
                              const char *prefix) {
    const char *p = req->path + strlen(prefix);
    int ok = bjm_pattern_is(p) ? bjm_pattern_valid(p) : bjm_subject_valid(p);
    if (!ok) {
        res_err(res, 400, "invalid subject or pattern: '.'-separated tokens "
                          "of [A-Za-z0-9_-], '*' for one token, '>' for the "
                          "rest (last only)\n");
        return NULL;
    }
    return p;
}

static void h_push_list(sukkal_req *req, sukkal_res *res) {
    sukkal_app *a = req->ctx;
    const uint8_t *out = NULL;
    size_t out_len = 0;
    int e = bjm_pusher_list(a->push, &out, &out_len);
    if (e) { res_err(res, status_for(e), "listing failed\n"); return; }
    res_bj(res, 200, out, out_len);
}

static void h_push_put(sukkal_req *req, sukkal_res *res) {
    sukkal_app *a = req->ctx;

    const char *pattern = pattern_of(req, res, "/push/");
    if (!pattern) return;

    char consumer[BJM_CONSUMER_MAX + 1];
    int has_consumer = query_consumer(req, res, consumer, sizeof consumer);
    if (has_consumer < 0) return;
    if (!has_consumer) {
        res_err(res, 400, "?consumer=<name> is required: it is the "
                          "subscription's identity and where its receipt "
                          "is kept\n");
        return;
    }

    bjm_push_sub cfg;
    memset(&cfg, 0, sizeof cfg);
    snprintf(cfg.pattern, sizeof cfg.pattern, "%s", pattern);

    if (req->query_get(req->impl, "callback", cfg.callback,
                              sizeof cfg.callback) != 1 ||
        !bjm_callback_valid(cfg.callback)) {
        res_err(res, 400, "?callback=<url> is required: an http:// or "
                          "https:// URL this broker can reach, which is "
                          "where messages are POSTed\n");
        return;
    }
    /*
     * Shipped on every delivery as a bearer token, so the subscriber can
     * tell this broker's POSTs from anybody else's. It is the subscriber
     * proving the broker, not the other way round — see README on the
     * asymmetry.
     */
    if (req->query_get(req->impl, "token", cfg.token, sizeof cfg.token) == 1 &&
        !bjm_token_valid(cfg.token)) {
        res_err(res, 400, "invalid token: up to 128 printable, space-free "
                          "bytes\n");
        return;
    }
    cfg.batch_bytes = query_u64(req, "batch", 0);

    /*
     * With ?group=, this registers a pushed *worker* rather than a
     * subscriber: the broker leases jobs from the queue group instead of
     * reading from a receipt, and the response settles them. Everything
     * else — the callback, the token, the retries — is the same.
     */
    int has_group = query_group(req, res, cfg.group, sizeof cfg.group);
    if (has_group < 0) return;
    cfg.max_jobs = query_u64(req, "max", 0);
    if (cfg.max_jobs > BJM_PUSH_JOBS_MAX) cfg.max_jobs = BJM_PUSH_JOBS_MAX;

    /*
     * A brand-new subscription gets its receipts written now, before the
     * first delivery, so where it starts is settled once. An existing one
     * keeps its place: re-registering is how a subscriber that restarted
     * on a different port says where it moved to, and it must not be a
     * way to accidentally replay the log.
     */
    int found = 0;
    bjm_push_sub existing;
    int e = bjm_push_get(a->store, consumer, &found, &existing);
    if (e) { res_err(res, status_for(e), "lookup failed\n"); return; }

    /* A queue group has no receipt to seed: where a worker starts is
     * wherever the group's own cursor already is, shared with every other
     * member. */
    if (!found && !has_group) {
        char start[8];
        int at_end = req->query_get(req->impl, "start", start,
                                           sizeof start) == 1 &&
                     strcmp(start, "last") == 0;
        bjm_pusher_seed(a->push, consumer, cfg.pattern, at_end,
                        query_u64(req, "from", 0));
    }

    e = bjm_push_set(a->store, consumer, &cfg);
    if (e) { res_err(res, status_for(e), "subscribe failed\n"); return; }
    if (bjm_pusher_add(a->push, consumer, &cfg) != 0) {
        res_err(res, 503, "out of memory\n");
        return;
    }

    bj_builder *b = a->bld;
    bj_builder_reset(b);
    bj_begin_object(b);
    bj_put_key(b, (const uint8_t *)"consumer", 8);
    bj_put_string(b, (const uint8_t *)consumer, (uint32_t)strlen(consumer));
    bj_put_key(b, (const uint8_t *)"pattern", 7);
    bj_put_string(b, (const uint8_t *)cfg.pattern,
                  (uint32_t)strlen(cfg.pattern));
    bj_put_key(b, (const uint8_t *)"callback", 8);
    bj_put_string(b, (const uint8_t *)cfg.callback,
                  (uint32_t)strlen(cfg.callback));
    bj_put_key(b, (const uint8_t *)"group", 5);
    bj_put_string(b, (const uint8_t *)cfg.group, (uint32_t)strlen(cfg.group));
    /* False means the subscription was already here and kept its place. */
    bj_put_key(b, (const uint8_t *)"created", 7);
    bj_put_bool(b, !found);
    bj_end_object(b);

    size_t out_len = 0;
    const uint8_t *out = bj_builder_data(b, &out_len);
    if (!out) { res_err(res, 500, "encode failed\n"); return; }
    res_bj(res, 200, out, out_len);
}

static void h_push_delete(sukkal_req *req, sukkal_res *res) {
    sukkal_app *a = req->ctx;

    char consumer[BJM_CONSUMER_MAX + 1];
    int has_consumer = query_consumer(req, res, consumer, sizeof consumer);
    if (has_consumer < 0) return;
    if (!has_consumer) {
        res_err(res, 400, "?consumer=<name> is required\n");
        return;
    }

    int deleted = 0;
    int e = bjm_push_delete(a->store, consumer, &deleted);
    if (e) { res_err(res, status_for(e), "delete failed\n"); return; }
    bjm_pusher_remove(a->push, consumer);

    /*
     * The receipt normally stays. Unregistering says "stop sending to
     * that address", which a durable subscriber does every time it shuts
     * down; forgetting how far it had read as well would replay the whole
     * log the next time it came back.
     *
     * ?purge=1 says this was a throwaway subscription that is not coming
     * back, and its position should go with it. Something has to: a
     * receipt holds retention off the messages below it, so a session
     * that merely stopped would pin the log it read forever.
     */
    int purged = 0;
    if (query_u64(req, "purge", 0)) {
        e = bjm_cursor_delete_consumer(a->store, consumer, &purged);
        if (e) { res_err(res, status_for(e), "purge failed\n"); return; }
    }

    bj_builder *b = a->bld;
    bj_builder_reset(b);
    bj_begin_object(b);
    bj_put_key(b, (const uint8_t *)"consumer", 8);
    bj_put_string(b, (const uint8_t *)consumer, (uint32_t)strlen(consumer));
    bj_put_key(b, (const uint8_t *)"deleted", 7);
    bj_put_bool(b, deleted);
    bj_put_key(b, (const uint8_t *)"purged", 6);
    bj_put_int(b, purged);
    bj_end_object(b);

    size_t out_len = 0;
    const uint8_t *out = bj_builder_data(b, &out_len);
    if (!out) { res_err(res, 500, "encode failed\n"); return; }
    res_bj(res, 200, out, out_len);
}

static void h_consumers(sukkal_req *req, sukkal_res *res) {
    sukkal_app *a = req->ctx;

    const char *subject = subject_of(req, res, "/consumers/");
    if (!subject) return;

    const uint8_t *out = NULL;
    size_t out_len = 0;
    int e = bjm_consumers(a->store, subject, &out, &out_len);
    if (e) { res_err(res, status_for(e), "listing failed\n"); return; }
    res_bj(res, 200, out, out_len);
}

static void h_unsubscribe(sukkal_req *req, sukkal_res *res) {
    sukkal_app *a = req->ctx;

    const char *subject = subject_of(req, res, "/consumers/");
    if (!subject) return;

    char consumer[BJM_CONSUMER_MAX + 1];
    int has_consumer = query_consumer(req, res, consumer, sizeof consumer);
    if (has_consumer < 0) return;
    if (!has_consumer) {
        res_err(res, 400, "?consumer=<name> is required\n");
        return;
    }

    int deleted = 0;
    int e = bjm_cursor_delete(a->store, subject, consumer, &deleted);
    if (e) { res_err(res, status_for(e), "delete failed\n"); return; }

    bj_builder *b = a->bld;
    bj_builder_reset(b);
    bj_begin_object(b);
    bj_put_key(b, (const uint8_t *)"subject", 7);
    bj_put_string(b, (const uint8_t *)subject, (uint32_t)strlen(subject));
    bj_put_key(b, (const uint8_t *)"consumer", 8);
    bj_put_string(b, (const uint8_t *)consumer, (uint32_t)strlen(consumer));
    bj_put_key(b, (const uint8_t *)"deleted", 7);
    bj_put_bool(b, deleted);
    bj_end_object(b);

    size_t out_len = 0;
    const uint8_t *out = bj_builder_data(b, &out_len);
    if (!out) { res_err(res, 500, "encode failed\n"); return; }
    /* Deleting a subscription that is already gone is a success: DELETE
     * is idempotent, and `deleted` carries whether anything was there.
     * (A 404 here would also have to be a text body, breaking the
     * binjson-on-success rule the clients decode against.) */
    res_bj(res, 200, out, out_len);
}

static void h_info(sukkal_req *req, sukkal_res *res) {
    sukkal_app *a = req->ctx;

    const char *subject = subject_of(req, res, "/info/");
    if (!subject) return;

    uint64_t base = 0, last = 0, bytes = 0;
    int e = bjm_subject_info(a->store, subject, &base, &last, &bytes);
    if (e) { res_err(res, status_for(e), "no such subject\n"); return; }

    int nconsumers = 0;
    uint64_t min_acked = 0;
    bjm_consumer_stats(a->store, subject, &nconsumers, &min_acked);

    bj_builder *b = a->bld;
    bj_builder_reset(b);
    bj_begin_object(b);
    bj_put_key(b, (const uint8_t *)"subject", 7);
    bj_put_string(b, (const uint8_t *)subject, (uint32_t)strlen(subject));
    /* The log holds (base, last]: `base` is where trimming has cut to, so
     * `first` is the oldest message still readable. */
    bj_put_key(b, (const uint8_t *)"base", 4);
    bj_put_int(b, (int64_t)base);
    bj_put_key(b, (const uint8_t *)"first", 5);
    bj_put_int(b, (int64_t)(last > base ? base + 1 : 0));
    bj_put_key(b, (const uint8_t *)"last", 4);
    bj_put_int(b, (int64_t)last);
    bj_put_key(b, (const uint8_t *)"messages", 8);
    bj_put_int(b, (int64_t)(last - base));
    bj_put_key(b, (const uint8_t *)"bytes", 5);
    bj_put_int(b, (int64_t)bytes);
    bj_put_key(b, (const uint8_t *)"consumers", 9);
    bj_put_int(b, nconsumers);
    bj_end_object(b);

    size_t out_len = 0;
    const uint8_t *out = bj_builder_data(b, &out_len);
    if (!out) { res_err(res, 500, "encode failed\n"); return; }
    res_bj(res, 200, out, out_len);
}

static void h_trim(sukkal_req *req, sukkal_res *res) {
    sukkal_app *a = req->ctx;

    const char *subject = subject_of(req, res, "/trim/");
    if (!subject) return;

    uint64_t last = bjm_last_index(a->store, subject);
    uint64_t before = query_u64(req, "before", 0);
    uint64_t keep = query_u64(req, "keep", 0);
    if (keep > 0) {
        /* Keep the newest `keep` messages: everything below that goes. */
        before = last > keep ? last - keep + 1 : 1;
    }
    if (before == 0) {
        res_err(res, 400, "?before=<n> or ?keep=<n> is required\n");
        return;
    }
    int force = query_u64(req, "force", 0) != 0;

    uint64_t new_base = 0, removed = 0;
    int e = bjm_trim(a->store, subject, before, force, &new_base, &removed);
    if (e) { res_err(res, status_for(e), "trim failed\n"); return; }

    bj_builder *b = a->bld;
    bj_builder_reset(b);
    bj_begin_object(b);
    bj_put_key(b, (const uint8_t *)"subject", 7);
    bj_put_string(b, (const uint8_t *)subject, (uint32_t)strlen(subject));
    bj_put_key(b, (const uint8_t *)"removed", 7);
    bj_put_int(b, (int64_t)removed);
    bj_put_key(b, (const uint8_t *)"base", 4);
    bj_put_int(b, (int64_t)new_base);
    bj_put_key(b, (const uint8_t *)"last", 4);
    bj_put_int(b, (int64_t)bjm_last_index(a->store, subject));
    bj_end_object(b);

    size_t out_len = 0;
    const uint8_t *out = bj_builder_data(b, &out_len);
    if (!out) { res_err(res, 500, "encode failed\n"); return; }
    res_bj(res, 200, out, out_len);
}

/* Every queue route needs a valid subject and group; answers the request
 * itself and returns 0 when either is missing. */
static int queue_target(sukkal_req *req, sukkal_res *res,
                        const char *prefix, const char **subject,
                        char *group, size_t group_size) {
    *subject = subject_of(req, res, prefix);
    if (!*subject) return 0;
    int has = query_group(req, res, group, group_size);
    if (has < 0) return 0;
    if (!has) {
        res_err(res, 400, "?group=<name> is required\n");
        return 0;
    }
    return 1;
}

static void h_take(sukkal_req *req, sukkal_res *res) {
    sukkal_app *a = req->ctx;

    const char *subject;
    char group[BJM_GROUP_MAX + 1];
    if (!queue_target(req, res, "/take/", &subject, group, sizeof group)) return;

    int max = (int)query_u64(req, "max", 1);
    /* UINT64_MAX means "whatever the group is configured for". */
    uint64_t lease = query_u64(req, "lease", UINT64_MAX);

    int count = 0;
    const uint8_t *out = NULL;
    size_t out_len = 0;
    int e = bjm_take(a->store, subject, group, max, lease,
                     &count, &out, &out_len);
    if (e) { res_err(res, status_for(e), "no such subject\n"); return; }

    char buf[32];
    snprintf(buf, sizeof buf, "%d", count);
    res->header(res->impl, "X-Sukkal-Count", buf);
    res_bj(res, 200, out, out_len);
}

static void h_job_end(sukkal_req *req, sukkal_res *res, int done) {
    sukkal_app *a = req->ctx;

    const char *subject;
    char group[BJM_GROUP_MAX + 1];
    const char *prefix = done ? "/done/" : "/fail/";
    if (!queue_target(req, res, prefix, &subject, group, sizeof group)) return;

    uint64_t index = query_u64(req, "index", 0);
    if (index == 0) {
        res_err(res, 400, "?index=<n> is required\n");
        return;
    }

    /* Absent ?delay= means "use the group's backoff policy". */
    uint64_t delay = query_u64(req, "delay", UINT64_MAX);

    int found = 0;
    uint64_t retry_in = 0;
    int e = done ? bjm_done(a->store, subject, group, index, &found)
                 : bjm_fail(a->store, subject, group, index, delay,
                            &found, &retry_in);
    if (e) { res_err(res, status_for(e), "queue update failed\n"); return; }

    bj_builder *b = a->bld;
    bj_builder_reset(b);
    bj_begin_object(b);
    bj_put_key(b, (const uint8_t *)"subject", 7);
    bj_put_string(b, (const uint8_t *)subject, (uint32_t)strlen(subject));
    bj_put_key(b, (const uint8_t *)"group", 5);
    bj_put_string(b, (const uint8_t *)group, (uint32_t)strlen(group));
    bj_put_key(b, (const uint8_t *)"index", 5);
    bj_put_int(b, (int64_t)index);
    /*
     * 0 means the lease was not held — the job had already expired and
     * been handed to somebody else, or it was never leased at all. Worth
     * reporting rather than swallowing: it is how a worker learns it ran
     * past its lease and its result may be a duplicate.
     */
    bj_put_key(b, (const uint8_t *)"held", 4);
    bj_put_bool(b, found);
    if (!done) {
        bj_put_key(b, (const uint8_t *)"retry_in_ms", 11);
        bj_put_int(b, (int64_t)retry_in);
    }
    bj_end_object(b);

    size_t out_len = 0;
    const uint8_t *out = bj_builder_data(b, &out_len);
    if (!out) { res_err(res, 500, "encode failed\n"); return; }
    res_bj(res, 200, out, out_len);
}

static void h_done(sukkal_req *req, sukkal_res *res) {
    h_job_end(req, res, 1);
}

static void h_fail(sukkal_req *req, sukkal_res *res) {
    h_job_end(req, res, 0);
}

/*
 * A subject's dead-letter channel.
 *
 * It is stored as the ordinary subject "<subject>.dead" and can be read
 * through /sub like any other — but only once something has died, because
 * a subject does not exist until it is published to. That makes an empty
 * channel a 404, which is the wrong answer: the channel is a property of
 * the subject, not a resource of its own, so "nothing has died here" is
 * an emptiness and not an absence. Both clients used to special-case the
 * 404; this route means neither has to.
 */
static void h_dead(sukkal_req *req, sukkal_res *res) {
    sukkal_app *a = req->ctx;

    const char *subject = subject_of(req, res, "/dead/");
    if (!subject) return;

    size_t slen = strlen(subject), dlen = sizeof BJM_DEAD_SUFFIX - 1;
    if (slen >= dlen && strcmp(subject + slen - dlen, BJM_DEAD_SUFFIX) == 0) {
        res_err(res, 400, "that is already a dead-letter channel; it does "
                          "not have one of its own\n");
        return;
    }

    char dead[BJM_SUBJECT_MAX + 1];
    int n = snprintf(dead, sizeof dead, "%s%s", subject, BJM_DEAD_SUFFIX);
    if (n < 0 || (size_t)n >= sizeof dead) {
        res_err(res, 400, "subject name too long to have a dead-letter "
                          "channel\n");
        return;
    }

    uint64_t from = query_u64(req, "from", 1);
    uint64_t max  = query_u64(req, "max", SUKKAL_DEFAULT_BATCH);
    if (max == 0 || max > SUKKAL_MAX_BODY_BYTES) max = SUKKAL_DEFAULT_BATCH;
    if (from == 0) from = 1;

    int count = 0;
    uint64_t last = 0;
    const uint8_t *out = NULL;
    size_t out_len = 0;
    int e = bjm_read(a->store, dead, from, (size_t)max,
                     &count, &out, &out_len, &last);

    if (e == BJ_ERR_STATE) {
        /* No channel yet, so nothing has died: an empty batch, shaped
         * exactly like a full one so a caller decodes it the same way. */
        bj_builder *b = a->bld;
        bj_builder_reset(b);
        bj_begin_array(b);
        bj_end_array(b);
        out = bj_builder_data(b, &out_len);
        if (!out) { res_err(res, 500, "encode failed\n"); return; }
        res->header(res->impl, "X-Sukkal-Count", "0");
        res_bj(res, 200, out, out_len);
        return;
    }
    if (e) { res_err(res, status_for(e), "dead-letter read failed\n"); return; }

    char buf[32];
    snprintf(buf, sizeof buf, "%d", count);
    res->header(res->impl, "X-Sukkal-Count", buf);
    snprintf(buf, sizeof buf, "%llu", (unsigned long long)last);
    res->header(res->impl, "X-Sukkal-Last-Index", buf);
    res_bj(res, 200, out, out_len);
}

static void h_requeue(sukkal_req *req, sukkal_res *res) {
    sukkal_app *a = req->ctx;

    const char *subject = subject_of(req, res, "/requeue/");
    if (!subject) return;

    uint64_t index = query_u64(req, "index", 0);
    if (index == 0) {
        res_err(res, 400, "?index=<n> is required (the index in "
                          "<subject>.dead, as shown by sukkal dead)\n");
        return;
    }

    /* A dead-letter channel has no channel of its own, so there is
     * nothing here to requeue from. Say that rather than reporting a
     * missing index. */
    size_t slen = strlen(subject), dlen = sizeof BJM_DEAD_SUFFIX - 1;
    if (slen >= dlen && strcmp(subject + slen - dlen, BJM_DEAD_SUFFIX) == 0) {
        res_err(res, 400, "that is already a dead-letter channel; requeue "
                          "names the original subject\n");
        return;
    }

    uint64_t at = 0;
    int e = bjm_requeue(a->store, subject, index, &at);
    if (e == BJ_ERR_VERIFY) {
        res_err(res, 422, "that message is not a dead-letter envelope\n");
        return;
    }
    if (e == BJ_ERR_RANGE) {
        res_err(res, 416, "no such index in the dead-letter channel; "
                          "sukkal dead lists them\n");
        return;
    }
    if (e) { res_err(res, status_for(e), "requeue failed\n"); return; }

    bj_builder *b = a->bld;
    bj_builder_reset(b);
    bj_begin_object(b);
    bj_put_key(b, (const uint8_t *)"subject", 7);
    bj_put_string(b, (const uint8_t *)subject, (uint32_t)strlen(subject));
    bj_put_key(b, (const uint8_t *)"from_dead_index", 15);
    bj_put_int(b, (int64_t)index);
    /* A new index, not the old one: the original is still in the log and
     * still dead, and reusing its id would be a lie about ordering. */
    bj_put_key(b, (const uint8_t *)"index", 5);
    bj_put_int(b, (int64_t)at);
    bj_end_object(b);

    size_t out_len = 0;
    const uint8_t *out = bj_builder_data(b, &out_len);
    if (!out) { res_err(res, 500, "encode failed\n"); return; }
    res_bj(res, 200, out, out_len);
}

static void h_queues(sukkal_req *req, sukkal_res *res) {
    sukkal_app *a = req->ctx;

    const char *subject = subject_of(req, res, "/queue/");
    if (!subject) return;

    const uint8_t *out = NULL;
    size_t out_len = 0;
    int e = bjm_queues(a->store, subject, &out, &out_len);
    if (e) { res_err(res, status_for(e), "listing failed\n"); return; }
    res_bj(res, 200, out, out_len);
}

static void h_queue_config(sukkal_req *req, sukkal_res *res) {
    sukkal_app *a = req->ctx;

    const char *subject;
    char group[BJM_GROUP_MAX + 1];
    if (!queue_target(req, res, "/queue/", &subject, group, sizeof group)) return;

    uint64_t lease = query_u64(req, "lease_ms", BJM_LEASE_DEFAULT_MS);
    uint64_t attempts = query_u64(req, "max_attempts", BJM_MAX_ATTEMPTS_DEFAULT);
    uint64_t backoff = query_u64(req, "backoff_ms", BJM_BACKOFF_DEFAULT_MS);
    uint64_t max_backoff = query_u64(req, "max_backoff_ms",
                                     BJM_MAX_BACKOFF_DEFAULT_MS);
    int e = bjm_queue_config(a->store, subject, group, lease, attempts,
                             backoff, max_backoff);
    if (e) { res_err(res, status_for(e), "queue config failed\n"); return; }
    h_queues(req, res);
}

static void h_queue_delete(sukkal_req *req, sukkal_res *res) {
    sukkal_app *a = req->ctx;

    const char *subject;
    char group[BJM_GROUP_MAX + 1];
    if (!queue_target(req, res, "/queue/", &subject, group, sizeof group)) return;

    int deleted = 0;
    int e = bjm_queue_delete(a->store, subject, group, &deleted);
    if (e) { res_err(res, status_for(e), "queue delete failed\n"); return; }

    bj_builder *b = a->bld;
    bj_builder_reset(b);
    bj_begin_object(b);
    bj_put_key(b, (const uint8_t *)"subject", 7);
    bj_put_string(b, (const uint8_t *)subject, (uint32_t)strlen(subject));
    bj_put_key(b, (const uint8_t *)"group", 5);
    bj_put_string(b, (const uint8_t *)group, (uint32_t)strlen(group));
    bj_put_key(b, (const uint8_t *)"deleted", 7);
    bj_put_bool(b, deleted);
    bj_end_object(b);

    size_t out_len = 0;
    const uint8_t *out = bj_builder_data(b, &out_len);
    if (!out) { res_err(res, 500, "encode failed\n"); return; }
    res_bj(res, 200, out, out_len);
}

static void h_policy_get(sukkal_req *req, sukkal_res *res) {
    sukkal_app *a = req->ctx;

    const char *subject = subject_of(req, res, "/policy/");
    if (!subject) return;

    int found = 0;
    bjm_policy p;
    int e = bjm_policy_get(a->store, subject, &found, &p);
    if (e) { res_err(res, status_for(e), "policy lookup failed\n"); return; }

    bj_builder *b = a->bld;
    bj_builder_reset(b);
    bj_begin_object(b);
    bj_put_key(b, (const uint8_t *)"subject", 7);
    bj_put_string(b, (const uint8_t *)subject, (uint32_t)strlen(subject));
    bj_put_key(b, (const uint8_t *)"policy", 6);
    bj_put_bool(b, found);
    bj_put_key(b, (const uint8_t *)"max_age_s", 9);
    bj_put_int(b, (int64_t)p.max_age_s);
    bj_put_key(b, (const uint8_t *)"max_messages", 12);
    bj_put_int(b, (int64_t)p.max_messages);
    bj_put_key(b, (const uint8_t *)"max_bytes", 9);
    bj_put_int(b, (int64_t)p.max_bytes);
    bj_put_key(b, (const uint8_t *)"ignore_consumers", 16);
    bj_put_bool(b, p.ignore_consumers);
    bj_end_object(b);

    size_t out_len = 0;
    const uint8_t *out = bj_builder_data(b, &out_len);
    if (!out) { res_err(res, 500, "encode failed\n"); return; }
    res_bj(res, 200, out, out_len);
}

static void h_policy_put(sukkal_req *req, sukkal_res *res) {
    sukkal_app *a = req->ctx;

    const char *subject = subject_of(req, res, "/policy/");
    if (!subject) return;

    /* Absent dimensions stay at 0, which is "unlimited" — a PUT replaces
     * the whole policy rather than patching it, so what you send is what
     * the subject has. */
    bjm_policy p = {
        .max_age_s        = query_u64(req, "max_age_s", 0),
        .max_messages     = query_u64(req, "max_messages", 0),
        .max_bytes        = query_u64(req, "max_bytes", 0),
        .ignore_consumers = query_u64(req, "ignore_consumers", 0) != 0,
    };
    if (!p.max_age_s && !p.max_messages && !p.max_bytes) {
        res_err(res, 400, "a policy needs at least one of max_age_s, "
                          "max_messages, max_bytes\n");
        return;
    }

    int e = bjm_policy_set(a->store, subject, &p);
    if (e) { res_err(res, status_for(e), "policy write failed\n"); return; }
    h_policy_get(req, res);
}

static void h_policy_delete(sukkal_req *req, sukkal_res *res) {
    sukkal_app *a = req->ctx;

    const char *subject = subject_of(req, res, "/policy/");
    if (!subject) return;

    int cleared = 0;
    int e = bjm_policy_clear(a->store, subject, &cleared);
    if (e) { res_err(res, status_for(e), "policy delete failed\n"); return; }

    bj_builder *b = a->bld;
    bj_builder_reset(b);
    bj_begin_object(b);
    bj_put_key(b, (const uint8_t *)"subject", 7);
    bj_put_string(b, (const uint8_t *)subject, (uint32_t)strlen(subject));
    bj_put_key(b, (const uint8_t *)"cleared", 7);
    bj_put_bool(b, cleared);
    bj_end_object(b);

    size_t out_len = 0;
    const uint8_t *out = bj_builder_data(b, &out_len);
    if (!out) { res_err(res, 500, "encode failed\n"); return; }
    res_bj(res, 200, out, out_len);
}

static void h_policies(sukkal_req *req, sukkal_res *res) {
    sukkal_app *a = req->ctx;
    const uint8_t *out = NULL;
    size_t out_len = 0;
    int e = bjm_policy_list(a->store, &out, &out_len);
    if (e) { res_err(res, status_for(e), "listing failed\n"); return; }
    res_bj(res, 200, out, out_len);
}

static void h_subjects(sukkal_req *req, sukkal_res *res) {
    sukkal_app *a = req->ctx;

    char pattern[BJM_SUBJECT_MAX + 1];
    int has = req->query_get(req->impl, "pattern", pattern, sizeof pattern);
    if (has == 1 && !bjm_pattern_valid(pattern)) {
        res_err(res, 400, "invalid pattern: '.'-separated tokens, '*' for one "
                          "token, '>' for the rest (last only)\n");
        return;
    }

    char *names = NULL;
    size_t names_len = 0;
    int owned = 0;
    if (bjm_store_listing(a->store, &names, &names_len, &owned) != BJ_OK) {
        res_err(res, 500, "listing failed\n");
        return;
    }

    const uint8_t *out = NULL;
    size_t out_len = 0;
    int e = bjm_subjects(a->store, names, names_len,
                         has == 1 ? pattern : NULL, &out, &out_len);
    if (owned) free(names);
    if (e) { res_err(res, status_for(e), "listing failed\n"); return; }
    res_bj(res, 200, out, out_len);
}

static void h_health(sukkal_req *req, sukkal_res *res) {
    sukkal_app *a = req->ctx;
    const char *backend = a->backend ? a->backend : "none";

    int nsubjects = 0;
    char *names = NULL;
    size_t names_len = 0;
    int owned = 0;
    if (bjm_store_listing(a->store, &names, &names_len, &owned) == BJ_OK) {
        bjm_subject_count(a->store, names, names_len, &nsubjects);
        if (owned) free(names);
    }

    bj_builder *b = a->bld;
    bj_builder_reset(b);
    bj_begin_object(b);
    bj_put_key(b, (const uint8_t *)"ok", 2);
    bj_put_bool(b, 1);
    bj_put_key(b, (const uint8_t *)"backend", 7);
    bj_put_string(b, (const uint8_t *)backend, (uint32_t)strlen(backend));
    bj_put_key(b, (const uint8_t *)"subjects", 8);
    bj_put_int(b, nsubjects);
    bj_put_key(b, (const uint8_t *)"connections", 11);
    bj_put_int(b, a->conn_count ? a->conn_count(a->conn_ctx) : 0);
    bj_put_key(b, (const uint8_t *)"uptime_s", 8);
    bj_put_int(b, (int64_t)(bjm_store_now_ms(a->store) / 1000 - a->started_s));
    bj_put_key(b, (const uint8_t *)"dedup_window_ms", 15);
    bj_put_int(b, (int64_t)bjm_dedup_window(a->store));
    bj_end_object(b);

    size_t out_len = 0;
    const uint8_t *out = bj_builder_data(b, &out_len);
    if (!out) { res_err(res, 500, "encode failed\n"); return; }
    res_bj(res, 200, out, out_len);
}

static void h_not_found(sukkal_req *req, sukkal_res *res) {
    (void)req;
    res_err(res, 404,
        "no such route. available:\n"
        "  POST   /pub/<subject>[?id=][&headers=1][&ack_subject=&ack_consumer=&ack_index=]\n"
        "  PUT    /push/<subject|pattern>?consumer=&callback=[&group=&max=&token=&batch=&start=last|&from=]\n"
        "  DELETE /push?consumer=\n"
        "  GET    /push\n"
        "  GET    /sub/<subject>?from=|consumer=\n"
        "  POST   /ack/<subject>?consumer=&index=\n"
        "  POST   /trim/<subject>?before=|keep=[&force=1]\n"
        "  POST   /take/<subject>?group=&max=&lease=\n"
        "  POST   /done/<subject>?group=&index=\n"
        "  POST   /fail/<subject>?group=&index=[&delay=]\n"
        "  GET    /dead/<subject>[?from=&max=]\n"
        "  POST   /requeue/<subject>?index=   (from <subject>.dead)\n"
        "  GET    /queue/<subject>\n"
        "  PUT    /queue/<subject>?group=&lease_ms=&max_attempts=&backoff_ms=\n"
        "  DELETE /queue/<subject>?group=\n"
        "  GET    /consumers/<subject>\n"
        "  DELETE /consumers/<subject>?consumer=\n"
        "  GET    /info/<subject>\n"
        "  GET    /subjects[?pattern=]\n"
        "  GET    /health\n");
}

/* ---- routing ---------------------------------------------------------
 *
 * The table IS the protocol, so it lives beside the handlers and compiles
 * wherever they do. Matching is exact, or a trailing '*' prefix — the whole
 * of what these routes need, and small enough that owning it costs less
 * than depending on someone else's.
 */

static const struct {
    const char    *method;
    const char    *path;
    sukkal_handler fn;
} ROUTES[] = {
    { "POST",   "/pub/*",       h_publish        },
    { "PUT",    "/push/*",      h_push_put       },
    { "DELETE", "/push",        h_push_delete    },
    { "GET",    "/push",        h_push_list      },
    { "GET",    "/sub/*",       h_subscribe      },
    { "POST",   "/ack/*",       h_ack            },
    { "POST",   "/trim/*",      h_trim           },
    { "POST",   "/take/*",      h_take           },
    { "POST",   "/done/*",      h_done           },
    { "POST",   "/fail/*",      h_fail           },
    { "GET",    "/dead/*",      h_dead           },
    { "POST",   "/requeue/*",   h_requeue        },
    { "GET",    "/queue/*",     h_queues         },
    { "PUT",    "/queue/*",     h_queue_config   },
    { "DELETE", "/queue/*",     h_queue_delete   },
    { "GET",    "/consumers/*", h_consumers      },
    { "DELETE", "/consumers/*", h_unsubscribe    },
    { "GET",    "/info/*",      h_info           },
    { "GET",    "/policy/*",    h_policy_get     },
    { "PUT",    "/policy/*",    h_policy_put     },
    { "DELETE", "/policy/*",    h_policy_delete  },
    { "GET",    "/policies",    h_policies       },
    { "GET",    "/subjects",    h_subjects       },
    { "GET",    "/health",      h_health         },
};

static int path_matches(const char *pat, const char *path) {
    size_t n = strlen(pat);
    if (pat[n - 1] == '*') return strncmp(path, pat, n - 1) == 0;
    return strcmp(path, pat) == 0;
}

/*
 * Look up a route, and say whether the PATH matched even when the method
 * did not — because those are different answers. http11c distinguished
 * them for us before, with an automatic 405 that never reached the
 * fallback; owning the table means owning that too, and collapsing both
 * into 404 would tell a client its URL was wrong when its verb was.
 */
static sukkal_handler route_lookup(const char *method, const char *path,
                                   int *path_matched) {
    *path_matched = 0;
    for (size_t i = 0; i < sizeof ROUTES / sizeof *ROUTES; i++) {
        if (!path_matches(ROUTES[i].path, path)) continue;
        *path_matched = 1;
        if (strcmp(method, ROUTES[i].method) == 0) return ROUTES[i].fn;
    }
    return NULL;
}

sukkal_handler sukkal_route(const char *method, const char *path) {
    int matched;
    return route_lookup(method, path, &matched);
}

void sukkal_dispatch(sukkal_req *req, sukkal_res *res) {
    int path_matched = 0;
    sukkal_handler fn = route_lookup(req->method, req->path, &path_matched);
    if (fn) { fn(req, res); return; }
    if (path_matched) { res_err(res, 405, "405 Method Not Allowed\n"); return; }
    h_not_found(req, res);
}

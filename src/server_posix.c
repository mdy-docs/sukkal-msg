/*
 * server_posix.c — http11c behind the request seam.
 *
 * src/server.c holds the routing table and every handler, written against
 * sukkal_req/sukkal_res and nothing else, so it compiles wherever the store
 * does. This file is the half that owns a socket: it runs http11c, adapts
 * each parsed request into the shim, and calls sukkal_dispatch. A WASM host
 * calls sukkal_dispatch directly instead, with no socket in the picture at
 * all (docs/wasm-plan.md).
 *
 * Note what did NOT move here: the routes. http11c has a perfectly good
 * router and it used to own all 24 of them, which meant the protocol was
 * defined by the transport. One fallback handler now stands in for the lot,
 * because a table that decides what this broker answers to belongs beside
 * the code that answers.
 */
#include "sukkal.h"
#include "binjson.h"
#include "http11c.h"

#include <signal.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

/* How long an idle keep-alive connection is held, and how often retention
 * is swept. Both are this transport's business: nothing in the routing
 * table has an opinion about either. */
#define IDLE_TIMEOUT_SECS 120
#define RETENTION_SWEEP_SECS 10

static http11c_server *g_srv;   /* for the signal handler only */

/* ---- the seam --------------------------------------------------------- */

static int q_get(void *impl, const char *key, char *buf, size_t buf_len) {
    return http11c_req_query_get((http11c_request *)impl, key, buf, buf_len);
}

static void r_status(void *impl, int code) {
    http11c_res_status((http11c_response *)impl, code);
}

static void r_header(void *impl, const char *name, const char *value) {
    http11c_res_header((http11c_response *)impl, name, value);
}

static void r_write(void *impl, const uint8_t *data, size_t len) {
    http11c_res_write((http11c_response *)impl, data, len);
}

/*
 * Every request, adapted and dispatched. http11c has already parsed the
 * line, the headers and the body, which is why the shim is mostly plain
 * fields: the work was done before this is called.
 */
static void on_request(http11c_request *req, http11c_response *res) {
    char type[64];
    int has_type = http11c_req_content_type(req, type, sizeof type) == 1;

    size_t body_len = 0;
    const void *body = http11c_req_body(req, &body_len);

    sukkal_req sreq = {
        .ctx = http11c_req_ctx(req),
        .impl = req,
        .method = http11c_req_method(req),
        .path = http11c_req_path(req),
        .body = body,
        .body_len = body_len,
        .content_type = has_type ? type : NULL,
        .query_get = q_get,
    };
    sukkal_res sres = { res, r_status, r_header, r_write };

    sukkal_dispatch(&sreq, &sres);
}

/* ---- lifecycle ------------------------------------------------------- */

static volatile sig_atomic_t g_running = 1;

/* bjm_store_on_publish's hook: the store does not know what a pusher is,
 * and does not need to. */
static void on_publish(void *ctx, const char *subject, uint64_t index) {
    sukkal_app *a = ctx;
    (void)index;
    bjm_pusher_notify(a->push, subject);
}

static void on_signal(int sig) {
    (void)sig;
    g_running = 0;
    if (g_srv) http11c_stop(g_srv);
}

int bjm_serve(const char *host, int port, const char *dir,
              uint64_t dedup_window_ms) {
    sukkal_app a = {0};
    a.dir = dir;
    a.store = bjm_store_open(dir);
    if (!a.store) {
        fprintf(stderr, "sukkal: cannot open store at %s\n", dir);
        return 1;
    }
    if (dedup_window_ms) bjm_dedup_set_window(a.store, dedup_window_ms);
    a.bld = bj_builder_new();
    if (!a.bld) { bjm_store_free(a.store); return 1; }

    a.started_s = (uint64_t)time(NULL);

    a.push = bjm_pusher_new(a.store, dir, SUKKAL_DEFAULT_BATCH);
    if (!a.push) {
        fprintf(stderr, "sukkal: cannot start the delivery engine\n");
        bj_builder_free(a.bld);
        bjm_store_free(a.store);
        return 1;
    }
    /* Every append wakes whatever is subscribed to that subject — a
     * publish, a requeue, a job being dead-lettered, all of them. */
    bjm_store_on_publish(a.store, on_publish, &a);

    http11c_server *s = http11c_server_new();
    if (!s) {
        bjm_pusher_free(a.push);
        bj_builder_free(a.bld);
        bjm_store_free(a.store);
        return 1;
    }
    /* What /health reports about whatever is carrying the requests. */
    a.backend = http11c_backend();
    a.conn_count = (int (*)(void *))http11c_conn_count;
    a.conn_ctx = s;

    http11c_set_ctx(s, &a);
    http11c_set_max_body(s, SUKKAL_MAX_BODY_BYTES);
    http11c_set_idle_timeout(s, IDLE_TIMEOUT_SECS);

    http11c_set_fallback(s, on_request);

    int rc = 0;
    if (http11c_listen(s, host, port) != 0) {
        fprintf(stderr, "sukkal: cannot listen on %s:%d\n", host, port);
        rc = 1;
        goto done;
    }

    g_srv = s;
    signal(SIGINT, on_signal);
    signal(SIGTERM, on_signal);
    signal(SIGPIPE, SIG_IGN);

    fprintf(stderr, "sukkal: serving %s on http://%s:%d (%s), "
                    "%d push subscription(s)\n",
            dir, host, http11c_port(s), http11c_backend(),
            bjm_pusher_count(a.push));

    /*
     * Our own loop rather than http11c_run, because the broker has two
     * jobs: answering requests and making them. bjm_pusher_pump starts
     * whatever deliveries are due, services those in flight, and answers
     * how long we may block before it wants attention again — a couple of
     * milliseconds mid-delivery, a full second when idle. Retention gets
     * its sweep from the same loop.
     */
    time_t last_sweep = time(NULL);
    while (g_running) {
        int wait = bjm_pusher_pump(a.push, bjm_now_ms());
        if (http11c_poll(s, wait) < 0) { rc = 1; break; }

        time_t now = time(NULL);
        if (now - last_sweep < RETENTION_SWEEP_SECS) continue;
        last_sweep = now;

        uint64_t removed = 0;
        int trimmed = 0;
        int e = bjm_retention_run(a.store, (uint64_t)now, &removed, &trimmed);
        if (e)
            fprintf(stderr, "sukkal: retention sweep failed (%d)\n", e);
        else if (removed)
            fprintf(stderr, "sukkal: retention removed %llu message(s) "
                            "from %d subject(s)\n",
                    (unsigned long long)removed, trimmed);
    }
    fprintf(stderr, "sukkal: stopped\n");

done:
    g_srv = NULL;
    /* Before the store: a delivery still in flight would otherwise report
     * its receipt into freed memory. */
    bjm_store_on_publish(a.store, NULL, NULL);
    bjm_pusher_free(a.push);
    http11c_server_free(s);
    bj_builder_free(a.bld);
    bjm_store_free(a.store);
    return rc;
}

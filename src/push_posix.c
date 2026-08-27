/*
 * push_posix.c — libcurl behind the delivery seam.
 *
 * src/push.c decides what to deliver, to whom, with which headers, and
 * what a failure means. This file is the part that opens a socket: it
 * keeps a curl multi handle, turns each delivery into a transfer, and
 * reports every completion back through bjm_pusher_delivered.
 *
 * The easy-handle options below used to live in push.c, where they read
 * like broker policy and were nothing of the kind — HTTP/1.1, no
 * redirects, a connect timeout, and the Expect: suppression are all facts
 * about talking to an HTTP server. A transport that is a function call has
 * no use for any of them.
 */
#include "sukkal.h"

#include <curl/curl.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>

/* Long enough that a slow subscriber is not mistaken for a dead one, short
 * enough that a dead one does not hold a delivery slot all day. */
#define PUSH_CONNECT_TIMEOUT_MS 2000
#define PUSH_TIMEOUT_MS         30000

typedef struct curl_transport curl_transport;

/* One subscription's persistent connection. The broker keeps a
 * subscription for as long as it is registered, so the easy handle lives
 * that long too and libcurl reuses the connection under it. */
typedef struct {
    CURL           *easy;
    void           *sub;          /* the psub this belongs to */
    curl_transport *t;
    struct curl_slist *hdrs;      /* rebuilt per delivery */
} conn;

struct curl_transport {
    CURLM      *multi;
    bjm_pusher *p;
};

uint64_t bjm_now_ms(void) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (uint64_t)tv.tv_sec * 1000 + (uint64_t)tv.tv_usec / 1000;
}

static size_t on_body(char *data, size_t size, size_t nmemb, void *user) {
    conn *c = user;
    size_t n = size * nmemb;
    bjm_pusher_on_body(c->t->p, c->sub, data, n);
    return n;
}

static size_t on_hdr(char *data, size_t size, size_t nmemb, void *user) {
    conn *c = user;
    size_t n = size * nmemb;
    bjm_pusher_on_header(c->t->p, c->sub, data, n);
    return n;
}

static void *t_open(void *ctx) {
    curl_transport *t = ctx;
    conn *c = calloc(1, sizeof *c);
    if (!c) return NULL;
    c->t = t;
    c->easy = curl_easy_init();
    if (!c->easy) { free(c); return NULL; }

    curl_easy_setopt(c->easy, CURLOPT_PRIVATE, c);
    curl_easy_setopt(c->easy, CURLOPT_WRITEFUNCTION, on_body);
    curl_easy_setopt(c->easy, CURLOPT_WRITEDATA, c);
    curl_easy_setopt(c->easy, CURLOPT_HEADERFUNCTION, on_hdr);
    curl_easy_setopt(c->easy, CURLOPT_HEADERDATA, c);
    curl_easy_setopt(c->easy, CURLOPT_HTTP_VERSION, CURL_HTTP_VERSION_1_1);
    curl_easy_setopt(c->easy, CURLOPT_CONNECTTIMEOUT_MS, (long)PUSH_CONNECT_TIMEOUT_MS);
    curl_easy_setopt(c->easy, CURLOPT_TIMEOUT_MS, (long)PUSH_TIMEOUT_MS);
    curl_easy_setopt(c->easy, CURLOPT_FOLLOWLOCATION, 0L);
    curl_easy_setopt(c->easy, CURLOPT_POST, 1L);
    return c;
}

static void t_close(void *ctx, void *conn_) {
    curl_transport *t = ctx;
    conn *c = conn_;
    if (!c) return;
    if (c->easy) {
        curl_multi_remove_handle(t->multi, c->easy);
        curl_easy_cleanup(c->easy);
    }
    if (c->hdrs) curl_slist_free_all(c->hdrs);
    free(c);
}

static int t_send(void *ctx, void *conn_, void *sub, const char *url,
                  const char *headers, int nheaders,
                  const uint8_t *body, size_t body_len) {
    curl_transport *t = ctx;
    conn *c = conn_;
    if (!c) return -1;
    c->sub = sub;

    /* The seam hands over a NUL-separated buffer; libcurl wants a list. */
    if (c->hdrs) { curl_slist_free_all(c->hdrs); c->hdrs = NULL; }
    const char *line = headers;
    for (int i = 0; i < nheaders; i++) {
        struct curl_slist *n = curl_slist_append(c->hdrs, line);
        if (n) c->hdrs = n;
        line += strlen(line) + 1;
    }

    curl_easy_setopt(c->easy, CURLOPT_URL, url);
    curl_easy_setopt(c->easy, CURLOPT_HTTPHEADER, c->hdrs);
    curl_easy_setopt(c->easy, CURLOPT_POSTFIELDS, (const char *)body);
    curl_easy_setopt(c->easy, CURLOPT_POSTFIELDSIZE_LARGE, (curl_off_t)body_len);

    return curl_multi_add_handle(t->multi, c->easy) == CURLM_OK ? 0 : -1;
}

curl_transport *bjm_push_curl_new(bjm_pusher *p) {
    curl_transport *t = calloc(1, sizeof *t);
    if (!t) return NULL;
    t->p = p;
    t->multi = curl_multi_init();
    if (!t->multi) { free(t); return NULL; }

    sukkal_transport xfer = { t, t_open, t_close, t_send };
    bjm_pusher_set_transport(p, &xfer);
    return t;
}

void bjm_push_curl_free(curl_transport *t) {
    if (!t) return;
    if (t->multi) curl_multi_cleanup(t->multi);
    free(t);
}

/*
 * Service whatever is in flight. Called from the broker's own loop right
 * after bjm_pusher_pump has started whatever was due — the split is that
 * pump decides and this delivers.
 */
void bjm_push_curl_service(curl_transport *t, uint64_t now) {
    int running = 0;
    curl_multi_perform(t->multi, &running);

    CURLMsg *m;
    int left = 0;
    while ((m = curl_multi_info_read(t->multi, &left))) {
        if (m->msg != CURLMSG_DONE) continue;
        conn *c = NULL;
        curl_easy_getinfo(m->easy_handle, CURLINFO_PRIVATE, (char **)&c);
        if (!c) continue;

        CURLcode rc = m->data.result;
        long status = 0;
        if (rc == CURLE_OK) curl_easy_getinfo(m->easy_handle, CURLINFO_RESPONSE_CODE, &status);
        curl_multi_remove_handle(t->multi, m->easy_handle);

        /* "reached" is the distinction the broker cares about: an HTTP
         * status is the subscriber answering, and a 404 will not become a
         * 200 on a retry, where a refused connection might. */
        bjm_pusher_delivered(c->t->p, c->sub, rc == CURLE_OK, status,
                             rc == CURLE_OK ? NULL : curl_easy_strerror(rc), now);
    }
}

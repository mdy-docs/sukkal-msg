/*
 * store_posix.c — the POSIX shell over the portable store.
 *
 * src/store.c reaches the world through a bj_ns and two hooks and nothing
 * else, which is what lets it compile for WASM (docs/wasm-plan.md). The
 * three things it therefore cannot do for itself — make a directory, read
 * a clock, and replace one file with another atomically — live here, and
 * this file is not part of the WASM build.
 *
 * It is also where the directory listing comes from. bjns has no list()
 * because enumeration is asynchronous in OPFS, so a listing is passed IN
 * to the calls that need one; on POSIX, producing it is a readdir. That
 * belongs to the caller that knows the directory rather than to the store,
 * which by then knows only a namespace.
 */
#include "sukkal.h"
#include "bjns.h"
#include "bjio_posix.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

static uint64_t posix_now_ms(void *ctx) {
    (void)ctx;
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (uint64_t)ts.tv_sec * 1000 + (uint64_t)(ts.tv_nsec / 1000000);
}

/*
 * Atomic replace. Both names are length-counted, as bjns' are, and both
 * are subject file names — short, and already validated by the caller —
 * so a stack copy is enough to make them NUL-terminated for renameat.
 */
static int32_t posix_adopt(void *ctx, const char *from, uint32_t from_len,
                           const char *to, uint32_t to_len) {
    int dirfd = (int)(intptr_t)ctx;
    char f[BJM_SUBJECT_MAX + 32], t[BJM_SUBJECT_MAX + 32];
    if (from_len >= sizeof f || to_len >= sizeof t) return BJ_ERR_RANGE;
    memcpy(f, from, from_len); f[from_len] = '\0';
    memcpy(t, to, to_len);     t[to_len] = '\0';
    return renameat(dirfd, f, dirfd, t) == 0 ? BJ_OK : BJ_ERR_STATE;
}

/* The listing hook: a readdir, freshly each time, because a directory can
 * gain a subject between one request and the next. */
static int posix_listing(void *ctx, char **out, size_t *out_len, int *owned) {
    *owned = 1;
    return bjm_dir_listing((const char *)ctx, out, out_len);
}

bjm_store *bjm_store_open(const char *dir) {
    if (mkdir(dir, 0755) != 0 && errno != EEXIST) return NULL;

    int dirfd = open(dir, O_RDONLY);
    if (dirfd < 0) return NULL;

    bj_ns ns;
    if (bjns_posix_open(dirfd, &ns) != BJ_OK) { close(dirfd); return NULL; }

    bjm_store *st = bjm_store_open_ns(ns);
    if (!st) { bjns_posix_free(&ns); close(dirfd); return NULL; }

    /* `ns` and `dirfd` outlive this call and are never released: the
     * broker opens one store and exits with it, and freeing them at
     * bjm_store_free would mean the store owning a namespace the header
     * says it borrows. A host that opens stores repeatedly wants
     * bjm_store_open_ns and its own lifetimes. */
    bjm_store_set_clock(st, posix_now_ms, NULL);
    bjm_store_set_listing(st, posix_listing, (void *)dir);
    bjm_store_set_adopt(st, posix_adopt, (void *)(intptr_t)dirfd);
    return st;
}

/*
 * The directory's file names, NUL-separated, as bjm_subjects wants them.
 * Caller frees. Takes the path rather than the store, so it holds no state
 * and cannot disagree with a store about which directory it means.
 */
int bjm_dir_listing(const char *dir, char **out, size_t *out_len) {
    DIR *d = opendir(dir);
    if (!d) return BJ_ERR_STATE;

    size_t cap = 1024, len = 0;
    char *buf = malloc(cap);
    if (!buf) { closedir(d); return BJ_ERR_OOM; }

    struct dirent *ent;
    while ((ent = readdir(d)) != NULL) {
        size_t n = strlen(ent->d_name) + 1;
        if (len + n > cap) {
            size_t want = cap * 2;
            while (want < len + n) want *= 2;
            char *grown = realloc(buf, want);
            if (!grown) { free(buf); closedir(d); return BJ_ERR_OOM; }
            buf = grown; cap = want;
        }
        memcpy(buf + len, ent->d_name, n);
        len += n;
    }
    closedir(d);

    *out = buf;
    *out_len = len;
    return BJ_OK;
}

/*
 * Copyright (c) 2015 Tatsuhiko Kubo (cubicdaiya@gmail.com>)
 * Copyright (C) 2018 Aleksei Konovkin (alkon2000@mail.ru)
 */

extern "C" {

#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_http.h>
#include <ngx_inet.h>


#include <assert.h>

}

#include "ngx_dynamic_upstream_module.h"
#include "ngx_dynamic_upstream_op.h"


template <class S> static ngx_int_t
ngx_dynamic_upstream_op_add(typename TypeSelect<S>::peers_type *primary,
    ngx_dynamic_upstream_op_t *op, ngx_slab_pool_t *shpool,
    ngx_pool_t *temp_pool, ngx_log_t *log);


template <class S> static ngx_int_t
ngx_dynamic_upstream_op_sync(typename TypeSelect<S>::peers_type *primary,
    ngx_dynamic_upstream_op_t *op, ngx_slab_pool_t *shpool,
    ngx_pool_t *temp_pool, ngx_log_t *log);


template <class S> static ngx_int_t
ngx_dynamic_upstream_op_del(typename TypeSelect<S>::peers_type *primary,
    ngx_dynamic_upstream_op_t *op, ngx_slab_pool_t *shpool,
    ngx_pool_t *temp_pool, ngx_log_t *log);


template <class S> static ngx_int_t
ngx_dynamic_upstream_op_update(typename TypeSelect<S>::peers_type *primary,
    ngx_dynamic_upstream_op_t *op, ngx_log_t *log);


template <class S> static ngx_int_t
ngx_dynamic_upstream_op_hash(typename TypeSelect<S>::peers_type *primary,
    ngx_dynamic_upstream_op_t *op);


template <class T> T*
ngx_shm_calloc(ngx_slab_pool_t *shpool, size_t size = 0)
{
    return (T*) ngx_slab_calloc(shpool, size == 0 ? sizeof(T) : size);
}


#define CALL(fun, peers, ...)                                          \
   (op->op_param & NGX_DYNAMIC_UPSTEAM_OP_PARAM_STREAM                 \
        ? fun<ngx_stream_upstream_srv_conf_t>(                         \
            (ngx_stream_upstream_rr_peers_t *) (peers), __VA_ARGS__)   \
        : fun<ngx_http_upstream_srv_conf_t>(                           \
            (ngx_http_upstream_rr_peers_t *) (peers), __VA_ARGS__))


ngx_int_t
ngx_dynamic_upstream_op_impl(ngx_log_t *log, ngx_dynamic_upstream_op_t *op,
    ngx_slab_pool_t *shpool, ngx_pool_t *temp_pool, void *peers)
{
    ngx_int_t rc = NGX_OK;

    op->status = NGX_HTTP_OK;
    op->err = "unexpected";

    switch (op->op) {

        case NGX_DYNAMIC_UPSTEAM_OP_ADD:
            rc = CALL(ngx_dynamic_upstream_op_add, peers, op, shpool,
                      temp_pool, log);
            break;

        case NGX_DYNAMIC_UPSTEAM_OP_REMOVE:
            rc = CALL(ngx_dynamic_upstream_op_del, peers, op, shpool,
                      temp_pool, log);
            break;

        case NGX_DYNAMIC_UPSTEAM_OP_SYNC:
            rc = CALL(ngx_dynamic_upstream_op_sync, peers, op, shpool,
                      temp_pool, log);
            break;

        case NGX_DYNAMIC_UPSTEAM_OP_PARAM:
            rc = CALL(ngx_dynamic_upstream_op_update, peers, op, log);
            break;

        case NGX_DYNAMIC_UPSTEAM_OP_HASH:
            rc = CALL(ngx_dynamic_upstream_op_hash, peers, op);
            break;

        case NGX_DYNAMIC_UPSTEAM_OP_LIST:
        default:
            rc = NGX_OK;
            break;

    }

    return rc;
}


static const ngx_str_t
noaddr = ngx_string("0.0.0.0:1");


ngx_int_t
is_reserved_addr(ngx_str_t *addr)
{
    return addr->len >= noaddr.len
           && ngx_memcmp(addr->data, noaddr.data, noaddr.len - 2) == 0;
}

static ngx_int_t
ngx_dynamic_upstream_parse_url(ngx_url_t *u, ngx_pool_t *pool,
    ngx_dynamic_upstream_op_t *op)
{
    ngx_memzero(u, sizeof(ngx_url_t));

    u->url = op->server;
    u->default_port = 80;
    u->no_resolve = op->op_param & NGX_DYNAMIC_UPSTEAM_OP_PARAM_RESOLVE_SYNC
        ? 0 : 1;

    if (ngx_parse_url(pool, u) != NGX_OK) {

        op->status = NGX_HTTP_INTERNAL_SERVER_ERROR;
        if (u->err)
            op->err = u->err;

        return NGX_ERROR;
    }

    if (u->naddrs == 0) {

        if (u->no_resolve) {

            u->url = noaddr;

            if (ngx_parse_url(pool, u) != NGX_OK) {

                op->status = NGX_HTTP_INTERNAL_SERVER_ERROR;
                if (u->err)
                    op->err = u->err;

                return NGX_ERROR;
            }

            u->url = op->server;
            return NGX_AGAIN;

        } else {

            op->status = NGX_HTTP_INTERNAL_SERVER_ERROR;
            op->err = "failed to resolve";

            return NGX_ERROR;
        }
    }

    return NGX_OK;
}


template <class PeerT> static ngx_flag_t
equals(PeerT *peer, ngx_str_t server, ngx_str_t name)
{
    if (server.data && name.data)
        return str_eq(server, peer->server) && str_eq(name, peer->name);

    assert(server.data != NULL);

    return str_eq(server, peer->server) || str_eq(server, peer->name);
}


static ngx_str_t
ngx_str_shm(ngx_slab_pool_t *shpool, ngx_str_t *s)
{
    ngx_str_t  sh = ngx_null_string;
    sh.data = ngx_shm_calloc<u_char>(shpool, s->len);
    if (sh.data != NULL) {
        ngx_memcpy(sh.data, s->data, s->len);
        sh.len = s->len;
    }
    return sh;
}


template <class S> struct SearchResult {
    typename TypeSelect<S>::peers_type  *peers;
    typename TypeSelect<S>::peer_type   *peer;
    typename TypeSelect<S>::peer_type   *prev;
};


template <class S> static SearchResult<S>
search_peer(typename TypeSelect<S>::peers_type *primary,
    ngx_str_t server, ngx_str_t name, ngx_flag_t exact = 0)
{
    SearchResult<S>  rv;
    ngx_uint_t       j;
    ngx_flag_t       is_reserved = is_reserved_addr(&name);

    for (rv.peers = primary, j = 0, rv.prev = NULL;
         rv.peers != NULL && j < 2;
         rv.peers = rv.peers->next, rv.prev = NULL, j++) {

        for (rv.peer = rv.peers->peer;
             rv.peer != NULL;
             rv.prev = rv.peer, rv.peer = rv.peer->next) {

            if (equals<typename TypeSelect<S>::peer_type>(rv.peer, server, name)
                || (!exact && is_reserved && str_eq(rv.peer->server, server)))
                return rv;
        }
    }

    return rv;
}


template <class S> void
update_tries(typename TypeSelect<S>::peers_type *peers)
{
    ngx_uint_t                           t;
    typename TypeSelect<S>::peer_type   *peer;

    t = 0;

    for (peer = peers->peer;
         peer != NULL;
         peer = peer->next)
    {
        if (!peer->down)
            t++;
    }

    peers->tries = t;
}

template <class S> static ngx_int_t
ngx_dynamic_upstream_op_add_peer(ngx_log_t *log,
    ngx_dynamic_upstream_op_t *op, ngx_slab_pool_t *shpool,
    typename TypeSelect<S>::peers_type *primary, ngx_url_t *u, int i)
{
    typename TypeSelect<S>::peers_type  *peers, *backup;
    typename TypeSelect<S>::peer_type   *npeer;
    SearchResult<S>                      found;

    peers = primary;
    backup = primary->next;

    if (u->addrs[i].name.data[0] == '[' &&
        !(op->op_param & NGX_DYNAMIC_UPSTEAM_OP_PARAM_IPV6)) {

        op->status = NGX_HTTP_NOT_MODIFIED;
        return NGX_OK;
    }

    op->status = NGX_HTTP_OK;

    found = search_peer<S>(primary, op->server, u->addrs[i].name);
    if (found.peer == NULL) {
        if (op->backup)
            goto backup;
        goto add;
    }

    if ((op->backup && found.peers == primary)
        || (!op->backup && found.peers == backup)) {
        op->status = NGX_HTTP_PRECONDITION_FAILED;
        op->err = "can't change server type (primary<->backup)";
        return NGX_ERROR;
    }

    op->status = NGX_HTTP_NOT_MODIFIED;
    op->err = "exists";

    return NGX_OK;

backup:

    if (backup == NULL) {
        backup = ngx_shm_calloc<typename TypeSelect<S>::peers_type>(shpool);
        if (backup == NULL) {
            op->status = NGX_HTTP_INTERNAL_SERVER_ERROR;
            op->err = "no shared memory";
            return NGX_ERROR;
        }
        backup->shpool = primary->shpool;
        backup->name = primary->name;
    }

    peers = backup;

add:

    npeer = ngx_shm_calloc<typename TypeSelect<S>::peer_type>(shpool);
    if (npeer == NULL)
        goto fail;

    npeer->server = ngx_str_shm(shpool, &u->url);
    npeer->name = ngx_str_shm(shpool, &u->addrs[i].name);
    npeer->sockaddr = ngx_shm_calloc<sockaddr>(shpool, u->addrs[i].socklen);

    if (npeer->server.data == NULL
        || npeer->name.data == NULL
        || npeer->sockaddr == NULL)
        goto fail;

    npeer->socklen = u->addrs[i].socklen;
    ngx_memcpy(npeer->sockaddr, u->addrs[i].sockaddr, npeer->socklen);

    if (op->op_param & NGX_DYNAMIC_UPSTEAM_OP_PARAM_WEIGHT) {
        npeer->weight = op->weight;
        npeer->effective_weight = op->weight;
    } else {
        npeer->weight = 1;
        npeer->effective_weight = 1;
    }

    if (op->op_param & NGX_DYNAMIC_UPSTEAM_OP_PARAM_MAX_FAILS)
        npeer->max_fails = op->max_fails;
    else
        npeer->max_fails = primary->peer->max_fails;

    if (op->op_param & NGX_DYNAMIC_UPSTEAM_OP_PARAM_FAIL_TIMEOUT)
        npeer->fail_timeout = op->fail_timeout;
    else
        npeer->fail_timeout = primary->peer->fail_timeout;

    if (op->op_param & NGX_DYNAMIC_UPSTEAM_OP_PARAM_MAX_CONNS)
        npeer->max_conns = op->max_conns;
    else
        npeer->max_conns = primary->peer->max_conns;

    if (op->op_param & NGX_DYNAMIC_UPSTEAM_OP_PARAM_DOWN)
        npeer->down = op->down;

    npeer->next = peers->peer;
    peers->peer = npeer;

    peers->total_weight += npeer->weight;
    peers->single = (peers->number == 0);
    peers->number++;
    peers->weighted = (peers->total_weight != peers->number);

    update_tries<S>(peers);

    primary->next = backup;

    if (!is_reserved_addr(&u->addrs[i].name)) {
        ngx_log_error(NGX_LOG_NOTICE, log, 0, "%V: added server %V peer %V",
                      &op->upstream, &u->url, &u->addrs[i].name);
    } else {
        ngx_log_error(NGX_LOG_NOTICE, log, 0,
                      "%V: added server %V peer -.-.-.-",
                      &op->upstream, &u->url);
    }

    return NGX_OK;

fail:

    if (npeer != NULL) {

        if (npeer->server.data != NULL)
            ngx_slab_free(shpool, npeer->server.data);

        if (npeer->name.data != NULL)
            ngx_slab_free(shpool, npeer->name.data);

        if (npeer->sockaddr != NULL)
            ngx_slab_free(shpool, npeer->sockaddr);

        ngx_slab_free(shpool, npeer);
    }

    if (backup != NULL && primary->next == NULL)
        ngx_slab_free(shpool, backup);

    op->status = NGX_HTTP_INTERNAL_SERVER_ERROR;
    op->err = "no shared memory";

    return NGX_ERROR;
}


template <class S> static ngx_int_t
ngx_dynamic_upstream_op_add_impl(ngx_log_t *log,
    ngx_dynamic_upstream_op_t *op, ngx_slab_pool_t *shpool,
    ngx_pool_t *temp_pool, typename TypeSelect<S>::peers_type *primary,
    ngx_url_t *u)
{
    unsigned                   j;
    unsigned                   count = 0;
    ngx_flag_t                 empty;
    ngx_dynamic_upstream_op_t  del_op;

    ngx_upstream_peers_wlock<typename TypeSelect<S>::peers_type>
        lock(primary, op->no_lock);

    empty = primary->single && is_reserved_addr(&primary->peer->server);

    for (j = 0; j < u->naddrs; j++) {

        if (ngx_dynamic_upstream_op_add_peer<S>(log, op, shpool, primary, u, j)
                == NGX_ERROR) {
            return NGX_ERROR;
        }

        if (op->status == NGX_HTTP_OK)
            count++;
    }

    if (empty && !primary->single) {

        ngx_memzero(&del_op, sizeof(ngx_dynamic_upstream_op_t));

        del_op.no_lock = 1;
        del_op.op = NGX_DYNAMIC_UPSTEAM_OP_REMOVE;
        del_op.upstream = op->upstream;
        del_op.server = noaddr;
        del_op.name = noaddr;

        ngx_dynamic_upstream_op_del<S>(primary, &del_op, shpool,
            temp_pool, log);
    }

    op->status = count != 0 ? NGX_HTTP_OK : NGX_HTTP_NOT_MODIFIED;

    return NGX_OK;
}


template <class S> static ngx_int_t
ngx_dynamic_upstream_op_add(typename TypeSelect<S>::peers_type *primary,
    ngx_dynamic_upstream_op_t *op, ngx_slab_pool_t *shpool,
    ngx_pool_t *temp_pool, ngx_log_t *log)
{
    ngx_url_t  u;
    ngx_int_t  rc;

    if ((rc = ngx_dynamic_upstream_parse_url(&u, temp_pool, op)) == NGX_ERROR)
        return NGX_ERROR;

    if (rc == NGX_AGAIN) {

        if (op->op_param & NGX_DYNAMIC_UPSTEAM_OP_PARAM_RESOLVE) {

            op->down = 1;
            op->op_param |= NGX_DYNAMIC_UPSTEAM_OP_PARAM_DOWN;

        } else {

            op->status = NGX_HTTP_BAD_REQUEST;
            op->err = "domain names are supported only for upstreams "
                      "with 'dns_update' directive";

            return NGX_ERROR;
        }
    }

    if (ngx_dynamic_upstream_op_add_impl<S>(log, op, shpool, temp_pool,
                                            primary, &u) == NGX_ERROR)
        return NGX_ERROR;

    if (op->status == NGX_HTTP_NOT_MODIFIED)
        return NGX_OK;

    if (rc == NGX_AGAIN) {

        op->status = NGX_HTTP_PROCESSING;
        op->err = "DNS resolving in progress";
    }

    return rc;
}


struct ngx_server_s {
    ngx_str_t name;
    ngx_int_t backup;
    ngx_int_t weight;
    ngx_int_t max_fails;

#if defined(nginx_version) && (nginx_version >= 1011005)
    ngx_int_t max_conns;
#endif

    ngx_int_t fail_timeout;
    ngx_url_t u;
};
typedef struct ngx_server_s ngx_server_t;


static ngx_uint_t
ngx_dynamic_upstream_op_server_exist(ngx_array_t *servers,
    ngx_str_t name)
{
    ngx_server_t  *server = (ngx_server_t *) servers->elts;
    unsigned       j;

    for (j = 0; j < servers->nelts; j++)
        if (str_eq(server[j].name, name))
            return 1;

    return 0;
}


template <class S> ngx_uint_t
ngx_dynamic_upstream_op_peer_exist(ngx_array_t *servers,
    typename TypeSelect<S>::peer_type *peer)
{
    ngx_server_t  *server = (ngx_server_t *) servers->elts;
    unsigned       i, j;

    for (j = 0; j < servers->nelts; j++) {

        if (!str_eq(server[j].name, peer->server))
            continue;

        if (server[j].u.naddrs == 0)
            return 1;

        for (i = 0; i < server[j].u.naddrs; i++) {

            if (str_eq(server[j].u.addrs[i].name, peer->name))
                return 1;
        }
    }

    return 0;
}


template <class S> static ngx_int_t
ngx_dynamic_upstream_op_servers(typename TypeSelect<S>::peers_type *primary,
    ngx_array_t *servers, ngx_pool_t *pool, ngx_uint_t *hash)
{
    typename TypeSelect<S>::peers_type  *peers;
    typename TypeSelect<S>::peer_type   *peer;

    ngx_server_t  *server;
    ngx_uint_t     j = 0;

    *hash = 0;

    ngx_upstream_peers_rlock<typename TypeSelect<S>::peers_type> lock(primary);

    for (peers = primary;
         peers != NULL && j < 2;
         peers = peers->next, j++) {

        for (peer = peers->peer;
             peer != NULL;
             peer = peer->next) {

            if (!ngx_dynamic_upstream_op_server_exist(servers, peer->server)) {

                server = (ngx_server_t *) ngx_array_push(servers);
                if (server == NULL)
                    return NGX_ERROR;

                server->name.data = (u_char *) ngx_pcalloc(pool,
                    peer->server.len + 1);
                if (server->name.data == NULL)
                    return NGX_ERROR;

                ngx_memcpy(server->name.data, peer->server.data,
                           peer->server.len);
                server->name.len     = peer->server.len;
                server->backup       = j == 1;
                server->weight       = peer->weight;
                server->max_fails    = peer->max_fails;
                server->fail_timeout = peer->fail_timeout;
#if defined(nginx_version) && (nginx_version >= 1011005)
                server->max_conns    = peer->max_conns;
#endif
            }

            *hash += ngx_crc32_short(peer->server.data, peer->server.len) << j;
        }
    }

    return NGX_OK;
}


template <class S> static ngx_int_t
ngx_dynamic_upstream_op_check_hash(typename TypeSelect<S>::peers_type *primary,
    ngx_uint_t *hash)
{
    typename TypeSelect<S>::peers_type  *peers;
    typename TypeSelect<S>::peer_type   *peer;

    ngx_uint_t  j;
    ngx_uint_t  old_hash = *hash;

    *hash = 0;

    for (peers = primary, j = 0;
         peers != NULL && j < 2;
         peers = peers->next, j++)

        for (peer = peers->peer;
             peer != NULL;
             peer = peer->next)

            *hash += ngx_crc32_short(peer->server.data, peer->server.len) << j;

    return *hash == old_hash ? NGX_OK : NGX_DECLINED;
}


template <class S> static ngx_int_t
ngx_dynamic_upstream_op_hash(typename TypeSelect<S>::peers_type *primary,
    ngx_dynamic_upstream_op_t *op)
{
    ngx_upstream_peers_rlock<typename TypeSelect<S>::peers_type> lock(primary);
    return ngx_dynamic_upstream_op_check_hash<S>(primary,
        &op->hash);
}


template <class S> static ngx_int_t
ngx_dynamic_upstream_op_sync(typename TypeSelect<S>::peers_type *primary,
    ngx_dynamic_upstream_op_t *op, ngx_slab_pool_t *shpool,
    ngx_pool_t *temp_pool, ngx_log_t *log)
{
    typename TypeSelect<S>::peers_type  *peers;
    typename TypeSelect<S>::peer_type   *peer;

    unsigned       i, j;
    ngx_server_t  *server;
    unsigned       count = 0;
    ngx_array_t   *servers;
    ngx_uint_t     hash = op->hash;

    if (ngx_dynamic_upstream_op_hash<S>(primary, op) == NGX_OK) {

        op->status = NGX_HTTP_NOT_MODIFIED;

        return NGX_OK;
    }

    op->hash = hash;

    servers = ngx_array_create(temp_pool, 100, sizeof(ngx_server_t));
    if (servers == NULL) {

        op->status = NGX_HTTP_INTERNAL_SERVER_ERROR;
        op->err = "no memory";

        return NGX_ERROR;
    }

again:

    if (ngx_dynamic_upstream_op_servers<S>(primary,
                                           servers,
                                           temp_pool,
                                           &hash)
            == NGX_ERROR)
    {
        op->status = NGX_HTTP_INTERNAL_SERVER_ERROR;
        op->err = "no memory";
        return NGX_ERROR;
    }

    server = (ngx_server_t *) servers->elts;

    for (j = 0; j < servers->nelts; j++) {

        op->server = server[j].name;

        if (ngx_dynamic_upstream_parse_url(&server[j].u, temp_pool,
                                           op) != NGX_OK) {

            ngx_log_error(NGX_LOG_WARN, log, 0, "%V: server %V: %s",
                          &op->upstream, &op->server, op->err);

            op->status = NGX_HTTP_OK;
            op->err = NULL;
        }
    }

    ngx_upstream_peers_wlock<typename TypeSelect<S>::peers_type> lock(primary);

    if (ngx_dynamic_upstream_op_check_hash<S>(primary, &hash) == NGX_DECLINED) {

        servers->nelts = 0;
        goto again;
    }

    op->no_lock = 1;

    op->op_param |= NGX_DYNAMIC_UPSTEAM_OP_PARAM_WEIGHT;
    op->op_param |= NGX_DYNAMIC_UPSTEAM_OP_PARAM_MAX_FAILS;
    op->op_param |= NGX_DYNAMIC_UPSTEAM_OP_PARAM_FAIL_TIMEOUT;
#if defined(nginx_version) && (nginx_version >= 1011005)
    op->op_param |= NGX_DYNAMIC_UPSTEAM_OP_PARAM_MAX_CONNS;
#endif

    for (j = 0; j < servers->nelts; j++) {

        op->server       = server[j].name;
        op->weight       = server[j].weight;
        op->backup       = server[j].backup;
        op->max_fails    = server[j].max_fails;
#if defined(nginx_version) && (nginx_version >= 1011005)
        op->max_conns    = server[j].max_conns;
#endif
        op->fail_timeout = server[j].fail_timeout;

        for (i = 0; i < server[j].u.naddrs; i++) {

            if (str_eq(op->server, server[j].u.addrs[i].name))
                break;

            if (ngx_dynamic_upstream_op_add_peer<S>
                    (log, op, shpool, primary, &server[j].u, i) == NGX_ERROR)
                return NGX_ERROR;

            if (op->status == NGX_HTTP_OK)
                count++;
        }
    }

    for (peers = primary, j = 0;
         peers != NULL && j < 2;
         peers = peers->next, j++) {

        for (peer = peers->peer;
             peer != NULL;
             peer = peer->next) {

            if ((peer->name.data[0] == '['
                 && !(op->op_param & NGX_DYNAMIC_UPSTEAM_OP_PARAM_IPV6))
                || !ngx_dynamic_upstream_op_peer_exist<S>(servers, peer)) {

                op->server = peer->server;
                op->name = peer->name;

                if (ngx_dynamic_upstream_op_del<S>(primary, op, shpool,
                                                   temp_pool, log) == NGX_ERROR)
                    return NGX_ERROR;

                count++;
            }
        }
    }

    op->hash = hash;
    op->status = count != 0 ? NGX_HTTP_OK : NGX_HTTP_NOT_MODIFIED;

    return NGX_OK;
}


typedef ngx_int_t (*cleanup_t) (ngx_slab_pool_t *shpool, void *peer);


typedef struct {
    ngx_slab_pool_t  *shpool;
    void             *peer;
    cleanup_t         free;
} ngx_dynamic_cleanup_t;


static void
gc_collect(ngx_event_t *ev);


static ngx_array_t *gc = NULL;


static ngx_array_t *
ngx_dynamic_gc_init()
{
    ngx_pool_t  *pool = ngx_create_pool(1024, ngx_cycle->log);
    if (pool == NULL)
        return NULL;

    gc = ngx_array_create(pool, 100, sizeof(ngx_dynamic_cleanup_t));
    if (gc == NULL)
        return NULL;

    static ngx_connection_t  c;
    static ngx_event_t       ev;

    c.fd = -1;
    ev.handler = gc_collect;
    ev.data = &c;
    ev.log = ngx_cycle->log;

    ngx_add_timer(&ev, 1000);

    return gc;
}


static void
ngx_dynamic_add_to_gc(ngx_slab_pool_t *shpool, void *peer, cleanup_t cb)
{
    ngx_dynamic_cleanup_t  *p;

    if (gc == NULL && ngx_dynamic_gc_init() == NULL)
        return;

    p = (ngx_dynamic_cleanup_t *) ngx_array_push(gc);
    if (p != NULL) {
        p->shpool = shpool;
        p->peer = peer;
        p->free = cb;
    }
}


static void
gc_collect(ngx_event_t *ev)
{
    ngx_dynamic_cleanup_t  *elts = (ngx_dynamic_cleanup_t *) gc->elts;
    ngx_uint_t              i, j = 0;

    if (gc->nelts == 0)
        goto settimer;

    for (i = 0; i < gc->nelts; i++)
        if (elts[i].free(elts[i].shpool, elts[i].peer) == NGX_AGAIN)
            elts[j++] = elts[i];

    gc->nelts = j;

settimer:

    if (ngx_exiting || ngx_terminate || ngx_quit)
        return;

    ngx_add_timer(ev, 1000);
}


template <class PeerT> struct GC
{
    static ngx_int_t collect(ngx_slab_pool_t *shpool, void *p)
    {
        PeerT *peer = static_cast<PeerT *>(p);

        ngx_upstream_peer_rlock<PeerT> lock(peer);

        if (peer->conns == 0) {
            ngx_slab_free(shpool, peer->server.data);
            ngx_slab_free(shpool, peer->name.data);
            ngx_slab_free(shpool, peer->sockaddr);

            lock.release();

            ngx_slab_free(shpool, peer);

            return NGX_OK;
        }

        return NGX_AGAIN;
    }
};


template <class PeerT> static void
ngx_dynamic_upstream_op_free_peer(ngx_slab_pool_t *shpool, PeerT *peer)
{
    if (GC<PeerT>::collect(shpool, peer) == NGX_AGAIN)
        ngx_dynamic_add_to_gc(shpool, peer, &GC<PeerT>::collect);
}


template <class S> static ngx_int_t
ngx_dynamic_upstream_op_del(typename TypeSelect<S>::peers_type *primary,
    ngx_dynamic_upstream_op_t *op, ngx_slab_pool_t *shpool,
    ngx_pool_t *temp_pool, ngx_log_t *log)
{
    SearchResult<S>            found;
    ngx_dynamic_upstream_op_t  add_op;

    op->status = NGX_HTTP_NOT_MODIFIED;

    ngx_upstream_peers_wlock<typename TypeSelect<S>::peers_type> lock(primary,
        op->no_lock);

again:

    found = search_peer<S>(primary, op->server, op->name, 1);
    if (found.peer == NULL)
        return NGX_OK;

    if (!found.peers->single || found.peers == primary->next)
        /* not single or backup */
        goto check;

    if (equals<typename TypeSelect<S>::peer_type>(found.peer, noaddr, noaddr))
        return NGX_OK;

    ngx_memzero(&add_op, sizeof(ngx_dynamic_upstream_op_t));

    add_op.no_lock = 1;
    add_op.op = NGX_DYNAMIC_UPSTEAM_OP_ADD;
    add_op.upstream = op->upstream;
    add_op.server = noaddr;
    add_op.name = noaddr;
    add_op.op_param |= NGX_DYNAMIC_UPSTEAM_OP_PARAM_DOWN;
    add_op.down = 1;

    if (ngx_dynamic_upstream_op_add<S>(primary, &add_op,
            shpool, temp_pool, log) != NGX_OK) {
        op->err = add_op.err;
        op->status = add_op.status;
        return NGX_ERROR;
    }

    goto again;

check:

    if (found.prev == NULL) {
        /* found head */
        found.peers->peer = found.peer->next;
        goto del;
    }

    if (found.peer->next == NULL) {
        /* found tail */
        found.prev->next = NULL;
        goto del;
    }

    /* found inside */
    found.prev->next = found.peer->next;

 del:

    found.peers->number--;
    found.peers->total_weight -= found.peer->weight;
    found.peers->single = found.peers->number == 1;
    found.peers->weighted = found.peers->total_weight != found.peers->number;

    update_tries<S>(found.peers);

    if (found.peers->number == 0) {
        assert(found.peers == primary->next);
        ngx_slab_free(shpool, primary->next);
        primary->next = NULL;
    }

    if (!is_reserved_addr(&found.peer->name))
        ngx_log_error(NGX_LOG_NOTICE, log, 0, "%V: removed server %V peer %V",
                      &op->upstream, &found.peer->server, &found.peer->name);

    ngx_dynamic_upstream_op_free_peer<typename TypeSelect<S>::peer_type>(shpool,
        found.peer);

    op->status = NGX_HTTP_OK;

    goto again;
}


template <class S> static void
ngx_dynamic_upstream_op_update_peer(typename TypeSelect<S>::peers_type *peers,
    typename TypeSelect<S>::peer_type *peer,
    ngx_dynamic_upstream_op_t *op, ngx_log_t *log)
{
    ngx_upstream_peer_wlock<typename TypeSelect<S>::peer_type> lock(peer);

    if (op->op_param & NGX_DYNAMIC_UPSTEAM_OP_PARAM_WEIGHT) {

        peers->total_weight -= peer->weight;
        peers->total_weight += op->weight;
        peers->weighted = peers->total_weight != peers->number;
        peer->weight = op->weight;
        peer->current_weight = op->weight;
        peer->effective_weight = op->weight;

    }

    if (op->op_param & NGX_DYNAMIC_UPSTEAM_OP_PARAM_MAX_FAILS)
        peer->max_fails = op->max_fails;

#if defined(nginx_version) && (nginx_version >= 1011005)
    if (op->op_param & NGX_DYNAMIC_UPSTEAM_OP_PARAM_MAX_CONNS)
        peer->max_conns = op->max_conns;
#endif

    if (op->op_param & NGX_DYNAMIC_UPSTEAM_OP_PARAM_FAIL_TIMEOUT)
        peer->fail_timeout = op->fail_timeout;

    if (op->op_param & NGX_DYNAMIC_UPSTEAM_OP_PARAM_UP) {

        peer->down = 0;
        peer->checked = ngx_time();
        peer->fails = 0;

        update_tries<S>(peers);

        ngx_log_error(NGX_LOG_NOTICE, log, 0, "%V: up peer %V",
                      &op->upstream, &peer->name);
    }

    if (op->op_param & NGX_DYNAMIC_UPSTEAM_OP_PARAM_DOWN) {

        peer->down = 1;
        peer->checked = ngx_time();
        peer->fails = peer->max_fails;

        update_tries<S>(peers);

        ngx_log_error(NGX_LOG_NOTICE, log, 0, "%V: down peer %V",
                      &op->upstream, &peer->name);
    }
}


template <class S> static ngx_int_t
ngx_dynamic_upstream_op_update(typename TypeSelect<S>::peers_type *primary,
    ngx_dynamic_upstream_op_t *op, ngx_log_t *log)
{
    SearchResult<S>  found;

    ngx_upstream_peers_rlock<typename TypeSelect<S>::peers_type> lock(primary,
        op->no_lock);

    found = search_peer<S>(primary, op->server, op->name, 1);
    if (found.peer == NULL) {
        op->status = NGX_HTTP_BAD_REQUEST;
        op->err = "server or peer is not found";
        return NGX_ERROR;
    }

    ngx_dynamic_upstream_op_update_peer<S>(found.peers, found.peer, op, log);

    return NGX_OK;
}

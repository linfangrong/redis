/*-----------------------------------------------------------------------------
 * Finite sorted set API
 *----------------------------------------------------------------------------*/

#include "redis.h"
#include <math.h>
#include "t_zset.h"

/*-----------------------------------------------------------------------------
 * Common finite sorted set API
 *----------------------------------------------------------------------------*/

unsigned int xsetLength(robj *zobj) {
    int length = -1;
    if (zobj->encoding == REDIS_ENCODING_ZIPLIST) {
        length = zzlLength(((xsetZiplist*)zobj->ptr)->zl);
    } else if (zobj->encoding == REDIS_ENCODING_SKIPLIST) {
        length = ((xset*)zobj->ptr)->zset->zsl->length;
    } else {
        redisPanic("Unknown finite sorted set encoding");
    }
    return length;
}

void xsetConvert(robj *zobj, int encoding) {
    xset *xs;
    xsetZiplist *xsz;
    zset *zs;
    zskiplistNode *node, *next;
    robj *ele;
    double score;

    if (zobj->encoding == encoding) return;
    if (zobj->encoding == REDIS_ENCODING_ZIPLIST) {
        xsz = zobj->ptr;
        unsigned char *zl = xsz->zl;
        unsigned char *eptr, *sptr;
        unsigned char *vstr;
        unsigned int vlen;
        long long vlong;

        if (encoding != REDIS_ENCODING_SKIPLIST)
            redisPanic("Unknown target encoding");

        zs = zmalloc(sizeof(*zs));
        zs->dict = dictCreate(&zsetDictType,NULL);
        zs->zsl = zslCreate();

        eptr = ziplistIndex(zl,0);
        redisAssertWithInfo(NULL,zobj,eptr != NULL);
        sptr = ziplistNext(zl,eptr);
        redisAssertWithInfo(NULL,zobj,sptr != NULL);

        while (eptr != NULL) {
            score = zzlGetScore(sptr);
            redisAssertWithInfo(NULL,zobj,ziplistGet(eptr,&vstr,&vlen,&vlong));
            if (vstr == NULL)
                ele = createStringObjectFromLongLong(vlong);
            else
                ele = createStringObject((char*)vstr,vlen);

            /* Has incremented refcount since it was just created. */
            node = zslInsert(zs->zsl,score,ele);
            redisAssertWithInfo(NULL,zobj,dictAdd(zs->dict,ele,&node->score) == DICT_OK);
            incrRefCount(ele); /* Added to dictionary. */
            zzlNext(zl,&eptr,&sptr);
        }

        xs = zmalloc(sizeof(*xs)); 
        xs->finity = xsz->finity;
        xs->pruning = xsz->pruning;
        xs->zset = zs;
        zfree(zobj->ptr);
        zobj->ptr = xs;
        zobj->encoding = REDIS_ENCODING_SKIPLIST;
    } else if (zobj->encoding == REDIS_ENCODING_SKIPLIST) {
        unsigned char *zl = ziplistNew();

        if (encoding != REDIS_ENCODING_ZIPLIST)
            redisPanic("Unknown target encoding");

        /* Approach similar to zslFree(), since we want to free the skiplist at
         * the same time as creating the ziplist. */
        xs = zobj->ptr;
        zs = xs->zset;
        dictRelease(zs->dict);
        node = zs->zsl->header->level[0].forward;
        zfree(zs->zsl->header);
        zfree(zs->zsl);

        while (node) {
            ele = getDecodedObject(node->obj);
            zl = zzlInsertAt(zl,NULL,ele,node->score);
            decrRefCount(ele);

            next = node->level[0].forward;
            zslFreeNode(node);
            node = next;
        }

        xsz = zmalloc(sizeof(*xsz));
        xsz->finity = xs->finity;
        xsz->pruning = xs->pruning;
        xsz->zl = zl;
        zfree(zobj->ptr);
        zobj->ptr = xsz;
        zobj->encoding = REDIS_ENCODING_ZIPLIST;
    } else {
        redisPanic("Unknown finite sorted set encoding");
    }
}

/*-----------------------------------------------------------------------------
 * Finite sorted set commands
 *----------------------------------------------------------------------------*/

size_t xsetGetFinite(robj *zobj) {
    size_t finity;
    if (zobj->encoding == REDIS_ENCODING_ZIPLIST) {
        finity = ((xsetZiplist*)zobj->ptr)->finity;
    } else if (zobj->encoding == REDIS_ENCODING_SKIPLIST) {
        finity = ((xset*)zobj->ptr)->finity;
    } else {
        redisPanic("Unknown finite sorted set encoding");
    }
    return finity;
}

int xsetGetPruning(robj *zobj) {
    int pruning;
    if (zobj->encoding == REDIS_ENCODING_ZIPLIST) {
        pruning = ((xsetZiplist*)zobj->ptr)->pruning;
    } else if (zobj->encoding == REDIS_ENCODING_SKIPLIST) {
        pruning = ((xset*)zobj->ptr)->pruning;
    } else {
        redisPanic("Unknown finite sorted set encoding");
    }
    return pruning;
}

#define XADD_NONE 0
#define XADD_INCR (1<<0)
#define XADD_NX (1<<1)
#define XADD_XX (1<<2)
#define XADD_CH (1<<3)
#define XADD_MODIFY_FINITY (1<<4)
#define XADD_MODIFY_PRUNING (1<<5)
#define XADD_REPLY_ELEMENTS (1<<6)
int xsetSetOptions(redisClient *c, robj *zobj, int flags, size_t finity, int pruning) {
    if (zobj->encoding == REDIS_ENCODING_ZIPLIST) {
        xsetZiplist *xsz = zobj->ptr;
        if (flags & XADD_MODIFY_FINITY) xsz->finity = finity;
        else finity = xsz->finity;
        if (flags & XADD_MODIFY_PRUNING) xsz->pruning = pruning;
        else pruning = xsz->pruning;
    } else if (zobj->encoding == REDIS_ENCODING_SKIPLIST) {
        xset *xs = zobj->ptr;
        if (flags & XADD_MODIFY_FINITY) xs->finity = finity;
        else finity = xs->finity;
        if (flags & XADD_MODIFY_PRUNING) xs->pruning = pruning;
        else pruning = xs->pruning;
    } else {
        redisPanic("Unknown finite sorted set encoding");
    }

    unsigned int length = xsetLength(zobj);
    unsigned long deleted;
    long start, end;

    if (length <= finity) {
        if (flags & XADD_REPLY_ELEMENTS) addReply(c, shared.emptymultibulk);
        return flags & XADD_REPLY_ELEMENTS;
    }
    if (pruning) {
        start = 1 + finity;
        end = length;
    } else {
        start = 1;
        end = length - finity;
    }
    if (flags & XADD_REPLY_ELEMENTS) {
        int rangelen = length-finity;
        addReplyMultiBulkLen(c, 2*rangelen);
        if (zobj->encoding == REDIS_ENCODING_ZIPLIST) {
            xsetZiplist *xsz = zobj->ptr;
            unsigned char *zl = xsz->zl;
            unsigned char *eptr, *sptr;
            unsigned char *vstr;
            unsigned int vlen;
            long long vlong;

            eptr = ziplistIndex(zl, 2*(start-1));
            redisAssertWithInfo(c, zobj, eptr != NULL);
            sptr = ziplistNext(zl, eptr);
            while (rangelen--) {
                redisAssertWithInfo(c, zobj, eptr != NULL && sptr != NULL);
                redisAssertWithInfo(c, zobj, ziplistGet(eptr, &vstr, &vlen, &vlong));
                if (vstr == NULL) addReplyBulkLongLong(c, vlong);
                else addReplyBulkCBuffer(c, vstr, vlen);
                addReplyDouble(c, zzlGetScore(sptr));
                zzlNext(zl, &eptr, &sptr);
            }
        } else if (zobj->encoding == REDIS_ENCODING_SKIPLIST) {
            xset *xs = zobj->ptr;
            zset *zs = xs->zset;
            zskiplist *zsl = zs->zsl;
            zskiplistNode *ln;
            robj * ele;

            ln = zslGetElementByRank(zsl, start);
            while (rangelen--) {
                redisAssertWithInfo(c, zobj, ln != NULL);
                ele = ln->obj;
                addReplyBulk(c, ele);
                addReplyDouble(c, ln->score);
                ln = ln->level[0].forward;
            }
        } else {
            redisPanic("Unknown finite sorted set encoding");
        }
    }
    if (zobj->encoding == REDIS_ENCODING_ZIPLIST) {
        xsetZiplist *xsz = zobj->ptr;
        xsz->zl = zzlDeleteRangeByRank(xsz->zl, start, end, &deleted);
    } else if (zobj->encoding == REDIS_ENCODING_SKIPLIST) {
        xset *xs = zobj->ptr;
        zset *zs = xs->zset;
        deleted = zslDeleteRangeByRank(zs->zsl, start, end, zs->dict);
        if (htNeedsResize(zs->dict)) dictResize(zs->dict);
    } else {
        redisPanic("Unknown finite sorted set encoding");
    }
    server.dirty += deleted;
    return flags & XADD_REPLY_ELEMENTS;
}

void xaddGenericCommand(redisClient *c, int flags) {
    static char *nanerr = "resulting score is not a number (NaN)";
    robj *key = c->argv[1];
    robj *ele;
    robj *zobj;
    robj *curobj;
    double score = 0, *scores = NULL, curscore = 0.0;
    int j, elements;
    int added = 0, updated = 0, protected = 0;
    int optionidx = 2;
    long finity = server.xset_finity;
    int pruning = server.xset_pruning;
    
    while (optionidx < c->argc) {
        if (!strcasecmp(c->argv[optionidx]->ptr, "nx")) {
            flags |= XADD_NX;
            optionidx++;
        } else if (!strcasecmp(c->argv[optionidx]->ptr, "xx")) {
            flags |= XADD_XX;
            optionidx++;
        } else if (!strcasecmp(c->argv[optionidx]->ptr, "ch")) {
            flags |= XADD_CH;
            optionidx++;
        } else if (!strcasecmp(c->argv[optionidx]->ptr, "incr")) {
            flags |= XADD_INCR;
            optionidx++;
        } else if (!strcasecmp(c->argv[optionidx]->ptr, "finity")) {
            if (c->argc <= optionidx+1) { addReply(c, shared.syntaxerr); return; }
            if (getLongFromObjectOrReply(c, c->argv[optionidx+1], &finity, "finity is not a number (NaN)") != REDIS_OK) return;
            if (finity <= 0) { addReplyError(c, "Invalid finity"); return; }
            flags |= XADD_MODIFY_FINITY;
            optionidx += 2;
        } else if (!strcasecmp(c->argv[optionidx]->ptr, "pruning")) {
            if (c->argc <= optionidx+1) { addReply(c, shared.syntaxerr); return; }
            if (!strcasecmp(c->argv[optionidx+1]->ptr, "minscore")) {
                pruning = 0;
            } else if (!strcasecmp(c->argv[optionidx+1]->ptr, "maxscore")) {
                pruning = 1;
            } else { addReply(c, shared.syntaxerr); return; }
            flags |= XADD_MODIFY_PRUNING;
            optionidx += 2;
        } else if (!strcasecmp(c->argv[optionidx]->ptr, "elements")) {
            flags |= XADD_REPLY_ELEMENTS;
            optionidx++;
        } else break;
    }

    /* Turn options into simple to check vars. */
    int incr = (flags & XADD_INCR) != 0;
    int nx = (flags & XADD_NX) != 0;
    int xx = (flags & XADD_XX) != 0;
    int ch = (flags & XADD_CH) != 0;

    /* After the options, we expect to have an even number of args, since
     * we expect any number of score-element pairs. */
    elements = c->argc - optionidx;
    if (elements <= 0 || elements % 2) {
        addReply(c,shared.syntaxerr);
        return;
    }
    elements /= 2;

    /* Check for incompatible options. */
    if (nx && xx) {
        addReplyError(c,
            "XX and NX options at the same time are not compatible");
        return;
    }

    if (incr && elements > 1) {
        addReplyError(c,
            "INCR option supports a single increment-element pair");
        return;
    }

    /* Start parsing all the scores, we need to emit any syntax error
     * before executing additions to the finite sorted set, as the command should
     * either execute fully or nothing at all. */
    scores = zmalloc(sizeof(double)*elements);
    for (j = 0; j < elements; j++) {
        if (getDoubleFromObjectOrReply(c,c->argv[optionidx+j*2],&scores[j],NULL)
            != REDIS_OK) goto cleanup;
    }

    /* Lookup the key and create the finite sorted set if does not exist. */
    zobj = lookupKeyWrite(c->db,key);
    if (zobj == NULL) {
        if (xx) goto reply_to_client; /* No key + XX option: nothing to do. */
        if (server.xset_max_ziplist_entries == 0 ||
            server.xset_max_ziplist_value < sdslen(c->argv[optionidx+1]->ptr))
        {
            zobj = createXsetObject(finity, pruning);
        } else {
            zobj = createXsetZiplistObject(finity, pruning);
        }
        dbAdd(c->db,key,zobj);
    } else {
        if (zobj->type != REDIS_XSET) {
            addReply(c,shared.wrongtypeerr);
            goto cleanup;
        }
    }

    for (j = 0; j < elements; j++) {
        score = scores[j];

        if (zobj->encoding == REDIS_ENCODING_ZIPLIST) {
            xsetZiplist *xsz = zobj->ptr;
            unsigned char *eptr;

            /* Prefer non-encoded element when dealing with ziplists. */
            ele = c->argv[optionidx+1+j*2];
            if ((eptr = zzlFind(xsz->zl,ele,&curscore)) != NULL) {
                if (nx) continue;
                if (incr) {
                    score += curscore;
                    if (isnan(score)) {
                        addReplyError(c,nanerr);
                        goto cleanup;
                    }
                }

                /* Remove and re-insert when score changed. */
                if (score != curscore) {
                    xsz->zl = zzlDelete(xsz->zl,eptr);
                    xsz->zl = zzlInsert(xsz->zl,ele,score);
                    server.dirty++;
                    updated++;
                }
                protected++;
            } else if (!xx) {
                /* Optimize: check if the element is too large or the list
                 * becomes too long *before* executing zzlInsert. */
                xsz->zl = zzlInsert(xsz->zl,ele,score);
                if (zzlLength(xsz->zl) > server.xset_max_ziplist_entries)
                    xsetConvert(zobj,REDIS_ENCODING_SKIPLIST);
                if (sdslen(ele->ptr) > server.xset_max_ziplist_value)
                    xsetConvert(zobj,REDIS_ENCODING_SKIPLIST);
                server.dirty++;
                added++;
                protected++;
            }
        } else if (zobj->encoding == REDIS_ENCODING_SKIPLIST) {
            zset *zs = ((xset*)zobj->ptr)->zset;
            zskiplistNode *znode;
            dictEntry *de;

            ele = c->argv[optionidx+1+j*2] = tryObjectEncoding(c->argv[optionidx+1+j*2]);
            de = dictFind(zs->dict,ele);
            if (de != NULL) {
                if (nx) continue;
                curobj = dictGetKey(de);
                curscore = *(double*)dictGetVal(de);

                if (incr) {
                    score += curscore;
                    if (isnan(score)) {
                        addReplyError(c,nanerr);
                        /* Don't need to check if the finite sorted set is empty
                         * because we know it has at least one element. */
                        goto cleanup;
                    }
                }

                /* Remove and re-insert when score changed. We can safely
                 * delete the key object from the skiplist, since the
                 * dictionary still has a reference to it. */
                if (score != curscore) {
                    redisAssertWithInfo(c,curobj,zslDelete(zs->zsl,curscore,curobj));
                    znode = zslInsert(zs->zsl,score,curobj);
                    incrRefCount(curobj); /* Re-inserted in skiplist. */
                    dictGetVal(de) = &znode->score; /* Update score ptr. */
                    server.dirty++;
                    updated++;
                }
                protected++;
            } else if (!xx) {
                znode = zslInsert(zs->zsl,score,ele);
                incrRefCount(ele); /* Inserted in skiplist. */
                redisAssertWithInfo(c,NULL,dictAdd(zs->dict,ele,&znode->score) == DICT_OK);
                incrRefCount(ele); /* Added to dictionary. */
                server.dirty++;
                added++;
                protected++;
            }
        } else {
            redisPanic("Unknown finite sorted set encoding");
        }
    }

reply_to_client:
    if (xsetSetOptions(c, zobj, flags, finity, pruning) == 0) {
        if (incr) { /* XINCRBY or INCR option. */
            if (protected)
                addReplyDouble(c, score);
            else
                addReply(c, shared.nullbulk);
        } else { /* XADD */
            addReplyLongLong(c, ch ? added+updated : added);
        }
    }

cleanup:
    zfree(scores);
    if (added || updated) {
        signalModifiedKey(c->db,key);
        notifyKeyspaceEvent(REDIS_NOTIFY_XSET,
            incr ? "xincr" : "xadd", key, c->db->id);
    }
}

void xaddCommand(redisClient *c) {
    xaddGenericCommand(c, XADD_NONE);
}

void xincrbyCommand(redisClient *c) {
    xaddGenericCommand(c, XADD_INCR);
}

void xremCommand(redisClient *c) {
    robj *key = c->argv[1];
    robj *zobj;
    int deleted = 0, keyremoved = 0, j;

    if ((zobj = lookupKeyWriteOrReply(c, key, shared.czero)) == NULL ||
        checkType(c, zobj, REDIS_XSET)) return;

    if (zobj->encoding == REDIS_ENCODING_ZIPLIST) {
        xsetZiplist *xsz = zobj->ptr;
        unsigned char *eptr;

        for (j = 2; j < c->argc; j++) {
            if ((eptr = zzlFind(xsz->zl, c->argv[j], NULL)) != NULL) {
                deleted++;
                xsz->zl = zzlDelete(xsz->zl, eptr);
                if (zzlLength(xsz->zl) == 0) {
                    dbDelete(c->db, key);
                    keyremoved = 1;
                    break;
                }
            }
        }
    } else if (zobj->encoding == REDIS_ENCODING_SKIPLIST) {
        zset *zs = ((xset*)zobj->ptr)->zset;
        dictEntry *de;
        double score;

        for (j = 2; j < c->argc; j++) {
            de = dictFind(zs->dict,c->argv[j]);
            if (de != NULL) {
                deleted++;

                /* Delete from the skiplist */
                score = *(double*)dictGetVal(de);
                redisAssertWithInfo(c,c->argv[j],zslDelete(zs->zsl,score,c->argv[j]));

                /* Delete from the hash table */
                dictDelete(zs->dict,c->argv[j]);
                if (htNeedsResize(zs->dict)) dictResize(zs->dict);
                if (dictSize(zs->dict) == 0) {
                    dbDelete(c->db,key);
                    keyremoved = 1;
                    break;
                }
            }
        }
    } else {
        redisPanic("Unknown finite sorted set encoding");
    }

    if (deleted) {
        notifyKeyspaceEvent(REDIS_NOTIFY_XSET,"xrem",key,c->db->id);
        if (keyremoved)
            notifyKeyspaceEvent(REDIS_NOTIFY_GENERIC,"del",key,c->db->id);
        signalModifiedKey(c->db,key);
        server.dirty += deleted;
    }
    addReplyLongLong(c,deleted);
}

void xrangeGenericCommand(redisClient *c, int reverse) {
    robj *key = c->argv[1];
    robj *zobj;
    int withscores = 0;
    long start;
    long end;
    int llen;
    int rangelen;

    if ((getLongFromObjectOrReply(c, c->argv[2], &start, NULL) != REDIS_OK) ||
        (getLongFromObjectOrReply(c, c->argv[3], &end, NULL) != REDIS_OK)) return;

    if (c->argc == 5 && !strcasecmp(c->argv[4]->ptr,"withscores")) {
        withscores = 1;
    } else if (c->argc >= 5) {
        addReply(c,shared.syntaxerr);
        return;
    }

    if ((zobj = lookupKeyReadOrReply(c,key,shared.emptymultibulk)) == NULL
         || checkType(c,zobj,REDIS_XSET)) return;

    /* Sanitize indexes. */
    llen = xsetLength(zobj);
    if (start < 0) start = llen+start;
    if (end < 0) end = llen+end;
    if (start < 0) start = 0;

    /* Invariant: start >= 0, so this test will be true when end < 0.
     * The range is empty when start > end or start >= length. */
    if (start > end || start >= llen) {
        addReply(c,shared.emptymultibulk);
        return;
    }
    if (end >= llen) end = llen-1;
    rangelen = (end-start)+1;

    /* Return the result in form of a multi-bulk reply */
    addReplyMultiBulkLen(c, withscores ? (rangelen*2) : rangelen);

    if (zobj->encoding == REDIS_ENCODING_ZIPLIST) {
        unsigned char *zl = ((xsetZiplist*)zobj->ptr)->zl;
        unsigned char *eptr, *sptr;
        unsigned char *vstr;
        unsigned int vlen;
        long long vlong;

        if (reverse)
            eptr = ziplistIndex(zl,-2-(2*start));
        else
            eptr = ziplistIndex(zl,2*start);

        redisAssertWithInfo(c,zobj,eptr != NULL);
        sptr = ziplistNext(zl,eptr);

        while (rangelen--) {
            redisAssertWithInfo(c,zobj,eptr != NULL && sptr != NULL);
            redisAssertWithInfo(c,zobj,ziplistGet(eptr,&vstr,&vlen,&vlong));
            if (vstr == NULL)
                addReplyBulkLongLong(c,vlong);
            else
                addReplyBulkCBuffer(c,vstr,vlen);

            if (withscores)
                addReplyDouble(c,zzlGetScore(sptr));

            if (reverse)
                zzlPrev(zl,&eptr,&sptr);
            else
                zzlNext(zl,&eptr,&sptr);
        }

    } else if (zobj->encoding == REDIS_ENCODING_SKIPLIST) {
        zset *zs = ((xset*)zobj->ptr)->zset;
        zskiplist *zsl = zs->zsl;
        zskiplistNode *ln;
        robj *ele;

        /* Check if starting point is trivial, before doing log(N) lookup. */
        if (reverse) {
            ln = zsl->tail;
            if (start > 0)
                ln = zslGetElementByRank(zsl,llen-start);
        } else {
            ln = zsl->header->level[0].forward;
            if (start > 0)
                ln = zslGetElementByRank(zsl,start+1);
        }

        while(rangelen--) {
            redisAssertWithInfo(c,zobj,ln != NULL);
            ele = ln->obj;
            addReplyBulk(c,ele);
            if (withscores)
                addReplyDouble(c,ln->score);
            ln = reverse ? ln->backward : ln->level[0].forward;
        }
    } else {
        redisPanic("Unknown finite sorted set encoding");
    }
}

void xrangeCommand(redisClient *c) {
    xrangeGenericCommand(c, 0);
}

void xrevrangeCommand(redisClient *c) {
    xrangeGenericCommand(c, 1);
}

void xcardCommand(redisClient *c) {
    robj *key = c->argv[1];
    robj *zobj;

    if ((zobj = lookupKeyReadOrReply(c, key, shared.czero)) == NULL ||
        checkType(c, zobj, REDIS_XSET)) return;

    addReplyLongLong(c, xsetLength(zobj));
}

void xscoreCommand(redisClient *c) {
    robj *key = c->argv[1];
    robj *zobj;
    double score;

    if ((zobj = lookupKeyReadOrReply(c, key, shared.nullbulk)) == NULL ||
        checkType(c, zobj, REDIS_XSET)) return;

    if (zobj->encoding == REDIS_ENCODING_ZIPLIST) {
        xsetZiplist *xsz = zobj->ptr;
        if (zzlFind(xsz->zl, c->argv[2], &score) != NULL)
            addReplyDouble(c, score);
        else
            addReply(c, shared.nullbulk);
    } else if (zobj->encoding == REDIS_ENCODING_SKIPLIST) {
        xset *xs = zobj->ptr;
        zset *zs = xs->zset;
        dictEntry *de;

        c->argv[2] = tryObjectEncoding(c->argv[2]);
        de = dictFind(zs->dict, c->argv[2]);
        if (de != NULL) {
            score = *(double*)dictGetVal(de);
            addReplyDouble(c, score);
        } else {
            addReply(c, shared.nullbulk);
        }
    } else {
        redisPanic("Unknown finite sorted set encoding");
    }
}

void xsetoptionsCommand(redisClient *c) {
    robj *key = c->argv[1];
    robj *zobj;

    if ((zobj = lookupKeyWriteOrReply(c, key, shared.czero)) == NULL ||
        checkType(c, zobj, REDIS_XSET)) return;

    int optionidx = 2;
    int flags = 0;
    long finity = 0;
    int pruning = 0;
    while (optionidx < c->argc) {
        if (!strcasecmp(c->argv[optionidx]->ptr, "finity")) {
            if (c->argc <= optionidx+1) { addReply(c, shared.syntaxerr); return; }
            if (getLongFromObjectOrReply(c, c->argv[optionidx+1], &finity, "finity is not a number (NaN)") != REDIS_OK) return;
            if (finity <= 0) { addReplyError(c, "Invalid finity"); return; }
            flags |= XADD_MODIFY_FINITY;
            optionidx += 2;
        } else if (!strcasecmp(c->argv[optionidx]->ptr, "pruning")) {
            if (c->argc <= optionidx+1) { addReply(c, shared.syntaxerr); return; }
            if (!strcasecmp(c->argv[optionidx+1]->ptr, "minscore")) {
                pruning = 0;
            } else if (!strcasecmp(c->argv[optionidx+1]->ptr, "maxscore")) {
                pruning = 1;
            } else { addReply(c, shared.syntaxerr); return; }
            flags |= XADD_MODIFY_PRUNING;
            optionidx += 2;
        } else if (!strcasecmp(c->argv[optionidx]->ptr, "elements")) {
            flags |= XADD_REPLY_ELEMENTS;
            optionidx++;
        } else { addReply(c, shared.syntaxerr); return; }
    }

    if (xsetSetOptions(c, zobj, flags, finity, pruning) == 0)
        addReply(c, shared.cone);
}

void xgetfinityCommand(redisClient *c) {
    robj *key = c->argv[1];
    robj *zobj;

    if ((zobj = lookupKeyReadOrReply(c, key, shared.czero)) == NULL ||
        checkType(c, zobj, REDIS_XSET)) return;

    addReplyLongLong(c, xsetGetFinite(zobj));
}

void xgetpruningCommand(redisClient *c) {
    robj *key = c->argv[1];
    robj *zobj;

    if ((zobj = lookupKeyReadOrReply(c, key, shared.nullbulk)) == NULL ||
        checkType(c, zobj, REDIS_XSET)) return;

    if (xsetGetPruning(zobj)) {
        addReplyBulkCString(c,"maxscore");
    } else {
        addReplyBulkCString(c,"minscore");
    }
}

/* Implements XREMRANGEBYRANK, XREMRANGEBYSCORE, XREMRANGEBYLEX commands. */
#define XRANGE_RANK 0
#define XRANGE_SCORE 1
#define XRANGE_LEX 2
void xremrangeGenericCommand(redisClient *c, int rangetype) {
    robj *key = c->argv[1];
    robj *zobj;
    int keyremoved = 0;
    unsigned long deleted;
    zrangespec range;
    zlexrangespec lexrange;
    long start, end, llen;
    
    /* Step 1: Parse the range. */
    if (rangetype == XRANGE_RANK) {
        if ((getLongFromObjectOrReply(c,c->argv[2],&start,NULL) != REDIS_OK) ||
            (getLongFromObjectOrReply(c,c->argv[3],&end,NULL) != REDIS_OK))
            return;
    } else if (rangetype == XRANGE_SCORE) {
        if (zslParseRange(c->argv[2],c->argv[3],&range) != REDIS_OK) {
            addReplyError(c,"min or max is not a float");
            return;
        }
    } else if (rangetype == XRANGE_LEX) {
        if (zslParseLexRange(c->argv[2],c->argv[3],&lexrange) != REDIS_OK) {
            addReplyError(c,"min or max not valid string range item");
            return;
        }
    }

    /* Step 2: Lookup & range sanity checks if needed. */
    if ((zobj = lookupKeyWriteOrReply(c,key,shared.czero)) == NULL ||
        checkType(c,zobj,REDIS_XSET)) goto cleanup;

    if (rangetype == XRANGE_RANK) {
        /* Sanitize indexes. */
        llen = xsetLength(zobj);
        if (start < 0) start = llen+start;
        if (end < 0) end = llen+end;
        if (start < 0) start = 0;

        /* Invariant: start >= 0, so this test will be true when end < 0.
         * The range is empty when start > end or start >= length. */
        if (start > end || start >= llen) {
            addReply(c,shared.czero);
            goto cleanup;
        }
        if (end >= llen) end = llen-1;
    }

    /* Step 3: Perform the range deletion operation. */
    if (zobj->encoding == REDIS_ENCODING_ZIPLIST) {
        xsetZiplist *xsz = zobj->ptr;
        switch(rangetype) {
        case XRANGE_RANK:
            xsz->zl = zzlDeleteRangeByRank(xsz->zl,start+1,end+1,&deleted);
            break;
        case XRANGE_SCORE:
            xsz->zl = zzlDeleteRangeByScore(xsz->zl,&range,&deleted);
            break;
        case XRANGE_LEX:
            xsz->zl = zzlDeleteRangeByLex(xsz->zl,&lexrange,&deleted);
            break;
        }
        if (zzlLength(xsz->zl) == 0) {
            dbDelete(c->db,key);
            keyremoved = 1;
        }
    } else if (zobj->encoding == REDIS_ENCODING_SKIPLIST) {
        xset *xs = zobj->ptr;
        zset *zs = xs->zset;
        switch(rangetype) {
        case XRANGE_RANK:
            deleted = zslDeleteRangeByRank(zs->zsl,start+1,end+1,zs->dict);
            break;
        case XRANGE_SCORE:
            deleted = zslDeleteRangeByScore(zs->zsl,&range,zs->dict);
            break;
        case XRANGE_LEX:
            deleted = zslDeleteRangeByLex(zs->zsl,&lexrange,zs->dict);
            break;
        }
        if (htNeedsResize(zs->dict)) dictResize(zs->dict);
        if (dictSize(zs->dict) == 0) {
            dbDelete(c->db,key);
            keyremoved = 1;
        }
    } else {
        redisPanic("Unknown finite sorted set encoding");
    }

    /* Step 4: Notifications and reply. */
    if (deleted) {
        char *event[3] = {"xremrangebyrank","xremrangebyscore","xremrangebylex"};
        signalModifiedKey(c->db,key);
        notifyKeyspaceEvent(REDIS_NOTIFY_XSET,event[rangetype],key,c->db->id);
        if (keyremoved)
            notifyKeyspaceEvent(REDIS_NOTIFY_GENERIC,"del",key,c->db->id);
    }
    server.dirty += deleted;
    addReplyLongLong(c,deleted);

cleanup:
    if (rangetype == XRANGE_LEX) zslFreeLexRange(&lexrange);
}

void xremrangebyrankCommand(redisClient *c) {
    xremrangeGenericCommand(c, XRANGE_RANK);
}

void xremrangebyscoreCommand(redisClient *c) {
    xremrangeGenericCommand(c, XRANGE_SCORE);
}

void xremrangebylexCommand(redisClient *c) {
    xremrangeGenericCommand(c, XRANGE_LEX);
}

/* This command implements XRANGEBYSCORE, XREVRANGEBYSCORE. */
void genericXrangebyscoreCommand(redisClient *c, int reverse) {
    zrangespec range;
    robj *key = c->argv[1];
    robj *zobj;
    long offset = 0, limit = -1;
    int withscores = 0;
    unsigned long rangelen = 0;
    void *replylen = NULL;
    int minidx, maxidx;

    /* Parse the range arguments. */
    if (reverse) {
        /* Range is given as [max,min] */
        maxidx = 2; minidx = 3;
    } else {
        /* Range is given as [min,max] */
        minidx = 2; maxidx = 3;
    }

    if (zslParseRange(c->argv[minidx],c->argv[maxidx],&range) != REDIS_OK) {
        addReplyError(c,"min or max is not a float");
        return;
    }

    /* Parse optional extra arguments. Note that XCOUNT will exactly have
     * 4 arguments, so we'll never enter the following code path. */
    if (c->argc > 4) {
        int remaining = c->argc - 4;
        int pos = 4;

        while (remaining) {
            if (remaining >= 1 && !strcasecmp(c->argv[pos]->ptr,"withscores")) {
                pos++; remaining--;
                withscores = 1;
            } else if (remaining >= 3 && !strcasecmp(c->argv[pos]->ptr,"limit")) {
                if ((getLongFromObjectOrReply(c, c->argv[pos+1], &offset, NULL) != REDIS_OK) ||
                    (getLongFromObjectOrReply(c, c->argv[pos+2], &limit, NULL) != REDIS_OK)) return;
                pos += 3; remaining -= 3;
            } else {
                addReply(c,shared.syntaxerr);
                return;
            }
        }
    }

    /* Ok, lookup the key and get the range */
    if ((zobj = lookupKeyReadOrReply(c,key,shared.emptymultibulk)) == NULL ||
        checkType(c,zobj,REDIS_XSET)) return;

    if (zobj->encoding == REDIS_ENCODING_ZIPLIST) {
        unsigned char *zl = ((xsetZiplist*)zobj->ptr)->zl;
        unsigned char *eptr, *sptr;
        unsigned char *vstr;
        unsigned int vlen;
        long long vlong;
        double score;

        /* If reversed, get the last node in range as starting point. */
        if (reverse) {
            eptr = zzlLastInRange(zl,&range);
        } else {
            eptr = zzlFirstInRange(zl,&range);
        }

        /* No "first" element in the specified interval. */
        if (eptr == NULL) {
            addReply(c, shared.emptymultibulk);
            return;
        }

        /* Get score pointer for the first element. */
        redisAssertWithInfo(c,zobj,eptr != NULL);
        sptr = ziplistNext(zl,eptr);

        /* We don't know in advance how many matching elements there are in the
         * list, so we push this object that will represent the multi-bulk
         * length in the output buffer, and will "fix" it later */
        replylen = addDeferredMultiBulkLength(c);

        /* If there is an offset, just traverse the number of elements without
         * checking the score because that is done in the next loop. */
        while (eptr && offset--) {
            if (reverse) {
                zzlPrev(zl,&eptr,&sptr);
            } else {
                zzlNext(zl,&eptr,&sptr);
            }
        }

        while (eptr && limit--) {
            score = zzlGetScore(sptr);

            /* Abort when the node is no longer in range. */
            if (reverse) {
                if (!zslValueGteMin(score,&range)) break;
            } else {
                if (!zslValueLteMax(score,&range)) break;
            }

            /* We know the element exists, so ziplistGet should always succeed */
            redisAssertWithInfo(c,zobj,ziplistGet(eptr,&vstr,&vlen,&vlong));

            rangelen++;
            if (vstr == NULL) {
                addReplyBulkLongLong(c,vlong);
            } else {
                addReplyBulkCBuffer(c,vstr,vlen);
            }

            if (withscores) {
                addReplyDouble(c,score);
            }

            /* Move to next node */
            if (reverse) {
                zzlPrev(zl,&eptr,&sptr);
            } else {
                zzlNext(zl,&eptr,&sptr);
            }
        }
    } else if (zobj->encoding == REDIS_ENCODING_SKIPLIST) {
        xset *xs = zobj->ptr;
        zset *zs = xs->zset;
        zskiplist *zsl = zs->zsl;
        zskiplistNode *ln;

        /* If reversed, get the last node in range as starting point. */
        if (reverse) {
            ln = zslLastInRange(zsl,&range);
        } else {
            ln = zslFirstInRange(zsl,&range);
        }

        /* No "first" element in the specified interval. */
        if (ln == NULL) {
            addReply(c, shared.emptymultibulk);
            return;
        }

        /* We don't know in advance how many matching elements there are in the
         * list, so we push this object that will represent the multi-bulk
         * length in the output buffer, and will "fix" it later */
        replylen = addDeferredMultiBulkLength(c);

        /* If there is an offset, just traverse the number of elements without
         * checking the score because that is done in the next loop. */
        while (ln && offset--) {
            if (reverse) {
                ln = ln->backward;
            } else {
                ln = ln->level[0].forward;
            }
        }

        while (ln && limit--) {
            /* Abort when the node is no longer in range. */
            if (reverse) {
                if (!zslValueGteMin(ln->score,&range)) break;
            } else {
                if (!zslValueLteMax(ln->score,&range)) break;
            }

            rangelen++;
            addReplyBulk(c,ln->obj);

            if (withscores) {
                addReplyDouble(c,ln->score);
            }

            /* Move to next node */
            if (reverse) {
                ln = ln->backward;
            } else {
                ln = ln->level[0].forward;
            }
        }
    } else {
        redisPanic("Unknown finite sorted set encoding");
    }

    if (withscores) {
        rangelen *= 2;
    }

    setDeferredMultiBulkLength(c, replylen, rangelen);
}

void xrangebyscoreCommand(redisClient *c) {
    genericXrangebyscoreCommand(c, 0);
}

void xrevrangebyscoreCommand(redisClient *c) {
    genericXrangebyscoreCommand(c, 1);
}

void xcountCommand(redisClient *c) {
    robj *key = c->argv[1];
    robj *zobj;
    zrangespec range;
    int count = 0;

    /* Parse the range arguments */
    if (zslParseRange(c->argv[2],c->argv[3],&range) != REDIS_OK) {
        addReplyError(c,"min or max is not a float");
        return;
    }

    /* Lookup the finite sorted set */
    if ((zobj = lookupKeyReadOrReply(c, key, shared.czero)) == NULL ||
        checkType(c, zobj, REDIS_XSET)) return;

    if (zobj->encoding == REDIS_ENCODING_ZIPLIST) {
        unsigned char *zl = ((xsetZiplist*)zobj->ptr)->zl;
        unsigned char *eptr, *sptr;
        double score;

        /* Use the first element in range as the starting point */
        eptr = zzlFirstInRange(zl,&range);

        /* No "first" element */
        if (eptr == NULL) {
            addReply(c, shared.czero);
            return;
        }

        /* First element is in range */
        sptr = ziplistNext(zl,eptr);
        score = zzlGetScore(sptr);
        redisAssertWithInfo(c,zobj,zslValueLteMax(score,&range));

        /* Iterate over elements in range */
        while (eptr) {
            score = zzlGetScore(sptr);

            /* Abort when the node is no longer in range. */
            if (!zslValueLteMax(score,&range)) {
                break;
            } else {
                count++;
                zzlNext(zl,&eptr,&sptr);
            }
        }
    } else if (zobj->encoding == REDIS_ENCODING_SKIPLIST) {
        xset *xs = zobj->ptr;
        zset *zs = xs->zset;
        zskiplist *zsl = zs->zsl;
        zskiplistNode *zn;
        unsigned long rank;

        /* Find first element in range */
        zn = zslFirstInRange(zsl, &range);

        /* Use rank of first element, if any, to determine preliminary count */
        if (zn != NULL) {
            rank = zslGetRank(zsl, zn->score, zn->obj);
            count = (zsl->length - (rank - 1));

            /* Find last element in range */
            zn = zslLastInRange(zsl, &range);

            /* Use rank of last element, if any, to determine the actual count */
            if (zn != NULL) {
                rank = zslGetRank(zsl, zn->score, zn->obj);
                count -= (zsl->length - rank);
            }
        }
    } else {
        redisPanic("Unknown finite sorted set encoding");
    }

    addReplyLongLong(c, count);
}

void xlexcountCommand(redisClient *c) {
    robj *key = c->argv[1];
    robj *zobj;
    zlexrangespec range;
    int count = 0;

    /* Parse the range arguments */
    if (zslParseLexRange(c->argv[2],c->argv[3],&range) != REDIS_OK) {
        addReplyError(c,"min or max not valid string range item");
        return;
    }

    /* Lookup the finite sorted set */
    if ((zobj = lookupKeyReadOrReply(c, key, shared.czero)) == NULL ||
        checkType(c, zobj, REDIS_XSET))
    {
        zslFreeLexRange(&range);
        return;
    }

    if (zobj->encoding == REDIS_ENCODING_ZIPLIST) {
        unsigned char *zl = ((xsetZiplist*)zobj->ptr)->zl;
        unsigned char *eptr, *sptr;

        /* Use the first element in range as the starting point */
        eptr = zzlFirstInLexRange(zl,&range);

        /* No "first" element */
        if (eptr == NULL) {
            zslFreeLexRange(&range);
            addReply(c, shared.czero);
            return;
        }

        /* First element is in range */
        sptr = ziplistNext(zl,eptr);
        redisAssertWithInfo(c,zobj,zzlLexValueLteMax(eptr,&range));

        /* Iterate over elements in range */
        while (eptr) {
            /* Abort when the node is no longer in range. */
            if (!zzlLexValueLteMax(eptr,&range)) {
                break;
            } else {
                count++;
                zzlNext(zl,&eptr,&sptr);
            }
        }
    } else if (zobj->encoding == REDIS_ENCODING_SKIPLIST) {
        xset *xs = zobj->ptr;
        zset *zs = xs->zset;
        zskiplist *zsl = zs->zsl;
        zskiplistNode *zn;
        unsigned long rank;

        /* Find first element in range */
        zn = zslFirstInLexRange(zsl, &range);

        /* Use rank of first element, if any, to determine preliminary count */
        if (zn != NULL) {
            rank = zslGetRank(zsl, zn->score, zn->obj);
            count = (zsl->length - (rank - 1));

            /* Find last element in range */
            zn = zslLastInLexRange(zsl, &range);

            /* Use rank of last element, if any, to determine the actual count */
            if (zn != NULL) {
                rank = zslGetRank(zsl, zn->score, zn->obj);
                count -= (zsl->length - rank);
            }
        }
    } else {
        redisPanic("Unknown finite sorted set encoding");
    }

    zslFreeLexRange(&range);
    addReplyLongLong(c, count);
}

/* This command implements XRANGEBYLEX, XREVRANGEBYLEX. */
void genericXrangebylexCommand(redisClient *c, int reverse) {
    zlexrangespec range;
    robj *key = c->argv[1];
    robj *zobj;
    long offset = 0, limit = -1;
    unsigned long rangelen = 0;
    void *replylen = NULL;
    int minidx, maxidx;

    /* Parse the range arguments. */
    if (reverse) {
        /* Range is given as [max,min] */
        maxidx = 2; minidx = 3;
    } else {
        /* Range is given as [min,max] */
        minidx = 2; maxidx = 3;
    }

    if (zslParseLexRange(c->argv[minidx],c->argv[maxidx],&range) != REDIS_OK) {
        addReplyError(c,"min or max not valid string range item");
        return;
    }

    /* Parse optional extra arguments. Note that XCOUNT will exactly have
     * 4 arguments, so we'll never enter the following code path. */
    if (c->argc > 4) {
        int remaining = c->argc - 4;
        int pos = 4;

        while (remaining) {
            if (remaining >= 3 && !strcasecmp(c->argv[pos]->ptr,"limit")) {
                if ((getLongFromObjectOrReply(c, c->argv[pos+1], &offset, NULL) != REDIS_OK) ||
                    (getLongFromObjectOrReply(c, c->argv[pos+2], &limit, NULL) != REDIS_OK)) return;
                pos += 3; remaining -= 3;
            } else {
                zslFreeLexRange(&range);
                addReply(c,shared.syntaxerr);
                return;
            }
        }
    }

    /* Ok, lookup the key and get the range */
    if ((zobj = lookupKeyReadOrReply(c,key,shared.emptymultibulk)) == NULL ||
        checkType(c,zobj,REDIS_XSET))
    {
        zslFreeLexRange(&range);
        return;
    }

    if (zobj->encoding == REDIS_ENCODING_ZIPLIST) {
        unsigned char *zl = ((xsetZiplist*)zobj->ptr)->zl;
        unsigned char *eptr, *sptr;
        unsigned char *vstr;
        unsigned int vlen;
        long long vlong;

        /* If reversed, get the last node in range as starting point. */
        if (reverse) {
            eptr = zzlLastInLexRange(zl,&range);
        } else {
            eptr = zzlFirstInLexRange(zl,&range);
        }

        /* No "first" element in the specified interval. */
        if (eptr == NULL) {
            addReply(c, shared.emptymultibulk);
            zslFreeLexRange(&range);
            return;
        }

        /* Get score pointer for the first element. */
        redisAssertWithInfo(c,zobj,eptr != NULL);
        sptr = ziplistNext(zl,eptr);

        /* We don't know in advance how many matching elements there are in the
         * list, so we push this object that will represent the multi-bulk
         * length in the output buffer, and will "fix" it later */
        replylen = addDeferredMultiBulkLength(c);

        /* If there is an offset, just traverse the number of elements without
         * checking the score because that is done in the next loop. */
        while (eptr && offset--) {
            if (reverse) {
                zzlPrev(zl,&eptr,&sptr);
            } else {
                zzlNext(zl,&eptr,&sptr);
            }
        }

        while (eptr && limit--) {
            /* Abort when the node is no longer in range. */
            if (reverse) {
                if (!zzlLexValueGteMin(eptr,&range)) break;
            } else {
                if (!zzlLexValueLteMax(eptr,&range)) break;
            }

            /* We know the element exists, so ziplistGet should always
             * succeed. */
            redisAssertWithInfo(c,zobj,ziplistGet(eptr,&vstr,&vlen,&vlong));

            rangelen++;
            if (vstr == NULL) {
                addReplyBulkLongLong(c,vlong);
            } else {
                addReplyBulkCBuffer(c,vstr,vlen);
            }

            /* Move to next node */
            if (reverse) {
                zzlPrev(zl,&eptr,&sptr);
            } else {
                zzlNext(zl,&eptr,&sptr);
            }
        }
    } else if (zobj->encoding == REDIS_ENCODING_SKIPLIST) {
        xset *xs = zobj->ptr;
        zset *zs = xs->zset;
        zskiplist *zsl = zs->zsl;
        zskiplistNode *ln;

        /* If reversed, get the last node in range as starting point. */
        if (reverse) {
            ln = zslLastInLexRange(zsl,&range);
        } else {
            ln = zslFirstInLexRange(zsl,&range);
        }

        /* No "first" element in the specified interval. */
        if (ln == NULL) {
            addReply(c, shared.emptymultibulk);
            zslFreeLexRange(&range);
            return;
        }

        /* We don't know in advance how many matching elements there are in the
         * list, so we push this object that will represent the multi-bulk
         * length in the output buffer, and will "fix" it later */
        replylen = addDeferredMultiBulkLength(c);

        /* If there is an offset, just traverse the number of elements without
         * checking the score because that is done in the next loop. */
        while (ln && offset--) {
            if (reverse) {
                ln = ln->backward;
            } else {
                ln = ln->level[0].forward;
            }
        }

        while (ln && limit--) {
            /* Abort when the node is no longer in range. */
            if (reverse) {
                if (!zslLexValueGteMin(ln->obj,&range)) break;
            } else {
                if (!zslLexValueLteMax(ln->obj,&range)) break;
            }

            rangelen++;
            addReplyBulk(c,ln->obj);

            /* Move to next node */
            if (reverse) {
                ln = ln->backward;
            } else {
                ln = ln->level[0].forward;
            }
        }
    } else {
        redisPanic("Unknown finite sorted set encoding");
    }

    zslFreeLexRange(&range);
    setDeferredMultiBulkLength(c, replylen, rangelen);
}

void xrangebylexCommand(redisClient *c) {
    genericXrangebylexCommand(c, 0);
}

void xrevrangebylexCommand(redisClient *c) {
    genericXrangebylexCommand(c, 1);
}

void xrankGenericCommand(redisClient *c, int reverse) {
    robj *key = c->argv[1];
    robj *ele = c->argv[2];
    robj *zobj;
    unsigned long llen;
    unsigned long rank;

    if ((zobj = lookupKeyReadOrReply(c,key,shared.nullbulk)) == NULL ||
        checkType(c,zobj,REDIS_XSET)) return;
    llen = xsetLength(zobj);

    redisAssertWithInfo(c,ele,sdsEncodedObject(ele));

    if (zobj->encoding == REDIS_ENCODING_ZIPLIST) {
        unsigned char *zl = ((xsetZiplist*)zobj->ptr)->zl;
        unsigned char *eptr, *sptr;

        eptr = ziplistIndex(zl,0);
        redisAssertWithInfo(c,zobj,eptr != NULL);
        sptr = ziplistNext(zl,eptr);
        redisAssertWithInfo(c,zobj,sptr != NULL);

        rank = 1;
        while(eptr != NULL) {
            if (ziplistCompare(eptr,ele->ptr,sdslen(ele->ptr)))
                break;
            rank++;
            zzlNext(zl,&eptr,&sptr);
        }

        if (eptr != NULL) {
            if (reverse)
                addReplyLongLong(c,llen-rank);
            else
                addReplyLongLong(c,rank-1);
        } else {
            addReply(c,shared.nullbulk);
        }
    } else if (zobj->encoding == REDIS_ENCODING_SKIPLIST) {
        xset *xs = zobj->ptr;
        zset *zs = xs->zset;
        zskiplist *zsl = zs->zsl;
        dictEntry *de;
        double score;

        ele = c->argv[2] = tryObjectEncoding(c->argv[2]);
        de = dictFind(zs->dict,ele);
        if (de != NULL) {
            score = *(double*)dictGetVal(de);
            rank = zslGetRank(zsl,score,ele);
            redisAssertWithInfo(c,ele,rank); /* Existing elements always have a rank. */
            if (reverse)
                addReplyLongLong(c,llen-rank);
            else
                addReplyLongLong(c,rank-1);
        } else {
            addReply(c,shared.nullbulk);
        }
    } else {
        redisPanic("Unknown finite sorted set encoding");
    }
}

void xrankCommand(redisClient *c) {
    xrankGenericCommand(c, 0);
}

void xrevrankCommand(redisClient *c) {
    xrankGenericCommand(c, 1);
}

void xscanCommand(redisClient *c) {
    robj *o;
    unsigned long cursor;
    
    if (parseScanCursorOrReply(c,c->argv[2],&cursor) == REDIS_ERR) return;
    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.emptyscan)) == NULL ||
        checkType(c,o,REDIS_XSET)) return;
    scanGenericCommand(c,o,cursor);
}

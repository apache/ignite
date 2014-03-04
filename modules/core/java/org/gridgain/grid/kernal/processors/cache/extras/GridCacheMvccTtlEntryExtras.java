/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.extras;

import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.typedef.internal.*;

/**
 * Extras where MVCC and TTL are set.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridCacheMvccTtlEntryExtras<K> extends GridCacheEntryExtrasAdapter<K> {
    /** MVCC. */
    private GridCacheMvcc<K> mvcc;

    /** TTL. */
    private long ttl;

    /** Expire time. */
    private long expireTime;

    /**
     * Constructor.
     *
     * @param mvcc MVCC.
     * @param ttl TTL.
     * @param expireTime Expire time.
     */
    public GridCacheMvccTtlEntryExtras(GridCacheMvcc<K> mvcc, long ttl, long expireTime) {
        assert mvcc != null;
        assert ttl != 0;

        this.mvcc = mvcc;
        this.ttl = ttl;
        this.expireTime = expireTime;
    }

    /** {@inheritDoc} */
    @Override public GridCacheEntryExtras<K> attributesData(GridLeanMap<String, Object> attrData) {
        return attrData != null ? new GridCacheAttributesMvccTtlEntryExtras<>(attrData, mvcc, ttl, expireTime) : this;
    }

    /** {@inheritDoc} */
    @Override public GridCacheMvcc<K> mvcc() {
        return mvcc;
    }

    /** {@inheritDoc} */
    @Override public GridCacheEntryExtras<K> mvcc(GridCacheMvcc<K> mvcc) {
        if (mvcc != null) {
            this.mvcc = mvcc;

            return this;
        }
        else
            return new GridCacheTtlEntryExtras<>(ttl, expireTime);
    }

    /** {@inheritDoc} */
    @Override public GridCacheEntryExtras<K> obsoleteVersion(GridCacheVersion obsoleteVer) {
        return obsoleteVer != null ? new GridCacheMvccObsoleteTtlEntryExtras<>(mvcc, obsoleteVer, ttl, expireTime) :
            this;
    }

    /** {@inheritDoc} */
    @Override public long ttl() {
        return ttl;
    }

    /** {@inheritDoc} */
    @Override public long expireTime() {
        return expireTime;
    }

    /** {@inheritDoc} */
    @Override public GridCacheEntryExtras<K> ttlAndExpireTime(long ttl, long expireTime) {
        if (ttl != 0) {
            this.ttl = ttl;
            this.expireTime = expireTime;

            return this;
        }
        else
            return new GridCacheMvccEntryExtras<>(mvcc);
    }

    /** {@inheritDoc} */
    @Override public int size() {
        return 24;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheMvccTtlEntryExtras.class, this);
    }
}


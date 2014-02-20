// @java.file.header

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
 * Extras where TTL and expire time are set.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridCacheTtlEntryExtras<K> extends GridCacheEntryExtrasAdapter<K> {
    /** TTL. */
    private long ttl;

    /** Expire time. */
    private long expireTime;

    /**
     * Constructor.
     *
     * @param ttl TTL.
     * @param expireTime Expire time.
     */
    public GridCacheTtlEntryExtras(long ttl, long expireTime) {
        assert ttl != 0;

        this.ttl = ttl;
        this.expireTime = expireTime;
    }

    /** {@inheritDoc} */
    @Override public GridCacheEntryExtras<K> attributesData(GridLeanMap<String, Object> attrData) {
        return attrData != null ? new GridCacheAttributesTtlEntryExtras<K>(attrData, ttl, expireTime) : this;
    }

    /** {@inheritDoc} */
    @Override public GridCacheEntryExtras<K> mvcc(GridCacheMvcc<K> mvcc) {
        return mvcc != null ? new GridCacheMvccTtlEntryExtras<>(mvcc, ttl, expireTime) : this;
    }

    /** {@inheritDoc} */
    @Override public GridCacheEntryExtras<K> obsoleteVersion(GridCacheVersion obsoleteVer) {
        return obsoleteVer != null ? new GridCacheObsoleteTtlEntryExtras<K>(obsoleteVer, ttl, expireTime) : this;
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
            return null;
    }

    /** {@inheritDoc} */
    @Override public int size() {
        return 16;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheTtlEntryExtras.class, this);
    }
}

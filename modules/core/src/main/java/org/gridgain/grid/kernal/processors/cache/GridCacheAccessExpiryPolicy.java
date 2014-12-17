/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache;

import org.gridgain.grid.kernal.processors.cache.distributed.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import javax.cache.expiry.*;

/**
 *
 */
public class GridCacheAccessExpiryPolicy {
    /** */
    private final long ttl;

    /** */
    private GridCacheTtlUpdateRequest req;

    /**
     * @param expiryPlc Expiry policy.
     * @return Access expire policy.
     */
    public static GridCacheAccessExpiryPolicy forPolicy(@Nullable ExpiryPolicy expiryPlc) {
        if (expiryPlc == null)
            return null;

        Duration duration = expiryPlc.getExpiryForAccess();

        if (duration == null)
            return null;

        return new GridCacheAccessExpiryPolicy(GridCacheMapEntry.toTtl(duration));
    }

    /**
     * @param ttl TTL for access.
     */
    public GridCacheAccessExpiryPolicy(long ttl) {
        assert ttl >= 0 : ttl;

        this.ttl = ttl;
    }

    /**
     * @return TTL.
     */
    public long ttl() {
        return ttl;
    }

    /**
     * @param key Entry key.
     * @param keyBytes Entry key bytes.
     * @param ver Entry version.
     */
    @SuppressWarnings("unchecked")
    public void ttlUpdated(Object key, byte[] keyBytes, GridCacheVersion ver) {
        if (req == null)
            req = new GridCacheTtlUpdateRequest(ttl);

        req.addEntry(key, keyBytes, ver);
    }

    /**
     * @return TTL update request.
     */
    @SuppressWarnings("unchecked")
    @Nullable public <K, V> GridCacheTtlUpdateRequest<K, V> request() {
        return (GridCacheTtlUpdateRequest<K, V>)req;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheAccessExpiryPolicy.class, this);
    }
}

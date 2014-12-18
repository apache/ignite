/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache;

import org.apache.ignite.lang.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jdk8.backport.*;
import org.jetbrains.annotations.*;

import javax.cache.expiry.*;
import java.util.*;

/**
 *
 */
public class GridCacheAccessExpiryPolicy {
    /** */
    private final long ttl;

    /** */
    private volatile Map<Object, IgniteBiTuple<byte[], GridCacheVersion>> entries;

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
     *
     */
    public void reset() {
        Map<Object, IgniteBiTuple<byte[], GridCacheVersion>> entries0 = entries;

        if (entries0 != null)
            entries0.clear();
    }

    /**
     * @param key Entry key.
     * @param keyBytes Entry key bytes.
     * @param ver Entry version.
     */
    @SuppressWarnings("unchecked")
    public void ttlUpdated(Object key, byte[] keyBytes, GridCacheVersion ver) {
        Map<Object, IgniteBiTuple<byte[], GridCacheVersion>> entries0 = entries;

        if (entries0 == null) {
            synchronized (this) {
                entries0 = entries;

                if (entries0 == null)
                    entries0 = entries = new ConcurrentHashMap8<>();
            }
        }

        entries0.put(key, new IgniteBiTuple<>(keyBytes, ver));
    }

    /**
     * @return TTL update request.
     */
    @Nullable public Map<Object, IgniteBiTuple<byte[], GridCacheVersion>> entries() {
        return entries;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheAccessExpiryPolicy.class, this);
    }
}

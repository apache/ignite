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
public class GridCacheAccessExpiryPolicy implements GridCacheExpiryPolicy {
    /** */
    private final long accessTtl;

    /** */
    private Map<Object, IgniteBiTuple<byte[], GridCacheVersion>> entries;

    /** */
    private Map<UUID, Collection<IgniteBiTuple<byte[], GridCacheVersion>>> rdrsMap;

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
     * @param accessTtl TTL for access.
     */
    public GridCacheAccessExpiryPolicy(long accessTtl) {
        assert accessTtl >= 0 : accessTtl;

        this.accessTtl = accessTtl;
    }

    /** {@inheritDoc} */
    @Override public long forAccess() {
        return accessTtl;
    }

    /** {@inheritDoc} */
    @Override public long forCreate() {
        return -1L;
    }

    /** {@inheritDoc} */
    @Override public long forUpdate() {
        return -1L;
    }

    /**
     *
     */
    public synchronized void reset() {
        if (entries != null)
            entries.clear();

        if (rdrsMap != null)
            rdrsMap.clear();
    }

    /**
     * @param key Entry key.
     * @param keyBytes Entry key bytes.
     * @param ver Entry version.
     */
    @SuppressWarnings("unchecked")
    @Override public synchronized void onAccessUpdated(Object key,
        byte[] keyBytes,
        GridCacheVersion ver,
        @Nullable Collection<UUID> rdrs) {
        if (entries == null)
            entries = new HashMap<>();

        IgniteBiTuple<byte[], GridCacheVersion> t = new IgniteBiTuple<>(keyBytes, ver);

        entries.put(key, t);

        if (rdrs != null && !rdrs.isEmpty()) {
            if (rdrsMap == null)
                rdrsMap = new HashMap<>();

            for (UUID nodeId : rdrs) {
                Collection<IgniteBiTuple<byte[], GridCacheVersion>> col = rdrsMap.get(nodeId);

                if (col == null)
                    rdrsMap.put(nodeId, col = new ArrayList<>());

                col.add(t);
            }
        }
    }

    /**
     * @return TTL update request.
     */
    @Nullable @Override public synchronized Map<Object, IgniteBiTuple<byte[], GridCacheVersion>> entries() {
        return entries;
    }

    /** {@inheritDoc} */
    @Nullable @Override public synchronized Map<UUID, Collection<IgniteBiTuple<byte[], GridCacheVersion>>> readers() {
        return rdrsMap;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheAccessExpiryPolicy.class, this);
    }
}

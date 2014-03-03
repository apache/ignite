/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache;

import org.jetbrains.annotations.*;

/**
 * Factory for cache entries.
 *
 * @author @java.author
 * @version @java.version
 */
public interface GridCacheMapEntryFactory<K, V> {
    /**
     * @param ctx Cache registry.
     * @param topVer Topology version.
     * @param key Cache key.
     * @param hash Key hash value.
     * @param val Entry value.
     * @param next Next entry in the linked list.
     * @param ttl Time to live.
     * @param hdrId Header id.
     * @return New cache entry.
     */
    public GridCacheMapEntry<K, V> create(GridCacheContext<K, V> ctx, long topVer, K key, int hash, V val,
        @Nullable GridCacheMapEntry<K, V> next, long ttl, int hdrId);
}

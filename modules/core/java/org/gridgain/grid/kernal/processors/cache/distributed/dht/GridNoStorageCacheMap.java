/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed.dht;

import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.lang.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Empty cache map that will never store any entries.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridNoStorageCacheMap<K, V> extends GridCacheConcurrentMap<K, V> {
    /** Empty triple. */
    private final GridTriple<GridCacheMapEntry<K,V>> emptyTriple =
        new GridTriple<>(null, null, null);

    /**
     * @param ctx Cache context.
     */
    public GridNoStorageCacheMap(GridCacheContext<K, V> ctx) {
        super(ctx, 0, 0.75f, 1);
    }

    /** {@inheritDoc} */
    @Override public boolean isEmpty() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public int size() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public int publicSize() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public boolean containsKey(Object key) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public GridCacheMapEntry<K, V> randomEntry() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public GridCacheMapEntry<K, V> getEntry(Object key) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public GridCacheMapEntry<K, V> putEntry(long topVer, K key, @Nullable V val, long ttl) {
        throw new AssertionError();
    }

    /** {@inheritDoc} */
    @Override public GridTriple<GridCacheMapEntry<K, V>> putEntryIfObsoleteOrAbsent(long topVer, K key, @Nullable V val,
        long ttl, boolean create) {
        if (create) {
            GridCacheMapEntry<K, V> entry = new GridDhtCacheEntry<>(ctx, topVer, key, hash(key.hashCode()), val,
                null, 0, 0);

            return new GridTriple<>(entry, null, null);
        }
        else
            return emptyTriple;
    }

    /** {@inheritDoc} */
    @Override public void putAll(Map<? extends K, ? extends V> m, long ttl) {
        throw new AssertionError();
    }

    /** {@inheritDoc} */
    @Override public boolean removeEntry(GridCacheEntryEx<K, V> e) {
        throw new AssertionError();
    }

    /** {@inheritDoc} */
    @Override public GridCacheMapEntry<K, V> removeEntryIfObsolete(K key) {
        throw new AssertionError();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridNoStorageCacheMap.class, this);
    }
}

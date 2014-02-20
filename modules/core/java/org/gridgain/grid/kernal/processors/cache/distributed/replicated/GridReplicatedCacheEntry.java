// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed.replicated;

import org.gridgain.grid.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.kernal.processors.cache.distributed.*;

/**
 * Replicated cache entry.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridReplicatedCacheEntry<K, V> extends GridDistributedCacheEntry<K, V> {
    /** */
    private static final int REPLICATED_SIZE_OVERHEAD = 4;

    /** Partition. */
    private int part;

    /**
     * @param ctx Cache context.
     * @param key Cache key.
     * @param hash Key hash value.
     * @param val Entry value.
     * @param next Next entry in the linked list.
     * @param ttl Time to live.
     * @param hdrId Header id.
     */
    public GridReplicatedCacheEntry(GridCacheContext<K, V> ctx, K key, int hash, V val, GridCacheMapEntry<K, V> next,
        long ttl, int hdrId) {
        super(ctx, key, hash, val, next, ttl, hdrId);

        part = ctx.affinity().partition(key);
    }

    /** {@inheritDoc} */
    @Override public int memorySize() throws GridException {
        return super.memorySize() + REPLICATED_SIZE_OVERHEAD;
    }

    /** {@inheritDoc} */
    @Override public int partition() {
        return part;
    }

    /** {@inheritDoc} */
    @Override public boolean isReplicated() {
        return true;
    }
}

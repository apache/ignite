package org.gridgain.grid.kernal.processors.cache;

import org.gridgain.grid.kernal.processors.cache.distributed.*;

/**
 * Lock and Unlock callbacks.
 */
public interface GridCacheMvccCallback<K, V> {
    /**
     * Called when entry gets a first candidate. This call
     * happens within entry internal synchronization.
     *
     * @param entry Entry.
     */
    public void onLocked(GridDistributedCacheEntry<K, V> entry);

    /**
     * Called when entry lock ownership changes. This call
     * happens outside of synchronization so external callbacks
     * can be made from this call.
     *
     * @param entry Entry.
     * @param prev Previous candidate.
     * @param owner Current owner.
     */
    public void onOwnerChanged(GridCacheEntryEx<K, V> entry, GridCacheMvccCandidate<K> prev,
        GridCacheMvccCandidate<K> owner);

    /**
     * Called when entry has no more candidates. This call happens
     * within entry internal synchronization.
     *
     * @param entry Entry
     */
    public void onFreed(GridDistributedCacheEntry<K, V> entry);
}
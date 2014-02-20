package org.gridgain.grid.kernal.processors.cache.local;

import org.gridgain.grid.kernal.processors.cache.*;

/**
 * @param <K> Key type.
 * @param <V> Value type.
 */
interface GridLocalLockCallback<K, V> {
    /**
     * Called when entry lock ownership changes. This call
     * happens outside of synchronization so external callbacks
     * can be made from this call.
     *
     * @param entry Entry whose owner has changed.
     * @param prev Previous candidate.
     * @param owner Current candidate.
     */
    public void onOwnerChanged(GridLocalCacheEntry<K, V> entry, GridCacheMvccCandidate<K> prev,
        GridCacheMvccCandidate<K> owner);
}
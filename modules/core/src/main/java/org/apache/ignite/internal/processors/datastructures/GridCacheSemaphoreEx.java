package org.apache.ignite.internal.processors.datastructures;

import org.apache.ignite.IgniteSemaphore;

/**
 * Created by vladisav on 20.9.15..
 */
public interface GridCacheSemaphoreEx extends IgniteSemaphore, GridCacheRemovable {
    /**
     * Get current semaphore key.
     *
     * @return Semaphore key.
     */
    public GridCacheInternalKey key();

    /**
     * Callback to notify semaphore on changes.
     *
     * @param val Id of the caller and number of permissions to acquire (or release; can be negative).
     */
    public void onUpdate(GridCacheSemaphoreState val);
}
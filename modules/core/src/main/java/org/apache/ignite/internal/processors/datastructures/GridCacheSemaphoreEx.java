package org.apache.ignite.internal.processors.datastructures;

import org.apache.ignite.IgniteSemaphore;

/**
 * Grid cache semaphore ({@code 'Ex'} stands for external).
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
     * @param val State containing the number of available permissions.
     */
    public void onUpdate(GridCacheSemaphoreState val);
}

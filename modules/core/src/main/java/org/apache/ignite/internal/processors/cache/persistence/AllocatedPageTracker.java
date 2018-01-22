package org.apache.ignite.internal.processors.cache.persistence;

/**
 * Tracks allocated pages.
 */
public interface AllocatedPageTracker {
    /**
     * Increments totalAllocatedPages counter.
     */
    public void updateTotalAllocatedPages(long delta);
}

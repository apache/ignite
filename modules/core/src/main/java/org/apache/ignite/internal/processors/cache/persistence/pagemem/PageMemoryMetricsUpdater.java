package org.apache.ignite.internal.processors.cache.persistence.pagemem;

/**
 *
 */
public interface PageMemoryMetricsUpdater {
    /**
     * Increments totalAllocatedPages counter.
     */
    public void updateTotalAllocatedPages(long delta);
}

package org.apache.ignite.internal.processors.query.calcite.exec.tracker;

/**
 * Memory allocation tracker interface.
 */
public interface MemoryTracker {
    /** */
    public void onMemoryAllocated(long size);

    /** */
    public void onMemoryReleased(long size);

    /** */
    public void reset();

    /** Currently allocated bytes. */
    public long allocated();
}

package org.apache.ignite.internal.processors.query.calcite.exec.tracker;

/**
 * Memory tracker that does nothing.
 */
public class NoOpMemoryTracker implements MemoryTracker {
    /** */
    public static final MemoryTracker INSTANCE = new NoOpMemoryTracker();

    /** */
    private NoOpMemoryTracker() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onMemoryAllocated(long size) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onMemoryReleased(long size) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void reset() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public long allocated() {
        return 0;
    }
}

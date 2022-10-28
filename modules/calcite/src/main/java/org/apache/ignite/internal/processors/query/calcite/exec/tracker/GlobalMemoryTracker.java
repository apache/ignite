package org.apache.ignite.internal.processors.query.calcite.exec.tracker;

import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.util.typedef.internal.A;

/**
 * Memory allocation tracker for all queries.
 */
public class GlobalMemoryTracker implements MemoryTracker {
    /** Global memory quota. */
    private final long quota;

    /** Currently allocated. */
    private final AtomicLong allocated = new AtomicLong();

    /** */
    public GlobalMemoryTracker(long quota) {
        A.ensure(quota > 0, "quota > 0");
        this.quota = quota;
    }

    /** {@inheritDoc} */
    @Override public void onMemoryAllocated(long size) {
        long wasAllocated;

        do {
            wasAllocated = allocated.get();

            if (wasAllocated + size > quota)
                throw new IgniteException("Global memory quota for SQL queries exceeded [quota=" + quota + ']');
        }
        while (!allocated.compareAndSet(wasAllocated, wasAllocated + size));
    }

    /** {@inheritDoc} */
    @Override public void onMemoryReleased(long size) {
        allocated.addAndGet(-size);
    }

    /** {@inheritDoc} */
    @Override public void reset() {
        allocated.set(0);
    }

    /** {@inheritDoc} */
    @Override public long allocated() {
        return allocated.get();
    }
}

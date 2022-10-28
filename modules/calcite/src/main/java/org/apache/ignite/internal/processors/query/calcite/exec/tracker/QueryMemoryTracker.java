package org.apache.ignite.internal.processors.query.calcite.exec.tracker;

import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteException;

/**
 * Memory allocation tracker for queries.
 * Each query can have more than one fragment and each fragment can be executed on it's own thread, implementation
 * must be thread safe.
 */
public class QueryMemoryTracker implements MemoryTracker {
    /** Parent (global) memory tracker. */
    private final MemoryTracker parent;

    /** Memory quota for each query. */
    private final long quota;

    /** Currently allocated. */
    private final AtomicLong allocated = new AtomicLong();

    /** */
    public QueryMemoryTracker(MemoryTracker parent, long quota) {
        this.parent = parent;
        this.quota = quota;
    }

    /** {@inheritDoc} */
    @Override public void onMemoryAllocated(long size) {
        long wasAllocated;

        do {
            wasAllocated = allocated.get();

            if (quota > 0 && wasAllocated + size > quota)
                throw new IgniteException("Query quota exceeded [quota=" + quota + ']');
        }
        while (!allocated.compareAndSet(wasAllocated, wasAllocated + size));

        try {
            parent.onMemoryAllocated(size);
        }
        catch (Exception e) {
            // Undo changes in case of global quota exceeded.
            release(size);

            throw e;
        }
    }

    /** {@inheritDoc} */
    @Override public void onMemoryReleased(long size) {
        long released = release(size);

        if (released > 0)
            parent.onMemoryReleased(released);
    }

    /** Release size, but no more than currently allocated. */
    private long release(long size) {
        long wasAllocated;
        long released;

        do {
            wasAllocated = allocated.get();

            released = Math.min(size, wasAllocated);
        }
        while (!allocated.compareAndSet(wasAllocated, wasAllocated - released));

        return released;
    }

    /** {@inheritDoc} */
    @Override public void reset() {
        long wasAllocated = allocated.getAndSet(0);

        if (wasAllocated > 0)
            parent.onMemoryReleased(wasAllocated);
    }

    /** {@inheritDoc} */
    @Override public long allocated() {
        return allocated.get();
    }
}

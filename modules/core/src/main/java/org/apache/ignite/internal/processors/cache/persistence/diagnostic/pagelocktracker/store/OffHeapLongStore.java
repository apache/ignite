package org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.store;

import org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.LongStore;
import org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.PageLockTrackerManager.MemoryCalculator;
import org.apache.ignite.internal.util.GridUnsafe;

public class OffHeapLongStore implements LongStore {
    /**
     *
     */
    private final int size;
    /**
     *
     */
    private final int capacity;
    /**
     *
     */
    private final long ptr;
    /**
     *
     */
    private final MemoryCalculator memCalc;

    /**
     *
     */
    public OffHeapLongStore(int capacity, MemoryCalculator memCalc) {
        this.capacity = capacity * 2;
        this.ptr = allocate(size = (this.capacity * 8));
        this.memCalc = memCalc;

        memCalc.onOffHeapAllocated(size);
    }

    /**
     *
     */
    private long allocate(int size) {
        long ptr = GridUnsafe.allocateMemory(size);

        GridUnsafe.setMemory(ptr, size, (byte)0);

        return ptr;
    }

    @Override public int capacity() {
        return capacity / 2;
    }

    /** {@inheritDoc} */
    @Override public long getByIndex(int idx) {
        return GridUnsafe.getLong(ptr + offset(idx));
    }

    /** {@inheritDoc} */
    @Override public void setByIndex(int idx, long val) {
        GridUnsafe.putLong(ptr + offset(idx), val);
    }

    /**
     *
     */
    private long offset(long headIdx) {
        return headIdx * 8;
    }

    @Override public long[] copy() {
        long[] arr = new long[capacity];

        GridUnsafe.copyMemory(null, ptr, arr, GridUnsafe.LONG_ARR_OFF, size);

        return arr;
    }

    /** {@inheritDoc} */
    @Override public void free() {
        GridUnsafe.freeMemory(ptr);

        memCalc.onOffHeapFree(size);
    }
}

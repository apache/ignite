package org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.store;

import org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.LongStore;
import org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.PageLockTrackerManager.MemoryCalculator;

public class HeapLongStore implements LongStore {
    private long[] arr;

    private final MemoryCalculator memoryCalc;

    public HeapLongStore(int capacity, MemoryCalculator memoryCalc) {
        this.arr = new long[capacity * 2];
        this.memoryCalc = memoryCalc;

        memoryCalc.onHeapAllocated(arr.length * 8);
    }

    @Override public int capacity() {
        return arr.length / 2;
    }

    @Override public long getByIndex(int idx) {
        return arr[idx];
    }

    @Override public void setByIndex(int idx, long val) {
        arr[idx] = val;
    }

    @Override public long[] copy() {
        return arr.clone();
    }

    @Override public void free() {
        memoryCalc.onOffHeapFree(arr.length * 8);

        arr = null;
    }
}

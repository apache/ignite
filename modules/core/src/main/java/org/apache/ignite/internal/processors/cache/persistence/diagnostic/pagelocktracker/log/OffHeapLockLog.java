package org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.log;

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.internal.util.GridUnsafe;

public class OffHeapLockLog extends LockLog {
    private final int logSize;

    private final long ptr;

    public OffHeapLockLog(String name, int capacity) {
        super(name, capacity);

        this.logSize = (capacity * 8) * 2;
        this.ptr = allocate(logSize);
    }

    @Override protected long getByIndex(int idx) {
        return GridUnsafe.getLong(ptr + offset(idx));
    }

    @Override protected void setByIndex(int idx, long val) {
        GridUnsafe.putLong(ptr + offset(idx), val);
    }

    private long offset(long headIdx) {
        return headIdx * 8;
    }

    private long allocate(int size) {
        long ptr = GridUnsafe.allocateMemory(size);

        GridUnsafe.setMemory(ptr, logSize, (byte)0);

        return ptr;
    }

    @Override protected void free() {
        GridUnsafe.freeMemory(ptr);
    }
}

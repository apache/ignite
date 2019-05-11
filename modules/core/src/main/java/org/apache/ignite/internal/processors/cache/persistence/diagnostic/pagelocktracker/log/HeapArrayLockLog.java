package org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.log;

import java.util.ArrayList;
import java.util.List;

public class HeapArrayLockLog extends LockLog {
    private final int logSize;

    private final long[] pageIdsLockLog;

    public HeapArrayLockLog(String name, int capacity) {
        super(name, capacity);

        this.pageIdsLockLog = new long[capacity * 2];
        this.logSize = capacity;
    }

    @Override public int capacity() {
        return logSize;
    }

    @Override protected long getByIndex(int idx) {
        return pageIdsLockLog[idx];
    }

    @Override protected void setByIndex(int idx, long val) {
        pageIdsLockLog[idx] = val;
    }

    @Override protected void free() {
        // No-op.
    }
}

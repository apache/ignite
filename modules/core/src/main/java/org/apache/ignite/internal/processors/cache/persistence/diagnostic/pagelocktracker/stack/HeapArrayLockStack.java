package org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.stack;

import static java.util.Arrays.copyOf;

public class HeapArrayLockStack extends LockStack {
    private final long[] pageIdLocksStack;

    public HeapArrayLockStack(String name, int capacity) {
        super(name, capacity);

        this.pageIdLocksStack = new long[capacity];
    }

    @Override protected long getByIndex(int idx) {
        return pageIdLocksStack[idx];
    }

    @Override protected void setByIndex(int idx, long val) {
        pageIdLocksStack[idx] = val;
    }

    @Override protected void free() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public LockStackSnapshot snapshot() {
        long[] stack = copyOf(pageIdLocksStack, pageIdLocksStack.length);

        return new LockStackSnapshot(
            name,
            System.currentTimeMillis(),
            headIdx,
            stack,
            nextOp,
            nextOpStructureId,
            nextOpPageId
        );
    }
}

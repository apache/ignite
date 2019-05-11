package org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker;

import org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.log.HeapArrayLockLog;
import org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.log.OffHeapLockLog;
import org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.stack.HeapArrayLockStack;
import org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.stack.OffHeapLockStack;

import static java.lang.String.valueOf;

public final class LockTracerFactory {
    public static final int HEAP_STACK = 1;
    public static final int HEAP_LOG = 2;
    public static final int OFF_HEAP_STACK = 3;
    public static final int OFF_HEAP_LOG = 4;

    public static final int DEFAULT_CAPACITY = 128;
    public static final int DEFAULT_TYPE = HEAP_STACK;

    public static PageLockTracker create(String name) {
        return create(DEFAULT_TYPE, name);
    }

    public static PageLockTracker create(int type, String name) {
        return create(type, name, DEFAULT_CAPACITY);
    }

    public static PageLockTracker create(int type, String name, int size) {
        switch (type) {
            case HEAP_STACK:
                return new HeapArrayLockStack(name, size);
            case HEAP_LOG:
                return new HeapArrayLockLog(name, size);
            case OFF_HEAP_STACK:
                return new OffHeapLockStack(name, size);
            case OFF_HEAP_LOG:
                return new OffHeapLockLog(name, size);

            default:
                throw new IllegalArgumentException(valueOf(type));
        }
    }
}

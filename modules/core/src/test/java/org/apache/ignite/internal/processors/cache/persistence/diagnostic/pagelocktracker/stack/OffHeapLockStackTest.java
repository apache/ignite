package org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.stack;

import org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.LockTracerFactory;
import org.apache.ignite.internal.processors.cache.persistence.diagnostic.PageLockStackTest;

import static org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.LockTracerFactory.OFF_HEAP_STACK;

public class OffHeapLockStackTest extends PageLockStackTest {
    @Override protected LockStack createLockStackTracer(String name) {
        return (LockStack)LockTracerFactory.create(OFF_HEAP_STACK, name);
    }
}

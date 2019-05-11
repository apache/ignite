package org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.log;

import org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.LockTracerFactory;
import org.apache.ignite.internal.processors.cache.persistence.diagnostic.PageLockLogTest;

import static org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.LockTracerFactory.OFF_HEAP_LOG;

public class OffHeapLockLogTest extends PageLockLogTest {

    @Override protected LockLog createLogStackTracer(String name) {
        return (LockLog)LockTracerFactory.create(OFF_HEAP_LOG, name);
    }
}
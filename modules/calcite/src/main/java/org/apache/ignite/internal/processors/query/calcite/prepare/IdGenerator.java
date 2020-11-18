package org.apache.ignite.internal.processors.query.calcite.prepare;

import java.util.concurrent.atomic.AtomicLong;

/**
 *
 */
public class IdGenerator {
    /** */
    private static final AtomicLong ID_GEN = new AtomicLong();

    /** */
    private IdGenerator() {}

    /** */
    public static long nextId() {
        return ID_GEN.getAndIncrement();
    }
}

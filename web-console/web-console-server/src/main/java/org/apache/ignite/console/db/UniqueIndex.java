package org.apache.ignite.console.db;

import java.util.function.Function;

/**
 * Index for unique constraint.
 */
public class UniqueIndex<T> {
    /** */
    private final Function<T, Object> keyGenerator;

    /** */
    private final Function<T, String> msgGenerator;

    /**
     * Constructor.
     */
    UniqueIndex(Function<T, Object> keyGenerator, Function<T, String> msgGenerator) {
        this.keyGenerator = keyGenerator;
        this.msgGenerator = msgGenerator;
    }

    /**
     * @param val Value.
     * @return Unique key.
     */
    public Object key(T val) {
        return keyGenerator.apply(val);
    }

    /**
     * @param val Value.
     */
    public String message(T val) {
        return msgGenerator.apply(val);
    }
}

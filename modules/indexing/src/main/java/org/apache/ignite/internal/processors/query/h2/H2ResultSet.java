package org.apache.ignite.internal.processors.query.h2;

import org.h2.value.Value;

/**
 * Result set for H2 query.
 */
public class H2ResultSet implements AutoCloseable {

    public boolean next() {
        return false;
    }

    public Value[] get() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void close() {

    }
}

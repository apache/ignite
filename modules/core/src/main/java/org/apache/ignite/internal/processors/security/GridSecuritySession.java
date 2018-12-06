package org.apache.ignite.internal.processors.security;

/**
 * Representation of Grid Security Session.
 */
public interface GridSecuritySession extends AutoCloseable {
    /** {@inheritDoc} */
    @Override public void close();
}

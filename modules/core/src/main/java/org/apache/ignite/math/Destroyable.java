package org.apache.ignite.math;

/**
 * TODO add description
 */
public interface Destroyable {
    /**
     * Destroys object if managed outside of JVM. It's a no-op in all other cases.
     */
    default void destroy() {
        // No-op.
    }
}

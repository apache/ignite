package org.apache.ignite.internal.processors.cache.persistence.pagemem;

/**
 * Strategy used to protect memory from exhaustion.
 */
public interface ThrottlingStrategy {
    /**
     * Computes next duration (in nanos) to throttle a thread.
     *
     * @return park time in nanos.
     */
    public long protectionParkTime();

    /**
     * Resets the state. Invoked when no throttling is needed anymore.
     *
     * @return {@code true} if the instance was not already in a reset state
     */
    public boolean reset();
}

package org.apache.ignite.internal.client;

/**
 *
 */
public interface IgniteClientInternal extends AutoCloseable {
    /**
     * Stop warm-up.
     *
     * @throws GridClientException In case of error.
     */
    public void stopWarmUp();
}

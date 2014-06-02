/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.cmd.dto.node;

import java.io.*;

/**
 * Store configuration data.
 */
public class VisorDrSenderHubConfig implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Data center connection configurations. */
    private final Iterable<VisorDrSenderHubConnectionConfig> connectionConfiguration;

    /** Maximum failed connect attempts. When all replica nodes reaches this limit replica become offline. */
    private final int maxFailedConnectAttempts;

    /** Maximum amount of errors received from the replica. When replica node reaches this limit, it is disconnected. */
    private final int maxErrors;

    /** Get health check frequency in milliseconds. */
    private final long healthCheckFrequency;

    /** System request timeout in milliseconds. */
    private final long systemRequestTimeout;

    /** Read timeout in milliseconds. */
    private final long readTimeout;

    /** Maximum wait queue size. */
    private final int maxQueueSize;

    /** Timeout after which node can be reconnected in case it was previously disconnected due to a failure. */
    private final long reconnectOnFailureTimeout;

    /** Cache names this sender hub works with. */
    private final String[] cacheNames;

    public VisorDrSenderHubConfig(Iterable<VisorDrSenderHubConnectionConfig> connectionConfiguration,
        int maxFailedConnectAttempts, int maxErrors, long healthCheckFrequency, long systemRequestTimeout,
        long readTimeout, int maxQueueSize, long reconnectOnFailureTimeout, String[] cacheNames) {
        this.connectionConfiguration = connectionConfiguration;
        this.maxFailedConnectAttempts = maxFailedConnectAttempts;
        this.maxErrors = maxErrors;
        this.healthCheckFrequency = healthCheckFrequency;
        this.systemRequestTimeout = systemRequestTimeout;
        this.readTimeout = readTimeout;
        this.maxQueueSize = maxQueueSize;
        this.reconnectOnFailureTimeout = reconnectOnFailureTimeout;
        this.cacheNames = cacheNames;
    }

    /**
     * @return Data center connection configurations.
     */
    public Iterable<VisorDrSenderHubConnectionConfig> connectionConfiguration() {
        return connectionConfiguration;
    }

    /**
     * @return Maximum failed connect attempts. When all replica nodes reaches this limit replica become offline.
     */
    public int maxFailedConnectAttempts() {
        return maxFailedConnectAttempts;
    }

    /**
     * @return Maximum amount of errors received from the replica. When replica node reaches this limit, it is
     * disconnected.
     */
    public int maxErrors() {
        return maxErrors;
    }

    /**
     * @return Get health check frequency in milliseconds.
     */
    public long healthCheckFrequency() {
        return healthCheckFrequency;
    }

    /**
     * @return System request timeout in milliseconds.
     */
    public long systemRequestTimeout() {
        return systemRequestTimeout;
    }

    /**
     * @return Read timeout in milliseconds.
     */
    public long readTimeout() {
        return readTimeout;
    }

    /**
     * @return Maximum wait queue size.
     */
    public int maxQueueSize() {
        return maxQueueSize;
    }

    /**
     * @return Timeout after which node can be reconnected in case it was previously disconnected due to a failure.
     */
    public long reconnectOnFailureTimeout() {
        return reconnectOnFailureTimeout;
    }

    /**
     * @return Cache names this sender hub works with.
     */
    public String[] cacheNames() {
        return cacheNames;
    }
}

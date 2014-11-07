/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.cmd.dto.node;

import org.gridgain.grid.dr.hub.sender.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;
import java.util.*;

/**
 * Data transfer object for DR sender hub configuration properties.
 */
public class VisorDrSenderHubConfig implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Data center connection configurations. */
    private Iterable<VisorDrSenderHubConnectionConfig> connectionConfiguration;

    /** Maximum failed connect attempts. When all replica nodes reaches this limit replica become offline. */
    private int maxFailedConnectAttempts;

    /** Maximum amount of errors received from the replica. When replica node reaches this limit, it is disconnected. */
    private int maxErrors;

    /** Get health check frequency in milliseconds. */
    private long maxHealthCheckFrequency;

    /** System request timeout in milliseconds. */
    private long systemRequestTimeout;

    /** Read timeout in milliseconds. */
    private long readTimeout;

    /** Maximum wait queue size. */
    private int maxQueueSize;

    /** Timeout after which node can be reconnected in case it was previously disconnected due to a failure. */
    private long reconnectOnFailureTimeout;

    /** Cache names this sender hub works with. */
    private String[] cacheNames;

    /**
     * @param sndCfg Data transfer object for DR sender hub configuration properties.
     * @return Data transfer object for DR sender hub configuration properties.
     */
    public static VisorDrSenderHubConfig from(GridDrSenderHubConfiguration sndCfg) {
        if (sndCfg == null)
            return null;

        Collection<VisorDrSenderHubConnectionConfig> rmtCfgs = new ArrayList<>();

        for (GridDrSenderHubConnectionConfiguration rmtCfg : sndCfg.getConnectionConfiguration())
            rmtCfgs.add(VisorDrSenderHubConnectionConfig.from(rmtCfg));

        VisorDrSenderHubConfig cfg = new VisorDrSenderHubConfig();

        cfg.connectionConfiguration(rmtCfgs);
        cfg.maxFailedConnectAttempts(sndCfg.getMaxFailedConnectAttempts());
        cfg.maxErrors(sndCfg.getMaxErrors());
        cfg.maxHealthCheckFrequency(sndCfg.getHealthCheckFrequency());
        cfg.systemRequestTimeout(sndCfg.getSystemRequestTimeout());
        cfg.readTimeout(sndCfg.getReadTimeout());
        cfg.maxQueueSize(sndCfg.getMaxQueueSize());
        cfg.reconnectOnFailureTimeout(sndCfg.getReconnectOnFailureTimeout());
        cfg.cacheNames(sndCfg.getCacheNames());

        return cfg;
    }

    /**
     * @return Data center connection configurations.
     */
    public Iterable<VisorDrSenderHubConnectionConfig> connectionConfiguration() {
        return connectionConfiguration;
    }

    /**
     * @param connConfiguration New data center connection configurations.
     */
    public void connectionConfiguration(Iterable<VisorDrSenderHubConnectionConfig> connConfiguration) {
        connectionConfiguration = connConfiguration;
    }

    /**
     * @return Maximum failed connect attempts. When all replica nodes reaches this limit replica become offline.
     */
    public int maxFailedConnectAttempts() {
        return maxFailedConnectAttempts;
    }

    /**
     * @param maxFailedConnectAttempts New maximum failed connect attempts. When all replica nodes reaches this limit
     * replica become offline.
     */
    public void maxFailedConnectAttempts(int maxFailedConnectAttempts) {
        this.maxFailedConnectAttempts = maxFailedConnectAttempts;
    }

    /**
     * @return Maximum amount of errors received from the replica. When replica node reaches this limit, it is
     * disconnected.
     */
    public int maxErrors() {
        return maxErrors;
    }

    /**
     * @param maxErrors New maximum amount of errors received from the replica. When replica node reaches this limit, it
     * is disconnected.
     */
    public void maxErrors(int maxErrors) {
        this.maxErrors = maxErrors;
    }

    /**
     * @return Get health check frequency in milliseconds.
     */
    public long maxHealthCheckFrequency() {
        return maxHealthCheckFrequency;
    }

    /**
     * @param healthCheckFreq New get health check frequency in milliseconds.
     */
    public void maxHealthCheckFrequency(long healthCheckFreq) {
        maxHealthCheckFrequency = healthCheckFreq;
    }

    /**
     * @return System request timeout in milliseconds.
     */
    public long systemRequestTimeout() {
        return systemRequestTimeout;
    }

    /**
     * @param sysReqTimeout New system request timeout in milliseconds.
     */
    public void systemRequestTimeout(long sysReqTimeout) {
        systemRequestTimeout = sysReqTimeout;
    }

    /**
     * @return Read timeout in milliseconds.
     */
    public long readTimeout() {
        return readTimeout;
    }

    /**
     * @param readTimeout New read timeout in milliseconds.
     */
    public void readTimeout(long readTimeout) {
        this.readTimeout = readTimeout;
    }

    /**
     * @return Maximum wait queue size.
     */
    public int maxQueueSize() {
        return maxQueueSize;
    }

    /**
     * @param maxQueueSize New maximum wait queue size.
     */
    public void maxQueueSize(int maxQueueSize) {
        this.maxQueueSize = maxQueueSize;
    }

    /**
     * @return Timeout after which node can be reconnected in case it was previously disconnected due to a failure.
     */
    public long reconnectOnFailureTimeout() {
        return reconnectOnFailureTimeout;
    }

    /**
     * @param reconnectOnFailureTimeout New timeout after which node can be reconnected in case it was previously
     * disconnected due to a failure.
     */
    public void reconnectOnFailureTimeout(long reconnectOnFailureTimeout) {
        this.reconnectOnFailureTimeout = reconnectOnFailureTimeout;
    }

    /**
     * @return Cache names this sender hub works with.
     */
    public String[] cacheNames() {
        return cacheNames;
    }

    /**
     * @param cacheNames New cache names this sender hub works with.
     */
    public void cacheNames(String[] cacheNames) {
        this.cacheNames = cacheNames;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorDrSenderHubConfig.class, this);
    }
}

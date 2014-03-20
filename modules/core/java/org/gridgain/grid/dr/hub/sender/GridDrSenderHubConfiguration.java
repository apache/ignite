/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.dr.hub.sender;

import org.gridgain.grid.dr.hub.sender.store.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.util.*;

import org.gridgain.grid.dr.hub.sender.store.memory.*;

/**
 * Data center replication sender hub configuration.
 */
public class GridDrSenderHubConfiguration {
    /** Default maximum failed connect attempts. */
    public static final int DFLT_MAX_FAILED_CONNECT_ATTEMPTS = 5;

    /** Default maximum processing errors. */
    public static final int DFLT_MAX_ERRORS = 10;

    /** Default health check frequency. */
    public static final long DFLT_HEALTH_CHECK_FREQ = 2000L;

    /** Default ping timeout. */
    public static final long DFLT_SYS_REQ_TIMEOUT = 5000L;

    /** Default read timeout. */
    public static final long DFLT_READ_TIMEOUT = 5000L;

    /** Default maximum amount of enqueued requests per remote receiver hub. */
    public static final int DFLT_MAX_QUEUE_SIZE = 100;

    /** Reconnect on error timeout. */
    public static final long DFLT_RECONNECT_ON_FAILURE_TIMEOUT = 5000L;

    /** Replicas with which this sender hub will communicate. */
    private GridDrSenderHubConnectionConfiguration[] connCfg;

    /** Maximum failed connection attempts after which remote sender hub is considered to be offline. */
    private int maxFailedConnectAttempts = DFLT_MAX_FAILED_CONNECT_ATTEMPTS;

    /** Maximum processing errors received from replication hub before it is considered to be offline. */
    private int maxErrors = DFLT_MAX_ERRORS;

    /** Receiving hubs health check frequency. */
    private long healthCheckFreq = DFLT_HEALTH_CHECK_FREQ;

    /** Ping timeout. */
    private long sysReqTimeout = DFLT_SYS_REQ_TIMEOUT;

    /** Read timeout. */
    private long readTimeout = DFLT_READ_TIMEOUT;

    /** Maximum queue size. */
    private int maxQueueSize = DFLT_MAX_QUEUE_SIZE;

    /** Reconnect-on-failure timeout. */
    private long reconnOnFailureTimeout = DFLT_RECONNECT_ON_FAILURE_TIMEOUT;

    /** Cache names this sender hub works with. */
    private String[] cacheNames;

    /** Store. */
    private GridDrSenderHubStore store;

    /**
     * Default constructor.
     */
    public GridDrSenderHubConfiguration() {
        // No-op.
    }

    /**
     * Copying constructor.
     *
     * @param cfg Another sender hub configuration.
     */
    public GridDrSenderHubConfiguration(GridDrSenderHubConfiguration cfg) {
        cacheNames = cfg.getCacheNames();
        healthCheckFreq = cfg.getHealthCheckFrequency();
        maxErrors = cfg.getMaxErrors();
        maxFailedConnectAttempts = cfg.getMaxFailedConnectAttempts();
        maxQueueSize = cfg.getMaxQueueSize();
        store = cfg.getStore();
        readTimeout = cfg.getReadTimeout();
        reconnOnFailureTimeout = cfg.getReconnectOnFailureTimeout();
        connCfg = cfg.getConnectionConfiguration();
        sysReqTimeout = cfg.getSystemRequestTimeout();
    }

    /**
     * Get remote data center connection configurations. Defines data centers this sender hubs will work with.
     *
     * @return Remote data center connection configurations.
     */
    public GridDrSenderHubConnectionConfiguration[] getConnectionConfiguration() {
        return connCfg;
    }

    /**
     * Set remote data center connection configurations. See {@link #getConnectionConfiguration()} for more
     * information.
     *
     * @param connCfg Remote data center connection configurations.
     */
    public void setConnectionConfiguration(GridDrSenderHubConnectionConfiguration... connCfg) {
        this.connCfg = connCfg;
    }

    /**
     * Gets maximum failed connect attempts. Once amount of failed sequential attmepts to connect to particular
     * remote receiver hubs exceeds this limit, receiver hub will be considered offline. Further attempts to
     * re-establish connection to this receiver hub will be performed according to
     * {@link #getReconnectOnFailureTimeout()} property.
     * <p>
     * Defaults to {@link #DFLT_MAX_FAILED_CONNECT_ATTEMPTS}.
     *
     * @return Maximum failed connect attempts.
     */
    public int getMaxFailedConnectAttempts() {
        return maxFailedConnectAttempts;
    }

    /**
     * Set maximum failed connect attempts. See {@link #getMaxFailedConnectAttempts()} for more information.
     *
     * @param maxFailedConnectAttempts Maximum failed connect attempts.
     */
    public void setMaxFailedConnectAttempts(int maxFailedConnectAttempts) {
        this.maxFailedConnectAttempts = maxFailedConnectAttempts;
    }

    /**
     * Gets maximum amount of sequential errors in receiver hub responses. When this threshold is reached, connection
     * to this faulty receiver hub will be closed and reciver hub will be considered offline. Further attempts to
     * re-establish connection to this receiver hub will be performed according to
     * {@link #getReconnectOnFailureTimeout()} property.
     * <p>
     * Defaults to {@link #DFLT_MAX_ERRORS}.
     *
     * @return Maximum amount of sequential errors in receiver hub responses
     */
    public int getMaxErrors() {
        return maxErrors;
    }

    /**
     * Sets maximum amount of sequential errors in receiver hub responses. See {@link #getMaxErrors()} for more
     * information.
     *
     * @param maxErrors Maximum amount of sequential errors in receiver hub responses
     */
    public void setMaxErrors(int maxErrors) {
        this.maxErrors = maxErrors;
    }

    /**
     * Gets health check frequency. Defines how often in milliseconds an attempt to restore connection to disconnected
     * receiver hubs is performed and how often connected receiver hubs will be checked for necessity to send a ping
     * request using {@link #getReadTimeout()} property.
     * <p>
     * Defaults to {@link #DFLT_HEALTH_CHECK_FREQ}.
     *
     * @return Health check frequency.
     */
    public long getHealthCheckFrequency() {
        return healthCheckFreq;
    }

    /**
     * Sets health check frequency. See {@link #getHealthCheckFrequency()} for more information.
     *
     * @param healthCheckFreq Health check frequency.
     */
    public void setHealthCheckFrequency(long healthCheckFreq) {
        this.healthCheckFreq = healthCheckFreq;
    }

    /**
     * Gets system request timeout. Sender hub processor performs two types of system requests: handshake and ping.
     * Handshake request contains information required by receiver hub and is only sent when network connection
     * is established. Ping request just checks that receiver hub is reachable and responds when there was no
     * reads from it for more than {@link #getReadTimeout()}.
     * <p>
     * In case any of these two requests was sent to receiver hub and there is no response for too long, it
     * indicates a problem with either network connectivity or receiver hub itself. Thus, when the given timeout is
     * exceeded, connection to receiver hub is closed.
     * <p>
     * Defaults to {@link #DFLT_SYS_REQ_TIMEOUT}.
     *
     * @return System request timeout.
     */
    public long getSystemRequestTimeout() {
        return sysReqTimeout;
    }

    /**
     * Sets system request timeout. See {@link #getSystemRequestTimeout()} for more information.
     *
     * @param sysReqTimeout System request timeout.
     */
    public void setSystemRequestTimeout(long sysReqTimeout) {
        this.sysReqTimeout = sysReqTimeout;
    }

    /**
     * Gets read timeout. When difference between current time and the time when last read operation on particular
     * remote receiver hub occurred is greater than this timeout (in milliseconds), ping request will be sent to
     * this receiver hub.
     * <p>
     * Defaults to {@link #DFLT_READ_TIMEOUT}.
     *
     * @return Read timeout.
     */
    public long getReadTimeout() {
        return readTimeout;
    }

    /**
     * Sets read timeout. See {@link #getReadTimeout()} for more information.
     *
     * @param readTimeout Read timeout.
     */
    public void setReadTimeout(long readTimeout) {
        this.readTimeout = readTimeout;
    }

    /**
     * Gets maximum wait queue size. Defines maximum amount of entries which are read from store but have not been
     * acknowledged yet.
     * <p>
     * Defaults to {@link #DFLT_MAX_QUEUE_SIZE}.
     *
     * @return Maximum wait queue size.
     */
    public int getMaxQueueSize() {
        return maxQueueSize;
    }

    /**
     * Sets maximum wait queue size. See {@link #getMaxQueueSize()} for more information.
     *
     * @param maxQueueSize Maximum wait queue size.
     */
    public void setMaxQueueSize(int maxQueueSize) {
        this.maxQueueSize = maxQueueSize;
    }

    /**
     * Gets reconnect-on-failure timeout. This values defines timeout in milliseconds after which connection to
     * remote receiver hub can be re-established in case it was previously disconnected due to a failure.
     * <p>
     * Defaults to {@link #DFLT_RECONNECT_ON_FAILURE_TIMEOUT}.
     *
     * @return Reconnect-on-failure timeout.
     */
    public long getReconnectOnFailureTimeout() {
        return reconnOnFailureTimeout;
    }

    /**
     * Sets reconnect-on-failure timeout. See {@link #getReconnectOnFailureTimeout()} for more information.
     *
     * @param reconnectOnFailureTimeout Reconnect-on-failure timeout.
     */
    public void setReconnectOnFailureTimeout(long reconnectOnFailureTimeout) {
        this.reconnOnFailureTimeout = reconnectOnFailureTimeout;
    }

    /**
     * Gets cache names this sender hub works with. All sender hubs working with concrete cache must have this
     * property equal. E.g., in case one sender hub works with caches A and B, then another sender hub cannot work
     * with caches B and C. Instead, both sender hubs must work with either {A, B}, {B, C} or even {A, B, C} caches.
     * However, it is possible for the first sender hub work with caches {A, B} and for the second sender hub to work
     * with caches {C, D}.
     *
     * @return Cache names this sender hub works with.
     */
    public String[] getCacheNames() {
        return cacheNames;
    }

    /**
     * Set cache names this sender hub works with. See {@link #getCacheNames()} for more information.
     *
     * @param cacheNames Cache names this sender hub works with.
     */
    @SuppressWarnings({"ZeroLengthArrayAllocation"})
    public void setCacheNames(@Nullable String... cacheNames) {
        this.cacheNames = cacheNames != null ? cacheNames : new String[0];

        // Mask cache names.
        for (int i = 0; i < this.cacheNames.length; i++)
            this.cacheNames[i] = CU.mask(this.cacheNames[i]);

        Arrays.sort(this.cacheNames);
    }

    /**
     * Gets store. All incoming sender cache batches will be saved in this store before being sent to receiver hubs.
     * Once acknowledge for particular batch is received from all destination receiver hubs, this batch will be
     * removed from store.
     * <p>
     * If set to {@code null} then {@link GridDrSenderHubInMemoryStore} will be used as a default.
     *
     * @return Store.
     */
    public GridDrSenderHubStore getStore() {
        return store;
    }

    /**
     * Sets store. See {@link #getStore()} for more information.
     *
     * @param store Store.
     */
    public void setStore(GridDrSenderHubStore store) {
        this.store = store;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDrSenderHubConfiguration.class, this);
    }
}

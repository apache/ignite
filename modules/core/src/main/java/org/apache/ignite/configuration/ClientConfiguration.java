/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.configuration;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import javax.cache.configuration.Factory;
import javax.net.ssl.SSLContext;

import org.apache.ignite.IgniteLogger;
import org.apache.ignite.client.ClientAddressFinder;
import org.apache.ignite.client.ClientPartitionAwarenessMapper;
import org.apache.ignite.client.ClientPartitionAwarenessMapperFactory;
import org.apache.ignite.client.ClientRetryAllPolicy;
import org.apache.ignite.client.ClientRetryPolicy;
import org.apache.ignite.client.SslMode;
import org.apache.ignite.client.SslProtocol;
import org.apache.ignite.internal.client.thin.TcpIgniteClient;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * {@link TcpIgniteClient} configuration.
 */
@SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
public final class ClientConfiguration implements Serializable {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** @serial Server addresses. */
    private String[] addrs;

    /** Server addresses finder. */
    private transient ClientAddressFinder addrFinder;

    /** @serial Tcp no delay. */
    private boolean tcpNoDelay = true;

    /** @serial Timeout. 0 means infinite. */
    private int timeout;

    /** @serial Send buffer size. 0 means system default. */
    private int sndBufSize = 32 * 1024;

    /** @serial Receive buffer size. 0 means system default. */
    private int rcvBufSize = 32 * 1024;

    /** @serial Configuration for Ignite binary objects. */
    private BinaryConfiguration binaryCfg;

    /** @serial Ssl mode. */
    private SslMode sslMode = SslMode.DISABLED;

    /** @serial Ssl client certificate key store path. */
    private String sslClientCertKeyStorePath;

    /** @serial Ssl client certificate key store password. */
    private String sslClientCertKeyStorePwd;

    /** @serial Ssl trust certificate key store path. */
    private String sslTrustCertKeyStorePath;

    /** @serial Ssl trust certificate key store password. */
    private String sslTrustCertKeyStorePwd;

    /** @serial Ssl client certificate key store type. */
    private String sslClientCertKeyStoreType;

    /** @serial Ssl trust certificate key store type. */
    private String sslTrustCertKeyStoreType;

    /** @serial Ssl key algorithm. */
    private String sslKeyAlgorithm;

    /** @serial Flag indicating if certificate validation errors should be ignored. */
    private boolean sslTrustAll;

    /** @serial Ssl protocol. */
    private SslProtocol sslProto = SslProtocol.TLS;

    /** @serial Ssl context factory. */
    private Factory<SSLContext> sslCtxFactory;

    /** @serial User name. */
    private String userName;

    /** @serial User password. */
    private String userPwd;

    /** User attributes. */
    private Map<String, String> userAttrs;

    /** Tx config. */
    private ClientTransactionConfiguration txCfg = new ClientTransactionConfiguration();

    /**
     * Whether partition awareness should be enabled.
     */
    private boolean partitionAwarenessEnabled = true;

    /**
     * This factory accepts as parameters a cache name and the number of cache partitions received from a server node and produces
     * a {@link ClientPartitionAwarenessMapper}. This mapper function is used only for local calculations key to a partition and
     * will not be passed to a server node.
     */
    private ClientPartitionAwarenessMapperFactory partitionAwarenessMapperFactory;

    /**
     * Reconnect throttling period (in milliseconds). There are no more than {@code reconnectThrottlingRetries}
     * attempts to reconnect will be made within {@code reconnectThrottlingPeriod} in case of connection loss.
     * Throttling is disabled if either {@code reconnectThrottlingRetries} or {@code reconnectThrottlingPeriod} is 0.
     */
    private long reconnectThrottlingPeriod = 30_000L;

    /** Reconnect throttling retries. See {@code reconnectThrottlingPeriod}. */
    private int reconnectThrottlingRetries = 3;

    /** Retry limit. */
    private int retryLimit;

    /** Retry policy. */
    private ClientRetryPolicy retryPolicy = new ClientRetryAllPolicy();

    /** Executor for async operations continuations. */
    private Executor asyncContinuationExecutor;

    /** Whether heartbeats should be enabled. */
    private boolean heartbeatEnabled;

    /** Heartbeat interval, in milliseconds. */
    private long heartbeatInterval = 30_000L;

    /**
     * Whether automatic binary configuration should be enabled.
     */
    private boolean autoBinaryConfigurationEnabled = true;

    /** Logger. */
    private IgniteLogger logger;

    /**
     * @return Host addresses.
     */
    public String[] getAddresses() {
        if (addrs != null)
            return Arrays.copyOf(addrs, addrs.length);

        return null;
    }

    /**
     * Set addresses of Ignite server nodes within a cluster. An address can be IPv4 address or hostname, with or
     * without port. If port is not set then Ignite will generate multiple addresses for default port range. See
     * {@link ClientConnectorConfiguration#DFLT_PORT}, {@link ClientConnectorConfiguration#DFLT_PORT_RANGE}.
     *
     * @param addrs Host addresses.
     * @return {@code this} for chaining.
     */
    public ClientConfiguration setAddresses(String... addrs) {
        if (addrs != null)
            this.addrs = Arrays.copyOf(addrs, addrs.length);

        return this;
    }

    /**
     * @return Finder that finds server node addresses.
     */
    public ClientAddressFinder getAddressesFinder() {
        return addrFinder;
    }

    /**
     * @param finder Finds server node addresses.
     * @return {@code this} for chaining.
     */
    public ClientConfiguration setAddressesFinder(ClientAddressFinder finder) {
        addrFinder = finder;

        return this;
    }

    /**
     * @return Whether Nagle's algorithm is enabled.
     */
    public boolean isTcpNoDelay() {
        return tcpNoDelay;
    }

    /**
     * @param tcpNoDelay whether Nagle's algorithm is enabled.
     * @return {@code this} for chaining.
     */
    public ClientConfiguration setTcpNoDelay(boolean tcpNoDelay) {
        this.tcpNoDelay = tcpNoDelay;

        return this;
    }

    /**
     * @return Send/receive timeout in milliseconds.
     */
    public int getTimeout() {
        return timeout;
    }

    /**
     * @param timeout Send/receive timeout in milliseconds.
     * @return {@code this} for chaining.
     */
    public ClientConfiguration setTimeout(int timeout) {
        this.timeout = timeout;

        return this;
    }

    /**
     * @return Send buffer size.
     */
    public int getSendBufferSize() {
        return sndBufSize;
    }

    /**
     * @param sndBufSize Send buffer size.
     * @return {@code this} for chaining.
     */
    public ClientConfiguration setSendBufferSize(int sndBufSize) {
        this.sndBufSize = sndBufSize;

        return this;
    }

    /**
     * @return Send buffer size.
     */
    public int getReceiveBufferSize() {
        return rcvBufSize;
    }

    /**
     * @param rcvBufSize Send buffer size.
     * @return {@code this} for chaining.
     */
    public ClientConfiguration setReceiveBufferSize(int rcvBufSize) {
        this.rcvBufSize = rcvBufSize;

        return this;
    }

    /**
     * @return Configuration for Ignite Binary objects.
     */
    public BinaryConfiguration getBinaryConfiguration() {
        return binaryCfg;
    }

    /**
     * @param binaryCfg Configuration for Ignite Binary objects.
     * @return {@code this} for chaining.
     */
    public ClientConfiguration setBinaryConfiguration(BinaryConfiguration binaryCfg) {
        this.binaryCfg = binaryCfg;

        return this;
    }

    /**
     * @return SSL mode.
     */
    public SslMode getSslMode() {
        return sslMode;
    }

    /**
     * @param sslMode SSL mode.
     * @return {@code this} for chaining.
     */
    public ClientConfiguration setSslMode(SslMode sslMode) {
        this.sslMode = sslMode;

        return this;
    }

    /**
     * @return Ssl client certificate key store path.
     */
    public String getSslClientCertificateKeyStorePath() {
        return sslClientCertKeyStorePath;
    }

    /**
     * @param newVal Ssl client certificate key store path.
     * @return {@code this} for chaining.
     */
    public ClientConfiguration setSslClientCertificateKeyStorePath(String newVal) {
        sslClientCertKeyStorePath = newVal;

        return this;
    }

    /**
     * @return Ssl client certificate key store password.
     */
    public String getSslClientCertificateKeyStorePassword() {
        return sslClientCertKeyStorePwd;
    }

    /**
     * @param newVal Ssl client certificate key store password.
     * @return {@code this} for chaining.
     */
    public ClientConfiguration setSslClientCertificateKeyStorePassword(String newVal) {
        sslClientCertKeyStorePwd = newVal;

        return this;
    }

    /**
     * @return Ssl client certificate key store type.
     */
    public String getSslClientCertificateKeyStoreType() {
        return sslClientCertKeyStoreType;
    }

    /**
     * @param newVal Ssl client certificate key store type.
     * @return {@code this} for chaining.
     */
    public ClientConfiguration setSslClientCertificateKeyStoreType(String newVal) {
        sslClientCertKeyStoreType = newVal;

        return this;
    }

    /**
     * @return Ssl trust certificate key store path.
     */
    public String getSslTrustCertificateKeyStorePath() {
        return sslTrustCertKeyStorePath;
    }

    /**
     * @param newVal Ssl trust certificate key store path.
     * @return {@code this} for chaining.
     */
    public ClientConfiguration setSslTrustCertificateKeyStorePath(String newVal) {
        sslTrustCertKeyStorePath = newVal;

        return this;
    }

    /**
     * @return Ssl trust certificate key store password.
     */
    public String getSslTrustCertificateKeyStorePassword() {
        return sslTrustCertKeyStorePwd;
    }

    /**
     * @param newVal Ssl trust certificate key store password.
     * @return {@code this} for chaining.
     */
    public ClientConfiguration setSslTrustCertificateKeyStorePassword(String newVal) {
        sslTrustCertKeyStorePwd = newVal;

        return this;
    }

    /**
     * @return Ssl trust certificate key store type.
     */
    public String getSslTrustCertificateKeyStoreType() {
        return sslTrustCertKeyStoreType;
    }

    /**
     * @param newVal Ssl trust certificate key store type.
     * @return {@code this} for chaining.
     */
    public ClientConfiguration setSslTrustCertificateKeyStoreType(String newVal) {
        sslTrustCertKeyStoreType = newVal;

        return this;
    }

    /**
     * @return Ssl key algorithm.
     */
    public String getSslKeyAlgorithm() {
        return sslKeyAlgorithm;
    }

    /**
     * @param newVal Ssl key algorithm.
     * @return {@code this} for chaining.
     */
    public ClientConfiguration setSslKeyAlgorithm(String newVal) {
        sslKeyAlgorithm = newVal;

        return this;
    }

    /**
     * @return Flag indicating if certificate validation errors should be ignored.
     */
    public boolean isSslTrustAll() {
        return sslTrustAll;
    }

    /**
     * @param newVal Flag indicating if certificate validation errors should be ignored.
     * @return {@code this} for chaining.
     */
    public ClientConfiguration setSslTrustAll(boolean newVal) {
        sslTrustAll = newVal;

        return this;
    }

    /**
     * @return Ssl protocol.
     */
    public SslProtocol getSslProtocol() {
        return sslProto;
    }

    /**
     * @param newVal Ssl protocol.
     * @return {@code this} for chaining.
     */
    public ClientConfiguration setSslProtocol(SslProtocol newVal) {
        sslProto = newVal;

        return this;
    }

    /**
     * @return User name.
     */
    public String getUserName() {
        return userName;
    }

    /**
     * @param newVal User name.
     * @return {@code this} for chaining.
     */
    public ClientConfiguration setUserName(String newVal) {
        userName = newVal;

        return this;
    }

    /**
     * @return User password.
     */
    public String getUserPassword() {
        return userPwd;
    }

    /**
     * @param newVal User password.
     * @return {@code this} for chaining.
     */
    public ClientConfiguration setUserPassword(String newVal) {
        userPwd = newVal;

        return this;
    }

    /**
     * @return SSL Context Factory.
     */
    public Factory<SSLContext> getSslContextFactory() {
        return sslCtxFactory;
    }

    /**
     * @param newVal SSL Context Factory.
     * @return {@code this} for chaining.
     */
    public ClientConfiguration setSslContextFactory(Factory<SSLContext> newVal) {
        sslCtxFactory = newVal;

        return this;
    }

    /**
     * Gets transactions configuration.
     *
     * @return Transactions configuration.
     */
    public ClientTransactionConfiguration getTransactionConfiguration() {
        return txCfg;
    }

    /**
     * Sets transactions configuration.
     *
     * @param txCfg Transactions configuration.
     * @return {@code this} for chaining.
     */
    public ClientConfiguration setTransactionConfiguration(ClientTransactionConfiguration txCfg) {
        this.txCfg = txCfg;

        return this;
    }

    /**
     * @return A value indicating whether partition awareness should be enabled.
     * <p>
     * Default is {@code true}: client sends requests directly to the primary node for the given cache key.
     * To do so, connection is established to every known server node.
     * <p>
     * When {@code false}, only one connection is established at a given moment to a random server node.
     */
    public boolean isPartitionAwarenessEnabled() {
        return partitionAwarenessEnabled;
    }

    /**
     * Sets a value indicating whether partition awareness should be enabled.
     * <p>
     * Default is {@code true}: client sends requests directly to the primary node for the given cache key.
     * To do so, connection is established to every known server node.
     * <p>
     * When {@code false}, only one connection is established at a given moment to a random server node.
     *
     * @param partitionAwarenessEnabled Value indicating whether partition awareness should be enabled.
     * @return {@code this} for chaining.
     */
    public ClientConfiguration setPartitionAwarenessEnabled(boolean partitionAwarenessEnabled) {
        this.partitionAwarenessEnabled = partitionAwarenessEnabled;

        return this;
    }

    /**
     * @return reconnect throttling period.
     */
    public long getReconnectThrottlingPeriod() {
        return reconnectThrottlingPeriod;
    }

    /**
     * Sets reconnect throttling period.
     *
     * @param reconnectThrottlingPeriod Reconnect throttling period.
     * @return {@code this} for chaining.
     */
    public ClientConfiguration setReconnectThrottlingPeriod(long reconnectThrottlingPeriod) {
        this.reconnectThrottlingPeriod = reconnectThrottlingPeriod;

        return this;
    }

    /**
     * @return Reconnect throttling retries.
     */
    public int getReconnectThrottlingRetries() {
        return reconnectThrottlingRetries;
    }

    /**
     * Sets reconnect throttling retries.
     *
     * @param reconnectThrottlingRetries Reconnect throttling retries.
     * @return {@code this} for chaining.
     */
    public ClientConfiguration setReconnectThrottlingRetries(int reconnectThrottlingRetries) {
        this.reconnectThrottlingRetries = reconnectThrottlingRetries;

        return this;
    }

    /**
     * @return Retry limit.
     */
    public int getRetryLimit() {
        return retryLimit;
    }

    /**
     * Sets the retry limit. When a request fails due to a connection error, and multiple server connections
     * are available, Ignite will retry the request on every connection. When this property is greater than zero,
     * Ignite will limit the number of retries.
     *
     * @param retryLimit Retry limit.
     * @return {@code this} for chaining.
     */
    public ClientConfiguration setRetryLimit(int retryLimit) {
        this.retryLimit = retryLimit;

        return this;
    }

    /**
     * Gets the retry policy.
     *
     * @return Retry policy.
     */
    public ClientRetryPolicy getRetryPolicy() {
        return retryPolicy;
    }

    /**
     * Sets the retry policy. When a request fails due to a connection error, and multiple server connections
     * are available, Ignite will retry the request if the specified policy allows it.
     * <p />
     * When {@link ClientConfiguration#retryLimit} is set, retry count will be limited even if the specified policy returns {@code true}.
     * <p />
     * Default is {@link ClientRetryAllPolicy}.
     *
     * @param retryPolicy Retry policy.
     * @return {@code this} for chaining.
     */
    public ClientConfiguration setRetryPolicy(ClientRetryPolicy retryPolicy) {
        this.retryPolicy = retryPolicy;

        return this;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ClientConfiguration.class, this);
    }

    /**
     * Returns user attributes which can be used on server node.
     *
     * @return User attributes.
     */
    public Map<String, String> getUserAttributes() {
        return userAttrs;
    }

    /**
     * Sets user attributes which can be used to send additional info to the server nodes.
     *
     * Sent attributes can be accessed on server nodes from
     * {@link org.apache.ignite.internal.processors.rest.request.GridRestRequest GridRestRequest} or
     * {@link org.apache.ignite.internal.processors.odbc.ClientListenerAbstractConnectionContext
     * ClientListenerAbstractConnectionContext} (depends on client type).
     *
     * @param userAttrs User attributes.
     * @return {@code this} for chaining.
     */
    public ClientConfiguration setUserAttributes(Map<String, String> userAttrs) {
        this.userAttrs = userAttrs;

        return this;
    }

    /**
     * Gets the async continuation executor.
     * <p />
     * When <code>null</code> (default), {@link ForkJoinPool#commonPool()} is used.
     * <p />
     * When async client operation completes, corresponding {@link org.apache.ignite.lang.IgniteFuture} listeners
     * will be invoked using this executor. Thin client operation results are handled by a dedicated thread.
     * This thread should be free from any extra work, and should not be not be used to execute future listeners
     * directly.
     *
     * @return Executor for async continuations.
     */
    public Executor getAsyncContinuationExecutor() {
        return asyncContinuationExecutor;
    }

    /**
     * Sets the async continuation executor.
     * <p />
     * When <code>null</code> (default), {@link ForkJoinPool#commonPool()} is used.
     * <p />
     * When async client operation completes, corresponding {@link org.apache.ignite.lang.IgniteFuture} listeners
     * will be invoked using this executor. Thin client operation results are handled by a dedicated thread.
     * This thread should be free from any extra work, and should not be not be used to execute future listeners
     * directly.
     *
     * @param asyncContinuationExecutor Executor for async continuations.
     * @return {@code this} for chaining.
     */
    public ClientConfiguration setAsyncContinuationExecutor(Executor asyncContinuationExecutor) {
        this.asyncContinuationExecutor = asyncContinuationExecutor;

        return this;
    }

    /**
     * Gets a value indicating whether heartbeats are enabled.
     * <p />
     * When thin client connection is idle (no operations are performed), heartbeat messages are sent periodically
     * to keep the connection alive and detect potential half-open state.
     * <p />
     * See also {@link ClientConfiguration#heartbeatInterval}.
     *
     * @return Whether heartbeats are enabled.
     */
    public boolean isHeartbeatEnabled() {
        return heartbeatEnabled;
    }

    /**
     * Sets a value indicating whether heartbeats are enabled.
     * <p />
     * When thin client connection is idle (no operations are performed), heartbeat messages are sent periodically
     * to keep the connection alive and detect potential half-open state.
     * <p />
     * See also {@link ClientConfiguration#heartbeatInterval}.
     *
     * @param heartbeatEnabled Whether to enable heartbeats.
     * @return {@code this} for chaining.
     */
    public ClientConfiguration setHeartbeatEnabled(boolean heartbeatEnabled) {
        this.heartbeatEnabled = heartbeatEnabled;

        return this;
    }

    /**
     * Gets the heartbeat message interval, in milliseconds. Default is <code>30_000</code>.
     * <p />
     * When server-side {@link ClientConnectorConfiguration#getIdleTimeout()} is not zero, effective heartbeat
     * interval is set to <code>min(heartbeatInterval, idleTimeout / 3)</code>.
     * <p />
     * When thin client connection is idle (no operations are performed), heartbeat messages are sent periodically
     * to keep the connection alive and detect potential half-open state.     *
     *
     * @return Heartbeat interval.
     */
    public long getHeartbeatInterval() {
        return heartbeatInterval;
    }

    /**
     * Sets the heartbeat message interval, in milliseconds. Default is <code>30_000</code>.
     * <p />
     * When server-side {@link ClientConnectorConfiguration#getIdleTimeout()} is not zero, effective heartbeat
     * interval is set to <code>min(heartbeatInterval, idleTimeout / 3)</code>.
     * <p />
     * When thin client connection is idle (no operations are performed), heartbeat messages are sent periodically
     * to keep the connection alive and detect potential half-open state.     *
     *
     * @param heartbeatInterval Heartbeat interval, in milliseconds.
     * @return {@code this} for chaining.
     */
    public ClientConfiguration setHeartbeatInterval(long heartbeatInterval) {
        this.heartbeatInterval = heartbeatInterval;

        return this;
    }

    /**
     * Gets a value indicating whether automatic binary configuration retrieval should be enabled.
     * <p />
     * When enabled, compact footer ({@link BinaryConfiguration#isCompactFooter()})
     * and name mapper ({@link BinaryConfiguration#getNameMapper()}) settings will be retrieved from the server
     * to match the cluster configuration.
     * <p />
     * Default is {@code true}.
     *
     * @return Whether automatic binary configuration is enabled.
     */
    public boolean isAutoBinaryConfigurationEnabled() {
        return autoBinaryConfigurationEnabled;
    }

    /**
     * Sets a value indicating whether automatic binary configuration retrieval should be enabled.
     * <p />
     * When enabled, compact footer ({@link BinaryConfiguration#isCompactFooter()})
     * and name mapper ({@link BinaryConfiguration#getNameMapper()}) settings will be retrieved from the server
     * to match the cluster configuration.
     * <p />
     * Default is {@code true}.
     *
     * @param autoBinaryConfigurationEnabled Whether automatic binary configuration is enabled.
     * @return {@code this} for chaining.
     */
    public ClientConfiguration setAutoBinaryConfigurationEnabled(boolean autoBinaryConfigurationEnabled) {
        this.autoBinaryConfigurationEnabled = autoBinaryConfigurationEnabled;

        return this;
    }

    /**
     * @param factory Factory that accepts as parameters a cache name and the number of cache partitions received from a server node
     * and produces key to partition mapping functions.
     * @return {@code this} for chaining.
     */
    public ClientConfiguration setPartitionAwarenessMapperFactory(ClientPartitionAwarenessMapperFactory factory) {
        partitionAwarenessMapperFactory = factory;

        return this;
    }

    /**
     * @return Factory that accepts as parameters a cache name and the number of cache partitions received from a server node
     * and produces key to partition mapping functions.
     */
    public ClientPartitionAwarenessMapperFactory getPartitionAwarenessMapperFactory() {
        return partitionAwarenessMapperFactory;
    }

    /**
     * Sets the logger.
     *
     * @param logger Logger.
     * @return {@code this} for chaining.
     */
    public ClientConfiguration setLogger(IgniteLogger logger) {
        this.logger = logger;

        return this;
    }

    /**
     * Gets the logger.
     *
     * @return Logger.
     */
    public IgniteLogger getLogger() {
        return logger;
    }
}

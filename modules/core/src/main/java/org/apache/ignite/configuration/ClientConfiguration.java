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
import java.util.Map;
import javax.cache.configuration.Factory;
import javax.net.ssl.SSLContext;
import org.apache.ignite.client.SslMode;
import org.apache.ignite.client.SslProtocol;
import org.apache.ignite.internal.client.thin.TcpIgniteClient;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * {@link TcpIgniteClient} configuration.
 */
public final class ClientConfiguration implements Serializable {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** @serial Server addresses. */
    private String[] addrs = null;

    /** @serial Tcp no delay. */
    private boolean tcpNoDelay = true;

    /** @serial Timeout. 0 means infinite. */
    private int timeout = 0;

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
     *
     * When {@code true} client attempts to send the request directly to the primary node for the given cache key.
     * To do so, connection is established to every known server node.
     * By default {@code false} only one connection is established at a given moment to a random server node.
     */
    private boolean partitionAwarenessEnabled;

    /**
     * Reconnect throttling period (in milliseconds). There are no more than {@code reconnectThrottlingRetries}
     * attempts to reconnect will be made within {@code reconnectThrottlingPeriod} in case of connection loss.
     * Throttling is disabled if either {@code reconnectThrottlingRetries} or {@code reconnectThrottlingPeriod} is 0.
     */
    private long reconnectThrottlingPeriod = 30_000L;

    /** Reconnect throttling retries. See {@code reconnectThrottlingPeriod}. */
    private int reconnectThrottlingRetries = 3;

    /**
     * @return Host addresses.
     */
    public String[] getAddresses() {
        return addrs;
    }

    /**
     * @param addrs Host addresses.
     */
    public ClientConfiguration setAddresses(String... addrs) {
        this.addrs = addrs;

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
     */
    public ClientConfiguration setSslContextFactory(Factory<SSLContext> newVal) {
        this.sslCtxFactory = newVal;

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
     * @return {@code this} for chaining.
     */
    public ClientConfiguration setTransactionConfiguration(ClientTransactionConfiguration txCfg) {
        this.txCfg = txCfg;

        return this;
    }

    /**
     * @return Whether partition awareness should be enabled.
     */
    public boolean isPartitionAwarenessEnabled() {
        return partitionAwarenessEnabled;
    }

    /**
     * Enable or disable partition awareness.
     *
     * @return {@code this} for chaining.
     */
    public ClientConfiguration setPartitionAwarenessEnabled(boolean partitionAwarenessEnabled) {
        this.partitionAwarenessEnabled = partitionAwarenessEnabled;

        return this;
    }

    /**
     * Gets reconnect throttling period.
     */
    public long getReconnectThrottlingPeriod() {
        return reconnectThrottlingPeriod;
    }

    /**
     * Sets reconnect throttling period.
     *
     * @return {@code this} for chaining.
     */
    public ClientConfiguration setReconnectThrottlingPeriod(long reconnectThrottlingPeriod) {
        this.reconnectThrottlingPeriod = reconnectThrottlingPeriod;

        return this;
    }

    /**
     * Gets reconnect throttling retries.
     */
    public int getReconnectThrottlingRetries() {
        return reconnectThrottlingRetries;
    }

    /**
     * Sets reconnect throttling retries.
     *
     * @return {@code this} for chaining.
     */
    public ClientConfiguration setReconnectThrottlingRetries(int reconnectThrottlingRetries) {
        this.reconnectThrottlingRetries = reconnectThrottlingRetries;

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
}

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

package org.apache.ignite.internal.client.thin;

import java.net.InetSocketAddress;
import java.util.Map;
import javax.cache.configuration.Factory;
import javax.net.ssl.SSLContext;
import org.apache.ignite.client.SslMode;
import org.apache.ignite.client.SslProtocol;
import org.apache.ignite.configuration.ClientConfiguration;

/**
 * Configuration required to initialize {@link TcpClientChannel}.
 */
final class ClientChannelConfiguration {
    /** Host. */
    private final InetSocketAddress addr;

    /** Ssl mode. */
    private final SslMode sslMode;

    /** Tcp no delay. */
    private final boolean tcpNoDelay;

    /** Timeout. */
    private final int timeout;

    /** Send buffer size. */
    private final int sndBufSize;

    /** Receive buffer size. */
    private final int rcvBufSize;

    /** Ssl client certificate key store path. */
    private final String sslClientCertKeyStorePath;

    /** Ssl client certificate key store type. */
    private final String sslClientCertKeyStoreType;

    /** Ssl client certificate key store password. */
    private final String sslClientCertKeyStorePwd;

    /** Ssl trust certificate key store path. */
    private final String sslTrustCertKeyStorePath;

    /** Ssl trust certificate key store type. */
    private final String sslTrustCertKeyStoreType;

    /** Ssl trust certificate key store password. */
    private final String sslTrustCertKeyStorePwd;

    /** Ssl key algorithm. */
    private final String sslKeyAlgorithm;

    /** Ssl protocol. */
    private final SslProtocol sslProto;

    /** Ssl trust all. */
    private final boolean sslTrustAll;

    /** SSL Context Factory. */
    private final Factory<SSLContext> sslCtxFactory;

    /** User. */
    private final String userName;

    /** Password. */
    private final String userPwd;

    /** Reconnect period (for throttling). */
    private final long reconnectThrottlingPeriod;

    /** Reconnect retries within period (for throttling). */
    private final int reconnectThrottlingRetries;

    /** User attributes. */
    private Map<String, String> userAttrs;

    /**
     * Constructor.
     */
    ClientChannelConfiguration(ClientConfiguration cfg, InetSocketAddress addr) {
        this.sslMode = cfg.getSslMode();
        this.tcpNoDelay = cfg.isTcpNoDelay();
        this.timeout = cfg.getTimeout();
        this.sndBufSize = cfg.getSendBufferSize();
        this.rcvBufSize = cfg.getReceiveBufferSize();
        this.sslClientCertKeyStorePath = cfg.getSslClientCertificateKeyStorePath();
        this.sslClientCertKeyStoreType = cfg.getSslClientCertificateKeyStoreType();
        this.sslClientCertKeyStorePwd = cfg.getSslClientCertificateKeyStorePassword();
        this.sslTrustCertKeyStorePath = cfg.getSslTrustCertificateKeyStorePath();
        this.sslTrustCertKeyStoreType = cfg.getSslTrustCertificateKeyStoreType();
        this.sslTrustCertKeyStorePwd = cfg.getSslTrustCertificateKeyStorePassword();
        this.sslKeyAlgorithm = cfg.getSslKeyAlgorithm();
        this.sslProto = cfg.getSslProtocol();
        this.sslTrustAll = cfg.isSslTrustAll();
        this.sslCtxFactory = cfg.getSslContextFactory();
        this.userName = cfg.getUserName();
        this.userPwd = cfg.getUserPassword();
        this.reconnectThrottlingPeriod = cfg.getReconnectThrottlingPeriod();
        this.reconnectThrottlingRetries = cfg.getReconnectThrottlingRetries();
        this.addr = addr;
        this.userAttrs = cfg.getUserAttributes();
    }

    /**
     * @return Address.
     */
    public InetSocketAddress getAddress() {
        return addr;
    }

    /**
     * @return SSL Mode.
     */
    public SslMode getSslMode() {
        return sslMode;
    }

    /**
     * @return Tcp no delay.
     */
    public boolean isTcpNoDelay() {
        return tcpNoDelay;
    }

    /**
     * @return Timeout.
     */
    public int getTimeout() {
        return timeout;
    }

    /**
     * @return Send buffer size.
     */
    public int getSendBufferSize() {
        return sndBufSize;
    }

    /**
     * @return Receive buffer size.
     */
    public int getReceiveBufferSize() {
        return rcvBufSize;
    }

    /**
     * @return Ssl client certificate key store path.
     */
    public String getSslClientCertificateKeyStorePath() {
        return sslClientCertKeyStorePath;
    }

    /**
     * @return Ssl client certificate key store type.
     */
    public String getSslClientCertificateKeyStoreType() {
        return sslClientCertKeyStoreType;
    }

    /**
     * @return Ssl client certificate key store password.
     */
    public String getSslClientCertificateKeyStorePassword() {
        return sslClientCertKeyStorePwd;
    }

    /**
     * @return Ssl trust certificate key store path.
     */
    public String getSslTrustCertificateKeyStorePath() {
        return sslTrustCertKeyStorePath;
    }

    /**
     * @return Ssl trust certificate key store type.
     */
    public String getSslTrustCertificateKeyStoreType() {
        return sslTrustCertKeyStoreType;
    }

    /**
     * @return Ssl trust certificate key store password.
     */
    public String getSslTrustCertificateKeyStorePassword() {
        return sslTrustCertKeyStorePwd;
    }

    /**
     * @return Ssl key algorithm.
     */
    public String getSslKeyAlgorithm() {
        return sslKeyAlgorithm;
    }

    /**
     * @return SSL Protocol.
     */
    public SslProtocol getSslProtocol() {
        return sslProto;
    }

    /**
     * @return SSL Trust All.
     */
    public boolean isSslTrustAll() {
        return sslTrustAll;
    }

    /**
     * @return SSL Context Factory.
     */
    public Factory<SSLContext> getSslContextFactory() {
        return sslCtxFactory;
    }

    /**
     * @return User.
     */
    public String getUserName() {
        return userName;
    }

    /**
     * @return Password.
     */
    public String getUserPassword() {
        return userPwd;
    }

    /**
     * @return Reconnect period (for throttling).
     */
    public long getReconnectThrottlingPeriod() {
        return reconnectThrottlingPeriod;
    }

    /**
     * @return Reconnect retries within period (for throttling).
     */
    public int getReconnectThrottlingRetries() {
        return reconnectThrottlingRetries;
    }

    /**
     * @return User attributes.
     */
    public Map<String, String> getUserAttributes() {
        return userAttrs;
    }
}

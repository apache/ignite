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

import javax.cache.configuration.Factory;
import javax.net.ssl.SSLContext;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.ssl.SslContextFactory;
import org.jetbrains.annotations.Nullable;

/**
 * Client connector configuration.
 */
public class ClientConnectorConfiguration {
    /** Default port. */
    public static final int DFLT_PORT = 10800;

    /** Default port range. */
    public static final int DFLT_PORT_RANGE = 100;

    /** Default socket send and receive buffer size. */
    public static final int DFLT_SOCK_BUF_SIZE = 0;

    /** Default value for {@code TCP_NODELAY} socket option (value is <tt>true</tt>). */
    public static final boolean DFLT_TCP_NO_DELAY = true;

    /** Default max number of open cursors per connection. */
    public static final int DFLT_MAX_OPEN_CURSORS_PER_CONN = 128;

    /** Default size of thread pool. */
    public static final int DFLT_THREAD_POOL_SIZE = IgniteConfiguration.DFLT_PUBLIC_THREAD_CNT;

    /** Default handshake timeout. */
    public static final int DFLT_HANDSHAKE_TIMEOUT = 10_000;

    /** Default idle timeout. */
    public static final int DFLT_IDLE_TIMEOUT = 0;

    /** Default value of whether to use Ignite SSL context factory. */
    public static final boolean DFLT_USE_IGNITE_SSL_CTX_FACTORY = true;

    /** Host. */
    private String host;

    /** Port. */
    private int port = DFLT_PORT;

    /** Port range. */
    private int portRange = DFLT_PORT_RANGE;

    /** Socket send buffer size. */
    private int sockSndBufSize = DFLT_SOCK_BUF_SIZE;

    /** Socket receive buffer size. */
    private int sockRcvBufSize = DFLT_SOCK_BUF_SIZE;

    /** TCP no delay. */
    private boolean tcpNoDelay = DFLT_TCP_NO_DELAY;

    /** Max number of opened cursors per connection. */
    private int maxOpenCursorsPerConn = DFLT_MAX_OPEN_CURSORS_PER_CONN;

    /** Thread pool size. */
    private int threadPoolSize = DFLT_THREAD_POOL_SIZE;

    /** Idle timeout. */
    private long idleTimeout = DFLT_IDLE_TIMEOUT;

    /** Handshake timeout. */
    private long handshakeTimeout = DFLT_HANDSHAKE_TIMEOUT;

    /** JDBC connections enabled flag. */
    private boolean jdbcEnabled = true;

    /** ODBC connections enabled flag. */
    private boolean odbcEnabled = true;

    /** JDBC connections enabled flag. */
    private boolean thinCliEnabled = true;

    /** SSL enable flag, default is disabled. */
    private boolean sslEnabled;

    /** If to use SSL context factory from Ignite configuration. */
    private boolean useIgniteSslCtxFactory = DFLT_USE_IGNITE_SSL_CTX_FACTORY;

    /** SSL need client auth flag. */
    private boolean sslClientAuth;

    /** SSL connection factory. */
    private Factory<SSLContext> sslCtxFactory;

    /** Thin-client specific configuration. */
    private ThinClientConfiguration thinCliCfg = new ThinClientConfiguration();

    /**
     * Creates SQL connector configuration with all default values.
     */
    public ClientConnectorConfiguration() {
        // No-op.
    }

    /**
     * Creates SQL connector configuration by copying all properties from given configuration.
     *
     * @param cfg Configuration to copy.
     */
    public ClientConnectorConfiguration(ClientConnectorConfiguration cfg) {
        assert cfg != null;

        host = cfg.getHost();
        maxOpenCursorsPerConn = cfg.getMaxOpenCursorsPerConnection();
        port = cfg.getPort();
        portRange = cfg.getPortRange();
        sockRcvBufSize = cfg.getSocketReceiveBufferSize();
        sockSndBufSize = cfg.getSocketSendBufferSize();
        tcpNoDelay = cfg.isTcpNoDelay();
        threadPoolSize = cfg.getThreadPoolSize();
        idleTimeout = cfg.getIdleTimeout();
        handshakeTimeout = cfg.getHandshakeTimeout();
        jdbcEnabled = cfg.jdbcEnabled;
        odbcEnabled = cfg.odbcEnabled;
        thinCliEnabled = cfg.thinCliEnabled;
        sslEnabled = cfg.isSslEnabled();
        sslClientAuth = cfg.isSslClientAuth();
        useIgniteSslCtxFactory = cfg.isUseIgniteSslContextFactory();
        sslCtxFactory = cfg.getSslContextFactory();
        thinCliCfg = new ThinClientConfiguration(cfg.getThinClientConfiguration());
    }

    /**
     * Get host.
     *
     * @return Host.
     */
    @Nullable public String getHost() {
        return host;
    }

    /**
     * Set host.
     *
     * @param host Host.
     * @return This instance for chaining.
     */
    public ClientConnectorConfiguration setHost(@Nullable String host) {
        this.host = host;

        return this;
    }

    /**
     * Get port.
     *
     * @return Port.
     */
    public int getPort() {
        return port;
    }

    /**
     * Set port.
     *
     * @param port Port.
     * @return This instance for chaining.
     */
    public ClientConnectorConfiguration setPort(int port) {
        this.port = port;

        return this;
    }

    /**
     * Get port range.
     *
     * @return Port range.
     */
    public int getPortRange() {
        return portRange;
    }

    /**
     * Set port range.
     *
     * @param portRange Port range.
     * @return This instance for chaining.
     */
    public ClientConnectorConfiguration setPortRange(int portRange) {
        this.portRange = portRange;

        return this;
    }

    /**
     * Gets socket send buffer size. When set to zero, operation system default will be used.
     * <p>
     * Defaults to {@link #DFLT_SOCK_BUF_SIZE}
     *
     * @return Socket send buffer size in bytes.
     */
    public int getSocketSendBufferSize() {
        return sockSndBufSize;
    }

    /**
     * Sets socket send buffer size. See {@link #getSocketSendBufferSize()} for more information.
     *
     * @param sockSndBufSize Socket send buffer size in bytes.
     * @return This instance for chaining.
     */
    public ClientConnectorConfiguration setSocketSendBufferSize(int sockSndBufSize) {
        this.sockSndBufSize = sockSndBufSize;

        return this;
    }

    /**
     * Gets socket receive buffer size. When set to zero, operation system default will be used.
     * <p>
     * Defaults to {@link #DFLT_SOCK_BUF_SIZE}.
     *
     * @return Socket receive buffer size in bytes.
     */
    public int getSocketReceiveBufferSize() {
        return sockRcvBufSize;
    }

    /**
     * Sets socket receive buffer size. See {@link #getSocketReceiveBufferSize()} for more information.
     *
     * @param sockRcvBufSize Socket receive buffer size in bytes.
     * @return This instance for chaining.
     */
    public ClientConnectorConfiguration setSocketReceiveBufferSize(int sockRcvBufSize) {
        this.sockRcvBufSize = sockRcvBufSize;

        return this;
    }

    /**
     * Get TCP NO_DELAY flag.
     *
     * @return TCP NO_DELAY flag.
     */
    public boolean isTcpNoDelay() {
        return tcpNoDelay;
    }

    /**
     * Set TCP NO_DELAY flag.
     *
     * @param tcpNoDelay TCP NO_DELAY flag.
     * @return This instance for chaining.
     */
    public ClientConnectorConfiguration setTcpNoDelay(boolean tcpNoDelay) {
        this.tcpNoDelay = tcpNoDelay;

        return this;
    }

    /**
     * Gets maximum number of opened cursors per connection.
     * <p>
     * Defaults to {@link #DFLT_MAX_OPEN_CURSORS_PER_CONN}.
     *
     * @return Maximum number of opened cursors.
     */
    public int getMaxOpenCursorsPerConnection() {
        return maxOpenCursorsPerConn;
    }

    /**
     * Sets maximum number of opened cursors per connection.
     *
     * @param maxOpenCursorsPerConn Maximum number of opened cursors.
     * @return This instance for chaining.
     */
    public ClientConnectorConfiguration setMaxOpenCursorsPerConnection(int maxOpenCursorsPerConn) {
        this.maxOpenCursorsPerConn = maxOpenCursorsPerConn;

        return this;
    }

    /**
     * Size of thread pool that is in charge of processing SQL requests.
     * <p>
     * Defaults {@link #DFLT_THREAD_POOL_SIZE}.
     *
     * @return Thread pool that is in charge of processing SQL requests.
     */
    public int getThreadPoolSize() {
        return threadPoolSize;
    }

    /**
     * Sets thread pool that is in charge of processing SQL requests. See {@link #getThreadPoolSize()} for more
     * information.
     *
     * @param threadPoolSize Thread pool that is in charge of processing SQL requests.
     * @return This instance for chaining.
     */
    public ClientConnectorConfiguration setThreadPoolSize(int threadPoolSize) {
        this.threadPoolSize = threadPoolSize;

        return this;
    }

    /**
     * Gets idle timeout for client connections.
     * If no packets come within idle timeout, the connection is closed.
     * Zero or negative means no timeout.
     *
     * @return Idle timeout in milliseconds.
     */
    public long getIdleTimeout() {
        return idleTimeout;
    }

    /**
     * Sets idle timeout for client connections.
     * If no packets come within idle timeout, the connection is closed.
     * Zero or negative means no timeout.
     *
     * @param idleTimeout Idle timeout in milliseconds.
     * @see #getIdleTimeout()
     * @return {@code this} for chaining.
     */
    public ClientConnectorConfiguration setIdleTimeout(long idleTimeout) {
        this.idleTimeout = idleTimeout;

        return this;
    }

    /**
     * Gets handshake timeout for client connections.
     * If no successful handshake is performed within this timeout upon successful establishment of TCP connection,
     * the connection is closed.
     * Zero or negative means no timeout.
     *
     * @return Handshake timeout in milliseconds.
     */
    public long getHandshakeTimeout() {
        return handshakeTimeout;
    }

    /**
     * Sets handshake timeout for client connections.
     * If no successful handshake is performed within this timeout upon successful establishment of TCP connection,
     * the connection is closed.
     * Zero or negative means no timeout.
     *
     * @param handshakeTimeout Idle timeout in milliseconds.
     * @see #getHandshakeTimeout()
     * @return {@code this} for chaining.
     */
    public ClientConnectorConfiguration setHandshakeTimeout(long handshakeTimeout) {
        this.handshakeTimeout = handshakeTimeout;

        return this;
    }

    /**
     * Gets whether access through JDBC is enabled.
     * <p>
     * Defaults to {@code true}.
     *
     * @return Whether access through JDBC is enabled.
     */
    public boolean isJdbcEnabled() {
        return jdbcEnabled;
    }

    /**
     * Sets whether access through JDBC is enabled.
     * <p>
     * Defaults to {@code true}.
     *
     * @param jdbcEnabled Whether access through JDBC is enabled.
     * @return {@code this} for chaining.
     */
    public ClientConnectorConfiguration setJdbcEnabled(boolean jdbcEnabled) {
        this.jdbcEnabled = jdbcEnabled;

        return this;
    }

    /**
     * Gets whether access through ODBC is enabled.
     * <p>
     * Defaults to {@code true}.
     *
     * @return Whether access through ODBC is enabled.
     */
    public boolean isOdbcEnabled() {
        return odbcEnabled;
    }

    /**
     * Sets whether access through ODBC is enabled.
     * <p>
     * Defaults to {@code true}.
     *
     * @param odbcEnabled Whether access through ODBC is enabled.
     * @return {@code this} for chaining.
     */
    public ClientConnectorConfiguration setOdbcEnabled(boolean odbcEnabled) {
        this.odbcEnabled = odbcEnabled;

        return this;
    }

    /**
     * Gets whether access through thin client is enabled.
     * <p>
     * Defaults to {@code true}.
     *
     * @return Whether access through thin client is enabled.
     */
    public boolean isThinClientEnabled() {
        return thinCliEnabled;
    }

    /**
     * Sets whether access through thin client is enabled.
     * <p>
     * Defaults to {@code true}.
     *
     * @param thinCliEnabled Whether access through thin client is enabled.
     * @return {@code this} for chaining.
     */
    public ClientConnectorConfiguration setThinClientEnabled(boolean thinCliEnabled) {
        this.thinCliEnabled = thinCliEnabled;

        return this;
    }

    /**
     * Whether secure socket layer should be enabled on client connector.
     * <p>
     * Note that if this flag is set to {@code true}, an instance of {@code Factory&lt;SSLContext&gt;}
     * should be provided, otherwise client connector will fail to start.
     *
     * @return {@code True} if SSL should be enabled.
     */
    public boolean isSslEnabled() {
        return sslEnabled;
    }

    /**
     * Sets whether Secure Socket Layer should be enabled for client connector.
     * <p/>
     * Note that if this flag is set to {@code true}, then a valid instance of {@code Factory&lt;SSLContext&gt;}
     * should be provided in {@link IgniteConfiguration}. Otherwise, TCP binary protocol will fail to start.
     *
     * @param sslEnabled {@code True} if SSL should be enabled.
     * @return {@code this} for chaining.
     */
    public ClientConnectorConfiguration setSslEnabled(boolean sslEnabled) {
        this.sslEnabled = sslEnabled;

        return this;
    }

    /**
     * Gets whether to use Ignite SSL context factory configured through
     * {@link IgniteConfiguration#getSslContextFactory()} if {@link #getSslContextFactory()} is not set.
     *
     * @return {@code True} if Ignite SSL context factory can be used.
     */
    public boolean isUseIgniteSslContextFactory() {
        return useIgniteSslCtxFactory;
    }

    /**
     * Sets whether to use Ignite SSL context factory. See {@link #isUseIgniteSslContextFactory()} for more information.
     *
     * @param useIgniteSslCtxFactory Whether to use Ignite SSL context factory
     * @return {@code this} for chaining.
     */
    public ClientConnectorConfiguration setUseIgniteSslContextFactory(boolean useIgniteSslCtxFactory) {
        this.useIgniteSslCtxFactory = useIgniteSslCtxFactory;

        return this;
    }

    /**
     * Gets a flag indicating whether or not remote clients will be required to have a valid SSL certificate which
     * validity will be verified with trust manager.
     *
     * @return Whether or not client authentication is required.
     */
    public boolean isSslClientAuth() {
        return sslClientAuth;
    }

    /**
     * Sets flag indicating whether or not SSL client authentication is required.
     *
     * @param sslClientAuth Whether or not client authentication is required.
     * @return {@code this} for chaining.
     */
    public ClientConnectorConfiguration setSslClientAuth(boolean sslClientAuth) {
        this.sslClientAuth = sslClientAuth;

        return this;
    }

    /**
     * Sets SSL context factory that will be used for creating a secure socket layer.
     *
     * @param sslCtxFactory Ssl context factory.
     * @see SslContextFactory
     * @return {@code this} for chaining.
     */
    public ClientConnectorConfiguration setSslContextFactory(Factory<SSLContext> sslCtxFactory) {
        this.sslCtxFactory = sslCtxFactory;

        return this;
    }

    /**
     * Returns SSL context factory that will be used for creating a secure socket layer.
     *
     * @return SSL connection factory.
     * @see SslContextFactory
     */
    public Factory<SSLContext> getSslContextFactory() {
        return sslCtxFactory;
    }

    /**
     * Gets thin-client specific configuration.
     */
    public ThinClientConfiguration getThinClientConfiguration() {
        return thinCliCfg;
    }

    /**
     * Sets thin-client specific configuration.
     *
     * @return {@code this} for chaining.
     */
    public ClientConnectorConfiguration setThinClientConfiguration(ThinClientConfiguration thinCliCfg) {
        this.thinCliCfg = thinCliCfg;

        return this;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ClientConnectorConfiguration.class, this);
    }
}

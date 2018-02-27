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

import java.net.Socket;
import javax.cache.configuration.Factory;
import javax.net.ssl.SSLContext;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.client.ssl.GridSslContextFactory;
import org.apache.ignite.ssl.SslContextFactory;
import org.jetbrains.annotations.Nullable;

/**
 * REST access configuration.
 */
public class ConnectorConfiguration {
    /** Default TCP server port. */
    public static final int DFLT_TCP_PORT = 11211;

    /** Default TCP_NODELAY flag. */
    public static final boolean DFLT_TCP_NODELAY = true;

    /** Default TCP direct buffer flag. */
    public static final boolean DFLT_TCP_DIRECT_BUF = false;

    /** Default REST idle timeout. */
    public static final int DFLT_IDLE_TIMEOUT = 7000;

    /** Default rest port range. */
    public static final int DFLT_PORT_RANGE = 100;

    /** Default size of REST thread pool. */
    public static final int DFLT_REST_CORE_THREAD_CNT = IgniteConfiguration.DFLT_PUBLIC_THREAD_CNT;

    /** Default max size of REST thread pool. */
    public static final int DFLT_REST_MAX_THREAD_CNT = IgniteConfiguration.DFLT_PUBLIC_THREAD_CNT;

    /** Default keep alive time for REST thread pool. */
    public static final long DFLT_KEEP_ALIVE_TIME = 0;

    /** Default max queue capacity of REST thread pool. */
    public static final int DFLT_THREADPOOL_QUEUE_CAP = Integer.MAX_VALUE;

    /** Default socket send and receive buffer size. */
    public static final int DFLT_SOCK_BUF_SIZE = 32 * 1024;

    /** Default REST idle timeout for query cursor. */
    private static final long DFLT_IDLE_QRY_CUR_TIMEOUT = 10 * 60 * 1000;

    /** Default REST check frequency for idle query cursor. */
    private static final long DFLT_IDLE_QRY_CUR_CHECK_FRQ = 60 * 1000;

    /** Jetty XML configuration path. */
    private String jettyPath;

    /** REST secret key. */
    private String secretKey;

    /** TCP host. */
    private String host;

    /** TCP port. */
    private int port = DFLT_TCP_PORT;

    /** TCP no delay flag. */
    private boolean noDelay = DFLT_TCP_NODELAY;

    /** REST TCP direct buffer flag. */
    private boolean directBuf = DFLT_TCP_DIRECT_BUF;

    /** REST TCP send buffer size. */
    private int sndBufSize = DFLT_SOCK_BUF_SIZE;

    /** REST TCP receive buffer size. */
    private int rcvBufSize = DFLT_SOCK_BUF_SIZE;

    /** REST idle timeout for query cursor. */
    private long idleQryCurTimeout = DFLT_IDLE_QRY_CUR_TIMEOUT;

    /** REST idle check frequency for query cursor. */
    private long idleQryCurCheckFreq = DFLT_IDLE_QRY_CUR_CHECK_FRQ;

    /** REST TCP send queue limit. */
    private int sndQueueLimit;

    /** REST TCP selector count. */
    private int selectorCnt = Math.min(4, Runtime.getRuntime().availableProcessors());

    /** Idle timeout. */
    private long idleTimeout = DFLT_IDLE_TIMEOUT;

    /** SSL enable flag, default is disabled. */
    private boolean sslEnabled;

    /** SSL need client auth flag. */
    private boolean sslClientAuth;

    /** SSL context factory for rest binary server. */
    private GridSslContextFactory sslCtxFactory;

    /** SSL context factory for rest binary server. */
    private Factory<SSLContext> sslFactory;

    /** Port range */
    private int portRange = DFLT_PORT_RANGE;

    /** REST requests thread pool size. */
    private int threadPoolSize = DFLT_REST_CORE_THREAD_CNT;

    /** Client message interceptor. */
    private ConnectorMessageInterceptor msgInterceptor;

    /**
     * Creates client connection configuration with all default values.
     */
    public ConnectorConfiguration() {
        // No-op.
    }

    /**
     * Creates client connection configuration by copying all properties from
     * given configuration.
     *
     * @param cfg Client configuration.
     */
    public ConnectorConfiguration(ConnectorConfiguration cfg) {
        assert cfg != null;

        msgInterceptor = cfg.getMessageInterceptor();
        threadPoolSize = cfg.getThreadPoolSize();
        idleTimeout = cfg.getIdleTimeout();
        jettyPath = cfg.getJettyPath();
        portRange = cfg.getPortRange();
        secretKey = cfg.getSecretKey();
        directBuf = cfg.isDirectBuffer();
        host = cfg.getHost();
        noDelay = cfg.isNoDelay();
        port = cfg.getPort();
        rcvBufSize = cfg.getReceiveBufferSize();
        selectorCnt = cfg.getSelectorCount();
        sndBufSize = cfg.getSendBufferSize();
        sndQueueLimit = cfg.getSendQueueLimit();
        sslClientAuth = cfg.isSslClientAuth();
        sslCtxFactory = cfg.getSslContextFactory();
        sslEnabled = cfg.isSslEnabled();
        sslFactory = cfg.getSslFactory();
        idleQryCurTimeout = cfg.getIdleQueryCursorTimeout();
        idleQryCurCheckFreq = cfg.getIdleQueryCursorCheckFrequency();
    }

    /**
     * Sets path, either absolute or relative to {@code IGNITE_HOME}, to {@code JETTY}
     * XML configuration file. {@code JETTY} is used to support REST over HTTP protocol for
     * accessing Ignite APIs remotely.
     *
     * @param jettyPath Path to {@code JETTY} XML configuration file.
     */
    public void setJettyPath(String jettyPath) {
        this.jettyPath = jettyPath;
    }

    /**
     * Gets path, either absolute or relative to {@code IGNITE_HOME}, to {@code Jetty}
     * XML configuration file. {@code Jetty} is used to support REST over HTTP protocol for
     * accessing Ignite APIs remotely.
     * <p>
     * If not provided, Jetty instance with default configuration will be started picking
     * {@link IgniteSystemProperties#IGNITE_JETTY_HOST} and {@link IgniteSystemProperties#IGNITE_JETTY_PORT}
     * as host and port respectively.
     *
     * @return Path to {@code JETTY} XML configuration file.
     * @see IgniteSystemProperties#IGNITE_JETTY_HOST
     * @see IgniteSystemProperties#IGNITE_JETTY_PORT
     */
    public String getJettyPath() {
        return jettyPath;
    }

    /**
     * Sets secret key to authenticate REST requests. If key is {@code null} or empty authentication is disabled.
     *
     * @param secretKey REST secret key.
     */
    public void setSecretKey(@Nullable String secretKey) {
        this.secretKey = secretKey;
    }

    /**
     * Gets secret key to authenticate REST requests. If key is {@code null} or empty authentication is disabled.
     *
     * @return Secret key.
     * @see IgniteSystemProperties#IGNITE_JETTY_HOST
     * @see IgniteSystemProperties#IGNITE_JETTY_PORT
     */
    @Nullable public String getSecretKey() {
        return secretKey;
    }

    /**
     * Gets host for TCP binary protocol server. This can be either an
     * IP address or a domain name.
     * <p>
     * If not defined, system-wide local address will be used
     * (see {@link IgniteConfiguration#getLocalHost()}.
     * <p>
     * You can also use {@code 0.0.0.0} value to bind to all
     * locally-available IP addresses.
     *
     * @return TCP host.
     */
    public String getHost() {
        return host;
    }

    /**
     * Sets host for TCP binary protocol server.
     *
     * @param host TCP host.
     */
    public void setHost(String host) {
        this.host = host;
    }

    /**
     * Gets port for TCP binary protocol server.
     * <p>
     * Default is {@link #DFLT_TCP_PORT}.
     *
     * @return TCP port.
     */
    public int getPort() {
        return port;
    }

    /**
     * Sets port for TCP binary protocol server.
     *
     * @param port TCP port.
     */
    public void setPort(int port) {
        this.port = port;
    }

    /**
     * Gets flag indicating whether {@code TCP_NODELAY} option should be set for accepted client connections.
     * Setting this option reduces network latency and should be set to {@code true} in majority of cases.
     * For more information, see {@link Socket#setTcpNoDelay(boolean)}
     * <p/>
     * If not specified, default value is {@link #DFLT_TCP_NODELAY}.
     *
     * @return Whether {@code TCP_NODELAY} option should be enabled.
     */
    public boolean isNoDelay() {
        return noDelay;
    }

    /**
     * Sets whether {@code TCP_NODELAY} option should be set for all accepted client connections.
     *
     * @param noDelay {@code True} if option should be enabled.
     * @see #isNoDelay()
     */
    public void setNoDelay(boolean noDelay) {
        this.noDelay = noDelay;
    }

    /**
     * Gets flag indicating whether REST TCP server should use direct buffers. A direct buffer is a buffer
     * that is allocated and accessed using native system calls, without using JVM heap. Enabling direct
     * buffer <em>may</em> improve performance and avoid memory issues (long GC pauses due to huge buffer
     * size).
     *
     * @return Whether direct buffer should be used.
     */
    public boolean isDirectBuffer() {
        return directBuf;
    }

    /**
     * Sets whether to use direct buffer for REST TCP server.
     *
     * @param directBuf {@code True} if option should be enabled.
     * @see #isDirectBuffer()
     */
    public void setDirectBuffer(boolean directBuf) {
        this.directBuf = directBuf;
    }

    /**
     * Gets REST TCP server send buffer size.
     *
     * @return REST TCP server send buffer size (0 for default).
     */
    public int getSendBufferSize() {
        return sndBufSize;
    }

    /**
     * Sets REST TCP server send buffer size.
     *
     * @param sndBufSize Send buffer size.
     * @see #getSendBufferSize()
     */
    public void setSendBufferSize(int sndBufSize) {
        this.sndBufSize = sndBufSize;
    }

    /**
     * Gets REST TCP server receive buffer size.
     *
     * @return REST TCP server receive buffer size (0 for default).
     */
    public int getReceiveBufferSize() {
        return rcvBufSize;
    }

    /**
     * Sets REST TCP server receive buffer size.
     *
     * @param rcvBufSize Receive buffer size.
     * @see #getReceiveBufferSize()
     */
    public void setReceiveBufferSize(int rcvBufSize) {
        this.rcvBufSize = rcvBufSize;
    }

    /**
     * Gets REST TCP server send queue limit. If the limit exceeds, all successive writes will
     * block until the queue has enough capacity.
     *
     * @return REST TCP server send queue limit (0 for unlimited).
     */
    public int getSendQueueLimit() {
        return sndQueueLimit;
    }

    /**
     * Sets REST TCP server send queue limit.
     *
     * @param sndQueueLimit REST TCP server send queue limit (0 for unlimited).
     * @see #getSendQueueLimit()
     */
    public void setSendQueueLimit(int sndQueueLimit) {
        this.sndQueueLimit = sndQueueLimit;
    }

    /**
     * Gets number of selector threads in REST TCP server. Higher value for this parameter
     * may increase throughput, but also increases context switching.
     *
     * @return Number of selector threads for REST TCP server.
     */
    public int getSelectorCount() {
        return selectorCnt;
    }

    /**
     * Sets number of selector threads for REST TCP server.
     *
     * @param selectorCnt Number of selector threads for REST TCP server.
     * @see #getSelectorCount()
     */
    public void setSelectorCount(int selectorCnt) {
        this.selectorCnt = selectorCnt;
    }

    /**
     * Gets idle timeout for REST server.
     * <p>
     * This setting is used to reject half-opened sockets. If no packets
     * come within idle timeout, the connection is closed.
     *
     * @return Idle timeout in milliseconds.
     */
    public long getIdleTimeout() {
        return idleTimeout;
    }

    /**
     * Sets idle timeout for REST server.
     *
     * @param idleTimeout Idle timeout in milliseconds.
     * @see #getIdleTimeout()
     */
    public void setIdleTimeout(long idleTimeout) {
        this.idleTimeout = idleTimeout;
    }

    /**
     * Whether secure socket layer should be enabled on binary rest server.
     * <p>
     * Note that if this flag is set to {@code true}, an instance of {@link GridSslContextFactory}
     * should be provided, otherwise binary rest protocol will fail to start.
     *
     * @return {@code True} if SSL should be enabled.
     */
    public boolean isSslEnabled() {
        return sslEnabled;
    }

    /**
     * Sets whether Secure Socket Layer should be enabled for REST TCP binary protocol.
     * <p/>
     * Note that if this flag is set to {@code true}, then a valid instance of {@link GridSslContextFactory}
     * should be provided in {@link IgniteConfiguration}. Otherwise, TCP binary protocol will fail to start.
     *
     * @param sslEnabled {@code True} if SSL should be enabled.
     */
    public void setSslEnabled(boolean sslEnabled) {
        this.sslEnabled = sslEnabled;
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
     */
    public void setSslClientAuth(boolean sslClientAuth) {
        this.sslClientAuth = sslClientAuth;
    }

    /**
     * Gets context factory that will be used for creating a secure socket layer of rest binary server.
     *
     * @return SslContextFactory instance.
     * @see GridSslContextFactory
     * @deprecated Use {@link #getSslFactory()} instead.
     */
    @Deprecated
    public GridSslContextFactory getSslContextFactory() {
        return sslCtxFactory;
    }

    /**
     * Sets instance of {@link GridSslContextFactory} that will be used to create an instance of {@code SSLContext}
     * for Secure Socket Layer on TCP binary protocol. This factory will only be used if
     * {@link #setSslEnabled(boolean)} is set to {@code true}.
     *
     * @param sslCtxFactory Instance of {@link GridSslContextFactory}
     * @deprecated Use {@link #setSslFactory(Factory)} instead.
     */
    @Deprecated
    public void setSslContextFactory(GridSslContextFactory sslCtxFactory) {
        this.sslCtxFactory = sslCtxFactory;
    }

    /**
     * Gets context factory that will be used for creating a secure socket layer of rest binary server.
     *
     * @return SSL context factory instance.
     * @see SslContextFactory
     */
    public Factory<SSLContext> getSslFactory() {
        return sslFactory;
    }

    /**
     * Sets instance of {@link Factory} that will be used to create an instance of {@code SSLContext}
     * for Secure Socket Layer on TCP binary protocol. This factory will only be used if
     * {@link #setSslEnabled(boolean)} is set to {@code true}.
     *
     * @param sslFactory Instance of {@link Factory}
     */
    public void setSslFactory(Factory<SSLContext> sslFactory) {
        this.sslFactory = sslFactory;
    }

    /**
     * Gets number of ports to try if configured port is already in use.
     *
     * If port range value is <tt>0</tt>, then implementation will try bind only to the port provided by
     * {@link #setPort(int)} method and fail if binding to this port did not succeed.
     *
     * @return Number of ports to try.
     */
    public int getPortRange() {
        return portRange;
    }

    /**
     * Sets number of ports to try if configured one is in use.
     *
     * @param portRange Port range.
     */
    public void setPortRange(int portRange) {
        this.portRange = portRange;
    }

    /**
     * Should return a thread pool size to be used for
     * processing of client messages (REST requests).
     *
     * @return Thread pool size to be used for processing of client
     *      messages.
     */
    public int getThreadPoolSize() {
        return threadPoolSize;
    }

    /**
     * Sets thread pool size to use for processing of client messages (REST requests).
     *
     * @param threadPoolSize Thread pool size to use for processing of client messages.
     * @see #getThreadPoolSize()
     */
    public void setThreadPoolSize(int threadPoolSize) {
        this.threadPoolSize = threadPoolSize;
    }

    /**
     * Gets interceptor for objects, moving to and from remote clients.
     * If this method returns {@code null} then no interception will be applied.
     * <p>
     * Setting interceptor allows to transform all objects exchanged via REST protocol.
     * For example if you use custom serialisation on client you can write interceptor
     * to transform binary representations received from client to Java objects and later
     * access them from java code directly.
     * <p>
     * Default value is {@code null}.
     *
     * @see ConnectorMessageInterceptor
     * @return Interceptor.
     */
    @Nullable public ConnectorMessageInterceptor getMessageInterceptor() {
        return msgInterceptor;
    }

    /**
     * Sets client message interceptor.
     * <p>
     * Setting interceptor allows to transform all objects exchanged via REST protocol.
     * For example if you use custom serialisation on client you can write interceptor
     * to transform binary representations received from client to Java objects and later
     * access them from java code directly.
     *
     * @param interceptor Interceptor.
     */
    public void setMessageInterceptor(ConnectorMessageInterceptor interceptor) {
        msgInterceptor = interceptor;
    }

    /**
     * Sets idle query cursors timeout.
     *
     * @param idleQryCurTimeout Idle query cursors timeout in milliseconds.
     * @see #getIdleQueryCursorTimeout()
     */
    public void setIdleQueryCursorTimeout(long idleQryCurTimeout) {
        this.idleQryCurTimeout = idleQryCurTimeout;
    }

    /**
     * Gets idle query cursors timeout in milliseconds.
     * <p>
     * This setting is used to reject open query cursors that is not used. If no fetch query request
     * come within idle timeout, it will be removed on next check for old query cursors
     * (see {@link #getIdleQueryCursorCheckFrequency()}).
     *
     * @return Idle query cursors timeout in milliseconds
     */
    public long getIdleQueryCursorTimeout() {
        return idleQryCurTimeout;
    }

    /**
     * Sets idle query cursor check frequency.
     *
     * @param idleQryCurCheckFreq Idle query check frequency in milliseconds.
     * @see #getIdleQueryCursorCheckFrequency()
     */
    public void setIdleQueryCursorCheckFrequency(long idleQryCurCheckFreq) {
        this.idleQryCurCheckFreq = idleQryCurCheckFreq;
    }

    /**
     * Gets idle query cursors check frequency.
     * This setting is used to reject open query cursors that is not used.
     * <p>
     * Scheduler tries with specified period to close queries' cursors that are overtime.
     *
     * @return Idle query cursor check frequency in milliseconds.
     */
    public long getIdleQueryCursorCheckFrequency() {
        return idleQryCurCheckFreq;
    }
}

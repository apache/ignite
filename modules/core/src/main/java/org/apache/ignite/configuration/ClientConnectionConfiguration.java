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

import org.apache.ignite.*;
import org.apache.ignite.internal.client.ssl.*;
import org.jetbrains.annotations.*;

import java.net.*;
import java.util.concurrent.*;

/**
 * REST access configuration.
 */
public class ClientConnectionConfiguration {
    /** Default TCP server port. */
    public static final int DFLT_TCP_PORT = 11211;

    /** Default TCP_NODELAY flag. */
    public static final boolean DFLT_TCP_NODELAY = true;

    /** Default TCP direct buffer flag. */
    public static final boolean DFLT_REST_TCP_DIRECT_BUF = false;

    /** Default REST idle timeout. */
    public static final int DFLT_REST_IDLE_TIMEOUT = 7000;

    /** Default rest port range. */
    public static final int DFLT_REST_PORT_RANGE = 100;

    /** Default size of REST thread pool. */
    public static final int DFLT_REST_CORE_THREAD_CNT = IgniteConfiguration.DFLT_PUBLIC_CORE_THREAD_CNT;

    /** Default max size of REST thread pool. */
    public static final int DFLT_REST_MAX_THREAD_CNT = IgniteConfiguration.DFLT_PUBLIC_CORE_THREAD_CNT;

    /** Default max queue capacity of REST thread pool. */
    public static final int DFLT_REST_THREADPOOL_QUEUE_CAP = Integer.MAX_VALUE;

    /** Default socket send and receive buffer size. */
    public static final int DFLT_SOCK_BUF_SIZE = 32 * 1024;

    /** Jetty XML configuration path. */
    private String jettyPath;

    /** REST secret key. */
    private String restSecretKey;

    /** TCP host. */
    private String restTcpHost;

    /** TCP port. */
    private int restTcpPort = DFLT_TCP_PORT;

    /** TCP no delay flag. */
    private boolean restTcpNoDelay = DFLT_TCP_NODELAY;

    /** REST TCP direct buffer flag. */
    private boolean restTcpDirectBuf = DFLT_REST_TCP_DIRECT_BUF;

    /** REST TCP send buffer size. */
    private int restTcpSndBufSize = DFLT_SOCK_BUF_SIZE;

    /** REST TCP receive buffer size. */
    private int restTcpRcvBufSize = DFLT_SOCK_BUF_SIZE;

    /** REST TCP send queue limit. */
    private int restTcpSndQueueLimit;

    /** REST TCP selector count. */
    private int restTcpSelectorCnt = Math.min(4, Runtime.getRuntime().availableProcessors());

    /** Idle timeout. */
    private long restIdleTimeout = DFLT_REST_IDLE_TIMEOUT;

    /** SSL enable flag, default is disabled. */
    private boolean restTcpSslEnabled;

    /** SSL need client auth flag. */
    private boolean restTcpSslClientAuth;

    /** SSL context factory for rest binary server. */
    private GridSslContextFactory restTcpSslCtxFactory;

    /** Port range */
    private int restPortRange = DFLT_REST_PORT_RANGE;

    /** REST requests executor service. */
    private ExecutorService restExecSvc;

    /** REST executor service shutdown flag. */
    private boolean restSvcShutdown = true;

    /** Client message interceptor. */
    private ClientMessageInterceptor clientMsgInterceptor;

    /**
     * Creates client connection configuration with all default values.
     */
    public ClientConnectionConfiguration() {
        // No-op.
    }

    /**
     * Creates client connection configuration by copying all properties from
     * given configuration.
     *
     * @param cfg Client configuration.
     */
    public ClientConnectionConfiguration(ClientConnectionConfiguration cfg) {
        assert cfg != null;

        clientMsgInterceptor = cfg.getClientMessageInterceptor();
        restExecSvc = cfg.getRestExecutorService();
        restSvcShutdown = cfg.isRestExecutorServiceShutdown();
        restIdleTimeout = cfg.getRestIdleTimeout();
        jettyPath = cfg.getRestJettyPath();
        restPortRange = cfg.getRestPortRange();
        restSecretKey = cfg.getRestSecretKey();
        restTcpDirectBuf = cfg.isRestTcpDirectBuffer();
        restTcpHost = cfg.getRestTcpHost();
        restTcpNoDelay = cfg.isRestTcpNoDelay();
        restTcpPort = cfg.getRestTcpPort();
        restTcpRcvBufSize = cfg.getRestTcpReceiveBufferSize();
        restTcpSelectorCnt = cfg.getRestTcpSelectorCount();
        restTcpSndBufSize = cfg.getRestTcpSendBufferSize();
        restTcpSndQueueLimit = cfg.getRestTcpSendQueueLimit();
        restTcpSslClientAuth = cfg.isRestTcpSslClientAuth();
        restTcpSslCtxFactory = cfg.getRestTcpSslContextFactory();
        restTcpSslEnabled = cfg.isRestTcpSslEnabled();
    }

    /**
     * Sets path, either absolute or relative to {@code IGNITE_HOME}, to {@code JETTY}
     * XML configuration file. {@code JETTY} is used to support REST over HTTP protocol for
     * accessing GridGain APIs remotely.
     *
     * @param jettyPath Path to {@code JETTY} XML configuration file.
     */
    public void setRestJettyPath(String jettyPath) {
        this.jettyPath = jettyPath;
    }

    /**
     * Gets path, either absolute or relative to {@code IGNITE_HOME}, to {@code Jetty}
     * XML configuration file. {@code Jetty} is used to support REST over HTTP protocol for
     * accessing GridGain APIs remotely.
     * <p>
     * If not provided, Jetty instance with default configuration will be started picking
     * {@link IgniteSystemProperties#IGNITE_JETTY_HOST} and {@link IgniteSystemProperties#IGNITE_JETTY_PORT}
     * as host and port respectively.
     *
     * @return Path to {@code JETTY} XML configuration file.
     * @see IgniteSystemProperties#IGNITE_JETTY_HOST
     * @see IgniteSystemProperties#IGNITE_JETTY_PORT
     */
    public String getRestJettyPath() {
        return jettyPath;
    }

    /**
     * Sets secret key to authenticate REST requests. If key is {@code null} or empty authentication is disabled.
     *
     * @param restSecretKey REST secret key.
     */
    public void setRestSecretKey(@Nullable String restSecretKey) {
        this.restSecretKey = restSecretKey;
    }

    /**
     * Gets secret key to authenticate REST requests. If key is {@code null} or empty authentication is disabled.
     *
     * @return Secret key.
     * @see IgniteSystemProperties#IGNITE_JETTY_HOST
     * @see IgniteSystemProperties#IGNITE_JETTY_PORT
     */
    @Nullable public String getRestSecretKey() {
        return restSecretKey;
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
    public String getRestTcpHost() {
        return restTcpHost;
    }

    /**
     * Sets host for TCP binary protocol server.
     *
     * @param restTcpHost TCP host.
     */
    public void setRestTcpHost(String restTcpHost) {
        this.restTcpHost = restTcpHost;
    }

    /**
     * Gets port for TCP binary protocol server.
     * <p>
     * Default is {@link #DFLT_TCP_PORT}.
     *
     * @return TCP port.
     */
    public int getRestTcpPort() {
        return restTcpPort;
    }

    /**
     * Sets port for TCP binary protocol server.
     *
     * @param restTcpPort TCP port.
     */
    public void setRestTcpPort(int restTcpPort) {
        this.restTcpPort = restTcpPort;
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
    public boolean isRestTcpNoDelay() {
        return restTcpNoDelay;
    }

    /**
     * Sets whether {@code TCP_NODELAY} option should be set for all accepted client connections.
     *
     * @param restTcpNoDelay {@code True} if option should be enabled.
     * @see #isRestTcpNoDelay()
     */
    public void setRestTcpNoDelay(boolean restTcpNoDelay) {
        this.restTcpNoDelay = restTcpNoDelay;
    }

    /**
     * Gets flag indicating whether REST TCP server should use direct buffers. A direct buffer is a buffer
     * that is allocated and accessed using native system calls, without using JVM heap. Enabling direct
     * buffer <em>may</em> improve performance and avoid memory issues (long GC pauses due to huge buffer
     * size).
     *
     * @return Whether direct buffer should be used.
     */
    public boolean isRestTcpDirectBuffer() {
        return restTcpDirectBuf;
    }

    /**
     * Sets whether to use direct buffer for REST TCP server.
     *
     * @param restTcpDirectBuf {@code True} if option should be enabled.
     * @see #isRestTcpDirectBuffer()
     */
    public void setRestTcpDirectBuffer(boolean restTcpDirectBuf) {
        this.restTcpDirectBuf = restTcpDirectBuf;
    }

    /**
     * Gets REST TCP server send buffer size.
     *
     * @return REST TCP server send buffer size (0 for default).
     */
    public int getRestTcpSendBufferSize() {
        return restTcpSndBufSize;
    }

    /**
     * Sets REST TCP server send buffer size.
     *
     * @param restTcpSndBufSize Send buffer size.
     * @see #getRestTcpSendBufferSize()
     */
    public void setRestTcpSendBufferSize(int restTcpSndBufSize) {
        this.restTcpSndBufSize = restTcpSndBufSize;
    }

    /**
     * Gets REST TCP server receive buffer size.
     *
     * @return REST TCP server receive buffer size (0 for default).
     */
    public int getRestTcpReceiveBufferSize() {
        return restTcpRcvBufSize;
    }

    /**
     * Sets REST TCP server receive buffer size.
     *
     * @param restTcpRcvBufSize Receive buffer size.
     * @see #getRestTcpReceiveBufferSize()
     */
    public void setRestTcpReceiveBufferSize(int restTcpRcvBufSize) {
        this.restTcpRcvBufSize = restTcpRcvBufSize;
    }

    /**
     * Gets REST TCP server send queue limit. If the limit exceeds, all successive writes will
     * block until the queue has enough capacity.
     *
     * @return REST TCP server send queue limit (0 for unlimited).
     */
    public int getRestTcpSendQueueLimit() {
        return restTcpSndQueueLimit;
    }

    /**
     * Sets REST TCP server send queue limit.
     *
     * @param restTcpSndQueueLimit REST TCP server send queue limit (0 for unlimited).
     * @see #getRestTcpSendQueueLimit()
     */
    public void setRestTcpSendQueueLimit(int restTcpSndQueueLimit) {
        this.restTcpSndQueueLimit = restTcpSndQueueLimit;
    }

    /**
     * Gets number of selector threads in REST TCP server. Higher value for this parameter
     * may increase throughput, but also increases context switching.
     *
     * @return Number of selector threads for REST TCP server.
     */
    public int getRestTcpSelectorCount() {
        return restTcpSelectorCnt;
    }

    /**
     * Sets number of selector threads for REST TCP server.
     *
     * @param restTcpSelectorCnt Number of selector threads for REST TCP server.
     * @see #getRestTcpSelectorCount()
     */
    public void setRestTcpSelectorCount(int restTcpSelectorCnt) {
        this.restTcpSelectorCnt = restTcpSelectorCnt;
    }

    /**
     * Gets idle timeout for REST server.
     * <p>
     * This setting is used to reject half-opened sockets. If no packets
     * come within idle timeout, the connection is closed.
     *
     * @return Idle timeout in milliseconds.
     */
    public long getRestIdleTimeout() {
        return restIdleTimeout;
    }

    /**
     * Sets idle timeout for REST server.
     *
     * @param restIdleTimeout Idle timeout in milliseconds.
     * @see #getRestIdleTimeout()
     */
    public void setRestIdleTimeout(long restIdleTimeout) {
        this.restIdleTimeout = restIdleTimeout;
    }

    /**
     * Whether secure socket layer should be enabled on binary rest server.
     * <p>
     * Note that if this flag is set to {@code true}, an instance of {@link GridSslContextFactory}
     * should be provided, otherwise binary rest protocol will fail to start.
     *
     * @return {@code True} if SSL should be enabled.
     */
    public boolean isRestTcpSslEnabled() {
        return restTcpSslEnabled;
    }

    /**
     * Sets whether Secure Socket Layer should be enabled for REST TCP binary protocol.
     * <p/>
     * Note that if this flag is set to {@code true}, then a valid instance of {@link GridSslContextFactory}
     * should be provided in {@code GridConfiguration}. Otherwise, TCP binary protocol will fail to start.
     *
     * @param restTcpSslEnabled {@code True} if SSL should be enabled.
     */
    public void setRestTcpSslEnabled(boolean restTcpSslEnabled) {
        this.restTcpSslEnabled = restTcpSslEnabled;
    }

    /**
     * Gets a flag indicating whether or not remote clients will be required to have a valid SSL certificate which
     * validity will be verified with trust manager.
     *
     * @return Whether or not client authentication is required.
     */
    public boolean isRestTcpSslClientAuth() {
        return restTcpSslClientAuth;
    }

    /**
     * Sets flag indicating whether or not SSL client authentication is required.
     *
     * @param needClientAuth Whether or not client authentication is required.
     */
    public void setRestTcpSslClientAuth(boolean needClientAuth) {
        restTcpSslClientAuth = needClientAuth;
    }

    /**
     * Gets context factory that will be used for creating a secure socket layer of rest binary server.
     *
     * @return SslContextFactory instance.
     * @see GridSslContextFactory
     */
    public GridSslContextFactory getRestTcpSslContextFactory() {
        return restTcpSslCtxFactory;
    }

    /**
     * Sets instance of {@link GridSslContextFactory} that will be used to create an instance of {@code SSLContext}
     * for Secure Socket Layer on TCP binary protocol. This factory will only be used if
     * {@link #setRestTcpSslEnabled(boolean)} is set to {@code true}.
     *
     * @param restTcpSslCtxFactory Instance of {@link GridSslContextFactory}
     */
    public void setRestTcpSslContextFactory(GridSslContextFactory restTcpSslCtxFactory) {
        this.restTcpSslCtxFactory = restTcpSslCtxFactory;
    }

    /**
     * Gets number of ports to try if configured port is already in use.
     *
     * @return Number of ports to try.
     */
    public int getRestPortRange() {
        return restPortRange;
    }

    /**
     * Sets number of ports to try if configured one is in use.
     *
     * @param restPortRange Port range.
     */
    public void setRestPortRange(int restPortRange) {
        this.restPortRange = restPortRange;
    }

    /**
     * Should return an instance of fully configured thread pool to be used for
     * processing of client messages (REST requests).
     * <p>
     * If not provided, new executor service will be created using the following
     * configuration:
     * <ul>
     *     <li>Core pool size - {@link #DFLT_REST_CORE_THREAD_CNT}</li>
     *     <li>Max pool size - {@link #DFLT_REST_MAX_THREAD_CNT}</li>
     *     <li>Queue capacity - {@link #DFLT_REST_THREADPOOL_QUEUE_CAP}</li>
     * </ul>
     *
     * @return Thread pool implementation to be used for processing of client
     *      messages.
     */
    public ExecutorService getRestExecutorService() {
        return restExecSvc;
    }

    /**
     * Sets thread pool to use for processing of client messages (REST requests).
     *
     * @param restExecSvc Thread pool to use for processing of client messages.
     */
    public void setRestExecutorService(ExecutorService restExecSvc) {
        this.restExecSvc = restExecSvc;
    }

    /**
     * Sets REST executor service shutdown flag.
     *
     * @param restSvcShutdown REST executor service shutdown flag.
     */
    public void setRestExecutorServiceShutdown(boolean restSvcShutdown) {
        this.restSvcShutdown = restSvcShutdown;
    }

    /**
     * Shutdown flag for REST executor service.
     * <p>
     * If not provided, default value {@code true} will be used which will shutdown
     * executor service when GridGain stops regardless whether it was started before GridGain
     * or by GridGain.
     *
     * @return REST executor service shutdown flag.
     */
    public boolean isRestExecutorServiceShutdown() {
        return restSvcShutdown;
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
     * @see ClientMessageInterceptor
     * @return Interceptor.
     */
    @Nullable public ClientMessageInterceptor getClientMessageInterceptor() {
        return clientMsgInterceptor;
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
    public void setClientMessageInterceptor(ClientMessageInterceptor interceptor) {
        clientMsgInterceptor = interceptor;
    }
}

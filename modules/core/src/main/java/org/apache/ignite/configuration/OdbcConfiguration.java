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

/**
 * ODBC configuration.
 */
public class OdbcConfiguration {
    /** Default TCP server port. */
    public static final int DFLT_TCP_PORT = 11443;

    /** Default TCP_NODELAY flag. */
    public static final boolean DFLT_TCP_NODELAY = true;

    /** Default TCP direct buffer flag. */
    public static final boolean DFLT_TCP_DIRECT_BUF = false;

    /** Default ODBC idle timeout. */
    public static final int DFLT_IDLE_TIMEOUT = 7000;

    /** Default socket send and receive buffer size. */
    public static final int DFLT_SOCK_BUF_SIZE = 32 * 1024;

    /** Default max number of open cursors per connection. */
    public static final int DFLT_MAX_OPEN_CURSORS = 128;

    /** TCP port. */
    private int port = DFLT_TCP_PORT;

    /** TCP host. */
    private String host;

    /** TCP no delay flag. */
    private boolean noDelay = DFLT_TCP_NODELAY;

    /** ODBC TCP direct buffer flag. */
    private boolean directBuf = DFLT_TCP_DIRECT_BUF;

    /** ODBC TCP send buffer size. */
    private int sndBufSize = DFLT_SOCK_BUF_SIZE;

    /** ODBC TCP receive buffer size. */
    private int rcvBufSize = DFLT_SOCK_BUF_SIZE;

    /** ODBC TCP send queue limit. */
    private int sndQueueLimit;

    /** ODBC TCP selector count. */
    private int selectorCnt = Math.min(4, Runtime.getRuntime().availableProcessors());

    /** Idle timeout. */
    private long idleTimeout = DFLT_IDLE_TIMEOUT;

    /** Max number of opened cursors per connection. */
    private int maxOpenCursors = DFLT_MAX_OPEN_CURSORS;

    /**
     * Creates ODBC server configuration with all default values.
     */
    public OdbcConfiguration() {
        // No-op.
    }

    /**
     * Creates ODBC server configuration by copying all properties from
     * given configuration.
     *
     * @param cfg ODBC server configuration.
     */
    public OdbcConfiguration(OdbcConfiguration cfg) {
        assert cfg != null;

        directBuf = cfg.isDirectBuffer();
        host = cfg.getHost();
        idleTimeout = cfg.getIdleTimeout();
        maxOpenCursors = cfg.getMaxOpenCursors();
        noDelay = cfg.isNoDelay();
        port = cfg.getPort();
        rcvBufSize = cfg.getReceiveBufferSize();
        selectorCnt = cfg.getSelectorCount();
        sndBufSize = cfg.getSendBufferSize();
        sndQueueLimit = cfg.getSendQueueLimit();
    }

    /**
     * Gets port for TCP ODBC server.
     * <p>
     * Default is {@link #DFLT_TCP_PORT}.
     *
     * @return TCP port.
     */
    public int getPort() {
        return port;
    }

    /**
     * Sets port for TCP ODBC server.
     *
     * @param port TCP port.
     */
    public void setPort(int port) {
        this.port = port;
    }

    /**
     * Gets host for TCP ODBC server. This can be either an
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
     * Sets host for TCP ODBC server.
     *
     * @param host TCP host.
     */
    public void setHost(String host) {
        this.host = host;
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
     * Sets whether {@code TCP_NODELAY} option should be set for all accepted ODBC client connections.
     *
     * @param noDelay {@code True} if option should be enabled.
     * @see #isNoDelay()
     */
    public void setNoDelay(boolean noDelay) {
        this.noDelay = noDelay;
    }

    /**
     * Gets flag indicating whether ODBC TCP server should use direct buffers. A direct buffer is a buffer
     * that is allocated and accessed using native system calls, without using JVM heap. Enabling direct
     * buffer <em>may</em> improve performance and avoid memory issues (long GC pauses due to huge buffer
     * size).
     * <p/>
     * If not specified, default value is {@link #DFLT_TCP_DIRECT_BUF}.
     *
     * @return Whether direct buffer should be used.
     */
    public boolean isDirectBuffer() {
        return directBuf;
    }

    /**
     * Sets whether to use direct buffer for ODBC TCP server.
     *
     * @param directBuf {@code True} if option should be enabled.
     * @see #isDirectBuffer()
     */
    public void setDirectBuffer(boolean directBuf) {
        this.directBuf = directBuf;
    }

    /**
     * Gets ODBC TCP server send buffer size.
     * <p/>
     * If not specified, default value is {@link #DFLT_SOCK_BUF_SIZE}.
     *
     * @return ODBC TCP server send buffer size (0 for default).
     */
    public int getSendBufferSize() {
        return sndBufSize;
    }

    /**
     * Sets ODBC TCP server send buffer size.
     *
     * @param sndBufSize Send buffer size.
     * @see #getSendBufferSize()
     */
    public void setSendBufferSize(int sndBufSize) {
        this.sndBufSize = sndBufSize;
    }

    /**
     * Gets ODBC TCP server receive buffer size.
     * <p/>
     * If not specified, default value is {@link #DFLT_SOCK_BUF_SIZE}.
     *
     * @return ODBC TCP server receive buffer size (0 for default).
     */
    public int getReceiveBufferSize() {
        return rcvBufSize;
    }

    /**
     * Sets ODBC TCP server receive buffer size.
     *
     * @param rcvBufSize Receive buffer size.
     * @see #getReceiveBufferSize()
     */
    public void setReceiveBufferSize(int rcvBufSize) {
        this.rcvBufSize = rcvBufSize;
    }

    /**
     * Gets ODBC TCP server send queue limit. If the limit exceeds, all successive writes will
     * block until the queue has enough capacity.
     *
     * @return ODBC TCP server send queue limit (0 for unlimited).
     */
    public int getSendQueueLimit() {
        return sndQueueLimit;
    }

    /**
     * Sets ODBC TCP server send queue limit.
     *
     * @param sndQueueLimit ODBC TCP server send queue limit (0 for unlimited).
     * @see #getSendQueueLimit()
     */
    public void setSendQueueLimit(int sndQueueLimit) {
        this.sndQueueLimit = sndQueueLimit;
    }

    /**
     * Gets number of selector threads in ODBC TCP server. Higher value for this parameter
     * may increase throughput, but also increases context switching.
     *
     * @return Number of selector threads for ODBC TCP server.
     */
    public int getSelectorCount() {
        return selectorCnt;
    }

    /**
     * Sets number of selector threads for ODBC TCP server.
     *
     * @param selectorCnt Number of selector threads for ODBC TCP server.
     * @see #getSelectorCount()
     */
    public void setSelectorCount(int selectorCnt) {
        this.selectorCnt = selectorCnt;
    }

    /**
     * Gets idle timeout for ODBC TCP server.
     * <p>
     * This setting is used to reject half-opened sockets. If no packets
     * come within idle timeout, the connection is closed.
     * <p/>
     * If not specified, default value is {@link #DFLT_IDLE_TIMEOUT}.
     *
     * @return Idle timeout in milliseconds.
     */
    public long getIdleTimeout() {
        return idleTimeout;
    }

    /**
     * Sets idle timeout for ODBC TCP server.
     *
     * @param idleTimeout Idle timeout in milliseconds.
     * @see #getIdleTimeout()
     */
    public void setIdleTimeout(long idleTimeout) {
        this.idleTimeout = idleTimeout;
    }

    /**
     * Gets maximum number of opened cursors per connection.
     * <p>
     * Defaults to {@link #DFLT_MAX_OPEN_CURSORS}.
     *
     * @return Maximum number of opened cursors.
     */
    public int getMaxOpenCursors() {
        return maxOpenCursors;
    }

    /**
     * Sets maximum number of opened cursors per connection. See {@link #getMaxOpenCursors()}.
     *
     * @param maxOpenCursors Maximum number of opened cursors.
     */
    public void setMaxOpenCursors(int maxOpenCursors) {
        this.maxOpenCursors = maxOpenCursors;
    }
}

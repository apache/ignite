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

/**
 * ODBC configuration.
 */
public class OdbcConfiguration {
    /** Default TCP host. */
    public static final String DFLT_TCP_HOST = "0.0.0.0";

    /** Default minimum TCP port range value. */
    public static final int DFLT_TCP_PORT_FROM = 10800;

    /** Default maximum TCP port range value. */
    public static final int DFLT_TCP_PORT_TO = 10810;

    /** Default max number of open cursors per connection. */
    public static final int DFLT_MAX_OPEN_CURSORS = 128;

    /** Default socket send and receive buffer size. */
    public static final int DFLT_SOCK_BUF_SIZE = 32 * 1024;

    /** Endpoint address. */
    private String endpointAddr;

    /** Max number of opened cursors per connection. */
    private int maxOpenCursors = DFLT_MAX_OPEN_CURSORS;

    /** Idle timeout. */
    private long idleTimeout = Long.MAX_VALUE;

    /** Socket send buffer size. */
    private int sockSndBufSize = DFLT_SOCK_BUF_SIZE;

    /** Socket receive buffer size. */
    private int sockRcvBufSize = DFLT_SOCK_BUF_SIZE;

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

        endpointAddr = cfg.getEndpointAddress();
        maxOpenCursors = cfg.getMaxOpenCursors();
        idleTimeout = cfg.getIdleTimeout();
        sockSndBufSize = cfg.getSocketSendBufferSize();
        sockRcvBufSize = cfg.getSocketReceiveBufferSize();
    }

    /**
     * Get ODBC endpoint address. Ignite will listen for incoming TCP connections on this address. Either single port
     * or port range could be used. In the latter case Ignite will start listening on the first available port
     * form the range.
     * <p>
     * The following address formats are permitted:
     * <ul>
     * <li>{@code hostname} - will use provided hostname and default port range;</li>
     * <li>{@code hostname:port} - will use provided hostname and port;</li>
     * <li>{@code hostname:port_from..port_to} - will use provided hostname and port range.</li>
     * </ul>
     * <p>
     * When set to {@code null}, ODBC processor will be bound to {@link #DFLT_TCP_HOST} host and default port range.
     * <p>
     * Default port range is from {@link #DFLT_TCP_PORT_FROM} to {@link #DFLT_TCP_PORT_TO}.
     *
     * @return ODBC endpoint address.
     */
    public String getEndpointAddress() {
        return endpointAddr;
    }

    /**
     * Set ODBC endpoint address. See {@link #getEndpointAddress()} for more information.
     *
     * @param addr ODBC endpoint address.
     * @return This instance for chaining.
     */
    public OdbcConfiguration setEndpointAddress(String addr) {
        this.endpointAddr = addr;

        return this;
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
     * @return This instance for chaining.
     */
    public OdbcConfiguration setMaxOpenCursors(int maxOpenCursors) {
        this.maxOpenCursors = maxOpenCursors;

        return this;
    }

    /**
     * Gets send buffer size.
     *
     * @return buffer size in bytes.
     */
    public int getSocketSendBufferSize() {
        return sockSndBufSize;
    }

    /**
     * Sets receive buffer size. See {@link #getSocketSendBufferSize()}.
     *
     * @param sockSndBufSize buffer size in bytes.
     * @return This instance for chaining.
     */
    public OdbcConfiguration setSocketSendBufferSize(int sockSndBufSize) {
        this.sockSndBufSize = sockSndBufSize;

        return this;
    }

    /**
     * Gets receive buffer size. Defaults to {@link #DFLT_SOCK_BUF_SIZE}.
     *
     * @return buffer size in bytes.
     */
    public int getSocketReceiveBufferSize() {
        return sockRcvBufSize;
    }

    /**
     * Sets receive buffer size. See {@link #getSocketReceiveBufferSize()}.
     *
     * @param sockRcvBufSize buffer size in bytes.
     * @return This instance for chaining.
     */
    public OdbcConfiguration setSocketReceiveBufferSize(int sockRcvBufSize) {
        this.sockRcvBufSize = sockRcvBufSize;

        return this;
    }

    /**
     * Gets idle timeout. Defaults to {@link #DFLT_SOCK_BUF_SIZE}.
     *
     * @return Idle timeout in milliseconds.
     */
    public long getIdleTimeout() {
        return idleTimeout;
    }

    /**
     * Sets idle timeout.
     *
     * @param idleTimeout Idle timeout in milliseconds.
     * @return This instance for chaining.
     */
    public OdbcConfiguration setIdleTimeout(long idleTimeout) {
        this.idleTimeout = idleTimeout;

        return this;
    }
}

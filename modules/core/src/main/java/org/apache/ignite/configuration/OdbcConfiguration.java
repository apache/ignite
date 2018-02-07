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

import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * ODBC configuration.
 * <p>
 * Deprecated as of Apache Ignite 2.1. Please use {@link ClientConnectorConfiguration} and
 * {@link IgniteConfiguration#setClientConnectorConfiguration(ClientConnectorConfiguration)} instead.
 */
@Deprecated
public class OdbcConfiguration {
    /** Default TCP host. */
    public static final String DFLT_TCP_HOST = "0.0.0.0";

    /** Default minimum TCP port range value. */
    public static final int DFLT_TCP_PORT_FROM = 10800;

    /** Default maximum TCP port range value. */
    public static final int DFLT_TCP_PORT_TO = 10810;

    /** Default socket send and receive buffer size. */
    public static final int DFLT_SOCK_BUF_SIZE = 0;

    /** Default max number of open cursors per connection. */
    public static final int DFLT_MAX_OPEN_CURSORS = 128;

    /** Default size of thread pool. */
    public static final int DFLT_THREAD_POOL_SIZE = IgniteConfiguration.DFLT_PUBLIC_THREAD_CNT;

    /** Endpoint address. */
    private String endpointAddr;

    /** Socket send buffer size. */
    private int sockSndBufSize = DFLT_SOCK_BUF_SIZE;

    /** Socket receive buffer size. */
    private int sockRcvBufSize = DFLT_SOCK_BUF_SIZE;

    /** Max number of opened cursors per connection. */
    private int maxOpenCursors = DFLT_MAX_OPEN_CURSORS;

    /** Thread pool size. */
    private int threadPoolSize = DFLT_THREAD_POOL_SIZE;

    /**
     * Creates ODBC server configuration with all default values.
     */
    public OdbcConfiguration() {
        // No-op.
    }

    /**
     * Creates ODBC server configuration by copying all properties from given configuration.
     *
     * @param cfg ODBC server configuration.
     */
    public OdbcConfiguration(OdbcConfiguration cfg) {
        assert cfg != null;

        endpointAddr = cfg.getEndpointAddress();
        maxOpenCursors = cfg.getMaxOpenCursors();
        sockRcvBufSize = cfg.getSocketReceiveBufferSize();
        sockSndBufSize = cfg.getSocketSendBufferSize();
        threadPoolSize = cfg.getThreadPoolSize();
    }

    /**
     * Get ODBC endpoint address. Ignite will listen for incoming TCP connections on this address. Either single port
     * or port range could be used. In the latter case Ignite will start listening on the first available port
     * form the range.
     * <p>
     * The following address formats are permitted:
     * <ul>
     *     <li>{@code hostname} - will use provided hostname and default port range;</li>
     *     <li>{@code hostname:port} - will use provided hostname and port;</li>
     *     <li>{@code hostname:port_from..port_to} - will use provided hostname and port range.</li>
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
    public OdbcConfiguration setSocketSendBufferSize(int sockSndBufSize) {
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
    public OdbcConfiguration setSocketReceiveBufferSize(int sockRcvBufSize) {
        this.sockRcvBufSize = sockRcvBufSize;

        return this;
    }

    /**
     * Size of thread pool that is in charge of processing ODBC tasks.
     * <p>
     * Defaults {@link #DFLT_THREAD_POOL_SIZE}.
     *
     * @return Thread pool that is in charge of processing ODBC tasks.
     */
    public int getThreadPoolSize() {
        return threadPoolSize;
    }

    /**
     * Sets thread pool that is in charge of processing ODBC tasks. See {@link #getThreadPoolSize()} for more
     * information.
     *
     * @param threadPoolSize Thread pool that is in charge of processing ODBC tasks.
     * @return This instance for chaining.
     */
    public OdbcConfiguration setThreadPoolSize(int threadPoolSize) {
        this.threadPoolSize = threadPoolSize;

        return this;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(OdbcConfiguration.class, this);
    }
}

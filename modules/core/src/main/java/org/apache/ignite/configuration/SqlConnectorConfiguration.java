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
import org.jetbrains.annotations.Nullable;

/**
 * SQL connection configuration.
 * <p>
 * Deprecated as of Apache Ignite 2.3. Please use {@link ClientConnectorConfiguration} and
 * {@link IgniteConfiguration#setClientConnectorConfiguration(ClientConnectorConfiguration)} instead.
 */
@Deprecated
public class SqlConnectorConfiguration {
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

    /**
     * Creates SQL connector configuration with all default values.
     */
    public SqlConnectorConfiguration() {
        // No-op.
    }

    /**
     * Creates SQL connector configuration by copying all properties from given configuration.
     *
     * @param cfg Configuration to copy.
     */
    public SqlConnectorConfiguration(SqlConnectorConfiguration cfg) {
        assert cfg != null;

        host = cfg.getHost();
        maxOpenCursorsPerConn = cfg.getMaxOpenCursorsPerConnection();
        port = cfg.getPort();
        portRange = cfg.getPortRange();
        sockRcvBufSize = cfg.getSocketReceiveBufferSize();
        sockSndBufSize = cfg.getSocketSendBufferSize();
        tcpNoDelay = cfg.isTcpNoDelay();
        threadPoolSize = cfg.getThreadPoolSize();
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
    public SqlConnectorConfiguration setHost(@Nullable String host) {
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
    public SqlConnectorConfiguration setPort(int port) {
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
    public SqlConnectorConfiguration setPortRange(int portRange) {
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
    public SqlConnectorConfiguration setSocketSendBufferSize(int sockSndBufSize) {
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
    public SqlConnectorConfiguration setSocketReceiveBufferSize(int sockRcvBufSize) {
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
    public SqlConnectorConfiguration setTcpNoDelay(boolean tcpNoDelay) {
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
    public SqlConnectorConfiguration setMaxOpenCursorsPerConnection(int maxOpenCursorsPerConn) {
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
    public SqlConnectorConfiguration setThreadPoolSize(int threadPoolSize) {
        this.threadPoolSize = threadPoolSize;

        return this;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SqlConnectorConfiguration.class, this);
    }
}

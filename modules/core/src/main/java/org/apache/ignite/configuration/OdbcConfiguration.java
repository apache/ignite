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

        host = cfg.getHost();
        maxOpenCursors = cfg.getMaxOpenCursors();
        port = cfg.getPort();
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

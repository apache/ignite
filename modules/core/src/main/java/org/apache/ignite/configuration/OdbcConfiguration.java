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
    /** Default TCP server port range. */
    public static final String DFLT_TCP_PORT_RANGE = "11443..11463";

    /** Default max number of open cursors per connection. */
    public static final int DFLT_MAX_OPEN_CURSORS = 128;

    /** Address of the ODBC server. */
    private String addr;

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

        addr = cfg.getAddress();
        maxOpenCursors = cfg.getMaxOpenCursors();
    }

    /**
     * Gets address for ODBC TCP server. This can be either an
     * IP address or a domain name with TCP port or port range.
     * <p>
     * The format of the address should be as follows:
     * address := domain_name:tcp_port1[..tcp_port2]
     * tcp_port2 > tcp_port1.
     * <p>
     * If not defined, system-wide local address will be used
     * (see {@link IgniteConfiguration#getLocalHost()} together
     * with {@link OdbcConfiguration#DFLT_TCP_PORT_RANGE}.
     * <p>
     * You can also use {@code 0.0.0.0} value to bind to all
     * locally-available IP addresses.
     *
     * @return Address of the ODBC server.
     */
    public String getAddress() {
        return addr;
    }

    /**
     * Sets address for ODBC TCP server.
     * <p>
     * The format of the address should be as follows:
     * address := domain_name:tcp_port1[..tcp_port2]
     * tcp_port2 > tcp_port1.
     *
     * @param host TCP host.
     */
    public void setAddress(String host) {
        this.addr = host;
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

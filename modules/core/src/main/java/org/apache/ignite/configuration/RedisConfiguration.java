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
 * Redis protocol configuration.
 */
public class RedisConfiguration extends ConnectorConfiguration {
    /** Default TCP server port. */
    public static final int DFLT_TCP_PORT = 6379;

    /** Host. */
    private String host;

    /** Port. */
    private int port = DFLT_TCP_PORT;

    /**
     * Creates Redis connection configuration with all default values.
     */
    public RedisConfiguration() {
        // No-op.
    }

    /**
     * Creates Redis configuration by copying all properties from given configuration.
     *
     * @param cfg Client configuration.
     */
    public RedisConfiguration(RedisConfiguration cfg) {
        assert cfg != null;

        host = cfg.getHost();
        port = cfg.getPort();
    }

    /**
     * Gets host for Redis protocol server. This can be either an
     * IP address or a domain name.
     * <p>
     * If not defined, system-wide local address will be used
     * (see {@link IgniteConfiguration#getLocalHost()}.
     * <p>
     * You can also use {@code 0.0.0.0} value to bind to all
     * locally-available IP addresses.
     *
     * @return Redis host.
     */
    @Override public String getHost() {
        return host;
    }

    /**
     * Sets host for Redis protocol server.
     *
     * @param host Redis host.
     */
    @Override public void setHost(String host) {
        this.host = host;
    }

    /**
     * Gets port for Redis protocol server.
     * <p>
     * Default is {@link #DFLT_TCP_PORT}.
     *
     * @return TCP port.
     */
    @Override public int getPort() {
        return port;
    }

    /**
     * Sets port for Redis protocol server.
     *
     * @param port TCP port.
     */
    @Override public void setPort(int port) {
        this.port = port;
    }
}

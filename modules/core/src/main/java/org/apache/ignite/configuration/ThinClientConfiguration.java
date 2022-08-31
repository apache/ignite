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
 * Server-side thin-client configuration.
 *
 * This configuration related only to lightweight (thin) Ignite clients and not related to ODBC and JDBC clients.
 */
public class ThinClientConfiguration {
    /** Default limit of active transactions count per connection. */
    public static final int DFLT_MAX_ACTIVE_TX_PER_CONNECTION = 100;

    /** Default limit of active compute tasks per connection. */
    public static final int DFLT_MAX_ACTIVE_COMPUTE_TASKS_PER_CONNECTION = 0;

    /** Active transactions count per connection limit. */
    private int maxActiveTxPerConn = DFLT_MAX_ACTIVE_TX_PER_CONNECTION;

    /** Active compute tasks per connection limit. */
    private int maxActiveComputeTasksPerConn = DFLT_MAX_ACTIVE_COMPUTE_TASKS_PER_CONNECTION;

    /**
     * @see new field serverExcStackTraceToClient.
     * If {@code true} sends a server exception stack trace to the client side.
     * @deprecated please now use new field serverExcStackTraceToClientbecause.
     */
    @Deprecated
    private boolean sendServerExcStackTraceToClient;

    /** If {@code true} a server exception stack trace is sent to the client side. */
    private boolean serverExcStackTraceToClient;

    /**
     * Creates thin-client configuration with all default values.
     */
    public ThinClientConfiguration() {
        // No-op.
    }

    /**
     * @param cfg Configuration to copy.
     * Creates thin-client configuration by copying all properties from given configuration.
     * @deprecated
     */
    @Deprecated
    public ThinClientConfiguration(ThinClientConfiguration cfg) {
        assert cfg != null;

        maxActiveTxPerConn = cfg.maxActiveTxPerConn;
        maxActiveComputeTasksPerConn = cfg.maxActiveComputeTasksPerConn;
        sendServerExcStackTraceToClient = cfg.sendServerExcStackTraceToClient;
    }

    /**
     * @return Active transactions count per connection limit.
     */
    public int getMaxActiveTxPerConnection() {
        return maxActiveTxPerConn;
    }

    /**
     * Sets active transactions count per connection limit.
     *
     * @param maxActiveTxPerConn Active transactions count per connection limit.
     * @return {@code this} for chaining.
     */
    public ThinClientConfiguration setMaxActiveTxPerConnection(int maxActiveTxPerConn) {
        this.maxActiveTxPerConn = maxActiveTxPerConn;

        return this;
    }

    /**
     * Gets active compute tasks per connection limit.
     *
     * @return {@code True} if compute is enabled for thin client.
     */
    public int getMaxActiveComputeTasksPerConnection() {
        return maxActiveComputeTasksPerConn;
    }

    /**
     * Sets active compute tasks per connection limit.
     * Value {@code 0} means that compute grid functionality is disabled for thin clients.
     *
     * @param maxActiveComputeTasksPerConn Active compute tasks per connection limit.
     * @return {@code this} for chaining.
     */
    public ThinClientConfiguration setMaxActiveComputeTasksPerConnection(int maxActiveComputeTasksPerConn) {
        this.maxActiveComputeTasksPerConn = maxActiveComputeTasksPerConn;

        return this;
    }

    /**
     * @return If {@code true} sends a server exception stack to the client side.
     * @see new getServerExceptionStackTraceToClient() and setServerExceptionStackTraceToClient().
     * @deprecated please now use getServerExceptionStackTraceToClient()
     */
    @Deprecated
    public boolean sendServerExceptionStackTraceToClient() {
        return sendServerExcStackTraceToClient;
    }

    /**
     * @param sendServerExcStackTraceToClient If {@code true} sends a server exception stack to the client side.
     * @return {@code this} for chaining.
     * @see new setServerExceptionStackTraceToClient().
     * @deprecated please now use new method setServerExceptionStackTraceToClient().
     */
    @Deprecated
    public ThinClientConfiguration sendServerExceptionStackTraceToClient(boolean sendServerExcStackTraceToClient) {
        this.sendServerExcStackTraceToClient = sendServerExcStackTraceToClient;

        return this;
    }

    /**
     * @return If {@code true} gets a server exception stack that is to be sent to the client side.
     */
    public boolean getServerExceptionStackTraceToClient() {
        return serverExcStackTraceToClient;
    }

    /**
     * @param serverExcStackTraceToClient if {@code true} sets a server exception stack that is to be sent to the client side.
     * @return {@code this} for chaining.
     */
    public ThinClientConfiguration setServerExceptionStackTraceToClient(boolean serverExcStackTraceToClient) {
        this.serverExcStackTraceToClient = serverExcStackTraceToClient;

        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return S.toString(ThinClientConfiguration.class, this);
    }
}

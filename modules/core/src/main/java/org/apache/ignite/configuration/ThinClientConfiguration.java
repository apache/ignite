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
     * Creates thin-client configuration with all default values.
     */
    public ThinClientConfiguration() {
        // No-op.
    }

    /**
     * Creates thin-client configuration by copying all properties from given configuration.
     *
     * @param cfg Configuration to copy.
     */
    public ThinClientConfiguration(ThinClientConfiguration cfg) {
        assert cfg != null;

        maxActiveTxPerConn = cfg.maxActiveTxPerConn;
        maxActiveComputeTasksPerConn = cfg.maxActiveComputeTasksPerConn;
    }

    /**
     * Gets active transactions count per connection limit.
     */
    public int getMaxActiveTxPerConnection() {
        return maxActiveTxPerConn;
    }

    /**
     * Sets active transactions count per connection limit.
     *
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
     * @return {@code this} for chaining.
     */
    public ThinClientConfiguration setMaxActiveComputeTasksPerConnection(int maxActiveComputeTasksPerConn) {
        this.maxActiveComputeTasksPerConn = maxActiveComputeTasksPerConn;

        return this;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ThinClientConfiguration.class, this);
    }
}

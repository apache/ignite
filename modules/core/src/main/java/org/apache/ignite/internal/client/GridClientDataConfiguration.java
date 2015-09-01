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

package org.apache.ignite.internal.client;

import org.apache.ignite.internal.client.balancer.GridClientLoadBalancer;
import org.apache.ignite.internal.client.balancer.GridClientRandomBalancer;

/**
 * Java client data configuration.
 */
public class GridClientDataConfiguration {
    /** Grid cache name. */
    private String name;

    /** Affinity. */
    private GridClientDataAffinity affinity;

    /** Balancer for pinned mode. */
    private GridClientLoadBalancer balancer = new GridClientRandomBalancer();

    /**
     * Creates empty configuration.
     */
    public GridClientDataConfiguration() {
        // No-op.
    }

    /**
     * Copy constructor.
     *
     * @param cfg Configuration to copy.
     */
    public GridClientDataConfiguration(GridClientDataConfiguration cfg) {
        // Preserve alphabetic order for maintenance.
        affinity = cfg.getAffinity();
        balancer = cfg.getPinnedBalancer();
        name = cfg.getName();
    }

    /**
     * Gets name of remote cache.
     *
     * @return Remote cache name.
     */
    public String getName() {
        return name;
    }

    /**
     * Sets grid cache name for this configuration.
     *
     * @param name Cache name.
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * Gets affinity to be used to communicate with remote cache.
     * Affinity allows to contact exactly the node where the data is and therefore
     * avoiding extra network hops.
     * <p>
     * Use {@link GridClientPartitionAffinity} as default affinity communication to
     * work with remote partitioned caches.
     *
     * @return Cache affinity to use.
     */
    public GridClientDataAffinity getAffinity() {
        return affinity;
    }

    /**
     * Sets client data affinity for this configuration.
     * Affinity allows to contact exactly the node where the data is and therefore
     * avoiding extra network hops.
     * <p>
     * Use {@link GridClientPartitionAffinity} as default affinity communication to
     * work with remote partitioned caches.
     *
     * @param affinity Client data affinity.
     */
    public void setAffinity(GridClientDataAffinity affinity) {
        this.affinity = affinity;
    }

    /**
     * Gets balancer to use for pinned nodes. See {@link GridClientLoadBalancer}
     * for more information.
     *
     * @return Node balancer for pinned mode.
     */
    public GridClientLoadBalancer getPinnedBalancer() {
        return balancer;
    }

    /**
     * Sets balancer for pinned mode for this configuration.
     *
     * @param balancer Balancer that will be used in pinned mode.
     */
    public void setBalancer(GridClientLoadBalancer balancer) {
        this.balancer = balancer;
    }
}
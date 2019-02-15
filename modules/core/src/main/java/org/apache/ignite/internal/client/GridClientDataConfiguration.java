/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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
     * @return {@code this} for chaining.
     */
    public GridClientDataConfiguration setName(String name) {
        this.name = name;

        return this;
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
     * @return {@code this} for chaining.
     */
    public GridClientDataConfiguration setAffinity(GridClientDataAffinity affinity) {
        this.affinity = affinity;

        return this;
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
     * @return {@code this} for chaining.
     */
    public GridClientDataConfiguration setBalancer(GridClientLoadBalancer balancer) {
        this.balancer = balancer;

        return this;
    }
}
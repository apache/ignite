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

package org.apache.ignite.internal.client.balancer;

import java.util.Collection;
import org.apache.ignite.internal.client.GridClientNode;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgnitePredicate;

/**
 * Base class for balancers. Contains common direct connection handling logic.
 */
public abstract class GridClientBalancerAdapter implements GridClientLoadBalancer {
    /** Selects connectable nodes. */
    private static final IgnitePredicate<GridClientNode> CONNECTABLE =
        new IgnitePredicate<GridClientNode>() {
            @Override public boolean apply(GridClientNode e) {
                return e.connectable();
            }
        };

    /** Prefer direct nodes. */
    private boolean preferDirectNodes;

    /**
     * If set to {@code true} balancer should prefer directly connectable
     * nodes over others.
     * <p>
     * In other words when working in router connection mode
     * client will prefer send requests to router nodes
     * if operation projection contains some of them.
     * <p>
     * Default value is {@code false}.
     *
     * @see GridClientNode#connectable()
     * @return Prefer direct nodes.
     */
    public boolean isPreferDirectNodes() {
        return preferDirectNodes;
    }

    /**
     * Sets prefer direct nodes.
     *
     * @param preferDirectNodes Prefer direct nodes.
     * @return {@code this} for chaining.
     */
    public GridClientBalancerAdapter setPreferDirectNodes(boolean preferDirectNodes) {
        this.preferDirectNodes = preferDirectNodes;

        return this;
    }

    /**
     * Returns only directly available nodes from given collection.
     *
     * @param nodes Nodes.
     * @return Directly available subset.
     */
    protected static Collection<GridClientNode> selectDirectNodes(Collection<? extends GridClientNode> nodes) {
        return F.viewReadOnly(nodes, F.<GridClientNode>identity(), CONNECTABLE);
    }
}
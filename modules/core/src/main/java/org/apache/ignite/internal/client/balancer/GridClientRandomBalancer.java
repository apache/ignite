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
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import org.apache.ignite.internal.client.GridClientNode;

/**
 * Simple balancer that relies on random node selection from a given collection. This implementation
 * has no any caches and treats each given collection as a new one.
 * <p>
 * More strictly, for any non-empty collection of size <tt>n</tt> the probability of selection of any
 * node in this collection will be <tt>1/n</tt>.
 */
public class GridClientRandomBalancer extends GridClientBalancerAdapter {
    /** Random for node selection. */
    private Random random = new Random();

    /**
     * Picks up a random node from a collection.
     *
     * @param nodes Nodes to pick from.
     * @return Random node from collection.
     */
    @Override public GridClientNode balancedNode(Collection<? extends GridClientNode> nodes) {
        assert !nodes.isEmpty();

        int size = nodes.size();

        if (isPreferDirectNodes()) {
            Collection<GridClientNode> direct = selectDirectNodes(nodes);

            int directSize = direct.size();

            // If set of direct nodes is not empty and differ from original one
            // replace original set of nodes with directly available.
            if (directSize > 0 && directSize < size) {
                nodes = direct;
                size = nodes.size();
            }
        }

        int idx = random.nextInt(size);

        if (nodes instanceof List)
            return ((List<GridClientNode>)nodes).get(idx);
        else {
            Iterator<? extends GridClientNode> it = nodes.iterator();

            while (idx > 0) {
                it.next();

                idx--;
            }

            return it.next();
        }
    }
}
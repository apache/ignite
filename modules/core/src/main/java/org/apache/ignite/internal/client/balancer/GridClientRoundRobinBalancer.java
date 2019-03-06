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
import java.util.LinkedList;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.ignite.internal.client.GridClientException;
import org.apache.ignite.internal.client.GridClientNode;
import org.apache.ignite.internal.client.GridClientTopologyListener;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Simple balancer that implements round-robin balancing.
 */
public class GridClientRoundRobinBalancer extends GridClientBalancerAdapter implements GridClientTopologyListener {
    /** Lock. */
    private Lock lock = new ReentrantLock();

    /** Nodes to share load. */
    private LinkedList<UUID> nodeQueue = new LinkedList<>();

    /** {@inheritDoc} */
    @Override public GridClientNode balancedNode(Collection<? extends GridClientNode> nodes)
        throws GridClientException {
        assert !nodes.isEmpty();

        if (isPreferDirectNodes()) {
            Collection<GridClientNode> direct = selectDirectNodes(nodes);

            int directSize = direct.size();

            // If set of direct nodes is not empty and differ from original one
            // replace original set of nodes with directly available.
            if (directSize > 0 && directSize < nodes.size())
                nodes = direct;
        }

        Map<UUID, GridClientNode> lookup = U.newHashMap(nodes.size());

        for (GridClientNode node : nodes)
            lookup.put(node.nodeId(), node);

        lock.lock();

        try {
            GridClientNode balanced = null;

            for (Iterator<UUID> iter = nodeQueue.iterator(); iter.hasNext();) {
                UUID nodeId = iter.next();

                balanced = lookup.get(nodeId);

                if (balanced != null) {
                    iter.remove();

                    break;
                }
            }

            if (balanced != null) {
                nodeQueue.addLast(balanced.nodeId());

                return balanced;
            }

            throw new GridClientException("Passed nodes doesn't present in topology " +
                "[nodes=" + nodes + ", top=" + nodeQueue);
        }
        finally {
            lock.unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public void onNodeAdded(GridClientNode node) {
        lock.lock();

        try {
            nodeQueue.addFirst(node.nodeId());
        }
        finally {
            lock.unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public void onNodeRemoved(GridClientNode node) {
        lock.lock();

        try {
            nodeQueue.remove(node.nodeId());
        }
        finally {
            lock.unlock();
        }
    }
}
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

package org.apache.ignite.spi.loadbalancing.roundrobin;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.ignite.cluster.ClusterNode;

/**
 * Load balancer for per-task configuration.
 */
class RoundRobinPerTaskLoadBalancer {
    /** Balancing nodes. */
    private ArrayDeque<ClusterNode> nodeQueue;

    /** Jobs mapped flag. */
    private volatile boolean isMapped;

    /** Mutex. */
    private final Object mux = new Object();

    /**
     * Call back for job mapped event.
     */
    void onMapped() {
        isMapped = true;
    }

    /**
     * Gets balanced node for given topology. This implementation
     * is to be used only from {@link org.apache.ignite.compute.ComputeTask#map(List, Object)} method
     * and, therefore, does not need to be thread-safe.
     *
     * @param top Topology to pick from.
     * @return Best balanced node.
     */
    ClusterNode getBalancedNode(List<ClusterNode> top) {
        assert top != null;
        assert !top.isEmpty();

        boolean readjust = isMapped;

        synchronized (mux) {
            // Populate first time.
            if (nodeQueue == null)
                nodeQueue = new ArrayDeque<>(top);

            // If job has been mapped, then it means
            // that it is most likely being failed over.
            // In this case topology might have changed
            // and we need to readjust with every apply.
            if (readjust)
                // Add missing nodes.
                for (ClusterNode node : top)
                    if (!nodeQueue.contains(node))
                        nodeQueue.offer(node);

            ClusterNode next = nodeQueue.poll();

            // If jobs have been mapped, we need to make sure
            // that queued node is still in topology.
            if (readjust && next != null) {
                while (!top.contains(next) && !nodeQueue.isEmpty())
                    next = nodeQueue.poll();

                // No nodes found and queue is empty.
                if (next != null && !top.contains(next))
                    return null;
            }

            if (next != null)
                // Add to the end.
                nodeQueue.offer(next);

            return next;
        }
    }

    /**
     * THIS METHOD IS USED ONLY FOR TESTING.
     *
     * @return Internal list of nodes.
     */
    List<ClusterNode> getNodes() {
        synchronized (mux) {
            return Collections.unmodifiableList(new ArrayList<>(nodeQueue));
        }
    }
}
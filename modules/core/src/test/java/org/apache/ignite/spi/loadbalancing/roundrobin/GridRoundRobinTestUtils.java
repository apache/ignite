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

import java.util.List;
import java.util.UUID;
import org.apache.ignite.GridTestJob;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeTaskSession;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Helper class for balancer tests.
 */
class GridRoundRobinTestUtils {
    /**
     * Performs two full cycles by round robin routine for check correct order.
     *
     * @param spi Load balancing SPI.
     * @param allNodes Topology nodes.
     * @param orderedNodes Balancing nodes.
     * @param ses Task session.
     */
    static void checkCyclicBalancing(RoundRobinLoadBalancingSpi spi, List<ClusterNode> allNodes,
        List<UUID> orderedNodes, ComputeTaskSession ses) {

        ClusterNode firstNode = spi.getBalancedNode(ses, allNodes, new GridTestJob());

        int startIdx = firstBalancedNodeIndex(firstNode, orderedNodes);

        // Two full cycles by round robin routine.
        for (int i = 0; i < allNodes.size() * 2; i++) {
            int actualIdx = (startIdx + i + 1) % allNodes.size();

            ClusterNode nextNode = spi.getBalancedNode(ses, allNodes, new GridTestJob());

            assertEquals("Balancer returns node out of order", nextNode.id(), orderedNodes.get(actualIdx));
        }
    }

    /**
     * Performs two full cycles by round robin routine for check correct order.
     * Switches between two task sessions by turns.
     *
     * @param spi Load balancing SPI.
     * @param allNodes Topology nodes.
     * @param orderedNodes Balancing nodes.
     * @param ses1 First task session.
     * @param ses2 Second task session.
     */
    static void checkCyclicBalancing(RoundRobinLoadBalancingSpi spi, List<ClusterNode> allNodes,
        List<UUID> orderedNodes, ComputeTaskSession ses1, ComputeTaskSession ses2) {

        ClusterNode firstNode = spi.getBalancedNode(ses1, allNodes, new GridTestJob());

        int startIdx = firstBalancedNodeIndex(firstNode, orderedNodes);

        // Two full cycles by round robin routine.
        for (int i = 0; i < allNodes.size() * 2; i++) {
            int actualIdx = (startIdx + i + 1) % allNodes.size();

            ClusterNode nextNode = spi.getBalancedNode(i % 2 == 0 ? ses1 : ses2, allNodes, new GridTestJob());

            assertEquals("Balancer returns node out of order", nextNode.id(), orderedNodes.get(actualIdx));
        }
    }

    /**
     * @param firstNode First node which was return by balancer.
     * @param orderedNodes Balancing nodes.
     * @return Index of first node which was return by balancer.
     */
    static int firstBalancedNodeIndex(ClusterNode firstNode, List<UUID> orderedNodes) {
        int startIdx = -1;

        for (int i = 0; i < orderedNodes.size(); i++) {
            if (firstNode.id() == orderedNodes.get(i))
                startIdx = i;
        }

        assertTrue("Can't find position of first balanced node", startIdx >= 0);

        return startIdx;
    }
}
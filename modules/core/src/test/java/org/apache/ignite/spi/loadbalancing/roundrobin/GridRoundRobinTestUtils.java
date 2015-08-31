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
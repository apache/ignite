/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.loadbalancing.roundrobin;

import org.apache.ignite.cluster.*;
import org.gridgain.grid.*;
import org.gridgain.grid.compute.*;

import java.util.*;

import static org.junit.Assert.*;

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
     * @throws GridException If balancer failed.
     */
    static void checkCyclicBalancing(GridRoundRobinLoadBalancingSpi spi, List<ClusterNode> allNodes,
        List<UUID> orderedNodes, GridComputeTaskSession ses) throws GridException {

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
     * @throws GridException If balancer failed.
     */
    static void checkCyclicBalancing(GridRoundRobinLoadBalancingSpi spi, List<ClusterNode> allNodes,
        List<UUID> orderedNodes, GridComputeTaskSession ses1, GridComputeTaskSession ses2) throws GridException {

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

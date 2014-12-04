/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.cache.affinity.fair;

import org.apache.ignite.cluster.*;
import org.apache.ignite.events.*;
import org.gridgain.grid.cache.affinity.*;
import org.gridgain.grid.kernal.processors.affinity.*;
import org.gridgain.testframework.*;
import org.gridgain.testframework.junits.common.*;

import java.util.*;

/**
 *
 */
public class GridCachePartitionFairAffinitySelfTest extends GridCommonAbstractTest {
    /**
     * @throws Exception If failed.
     */
    public void testNodeRemovedNoBackups() throws Exception {
        checkNodeRemoved(0);
    }

    /**
     * @throws Exception If failed.
     */
    public void testNodeRemovedOneBackup() throws Exception {
        checkNodeRemoved(1);
    }

    /**
     * @throws Exception If failed.
     */
    public void testNodeRemovedTwoBackups() throws Exception {
        checkNodeRemoved(2);
    }

    /**
     * @throws Exception If failed.
     */
    public void testNodeRemovedThreeBackups() throws Exception {
        checkNodeRemoved(3);
    }

    /**
     * @throws Exception If failed.
     */
    public void testRandomReassignmentNoBackups() throws Exception {
        checkRandomReassignment(0);
    }

    /**
     * @throws Exception If failed.
     */
    public void testRandomReassignmentOneBackup() throws Exception {
        checkRandomReassignment(1);
    }

    /**
     * @throws Exception If failed.
     */
    public void testRandomReassignmentTwoBackups() throws Exception {
        checkRandomReassignment(2);
    }

    /**
     * @throws Exception If failed.
     */
    public void testRandomReassignmentThreeBackups() throws Exception {
        checkRandomReassignment(3);
    }

    /**
     * @throws Exception If failed.
     */
    private void checkNodeRemoved(int backups) throws Exception {
        int parts = 256;

        GridCacheAffinityFunction aff = new GridCachePartitionFairAffinity(parts);

        int nodesCnt = 50;

        List<ClusterNode> nodes = new ArrayList<>(nodesCnt);

        List<List<ClusterNode>> prev = null;

        for (int i = 0; i < nodesCnt; i++) {
            info("======================================");
            info("Assigning partitions: " + i);
            info("======================================");

            ClusterNode node = new GridTestNode(UUID.randomUUID());

            nodes.add(node);

            IgniteDiscoveryEvent discoEvt = new IgniteDiscoveryEvent(node, "", IgniteEventType.EVT_NODE_JOINED,
                node);

            List<List<ClusterNode>> assignment = aff.assignPartitions(
                new GridCacheAffinityFunctionContextImpl(nodes, prev, discoEvt, i, backups));

            info("Assigned.");

            verifyAssignment(assignment, backups, parts, nodes.size());

            prev = assignment;
        }

        info("======================================");
        info("Will remove nodes.");
        info("======================================");

        for (int i = 0; i < nodesCnt - 1; i++) {
            info("======================================");
            info("Assigning partitions: " + i);
            info("======================================");

            ClusterNode rmv = nodes.remove(nodes.size() - 1);

            IgniteDiscoveryEvent discoEvt = new IgniteDiscoveryEvent(rmv, "", IgniteEventType.EVT_NODE_LEFT, rmv);

            List<List<ClusterNode>> assignment = aff.assignPartitions(
                new GridCacheAffinityFunctionContextImpl(nodes, prev, discoEvt, i, backups));

            info("Assigned.");

            verifyAssignment(assignment, backups, parts, nodes.size());

            prev = assignment;
        }
    }

    @SuppressWarnings("IfMayBeConditional")
    private void checkRandomReassignment(int backups) {
        int parts = 256;

        GridCacheAffinityFunction aff = new GridCachePartitionFairAffinity(parts);

        Random rnd = new Random();

        int maxNodes = 50;

        List<ClusterNode> nodes = new ArrayList<>(maxNodes);

        List<List<ClusterNode>> prev = null;

        int state = 0;

        int i = 0;

        while (true) {
            boolean add;

            if (nodes.size() < 2) {
                // Returned back to one node?
                if (state == 1)
                    return;

                add = true;
            }
            else if (nodes.size() == maxNodes) {
                if (state == 0)
                    state = 1;

                add = false;
            }
            else {
                // Nodes size in [2, maxNodes - 1].
                if (state == 0)
                    add = rnd.nextInt(3) != 0; // 66% to add, 33% to remove.
                else
                    add = rnd.nextInt(3) == 0; // 33% to add, 66% to remove.
            }

            IgniteDiscoveryEvent discoEvt;

            if (add) {
                ClusterNode addedNode = new GridTestNode(UUID.randomUUID());

                nodes.add(addedNode);

                discoEvt = new IgniteDiscoveryEvent(addedNode, "", IgniteEventType.EVT_NODE_JOINED, addedNode);
            }
            else {
                ClusterNode rmvNode = nodes.remove(rnd.nextInt(nodes.size()));

                discoEvt = new IgniteDiscoveryEvent(rmvNode, "", IgniteEventType.EVT_NODE_LEFT, rmvNode);
            }

            info("======================================");
            info("Assigning partitions [iter=" + i + ", discoEvt=" + discoEvt + ", nodesSize=" + nodes.size() + ']');
            info("======================================");

            List<List<ClusterNode>> assignment = aff.assignPartitions(
                new GridCacheAffinityFunctionContextImpl(nodes, prev, discoEvt, i, backups));

            verifyAssignment(assignment, backups, parts, nodes.size());

            prev = assignment;

            i++;
        }
    }

    /**
     * @param assignment Assignment to verify.
     */
    private void verifyAssignment(List<List<ClusterNode>> assignment, int keyBackups, int partsCnt, int topSize) {
        Map<UUID, Collection<Integer>> mapping = new HashMap<>();

        int ideal = Math.round((float)partsCnt / topSize * Math.min(keyBackups + 1, topSize));

        for (int part = 0; part < assignment.size(); part++) {
            for (ClusterNode node : assignment.get(part)) {
                assert node != null;

                Collection<Integer> parts = mapping.get(node.id());

                if (parts == null) {
                    parts = new HashSet<>();

                    mapping.put(node.id(), parts);
                }

                assertTrue(parts.add(part));
            }
        }

        int max = -1, min = Integer.MAX_VALUE;

        for (Collection<Integer> parts : mapping.values()) {
            max = Math.max(max, parts.size());
            min = Math.min(min, parts.size());
        }

        log().warning("max=" + max + ", min=" + min + ", ideal=" + ideal + ", minDev=" + deviation(min, ideal) + "%, " +
            "maxDev=" + deviation(max, ideal) + "%");

        assertTrue("max=" + max + ", min=" + min, max - min < (keyBackups + 1) * topSize);
    }

    private static int deviation(int val, int ideal) {
        return Math.round(Math.abs(((float)val - ideal) / ideal * 100));
    }
}

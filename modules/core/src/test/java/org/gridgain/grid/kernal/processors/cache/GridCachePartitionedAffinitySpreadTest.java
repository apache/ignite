/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.affinity.consistenthash.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.testframework.*;
import org.gridgain.testframework.junits.common.*;

import java.util.*;

import static org.gridgain.grid.cache.affinity.consistenthash.GridCacheConsistentHashAffinityFunction.*;

/**
 *
 */
public class GridCachePartitionedAffinitySpreadTest extends GridCommonAbstractTest {
    /** */
    public static final int NODES_CNT = 50;

    /**
     * @throws Exception If failed.
     */
    public void testPartitionSpreading() throws Exception {
        System.out.printf("%6s, %6s, %6s, %6s, %8s\n", "Nodes", "Reps", "Min", "Max", "Dev");

        for (int i = 5; i < NODES_CNT; i = i * 3 / 2) {
            for (int replicas = 128; replicas <= 4096; replicas*=2) {
                Collection<GridNode> nodes = createNodes(i, replicas);

                GridCacheConsistentHashAffinityFunction aff = new GridCacheConsistentHashAffinityFunction(false, 10000);

                checkDistribution(aff, nodes);
            }

            System.out.println();
        }
    }

    /**
     * @param nodesCnt Nodes count.
     * @param replicas Value of
     * @return Collection of test nodes.
     */
    private Collection<GridNode> createNodes(int nodesCnt, int replicas) {
        Collection<GridNode> nodes = new ArrayList<>(nodesCnt);

        for (int i = 0; i < nodesCnt; i++)
            nodes.add(new TestRichNode(replicas));

        return nodes;
    }

    /**
     * @param aff Affinity to check.
     * @param nodes Collection of nodes to test on.
     */
    private void checkDistribution(GridCacheConsistentHashAffinityFunction aff, Collection<GridNode> nodes) {
        Map<GridNode, Integer> parts = new HashMap<>(nodes.size());

        for (int part = 0; part < aff.getPartitions(); part++) {
            Collection<GridNode> affNodes = aff.nodes(part, nodes, 0);

            assertEquals(1, affNodes.size());

            GridNode node = F.first(affNodes);

            parts.put(node, parts.get(node) != null ? parts.get(node) + 1 : 1);
        }

        int min = Integer.MAX_VALUE;
        int max = Integer.MIN_VALUE;
        int total = 0;

        float mean = 0;
        float m2 = 0;
        int n = 0;

        for (GridNode node : nodes) {
            int partsCnt = parts.get(node) != null ? parts.get(node) : 0;

            total += partsCnt;

            if (partsCnt < min)
                min = partsCnt;

            if (partsCnt > max)
                max = partsCnt;

            n++;
            float delta = partsCnt - mean;
            mean += delta / n;
            m2 += delta * (partsCnt - mean);
        }

        m2 /= (n - 1);
        assertEquals(aff.getPartitions(), total);

        System.out.printf("%6s, %6s, %6s, %6s, %8.4f\n", nodes.size(),
            F.first(nodes).attribute(DFLT_REPLICA_COUNT_ATTR_NAME), min, max, Math.sqrt(m2));
    }

    /**
     * Rich node stub to use in emulated server topology.
     */
    private static class TestRichNode extends GridTestNode {
        /** */
        private final UUID nodeId;

        /** */
        private final int replicas;

        /**
         * Externalizable class requires public no-arg constructor.
         */
        @SuppressWarnings("UnusedDeclaration")
        private TestRichNode(int replicas) {
            this(UUID.randomUUID(), replicas);
        }

        /**
         * Constructs rich node stub to use in emulated server topology.
         *
         * @param nodeId Node id.
         */
        private TestRichNode(UUID nodeId, int replicas) {
            this.nodeId = nodeId;
            this.replicas = replicas;
        }

        /**
         * Unused constructor for externalizable support.
         */
        public TestRichNode() {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        @Override public UUID id() {
            return nodeId;
        }

        /** {@inheritDoc} */
        @Override public <T> T attribute(String name) {
            if (DFLT_REPLICA_COUNT_ATTR_NAME.equals(name))
                return (T)new Integer(replicas);

            return super.attribute(name);
        }
    }
}

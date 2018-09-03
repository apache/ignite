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

package org.apache.ignite.internal.processors.cache;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.GridTestNode;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

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
                Collection<ClusterNode> nodes = createNodes(i, replicas);

                RendezvousAffinityFunction aff = new RendezvousAffinityFunction(false, 10000);

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
    private Collection<ClusterNode> createNodes(int nodesCnt, int replicas) {
        Collection<ClusterNode> nodes = new ArrayList<>(nodesCnt);

        for (int i = 0; i < nodesCnt; i++)
            nodes.add(new TestRichNode(replicas));

        return nodes;
    }

    /**
     * @param aff Affinity to check.
     * @param nodes Collection of nodes to test on.
     */
    private void checkDistribution(RendezvousAffinityFunction aff, Collection<ClusterNode> nodes) {
        Map<ClusterNode, Integer> parts = new HashMap<>(nodes.size());

        for (int part = 0; part < aff.getPartitions(); part++) {
            Collection<ClusterNode> affNodes = aff.assignPartition(null,
                part,
                new ArrayList<>(nodes),
                new HashMap<ClusterNode, byte[]>(),
                0,
                null);

            assertEquals(1, affNodes.size());

            ClusterNode node = F.first(affNodes);

            parts.put(node, parts.get(node) != null ? parts.get(node) + 1 : 1);
        }

        int min = Integer.MAX_VALUE;
        int max = Integer.MIN_VALUE;
        int total = 0;

        float mean = 0;
        float m2 = 0;
        int n = 0;

        for (ClusterNode node : nodes) {
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

        System.out.printf("%6s, %6s, %6s, %8.4f\n", nodes.size(),min, max, Math.sqrt(m2));
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
            return super.attribute(name);
        }
    }
}
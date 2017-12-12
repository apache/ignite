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

package org.apache.ignite.spi.discovery;

import java.util.BitSet;
import java.util.List;
import org.apache.ignite.cluster.ClusterNode;

/**
 *
 */
public class DefaultCommunicationProblemResolver implements CommunicationProblemResolver {
    /** {@inheritDoc} */
    @Override public void resolve(CommunicationProblemContext ctx) {
        ClusterGraph graph = new ClusterGraph(ctx);

        BitSet cluster = graph.findLargestIndependentCluster();

        List<ClusterNode> nodes = ctx.topologySnapshot();

        if (graph.checkFullyConnected(cluster) && cluster.cardinality() < nodes.size()) {
            for (int i = 0; i < nodes.size(); i++) {
                if (!cluster.get(i))
                    ctx.killNode(nodes.get(i));
            }
        }
    }

    /**
     *
     */
    private static class ClusterGraph {
        /** */
        private final static int WORD_IDX_SHIFT = 6;

        /**
         * @param bitIndex Bit index.
         * @return Word index containing bit with given index.
         */
        private static int wordIndex(int bitIndex) {
            return bitIndex >> WORD_IDX_SHIFT;
        }

        /** */
        private final int nodeCnt;

        /** */
        private final long[] visitBitSet;

        /** */
        private final CommunicationProblemContext ctx;

        /** */
        private final List<ClusterNode> nodes;

        ClusterGraph(CommunicationProblemContext ctx) {
            this.ctx = ctx;

            nodes = ctx.topologySnapshot();

            nodeCnt = nodes.size();

            assert nodeCnt > 0;

            visitBitSet = initBitSet(nodeCnt);
        }

        static long[] initBitSet(int bitCnt) {
            return new long[wordIndex(bitCnt - 1) + 1];
        }

        BitSet findLargestIndependentCluster() {
            BitSet maxCluster = null;
            int maxClusterSize = 0;

            for (int i = 0; i < nodeCnt; i++) {
                if (getBit(visitBitSet, i))
                    continue;

                BitSet cluster = new BitSet(nodeCnt);

                search(cluster, i);

                int size = cluster.cardinality();

                if (maxCluster == null || size > maxClusterSize) {
                    maxCluster = cluster;
                    maxClusterSize = size;
                }
            }

            return maxCluster;
        }

        boolean checkFullyConnected(BitSet cluster) {
            int startIdx = 0;

            int clusterNodes = cluster.cardinality();

            for (;;) {
                int idx = cluster.nextSetBit(startIdx);

                if (idx == -1)
                    break;

                ClusterNode node1 = nodes.get(idx);

                for (int i = 0; i < clusterNodes; i++) {
                    if (!cluster.get(i) || i == idx)
                        continue;

                    ClusterNode node2 = nodes.get(i);

                    if (cluster.get(i) && ctx.connectionAvailable(node1, node2))
                        return false;
                }

                startIdx = idx + 1;
            }

            return true;
        }

        void search(BitSet cluster, int idx) {
            setBit(visitBitSet, idx);

            cluster.set(idx);

            ClusterNode node1 = nodes.get(idx);

            for (int i = 0; i < nodeCnt; i++) {
                if (i == idx || getBit(visitBitSet, i))
                    continue;

                ClusterNode node2 = nodes.get(i);

                boolean connected = ctx.connectionAvailable(node1, node2) ||
                    ctx.connectionAvailable(node2, node1);

                if (connected)
                    search(cluster, i);
            }
        }

        static void setBit(long words[], int bitIndex) {
            int wordIndex = wordIndex(bitIndex);

            words[wordIndex] |= (1L << bitIndex);
        }

        static boolean getBit(long[] words, int bitIndex) {
            int wordIndex = wordIndex(bitIndex);

            return (words[wordIndex] & (1L << bitIndex)) != 0;
        }
    }
}

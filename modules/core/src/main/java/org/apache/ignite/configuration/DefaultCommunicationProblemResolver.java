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

package org.apache.ignite.configuration;

import java.util.BitSet;
import java.util.List;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.resources.LoggerResource;

/**
 *
 */
public class DefaultCommunicationProblemResolver implements CommunicationProblemResolver {
    /** */
    @LoggerResource
    private IgniteLogger log;

    /** {@inheritDoc} */
    @Override public void resolve(CommunicationProblemContext ctx) {
        ClusterGraph graph = new ClusterGraph(log, ctx);

        ClusterSearch cluster = graph.findLargestIndependentCluster();

        List<ClusterNode> nodes = ctx.topologySnapshot();

        assert nodes.size() > 0;
        assert cluster != null;

        if (graph.checkFullyConnected(cluster.nodesBitSet)) {
            assert cluster.nodeCnt <= nodes.size();

            if (cluster.nodeCnt < nodes.size()) {
                if (log.isInfoEnabled()) {
                    log.info("Communication problem resolver found fully connected independent cluster [" +
                        "clusterSrvCnt=" + cluster.srvCnt +
                        ", clusterTotalNodes=" + cluster.nodeCnt +
                        ", totalAliveNodes=" + nodes.size() + "]");
                }

                for (int i = 0; i < nodes.size(); i++) {
                    if (!cluster.nodesBitSet.get(i))
                        ctx.killNode(nodes.get(i));
                }
            }
            else
                U.warn(log, "All alive nodes are fully connected, this should be resolved automatically.");
        }
        else {
            if (log.isInfoEnabled()) {
                log.info("Communication problem resolver failed to find fully connected independent cluster.");
            }
        }
    }

    /**
     * @param cluster Cluster nodes mask.
     * @param nodes Nodes.
     * @param limit IDs limit.
     * @return Cluster node IDs string.
     */
    private static String clusterNodeIds(BitSet cluster, List<ClusterNode> nodes, int limit) {
        int startIdx = 0;

        StringBuilder builder = new StringBuilder();

        int cnt = 0;

        for (;;) {
            int idx = cluster.nextSetBit(startIdx);

            if (idx == -1)
                break;

            startIdx = idx + 1;

            if (builder.length() == 0) {
                builder.append('[');
            }
            else
                builder.append(", ");

            builder.append(nodes.get(idx).id());

            if (cnt++ > limit)
                builder.append(", ...");
        }

        builder.append(']');

        return builder.toString();
    }

    /**
     *
     */
    private static class ClusterSearch {
        /** */
        int srvCnt;

        /** */
        int nodeCnt;

        /** */
        final BitSet nodesBitSet;

        /**
         * @param nodes Total nodes.
         */
        ClusterSearch(int nodes) {
            nodesBitSet = new BitSet(nodes);
        }
    }

    /**
     *
     */
    private static class ClusterGraph {
        /** */
        private final static int WORD_IDX_SHIFT = 6;

        /** */
        private final IgniteLogger log;

        /** */
        private final int nodeCnt;

        /** */
        private final long[] visitBitSet;

        /** */
        private final CommunicationProblemContext ctx;

        /** */
        private final List<ClusterNode> nodes;

        /**
         * @param log Logger.
         * @param ctx Context.
         */
        ClusterGraph(IgniteLogger log, CommunicationProblemContext ctx) {
            this.log = log;
            this.ctx = ctx;

            nodes = ctx.topologySnapshot();

            nodeCnt = nodes.size();

            assert nodeCnt > 0;

            visitBitSet = initBitSet(nodeCnt);
        }

        /**
         * @param bitIndex Bit index.
         * @return Word index containing bit with given index.
         */
        private static int wordIndex(int bitIndex) {
            return bitIndex >> WORD_IDX_SHIFT;
        }

        /**
         * @param bitCnt Number of bits.
         * @return Bit set words.
         */
        static long[] initBitSet(int bitCnt) {
            return new long[wordIndex(bitCnt - 1) + 1];
        }

        /**
         * @return Cluster nodes bit set.
         */
        ClusterSearch findLargestIndependentCluster() {
            ClusterSearch maxCluster = null;

            for (int i = 0; i < nodeCnt; i++) {
                if (getBit(visitBitSet, i))
                    continue;

                ClusterSearch cluster = new ClusterSearch(nodeCnt);

                search(cluster, i);

                if (log.isInfoEnabled()) {
                    log.info("Communication problem resolver found cluster [srvCnt=" + cluster.srvCnt +
                        ", totalNodeCnt=" + cluster.nodeCnt +
                        ", nodeIds=" + clusterNodeIds(cluster.nodesBitSet, nodes, 1000) + "]");
                }

                if (maxCluster == null || cluster.srvCnt > maxCluster.srvCnt)
                    maxCluster = cluster;
            }

            return maxCluster;
        }

        /**
         * @param cluster Cluster nodes bit set.
         * @return {@code True} if all cluster nodes are able to connect to each other.
         */
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

                    if (cluster.get(i) && !ctx.connectionAvailable(node1, node2))
                        return false;
                }

                startIdx = idx + 1;
            }

            return true;
        }

        /**
         * @param cluster Current cluster bit set.
         * @param idx Node index.
         */
        void search(ClusterSearch cluster, int idx) {
            assert !getBit(visitBitSet, idx);

            setBit(visitBitSet, idx);

            cluster.nodesBitSet.set(idx);
            cluster.nodeCnt++;

            ClusterNode node1 = nodes.get(idx);

            if (!CU.clientNode(node1))
                cluster.srvCnt++;

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

        /**
         * @param words Bit set words.
         * @param bitIndex Bit index.
         */
        static void setBit(long words[], int bitIndex) {
            int wordIndex = wordIndex(bitIndex);

            words[wordIndex] |= (1L << bitIndex);
        }

        /**
         * @param words Bit set words.
         * @param bitIndex Bit index.
         * @return Bit value.
         */
        static boolean getBit(long[] words, int bitIndex) {
            int wordIndex = wordIndex(bitIndex);

            return (words[wordIndex] & (1L << bitIndex)) != 0;
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(DefaultCommunicationProblemResolver.class, this);
    }
}

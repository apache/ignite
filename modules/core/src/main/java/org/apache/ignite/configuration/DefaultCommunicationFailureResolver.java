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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.cluster.graph.BitSetIterator;
import org.apache.ignite.internal.cluster.graph.ClusterGraph;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.resources.LoggerResource;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Default Communication Failure Resolver.
 */
public class DefaultCommunicationFailureResolver implements CommunicationFailureResolver {
    /** */
    @LoggerResource
    private IgniteLogger log;

    /** {@inheritDoc} */
    @Override public void resolve(CommunicationFailureContext ctx) {
        ClusterPart largestCluster = findLargestConnectedCluster(ctx);

        if (largestCluster == null)
            return;

        log.info("Communication problem resolver found fully connected independent cluster ["
            + "serverNodesCnt=" + largestCluster.srvNodesCnt + ", "
            + "clientNodesCnt=" + largestCluster.connectedClients.size() + ", "
            + "totalAliveNodes=" + ctx.topologySnapshot().size() + ", "
            + "serverNodesIds=" + clusterNodeIds(largestCluster.srvNodesSet, ctx.topologySnapshot(), 1000) + "]");

        keepCluster(ctx, largestCluster);
    }

    /**
     * Finds largest part of the cluster where each node is able to connect to each other.
     *
     * @param ctx Communication failure context.
     * @return Largest part of the cluster nodes to keep.
     */
    @Nullable private ClusterPart findLargestConnectedCluster(CommunicationFailureContext ctx) {
        List<ClusterNode> srvNodes = ctx.topologySnapshot()
            .stream()
            .filter(node -> !node.isClient())
            .collect(Collectors.toList());

        // Exclude client nodes from analysis.
        ClusterGraph graph = new ClusterGraph(ctx, ClusterNode::isClient);

        List<BitSet> components = graph.findConnectedComponents();

        if (components.isEmpty()) {
            U.warn(log, "Unable to find at least one alive server node in the cluster " + ctx);

            return null;
        }

        if (components.size() == 1) {
            BitSet nodesSet = components.get(0);
            int nodeCnt = nodesSet.cardinality();

            boolean fullyConnected = graph.checkFullyConnected(nodesSet);

            if (fullyConnected && nodeCnt == srvNodes.size()) {
                U.warn(log, "All alive nodes are fully connected, this should be resolved automatically.");

                return null;
            }

            if (log.isInfoEnabled())
                log.info("Communication problem resolver detected partial lost for some connections inside cluster. "
                    + "Will keep largest set of healthy fully-connected nodes. Other nodes will be killed forcibly.");

            BitSet fullyConnectedPart = graph.findLargestFullyConnectedComponent(nodesSet);
            Set<ClusterNode> connectedClients = findConnectedClients(ctx, fullyConnectedPart);

            return new ClusterPart(fullyConnectedPart, connectedClients);
        }

        // If cluster has splitted on several parts and there are at least 2 parts which aren't single node
        // It means that split brain has happened.
        boolean isSplitBrain = components.size() > 1 &&
            components.stream().filter(cmp -> cmp.size() > 1).count() > 1;

        if (isSplitBrain)
            U.warn(log, "Communication problem resolver detected split brain. "
                + "Cluster has splitted on " + components.size() + " independent parts. "
                + "Will keep only one largest fully-connected part. "
                + "Other nodes will be killed forcibly.");
        else
            U.warn(log, "Communication problem resolver detected full lost for some connections inside cluster. "
                + "Problem nodes will be found and killed forcibly.");

        // For each part of splitted cluster extract largest fully-connected component.
        ClusterPart largestCluster = null;
        for (int i = 0; i < components.size(); i++) {
            BitSet clusterPart = components.get(i);

            BitSet fullyConnectedPart = graph.findLargestFullyConnectedComponent(clusterPart);
            Set<ClusterNode> connectedClients = findConnectedClients(ctx, fullyConnectedPart);

            ClusterPart curr = new ClusterPart(fullyConnectedPart, connectedClients);

            if (largestCluster == null || curr.compareTo(largestCluster) > 0)
                largestCluster = curr;
        }

        assert largestCluster != null
            : "Unable to find at least one alive independent cluster.";

        return largestCluster;
    }

    /**
     * Keeps server cluster nodes presented in given {@code srvNodesSet}.
     * Client nodes which have connections to presented {@code srvNodesSet} will be also keeped.
     * Other nodes will be killed forcibly.
     *
     * @param ctx Communication failure context.
     * @param clusterPart Set of nodes need to keep in the cluster.
     */
    private void keepCluster(CommunicationFailureContext ctx, ClusterPart clusterPart) {
        List<ClusterNode> allNodes = ctx.topologySnapshot();

        // Kill server nodes.
        for (int idx = 0; idx < allNodes.size(); idx++) {
            ClusterNode node = allNodes.get(idx);

            // Client nodes will be processed separately.
            if (node.isClient())
                continue;

            if (!clusterPart.srvNodesSet.get(idx))
                ctx.killNode(node);
        }

        // Kill client nodes unable to connect to the presented part of cluster.
        for (int idx = 0; idx < allNodes.size(); idx++) {
            ClusterNode node = allNodes.get(idx);

            if (node.isClient() && !clusterPart.connectedClients.contains(node))
                ctx.killNode(node);
        }
    }

    /**
     * Finds set of the client nodes which are able to connect to given set of server nodes {@code srvNodesSet}.
     *
     * @param ctx Communication failure context.
     * @param srvNodesSet Server nodes set.
     * @return Set of client nodes.
     */
    private Set<ClusterNode> findConnectedClients(CommunicationFailureContext ctx, BitSet srvNodesSet) {
        Set<ClusterNode> connectedClients = new HashSet<>();

        List<ClusterNode> allNodes = ctx.topologySnapshot();

        for (ClusterNode node : allNodes) {
            if (!node.isClient())
                continue;

            boolean hasConnections = true;

            Iterator<Integer> it = new BitSetIterator(srvNodesSet);
            while (it.hasNext()) {
                int srvNodeIdx = it.next();
                ClusterNode srvNode = allNodes.get(srvNodeIdx);

                if (!ctx.connectionAvailable(node, srvNode) || !ctx.connectionAvailable(srvNode, node)) {
                    hasConnections = false;

                    break;
                }
            }

            if (hasConnections)
                connectedClients.add(node);
        }

        return connectedClients;
    }

    /**
     * Class representing part of cluster.
     */
    private static class ClusterPart implements Comparable<ClusterPart> {
        /** Server nodes count. */
        int srvNodesCnt;

        /** Server nodes set. */
        BitSet srvNodesSet;

        /** Set of client nodes are able to connect to presented part of server nodes. */
        Set<ClusterNode> connectedClients;

        /**
         * Constructor.
         *
         * @param srvNodesSet Server nodes set.
         * @param connectedClients Set of client nodes.
         */
        public ClusterPart(BitSet srvNodesSet, Set<ClusterNode> connectedClients) {
            this.srvNodesSet = srvNodesSet;
            this.srvNodesCnt = srvNodesSet.cardinality();
            this.connectedClients = connectedClients;
        }

        /** {@inheritDoc} */
        @Override public int compareTo(@NotNull ClusterPart o) {
            int srvNodesCmp = Integer.compare(srvNodesCnt, o.srvNodesCnt);

            if (srvNodesCmp != 0)
                return srvNodesCmp;

            return Integer.compare(connectedClients.size(), o.connectedClients.size());
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

            if (builder.length() == 0)
                builder.append('[');
            else
                builder.append(", ");

            builder.append(nodes.get(idx).id());

            if (cnt++ > limit)
                builder.append(", ...");
        }

        builder.append(']');

        return builder.toString();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(DefaultCommunicationFailureResolver.class, this);
    }
}

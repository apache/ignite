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
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
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
            + "serverNodesIds=" + clusterNodeIds(largestCluster.nodesSet, ctx.topologySnapshot(), 1000) + "]");

        keepCluster(ctx, largestCluster);
    }

    @Nullable private ClusterPart findLargestConnectedCluster(CommunicationFailureContext ctx) {
        List<ClusterNode> srvNodes = ctx.topologySnapshot()
            .stream()
            .filter(node -> !CU.clientNode(node))
            .collect(Collectors.toList());

        ClusterGraph graph = new ClusterGraph(log, ctx, CU::clientNode);

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
            Set<ClusterNode> connectedClients = connectedClients(ctx.topologySnapshot(), fullyConnectedPart);

            return new ClusterPart(fullyConnectedPart, connectedClients);
        }

        // If cluster has splitted on several parts and there are at least 2 parts which aren't single node
        // It means that split brain has happened.
        boolean isSplitBrain = components.size() > 1 &&
            components.stream().filter(cmp -> cmp.size() > 1).count() >= 2;

        if (isSplitBrain)
            U.warn(log, "Communication problem resolver detected split brain. "
                + "Cluster has splitted on " + components.size() + " independent parts. "
                + "Will keep only one largest fully-connected part. "
                + "Other nodes will be killed forcibly.");
        else
            U.warn(log, "Communication problem resolver detected full lost for some connections inside cluster. "
                + "Problem nodes will be found and killed forcibly.");

        ClusterPart largestCluster = null;
        for (int i = 0; i < components.size(); i++) {
            BitSet clusterPart = components.get(i);

            BitSet fullyConnectedPart = graph.findLargestFullyConnectedComponent(clusterPart);
            Set<ClusterNode> connectedClients = connectedClients(ctx.topologySnapshot(), fullyConnectedPart);

            ClusterPart current = new ClusterPart(fullyConnectedPart, connectedClients);

            if (largestCluster == null || current.compareTo(largestCluster) > 0)
                largestCluster = current;
        }

        assert largestCluster != null
            : "Unable to find at least one alive independent cluster.";

        return largestCluster;
    }

    /**
     * Keeps in the server cluster nodes presented in given {@code nodesSet}.
     * Client nodes which have connections to presented {@code nodesSet} will be also keeped.
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
            if (CU.clientNode(node))
                continue;

            if (!clusterPart.nodesSet.get(idx))
                ctx.killNode(node);
        }

        // Kill client nodes unable to connect to the presented part of cluster.
        for (int idx = 0; idx < allNodes.size(); idx++) {
            ClusterNode node = allNodes.get(idx);

            if (CU.clientNode(node) && !clusterPart.connectedClients.contains(node))
                ctx.killNode(node);
        }
    }

    /**
     * Find set of the client nodes which are able to connect to given set of server nodes {@code srvNodesSet}.
     *
     * @param allNodes All nodes.
     * @param srvNodesSet Server nodes set.
     * @return Set of client nodes.
     */
    private Set<ClusterNode> connectedClients(List<ClusterNode> allNodes, BitSet srvNodesSet) {
        return Collections.emptySet();
    }

    private static class ClusterPart implements Comparable<ClusterPart> {
        int srvNodesCnt;

        BitSet nodesSet;

        Set<ClusterNode> connectedClients;

        public ClusterPart(BitSet nodesSet, Set<ClusterNode> connectedClients) {
            this.nodesSet = nodesSet;
            this.srvNodesCnt = nodesSet.cardinality();
            this.connectedClients = connectedClients;
        }

        @Override public int compareTo(@NotNull ClusterPart o) {
            int srvNodesCmp = Integer.compare(o.srvNodesCnt, srvNodesCnt);

            if (srvNodesCmp != 0)
                return srvNodesCmp;

            return Integer.compare(o.connectedClients.size(), connectedClients.size());
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

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(DefaultCommunicationFailureResolver.class, this);
    }
}

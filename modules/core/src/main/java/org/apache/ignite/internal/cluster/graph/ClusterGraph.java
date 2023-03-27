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

package org.apache.ignite.internal.cluster.graph;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Iterator;
import java.util.List;
import java.util.function.Predicate;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CommunicationFailureContext;

/**
 * Class to represent cluster nodes avalaible connections as graph.
 * Provides several graph algorithms to analyze cluster nodes connections.
 */
public class ClusterGraph {
    /** Number of all cluster nodes. */
    private final int nodeCnt;

    /** List of the all cluster nodes. */
    private final List<ClusterNode> nodes;

    /** Connectivity (adjacency) matrix between cluster nodes. */
    private final BitSet[] connections;

    /** Fully-connected component searcher. */
    private final FullyConnectedComponentSearcher fccSearcher;

    /**
     * Constructor.
     *
     * @param ctx Communication failure context.
     * @param nodeFilterOut Filter to exclude some cluster nodes from graph.
     */
    public ClusterGraph(CommunicationFailureContext ctx, Predicate<ClusterNode> nodeFilterOut) {
        nodes = ctx.topologySnapshot();

        nodeCnt = nodes.size();

        assert nodeCnt > 0;

        connections = buildConnectivityMatrix(ctx, nodeFilterOut);

        fccSearcher = new FullyConnectedComponentSearcher(connections);
    }

    /**
     * Builds connectivity matrix (adjacency matrix) for all cluster nodes.
     *
     * @param ctx Communication failure context.
     * @param nodeFilterOut Filter to exclude some cluster nodes from graph.
     * @return Connections bit set for each node, where set bit means avalable connection.
     */
    private BitSet[] buildConnectivityMatrix(CommunicationFailureContext ctx, Predicate<ClusterNode> nodeFilterOut) {
        BitSet[] connections = new BitSet[nodeCnt];

        for (int i = 0; i < nodeCnt; i++) {
            ClusterNode node = nodes.get(i);

            if (nodeFilterOut.test(node)) {
                connections[i] = null;
                continue;
            }

            connections[i] = new BitSet(nodeCnt);
            for (int j = 0; j < nodeCnt; j++) {
                ClusterNode to = nodes.get(j);

                if (nodeFilterOut.test(to))
                    continue;

                if (i == j || ctx.connectionAvailable(node, to))
                    connections[i].set(j);
            }
        }

        // Remove unidirectional connections (node A can connect to B, but B can't connect to A).
        for (int i = 0; i < nodeCnt; i++)
            for (int j = i + 1; j < nodeCnt; j++) {
                if (connections[i] == null || connections[j] == null)
                    continue;

                if (connections[i].get(j) ^ connections[j].get(i)) {
                    connections[i].set(j, false);
                    connections[j].set(i, false);
                }
            }

        return connections;
    }

    /**
     * Finds connected components in cluster graph.
     *
     * @return List of set of nodes, each set represents connected component.
     */
    public List<BitSet> findConnectedComponents() {
        List<BitSet> connectedComponets = new ArrayList<>();

        BitSet visitSet = new BitSet(nodeCnt);

        for (int i = 0; i < nodeCnt; i++) {
            if (visitSet.get(i) || connections[i] == null)
                continue;

            BitSet currComponent = new BitSet(nodeCnt);

            dfs(i, currComponent, visitSet);

            connectedComponets.add(currComponent);
        }

        return connectedComponets;
    }

    /**
     * Deep-first search to find connected components in connections graph.
     *
     * @param nodeIdx Current node index to traverse from.
     * @param currComponent Current connected component to populate.
     * @param allVisitSet Set of the visited nodes in whole graph during traversal.
     */
    private void dfs(int nodeIdx, BitSet currComponent, BitSet allVisitSet) {
        assert !allVisitSet.get(nodeIdx)
            : "Incorrect node visit " + nodeIdx;

        assert connections[nodeIdx] != null
            : "Incorrect node visit. Node has not passed filter " + nodes.get(nodeIdx);

        allVisitSet.set(nodeIdx);

        currComponent.set(nodeIdx);

        for (int toIdx = 0; toIdx < nodeCnt; toIdx++) {
            if (toIdx == nodeIdx || allVisitSet.get(toIdx) || connections[toIdx] == null)
                continue;

            boolean connected = connections[nodeIdx].get(toIdx) && connections[toIdx].get(nodeIdx);

            if (connected)
                dfs(toIdx, currComponent, allVisitSet);
        }
    }

    /**
     * Finds largest fully-connected component from given {@code nodesSet}.
     *
     * @param nodesSet Set of nodes.
     * @return Set of nodes which forms largest fully-connected component.
     */
    public BitSet findLargestFullyConnectedComponent(BitSet nodesSet) {
        // Check that current set is already fully connected.
        boolean fullyConnected = checkFullyConnected(nodesSet);

        if (fullyConnected)
            return nodesSet;

        BitSet res = fccSearcher.findLargest(nodesSet);

        assert checkFullyConnected(res)
            : "Not fully connected component was found [result=" + res + ", nodesSet=" + nodesSet + "]";

        return res;
    }

    /**
     * Checks that given {@code nodesSet} forms fully-connected component.
     *
     * @param nodesSet Set of cluster nodes.
     * @return {@code True} if all given cluster nodes are able to connect to each other.
     */
    public boolean checkFullyConnected(BitSet nodesSet) {
        int maxIdx = nodesSet.length();

        Iterator<Integer> it = new BitSetIterator(nodesSet);

        while (it.hasNext()) {
            int idx = it.next();

            for (int i = 0; i < maxIdx; i++) {
                if (i == idx)
                    continue;

                if (nodesSet.get(i) && !connections[idx].get(i))
                    return false;
            }
        }

        return true;
    }
}

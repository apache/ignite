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

package org.apache.ignite.hadoop.mapreduce;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.igfs.IgfsBlockLocation;
import org.apache.ignite.igfs.IgfsPath;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.hadoop.HadoopFileBlock;
import org.apache.ignite.internal.processors.hadoop.HadoopInputSplit;
import org.apache.ignite.internal.processors.hadoop.HadoopJob;
import org.apache.ignite.internal.processors.hadoop.HadoopMapReducePlan;
import org.apache.ignite.internal.processors.hadoop.HadoopMapReducePlanner;
import org.apache.ignite.internal.processors.hadoop.igfs.HadoopIgfsEndpoint;
import org.apache.ignite.internal.processors.hadoop.planner.HadoopDefaultMapReducePlan;
import org.apache.ignite.internal.processors.igfs.IgfsEx;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.IgniteFileSystem.IGFS_SCHEME;

/**
 * Default map-reduce planner implementation.
 */
public class IgniteHadoopMapReducePlanner implements HadoopMapReducePlanner {
    /** Injected grid. */
    @IgniteInstanceResource
    private Ignite ignite;

    /** Logger. */
    @SuppressWarnings("UnusedDeclaration")
    @LoggerResource
    private IgniteLogger log;

    /** {@inheritDoc} */
    @Override public HadoopMapReducePlan preparePlan(HadoopJob job, Collection<ClusterNode> top,
        @Nullable HadoopMapReducePlan oldPlan) throws IgniteCheckedException {
        // Convert collection of topology nodes to collection of topology node IDs.
        Collection<UUID> topIds = new HashSet<>(top.size(), 1.0f);

        for (ClusterNode topNode : top)
            topIds.add(topNode.id());

        Map<UUID, Collection<HadoopInputSplit>> mappers = mappers(top, topIds, job.input());

        int rdcCnt = job.info().reducers();

        if (rdcCnt < 0)
            throw new IgniteCheckedException("Number of reducers must be non-negative, actual: " + rdcCnt);

        Map<UUID, int[]> reducers = reducers(top, mappers, rdcCnt);

        return new HadoopDefaultMapReducePlan(mappers, reducers);
    }

    /**
     * Create plan for mappers.
     *
     * @param top Topology nodes.
     * @param topIds Topology node IDs.
     * @param splits Splits.
     * @return Mappers map.
     * @throws IgniteCheckedException If failed.
     */
    private Map<UUID, Collection<HadoopInputSplit>> mappers(Collection<ClusterNode> top, Collection<UUID> topIds,
        Iterable<HadoopInputSplit> splits) throws IgniteCheckedException {
        Map<UUID, Collection<HadoopInputSplit>> mappers = new HashMap<>();

        Map<String, Collection<UUID>> nodes = hosts(top);

        Map<UUID, Integer> nodeLoads = new HashMap<>(top.size(), 1.0f); // Track node load.

        for (UUID nodeId : topIds)
            nodeLoads.put(nodeId, 0);

        for (HadoopInputSplit split : splits) {
            UUID nodeId = nodeForSplit(split, topIds, nodes, nodeLoads);

            if (log.isDebugEnabled())
                log.debug("Mapped split to node [split=" + split + ", nodeId=" + nodeId + ']');

            Collection<HadoopInputSplit> nodeSplits = mappers.get(nodeId);

            if (nodeSplits == null) {
                nodeSplits = new ArrayList<>();

                mappers.put(nodeId, nodeSplits);
            }

            nodeSplits.add(split);

            // Updated node load.
            nodeLoads.put(nodeId, nodeLoads.get(nodeId) + 1);
        }

        return mappers;
    }

    /**
     * Groups nodes by host names.
     *
     * @param top Topology to group.
     * @return Map.
     */
    private static Map<String, Collection<UUID>> hosts(Collection<ClusterNode> top) {
        Map<String, Collection<UUID>> grouped = U.newHashMap(top.size());

        for (ClusterNode node : top) {
            for (String host : node.hostNames()) {
                Collection<UUID> nodeIds = grouped.get(host);

                if (nodeIds == null) {
                    // Expecting 1-2 nodes per host.
                    nodeIds = new ArrayList<>(2);

                    grouped.put(host, nodeIds);
                }

                nodeIds.add(node.id());
            }
        }

        return grouped;
    }

    /**
     * Determine the best node for this split.
     *
     * @param split Split.
     * @param topIds Topology node IDs.
     * @param nodes Nodes.
     * @param nodeLoads Node load tracker.
     * @return Node ID.
     */
    @SuppressWarnings("unchecked")
    private UUID nodeForSplit(HadoopInputSplit split, Collection<UUID> topIds, Map<String, Collection<UUID>> nodes,
        Map<UUID, Integer> nodeLoads) throws IgniteCheckedException {
        if (split instanceof HadoopFileBlock) {
            HadoopFileBlock split0 = (HadoopFileBlock)split;

            if (IGFS_SCHEME.equalsIgnoreCase(split0.file().getScheme())) {
                HadoopIgfsEndpoint endpoint = new HadoopIgfsEndpoint(split0.file().getAuthority());

                IgfsEx igfs = null;

                if (F.eq(ignite.name(), endpoint.grid()))
                    igfs = (IgfsEx)((IgniteEx)ignite).igfsx(endpoint.igfs());

                if (igfs != null && !igfs.isProxy(split0.file())) {
                    Collection<IgfsBlockLocation> blocks;

                    try {
                        blocks = igfs.affinity(new IgfsPath(split0.file()), split0.start(), split0.length());
                    }
                    catch (IgniteException e) {
                        throw new IgniteCheckedException(e);
                    }

                    assert blocks != null;

                    if (blocks.size() == 1)
                        // Fast-path, split consists of one IGFS block (as in most cases).
                        return bestNode(blocks.iterator().next().nodeIds(), topIds, nodeLoads, false);
                    else {
                        // Slow-path, file consists of multiple IGFS blocks. First, find the most co-located nodes.
                        Map<UUID, Long> nodeMap = new HashMap<>();

                        List<UUID> bestNodeIds = null;
                        long bestLen = -1L;

                        for (IgfsBlockLocation block : blocks) {
                            for (UUID blockNodeId : block.nodeIds()) {
                                if (topIds.contains(blockNodeId)) {
                                    Long oldLen = nodeMap.get(blockNodeId);
                                    long newLen = oldLen == null ? block.length() : oldLen + block.length();

                                    nodeMap.put(blockNodeId, newLen);

                                    if (bestNodeIds == null || bestLen < newLen) {
                                        bestNodeIds = new ArrayList<>(1);

                                        bestNodeIds.add(blockNodeId);

                                        bestLen = newLen;
                                    }
                                    else if (bestLen == newLen) {
                                        assert !F.isEmpty(bestNodeIds);

                                        bestNodeIds.add(blockNodeId);
                                    }
                                }
                            }
                        }

                        if (bestNodeIds != null) {
                            return bestNodeIds.size() == 1 ? bestNodeIds.get(0) :
                                bestNode(bestNodeIds, topIds, nodeLoads, true);
                        }
                    }
                }
            }
        }

        // Cannot use local IGFS for some reason, try selecting the node by host.
        Collection<UUID> blockNodes = null;

        for (String host : split.hosts()) {
            Collection<UUID> hostNodes = nodes.get(host);

            if (!F.isEmpty(hostNodes)) {
                if (blockNodes == null)
                    blockNodes = new ArrayList<>(hostNodes);
                else
                    blockNodes.addAll(hostNodes);
            }
        }

        return bestNode(blockNodes, topIds, nodeLoads, false);
    }

    /**
     * Finds the best (the least loaded) node among the candidates.
     *
     * @param candidates Candidates.
     * @param topIds Topology node IDs.
     * @param nodeLoads Known node loads.
     * @param skipTopCheck Whether to skip topology check.
     * @return The best node.
     */
    private UUID bestNode(@Nullable Collection<UUID> candidates, Collection<UUID> topIds, Map<UUID, Integer> nodeLoads,
        boolean skipTopCheck) {
        UUID bestNode = null;
        int bestLoad = Integer.MAX_VALUE;

        if (candidates != null) {
            for (UUID candidate : candidates) {
                if (skipTopCheck || topIds.contains(candidate)) {
                    int load = nodeLoads.get(candidate);

                    if (bestNode == null || bestLoad > load) {
                        bestNode = candidate;
                        bestLoad = load;

                        if (bestLoad == 0)
                            break; // Minimum load possible, no need for further iterations.
                    }
                }
            }
        }

        if (bestNode == null) {
            // Blocks are located on nodes which are not Hadoop-enabled, assign to the least loaded one.
            bestLoad = Integer.MAX_VALUE;

            for (UUID nodeId : topIds) {
                int load = nodeLoads.get(nodeId);

                if (bestNode == null || bestLoad > load) {
                    bestNode = nodeId;
                    bestLoad = load;

                    if (bestLoad == 0)
                        break; // Minimum load possible, no need for further iterations.
                }
            }
        }

        assert bestNode != null;

        return bestNode;
    }

    /**
     * Create plan for reducers.
     *
     * @param top Topology.
     * @param mappers Mappers map.
     * @param reducerCnt Reducers count.
     * @return Reducers map.
     */
    private Map<UUID, int[]> reducers(Collection<ClusterNode> top,
        Map<UUID, Collection<HadoopInputSplit>> mappers, int reducerCnt) {
        // Determine initial node weights.
        int totalWeight = 0;

        List<WeightedNode> nodes = new ArrayList<>(top.size());

        for (ClusterNode node : top) {
            Collection<HadoopInputSplit> split = mappers.get(node.id());

            int weight = reducerNodeWeight(node, split != null ? split.size() : 0);

            nodes.add(new WeightedNode(node.id(), weight, weight));

            totalWeight += weight;
        }

        // Adjust weights.
        int totalAdjustedWeight = 0;

        for (WeightedNode node : nodes) {
            node.floatWeight = ((float)node.weight * reducerCnt) / totalWeight;

            node.weight = Math.round(node.floatWeight);

            totalAdjustedWeight += node.weight;
        }

        // Apply redundant/lost reducers.
        Collections.sort(nodes);

        if (totalAdjustedWeight > reducerCnt) {
            // Too much reducers set.
            ListIterator<WeightedNode> iter = nodes.listIterator(nodes.size() - 1);

            while (totalAdjustedWeight != reducerCnt) {
                if (!iter.hasPrevious())
                    iter = nodes.listIterator(nodes.size() - 1);

                WeightedNode node = iter.previous();

                if (node.weight > 0) {
                    node.weight -= 1;

                    totalAdjustedWeight--;
                }
            }
        }
        else if (totalAdjustedWeight < reducerCnt) {
            // Not enough reducers set.
            ListIterator<WeightedNode> iter = nodes.listIterator(0);

            while (totalAdjustedWeight != reducerCnt) {
                if (!iter.hasNext())
                    iter = nodes.listIterator(0);

                WeightedNode node = iter.next();

                if (node.floatWeight > 0.0f) {
                    node.weight += 1;

                    totalAdjustedWeight++;
                }
            }
        }

        int idx = 0;

        Map<UUID, int[]> reducers = new HashMap<>(nodes.size(), 1.0f);

        for (WeightedNode node : nodes) {
            if (node.weight > 0) {
                int[] arr = new int[node.weight];

                for (int i = 0; i < arr.length; i++)
                    arr[i] = idx++;

                reducers.put(node.nodeId, arr);
            }
        }

        return reducers;
    }

    /**
     * Calculate node weight based on node metrics and data co-location.
     *
     * @param node Node.
     * @param splitCnt Splits mapped to this node.
     * @return Node weight.
     */
    @SuppressWarnings("UnusedParameters")
    protected int reducerNodeWeight(ClusterNode node, int splitCnt) {
        return splitCnt;
    }

    /**
     * Weighted node.
     */
    private static class WeightedNode implements Comparable<WeightedNode> {
        /** Node ID. */
        private final UUID nodeId;

        /** Weight. */
        private int weight;

        /** Floating point weight. */
        private float floatWeight;

        /**
         * Constructor.
         *
         * @param nodeId Node ID.
         * @param weight Weight.
         * @param floatWeight Floating point weight.
         */
        private WeightedNode(UUID nodeId, int weight, float floatWeight) {
            this.nodeId = nodeId;
            this.weight = weight;
            this.floatWeight = floatWeight;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object obj) {
            return obj != null && obj instanceof WeightedNode && F.eq(nodeId, ((WeightedNode)obj).nodeId);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return nodeId.hashCode();
        }

        /** {@inheritDoc} */
        @Override public int compareTo(@NotNull WeightedNode other) {
            float res = other.floatWeight - floatWeight;

            return res > 0.0f ? 1 : res < 0.0f ? -1 : nodeId.compareTo(other.nodeId);
        }
    }
}
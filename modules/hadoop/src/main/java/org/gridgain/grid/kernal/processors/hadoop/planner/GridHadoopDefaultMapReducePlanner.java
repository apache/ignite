/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.planner;

import org.apache.ignite.*;
import org.gridgain.grid.*;
import org.gridgain.grid.ggfs.*;
import org.gridgain.grid.hadoop.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.ggfs.hadoop.*;
import org.gridgain.grid.kernal.processors.ggfs.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.resources.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.util.*;

import static org.gridgain.grid.ggfs.GridGgfs.*;

/**
 * Default map-reduce planner implementation.
 */
public class GridHadoopDefaultMapReducePlanner implements GridHadoopMapReducePlanner {
    /** Injected grid. */
    @GridInstanceResource
    private Ignite ignite;

    /** Logger. */
    @SuppressWarnings("UnusedDeclaration")
    @GridLoggerResource
    private GridLogger log;

    /** {@inheritDoc} */
    @Override public GridHadoopMapReducePlan preparePlan(GridHadoopJob job, Collection<GridNode> top,
        @Nullable GridHadoopMapReducePlan oldPlan) throws GridException {
        // Convert collection of topology nodes to collection of topology node IDs.
        Collection<UUID> topIds = new HashSet<>(top.size(), 1.0f);

        for (GridNode topNode : top)
            topIds.add(topNode.id());

        Map<UUID, Collection<GridHadoopInputSplit>> mappers = mappers(top, topIds, job.input());

        int rdcCnt = job.info().reducers();

        if (rdcCnt < 0)
            throw new GridException("Number of reducers must be non-negative, actual: " + rdcCnt);

        Map<UUID, int[]> reducers = reducers(top, mappers, rdcCnt);

        return new GridHadoopDefaultMapReducePlan(mappers, reducers);
    }

    /**
     * Create plan for mappers.
     *
     * @param top Topology nodes.
     * @param topIds Topology node IDs.
     * @param splits Splits.
     * @return Mappers map.
     * @throws GridException If failed.
     */
    private Map<UUID, Collection<GridHadoopInputSplit>> mappers(Collection<GridNode> top, Collection<UUID> topIds,
        Iterable<GridHadoopInputSplit> splits) throws GridException {
        Map<UUID, Collection<GridHadoopInputSplit>> mappers = new HashMap<>();

        Map<String, Collection<UUID>> nodes = hosts(top);

        Map<UUID, Integer> nodeLoads = new HashMap<>(top.size(), 1.0f); // Track node load.

        for (UUID nodeId : topIds)
            nodeLoads.put(nodeId, 0);

        for (GridHadoopInputSplit split : splits) {
            UUID nodeId = nodeForSplit(split, topIds, nodes, nodeLoads);

            if (log.isDebugEnabled())
                log.debug("Mapped split to node [split=" + split + ", nodeId=" + nodeId + ']');

            Collection<GridHadoopInputSplit> nodeSplits = mappers.get(nodeId);

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
    private static Map<String, Collection<UUID>> hosts(Collection<GridNode> top) {
        Map<String, Collection<UUID>> grouped = U.newHashMap(top.size());

        for (GridNode node : top) {
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
    private UUID nodeForSplit(GridHadoopInputSplit split, Collection<UUID> topIds, Map<String, Collection<UUID>> nodes,
        Map<UUID, Integer> nodeLoads) throws GridException {
        if (split instanceof GridHadoopFileBlock) {
            GridHadoopFileBlock split0 = (GridHadoopFileBlock)split;

            if (GGFS_SCHEME.equalsIgnoreCase(split0.file().getScheme())) {
                GridGgfsHadoopEndpoint endpoint = new GridGgfsHadoopEndpoint(split0.file().getAuthority());

                GridGgfsEx ggfs = null;

                if (F.eq(ignite.name(), endpoint.grid()))
                    ggfs = (GridGgfsEx) ((GridEx) ignite).ggfsx(endpoint.ggfs());

                if (ggfs != null && !ggfs.isProxy(split0.file())) {
                    Collection<GridGgfsBlockLocation> blocks = ggfs.affinity(new GridGgfsPath(split0.file()),
                        split0.start(), split0.length());

                    assert blocks != null;

                    if (blocks.size() == 1)
                        // Fast-path, split consists of one GGFS block (as in most cases).
                        return bestNode(blocks.iterator().next().nodeIds(), topIds, nodeLoads, false);
                    else {
                        // Slow-path, file consists of multiple GGFS blocks. First, find the most co-located nodes.
                        Map<UUID, Long> nodeMap = new HashMap<>();

                        List<UUID> bestNodeIds = null;
                        long bestLen = -1L;

                        for (GridGgfsBlockLocation block : blocks) {
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

        // Cannot use local GGFS for some reason, try selecting the node by host.
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
     * @throws GridException If failed.
     */
    private Map<UUID, int[]> reducers(Collection<GridNode> top,
        Map<UUID, Collection<GridHadoopInputSplit>> mappers, int reducerCnt) throws GridException {
        // Determine initial node weights.
        int totalWeight = 0;

        List<WeightedNode> nodes = new ArrayList<>(top.size());

        for (GridNode node : top) {
            Collection<GridHadoopInputSplit> split = mappers.get(node.id());

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
    protected int reducerNodeWeight(GridNode node, int splitCnt) {
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

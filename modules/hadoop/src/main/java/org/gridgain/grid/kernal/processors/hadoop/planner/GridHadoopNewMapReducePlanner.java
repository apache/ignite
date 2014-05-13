/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.planner;

import org.gridgain.grid.*;
import org.gridgain.grid.ggfs.*;
import org.gridgain.grid.hadoop.*;
import org.gridgain.grid.kernal.processors.ggfs.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.resources.*;
import org.gridgain.grid.util.typedef.*;
import org.jdk8.backport.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Default map-reduce planner implementation.
 */
public class GridHadoopNewMapReducePlanner implements GridHadoopMapReducePlanner {
    /** Injected grid. */
    @GridInstanceResource
    private Grid grid;

    /** Logger. */
    @SuppressWarnings("UnusedDeclaration")
    @GridLoggerResource
    private GridLogger log;

    /** {@inheritDoc} */
    @Override public GridHadoopMapReducePlan preparePlan(Collection<GridHadoopInputSplit> splits,
        Collection<GridNode> top, GridHadoopJob job, @Nullable GridHadoopMapReducePlan oldPlan) throws GridException {
        Map<UUID, Collection<GridHadoopInputSplit>> mappers = mappers(top, splits);

        Map<UUID, int[]> reducers = reducers(top, mappers, job.reducers());

        return new GridHadoopDefaultMapReducePlan(mappers, reducers);
    }

    /**
     * Create plan for mappers.
     *
     * @param top Topology.
     * @param splits Splits.
     * @return Mappers map.
     * @throws GridException If failed.
     */
    private Map<UUID, Collection<GridHadoopInputSplit>> mappers(Collection<GridNode> top,
        Iterable<GridHadoopInputSplit> splits) throws GridException {
        Map<UUID, Collection<GridHadoopInputSplit>> mappers = new HashMap<>();

        Map<String, Collection<GridNode>> nodes = hosts(top);

        for (GridHadoopInputSplit split : splits) {
            UUID nodeId = nodeForSplit(split, top, nodes);

            if (log.isDebugEnabled())
                log.debug("Mapped split to node [split=" + split + ", nodeId=" + nodeId + ']');

            Collection<GridHadoopInputSplit> nodeSplits = mappers.get(nodeId);

            if (nodeSplits == null) {
                nodeSplits = new ArrayList<>();

                mappers.put(nodeId, nodeSplits);
            }

            nodeSplits.add(split);
        }

        return mappers;
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

            while (totalAdjustedWeight != totalWeight) {
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

            while (totalAdjustedWeight != totalWeight) {
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
    private int reducerNodeWeight(GridNode node, int splitCnt) {
        // TODO: Use more intelligent approach taking in count node load, hardware, etc.
        return splitCnt;
    }

    /**
     * Determine the best node for this split.
     *
     * @param split Split.
     * @param top Topology.
     * @param nodes Nodes.
     * @return Node ID.
     */
    private UUID nodeForSplit(GridHadoopInputSplit split, Collection<GridNode> top,
        Map<String, Collection<GridNode>> nodes) throws GridException {
        if (split instanceof GridHadoopFileBlock) {
            GridHadoopFileBlock split0 = (GridHadoopFileBlock)split;

            if (GridGgfs.GGFS_SCHEME.equalsIgnoreCase(split0.file().getScheme())) {
                // TODO GG-8300: Get GGFS by name based on URI.
                GridGgfsEx ggfs = (GridGgfsEx)grid.ggfs("ggfs");

                if (ggfs != null && !ggfs.isProxy(split0.file())) {
                    Collection<GridGgfsBlockLocation> blocks = ggfs.affinity(new GridGgfsPath(split0.file()),
                        split0.start(), split0.length());

                    // Find the node hosting most of the split.
                    UUID bestNodeId = null;
                    long bestNodeLen = 0L;

                    Map<UUID, Long> nodeMap = new HashMap<>();

                    for (GridGgfsBlockLocation block : blocks) {
                        for (UUID nodeId : block.nodeIds()) {
                            Long len = nodeMap.get(nodeId);

                            long newLen;

                            if (len == null)
                                newLen = block.length();
                            else
                                newLen = len + block.length();

                            nodeMap.put(nodeId, newLen);

                            if (bestNodeId == null || bestNodeLen < newLen) {
                                bestNodeId = nodeId;
                                bestNodeLen = newLen;
                            }
                        }
                    }

                    assert bestNodeId != null;

                    return bestNodeId;
                }
            }
        }

        // Cannot use local GGFS for some reason, so fallback to straightforward approach.
        for (String host : split.hosts()) {
            GridNode node = F.first(nodes.get(host));

            if (node != null)
                return node.id();
        }

        // No nodes were selected based on host, return random node.
        int idx = ThreadLocalRandom8.current().nextInt(top.size());

        Iterator<GridNode> it = top.iterator();

        int i = 0;

        while (it.hasNext()) {
            GridNode node = it.next();

            if (i == idx)
                return node.id();

            i++;
        }

        return null;
    }

    /**
     * Groups nodes by host names.
     *
     * @param top Topology to group.
     * @return Map.
     */
    private static Map<String, Collection<GridNode>> hosts(Collection<GridNode> top) {
        Map<String, Collection<GridNode>> grouped = new HashMap<>(top.size());

        for (GridNode node : top) {
            for (String host : node.hostNames()) {
                Collection<GridNode> nodes = grouped.get(host);

                if (nodes == null) {
                    // Expecting 1-2 nodes per host.
                    nodes = new ArrayList<>(2);

                    grouped.put(host, nodes);
                }

                nodes.add(node);
            }
        }

        return grouped;
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

            return res > 0.0f ? 1 : res < 0.0f ? -1 : other.nodeId.compareTo(nodeId);
        }
    }
}

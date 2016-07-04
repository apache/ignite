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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.igfs.IgfsBlockLocation;
import org.apache.ignite.igfs.IgfsPath;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.hadoop.HadoopFileBlock;
import org.apache.ignite.internal.processors.hadoop.HadoopInputSplit;
import org.apache.ignite.internal.processors.hadoop.HadoopJob;
import org.apache.ignite.internal.processors.hadoop.HadoopMapReducePlan;
import org.apache.ignite.internal.processors.hadoop.igfs.HadoopIgfsEndpoint;
import org.apache.ignite.internal.processors.hadoop.planner.HadoopAbstractMapReducePlanner;
import org.apache.ignite.internal.processors.hadoop.planner.HadoopDefaultMapReducePlan;
import org.apache.ignite.internal.processors.hadoop.planner.HadoopMapReducePlanGroup;
import org.apache.ignite.internal.processors.hadoop.planner.HadoopMapReducePlanTopology;
import org.apache.ignite.internal.processors.igfs.IgfsEx;
import org.apache.ignite.internal.util.typedef.F;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.ignite.IgniteFileSystem.IGFS_SCHEME;

/**
 * Map-reduce planner which tries to assign map jobs to affinity nodes.
 */
public class IgniteHadoopAffinityMapReducePlanner extends HadoopAbstractMapReducePlanner {
    /** Defautl value of affinity node preference factor. */
    private static final float DFLT_AFF_NODE_PREFERENCE_FACTOR = 2.0f;

    /** Affinity node preference factor. */
    private float affNodePreferenceFactor = DFLT_AFF_NODE_PREFERENCE_FACTOR;

    /** {@inheritDoc} */
    @Override public HadoopMapReducePlan preparePlan(HadoopJob job, Collection<ClusterNode> nodes,
        @Nullable HadoopMapReducePlan oldPlan) throws IgniteCheckedException {
        Collection<HadoopInputSplit> inputSplits = job.input();
        int reducerCnt = job.info().reducers();

        if (reducerCnt < 0)
            throw new IgniteCheckedException("Number of reducers must be non-negative, actual: " + reducerCnt);

        HadoopMapReducePlanTopology top = topology(nodes);

        Map<UUID, Collection<HadoopInputSplit>> mappers = mappers(inputSplits, top);

        Map<UUID, int[]> reducers = reducers(top, mappers, reducerCnt);

        return new HadoopDefaultMapReducePlan(mappers, reducers);
    }

    /**
     * Generate mappers.
     *
     * @param inputSplits Input splits.
     * @param top Topology.
     * @return Mappers.
     * @throws IgniteCheckedException If failed.
     */
    private Map<UUID, Collection<HadoopInputSplit>> mappers(Collection<HadoopInputSplit> inputSplits,
        HadoopMapReducePlanTopology top) throws IgniteCheckedException {
        Map<UUID, Collection<HadoopInputSplit>> res = new HashMap<>();

        // Sort input splits by length, the longest goes first. This way we ensure that the longest splits
        // are processed first and assigned in the most efficient way.
        for (HadoopInputSplit inputSplit : sortInputSplits(inputSplits)) {
            // Try getting IGFS affinity.
            Collection<UUID> nodeIds = igfsAffinity(inputSplit);

            if (nodeIds != null)
                nodeIds = affinity(inputSplit, top);

            // Get best node.
            UUID node = bestMapperNode(nodeIds, top);

            // Add to result.
            Collection<HadoopInputSplit> nodeSplits = res.get(node);

            if (nodeSplits == null) {
                nodeSplits = new HashSet<>();

                res.put(node, nodeSplits);
            }

            nodeSplits.add(inputSplit);
        }

        return res;
    }

    /**
     * Sort input splits by length. The longest split goes first.
     *
     * @param inputSplits Original input splits.
     * @return Sorted input splits.
     */
    private List<HadoopInputSplit> sortInputSplits(Collection<HadoopInputSplit> inputSplits) {
        int id = 0;

        TreeSet<SplitSortWrapper> sortedSplits = new TreeSet<>();

        for (HadoopInputSplit inputSplit : inputSplits) {
            long len = inputSplit instanceof HadoopFileBlock ? ((HadoopFileBlock)inputSplit).length() : 0;

            sortedSplits.add(new SplitSortWrapper(id++, inputSplit, len));
        }

        ArrayList<HadoopInputSplit> res = new ArrayList<>(sortedSplits.size());

        for (SplitSortWrapper sortedSplit : sortedSplits)
            res.add(sortedSplit.split);

        return res;

    }

    /**
     * Get IGFS affinity.
     *
     * @param split Input split.
     * @return IGFS affinity or {@code null} if IGFS is not available.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable private Collection<UUID> igfsAffinity(HadoopInputSplit split) throws IgniteCheckedException {
        if (split instanceof HadoopFileBlock) {
            HadoopFileBlock split0 = (HadoopFileBlock)split;

            if (IGFS_SCHEME.equalsIgnoreCase(split0.file().getScheme())) {
                HadoopIgfsEndpoint endpoint = new HadoopIgfsEndpoint(split0.file().getAuthority());

                IgfsEx igfs = null;

                if (F.eq(ignite.name(), endpoint.grid()))
                    igfs = (IgfsEx)((IgniteEx)ignite).igfsx(endpoint.igfs());

                if (igfs != null && !igfs.isProxy(split0.file())) {
                    IgfsPath path = new IgfsPath(split0.file());

                    if (igfs.exists(path)) {
                        Collection<IgfsBlockLocation> blocks;

                        try {
                            blocks = igfs.affinity(path, split0.start(), split0.length());
                        }
                        catch (IgniteException e) {
                            throw new IgniteCheckedException("Failed to get IGFS file block affinity [path=" + path +
                                ", start=" + split0.start() + ", len=" + split0.length() + ']', e);
                        }

                        assert blocks != null;

                        if (blocks.size() == 1)
                            return blocks.iterator().next().nodeIds();
                        else {
                            // The most "local" nodes go first.
                            Map<UUID, Long> idToLen = new HashMap<>();

                            for (IgfsBlockLocation block : blocks) {
                                for (UUID id : block.nodeIds()) {
                                    Long len = idToLen.get(id);

                                    idToLen.put(id, len == null ? block.length() : block.length() + len);
                                }
                            }

                            Map<NodeIdAndLength, UUID> res = new TreeMap<>();

                            for (Map.Entry<UUID, Long> idToLenEntry : idToLen.entrySet()) {
                                UUID id = idToLenEntry.getKey();

                                res.put(new NodeIdAndLength(id, idToLenEntry.getValue()), id);
                            }

                            return new HashSet<>(res.values());
                        }
                    }
                }
            }
        }

        return null;
    }

    /**
     * Get affinity.
     *
     * @param split Input split.
     * @param top Topology.
     * @return Affinity.
     */
    private Collection<UUID> affinity(HadoopInputSplit split, HadoopMapReducePlanTopology top) {
        Collection<UUID> res = new HashSet<>();

        for (String host : split.hosts()) {
            HadoopMapReducePlanGroup grp = top.groupForHost(host);

            if (grp != null) {
                for (int i = 0; i < grp.nodeCount(); i++)
                    res.add(grp.node(i).id());
            }
        }

        return res;
    }

    /**
     * Get best mapper node.
     *
     * @param affIds Affintiy node IDs.
     * @param top Topology.
     * @return Result.
     */
    private UUID bestMapperNode(@Nullable Collection<UUID> affIds, HadoopMapReducePlanTopology top) {
        int affWeight = 100;
        int nonAffWeight = Math.round(affWeight * affNodePreferenceFactor);

        // Priority node.
        UUID priorityAffId = F.isEmpty(affIds) ? null : affIds.iterator().next();

        // Find group with the least weight.
        HadoopMapReducePlanGroup leastGrp = null;
        int leastPriority = 0;
        int leastWeight = Integer.MAX_VALUE;

        for (HadoopMapReducePlanGroup grp : top.groups()) {
            int priority = groupPriority(grp, affIds, priorityAffId);
            int weight = grp.mappersWeight() + (leastPriority == 0 ? nonAffWeight : affWeight);

            if (leastGrp == null) {
                leastGrp = grp;
                leastPriority = priority;
                leastWeight = weight;
            }
            else if (weight < leastWeight || weight == leastWeight && priority > leastPriority) {
                leastGrp = grp;
                leastPriority = priority;
                leastWeight = weight;
            }
        }

        assert leastGrp != null;

        // Update group weight for further runs.
        leastGrp.mappersWeight(leastWeight);

        // Return the best node from the group.
        int idx = 0;

        // This is rare situation when several nodes are started on the same host.
        if (!leastGrp.single()) {
            if (leastPriority == 0)
                // Pick any node.
                idx = ThreadLocalRandom.current().nextInt(leastGrp.nodeCount());
            else if (leastPriority == 1) {
                // Pick any affinity node.
                assert affIds != null;

                List<Integer> cands = new ArrayList<>();

                for (int i = 0; i < leastGrp.nodeCount(); i++) {
                    UUID id = leastGrp.node(i).id();

                    if (affIds.contains(id))
                        cands.add(i);
                }

                idx = cands.get(ThreadLocalRandom.current().nextInt(cands.size()));
            }
            else {
                // Find primary node.
                assert priorityAffId != null;

                for (int i = 0; i < leastGrp.nodeCount(); i++) {
                    UUID id = leastGrp.node(i).id();

                    if (F.eq(id, priorityAffId)) {
                        idx = i;

                        break;
                    }
                }
            }
        }

        return leastGrp.node(idx).id();
    }

    /**
     * Generate reducers.
     *
     * @param top Topology.
     * @param mappers Mappers.
     * @param reducerCnt Reducer count.
     * @return Reducers.
     * @throws IgniteCheckedException If fialed.
     */
    private Map<UUID, int[]> reducers(HadoopMapReducePlanTopology top, Map<UUID, Collection<HadoopInputSplit>> mappers,
        int reducerCnt) throws IgniteCheckedException {
        // TODO
        return null;
    }

    /**
     * Calculate group priority.
     *
     * @param grp Group.
     * @param affIds Affintiy IDs.
     * @param priorityAffId Priority affinity ID.
     * @return Group priority.
     */
    private static int groupPriority(HadoopMapReducePlanGroup grp, @Nullable Collection<UUID> affIds,
        @Nullable UUID priorityAffId) {
        if (F.isEmpty(affIds)) {
            assert priorityAffId == null;

            return 0;
        }

        int priority = 0;

        for (int i = 0; i < grp.nodeCount(); i++) {
            UUID id = grp.node(i).id();

            if (affIds.contains(id)) {
                priority = 1;

                if (F.eq(priorityAffId, id)) {
                    priority = 2;

                    break;
                }
            }
        }

        return priority;
    }

    public float getAffinityNodePreferenceFactor() {
        return affNodePreferenceFactor;
    }

    public void setAffinityNodePreferenceFactor(float affNodePreferenceFactor) {
        this.affNodePreferenceFactor = affNodePreferenceFactor;
    }

    /**
     * Node ID and length.
     */
    private static class NodeIdAndLength implements Comparable<NodeIdAndLength> {
        /** Node ID. */
        private final UUID id;

        /** Length. */
        private final long len;

        /**
         * Constructor.
         *
         * @param id Node ID.
         * @param len Length.
         */
        public NodeIdAndLength(UUID id, long len) {
            this.id = id;
            this.len = len;
        }

        /** {@inheritDoc} */
        @SuppressWarnings("NullableProblems")
        @Override public int compareTo(NodeIdAndLength obj) {
            long res = len - obj.len;

            if (res > 0)
                return 1;
            else if (res < 0)
                return -1;
            else
                return id.compareTo(obj.id);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return id.hashCode();
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object obj) {
            return obj instanceof NodeIdAndLength && F.eq(id, ((NodeIdAndLength)obj).id);
        }
    }

    /**
     * Split wrapper for sorting.
     */
    private static class SplitSortWrapper implements Comparable<SplitSortWrapper> {
        /** Unique ID. */
        private final int id;

        /** Split. */
        private final HadoopInputSplit split;

        /** Split length. */
        private final long len;

        /**
         * Constructor.
         *
         * @param id Unique ID.
         * @param split Split.
         * @param len Split length.
         */
        public SplitSortWrapper(int id, HadoopInputSplit split, long len) {
            this.id = id;
            this.split = split;
            this.len = len;
        }

        /** {@inheritDoc} */
        @SuppressWarnings("NullableProblems")
        @Override public int compareTo(SplitSortWrapper other) {
            assert other != null;

            long res = len - other.len;

            if (res > 0)
                return 1;
            else if (res < 0)
                return -1;
            else
                return id - other.id;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return id;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object obj) {
            return obj instanceof SplitSortWrapper && id == ((SplitSortWrapper)obj).id;
        }
    }
}
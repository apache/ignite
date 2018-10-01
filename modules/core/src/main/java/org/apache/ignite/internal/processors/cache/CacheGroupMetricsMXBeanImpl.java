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
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.LongAdder;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.processors.affinity.AffinityAssignment;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionFullMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionMap;
import org.apache.ignite.internal.processors.cache.persistence.AllocatedPageTracker;
import org.apache.ignite.internal.processors.cache.persistence.DataRegion;
import org.apache.ignite.internal.processors.cache.persistence.DataRegionMetricsImpl;
import org.apache.ignite.mxbean.CacheGroupMetricsMXBean;

/**
 * Management bean that provides access to {@link CacheGroupContext}.
 */
public class CacheGroupMetricsMXBeanImpl implements CacheGroupMetricsMXBean {
    /** Cache group context. */
    private final CacheGroupContext ctx;

    /** */
    private final GroupAllocationTracker groupPageAllocationTracker;

    /** Interface describing a predicate of two integers. */
    private interface IntBiPredicate {
        /**
         * Predicate body.
         *
         * @param targetVal Target value.
         * @param nextVal Next comparable value.
         */
        boolean apply(int targetVal, int nextVal);
    }

    /**
     *
     */
    public static class GroupAllocationTracker implements AllocatedPageTracker {
        /** */
        private final LongAdder totalAllocatedPages = new LongAdder();

        /** */
        private final AllocatedPageTracker delegate;

        /**
         * @param delegate Delegate allocation tracker.
         */
        public GroupAllocationTracker(AllocatedPageTracker delegate) {
            this.delegate = delegate;
        }

        /** {@inheritDoc} */
        @Override public void updateTotalAllocatedPages(long delta) {
            totalAllocatedPages.add(delta);

            delegate.updateTotalAllocatedPages(delta);
        }
    }

    /**
     * Creates Group metrics MBean.
     *
     * @param ctx Cache group context.
     */
    public CacheGroupMetricsMXBeanImpl(CacheGroupContext ctx) {
        this.ctx = ctx;

        DataRegion region = ctx.dataRegion();

        // On client node, region is null.
        if (region != null) {
            DataRegionMetricsImpl dataRegionMetrics = ctx.dataRegion().memoryMetrics();

            this.groupPageAllocationTracker = dataRegionMetrics.getOrAllocateGroupPageAllocationTracker(ctx.groupId());
        }
        else
            this.groupPageAllocationTracker = new GroupAllocationTracker(AllocatedPageTracker.NO_OP);
    }

    /** {@inheritDoc} */
    @Override public int getGroupId() {
        return ctx.groupId();
    }

    /** {@inheritDoc} */
    @Override public String getGroupName() {
        return ctx.name();
    }

    /** {@inheritDoc} */
    @Override public List<String> getCaches() {
        List<String> caches = new ArrayList<>(ctx.caches().size());

        for (GridCacheContext cache : ctx.caches())
            caches.add(cache.name());

        Collections.sort(caches);

        return caches;
    }

    /** {@inheritDoc} */
    @Override public int getBackups() {
        return ctx.config().getBackups();
    }

    /** {@inheritDoc} */
    @Override public int getPartitions() {
        return ctx.topology().partitions();
    }

    /**
     * Calculates the number of partition copies for all partitions of this cache group and filter values by the
     * predicate.
     *
     * @param pred Predicate.
     */
    private int numberOfPartitionCopies(IntBiPredicate pred) {
        int parts = ctx.topology().partitions();

        GridDhtPartitionFullMap partFullMap = ctx.topology().partitionMap(false);

        int res = -1;

        for (int part = 0; part < parts; part++) {
            int cnt = 0;

            for (Map.Entry<UUID, GridDhtPartitionMap> entry : partFullMap.entrySet()) {
                if (entry.getValue().get(part) == GridDhtPartitionState.OWNING)
                    cnt++;
            }

            if (part == 0 || pred.apply(res, cnt))
                res = cnt;
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override public int getMinimumNumberOfPartitionCopies() {
        return numberOfPartitionCopies(new IntBiPredicate() {
            @Override public boolean apply(int targetVal, int nextVal) {
                return nextVal < targetVal;
            }
        });
    }

    /** {@inheritDoc} */
    @Override public int getMaximumNumberOfPartitionCopies() {
        return numberOfPartitionCopies(new IntBiPredicate() {
            @Override public boolean apply(int targetVal, int nextVal) {
                return nextVal > targetVal;
            }
        });
    }

    /**
     * Count of partitions with a given state on the node.
     *
     * @param nodeId Node id.
     * @param state State.
     */
    private int nodePartitionsCountByState(UUID nodeId, GridDhtPartitionState state) {
        int parts = ctx.topology().partitions();

        GridDhtPartitionMap partMap = ctx.topology().partitionMap(false).get(nodeId);

        int cnt = 0;

        for (int part = 0; part < parts; part++)
            if (partMap.get(part) == state)
                cnt++;

        return cnt;
    }

    /**
     * Count of partitions with a given state in the entire cluster.
     *
     * @param state State.
     */
    private int clusterPartitionsCountByState(GridDhtPartitionState state) {
        GridDhtPartitionFullMap partFullMap = ctx.topology().partitionMap(true);

        int cnt = 0;

        for (UUID nodeId : partFullMap.keySet())
            cnt += nodePartitionsCountByState(nodeId, state);

        return cnt;
    }

    /**
     * Count of partitions with a given state on the local node.
     *
     * @param state State.
     */
    private int localNodePartitionsCountByState(GridDhtPartitionState state) {
        int cnt = 0;

        for (GridDhtLocalPartition part : ctx.topology().localPartitions()) {
            if (part.state() == state)
                cnt++;
        }

        return cnt;
    }

    /** {@inheritDoc} */
    @Override public int getLocalNodeOwningPartitionsCount() {
        return localNodePartitionsCountByState(GridDhtPartitionState.OWNING);
    }

    /** {@inheritDoc} */
    @Override public int getLocalNodeMovingPartitionsCount() {
        return localNodePartitionsCountByState(GridDhtPartitionState.MOVING);
    }

    /** {@inheritDoc} */
    @Override public int getLocalNodeRentingPartitionsCount() {
        return localNodePartitionsCountByState(GridDhtPartitionState.RENTING);
    }

    /** {@inheritDoc} */
    @Override public long getLocalNodeRentingEntriesCount() {
        long entriesCnt = 0;

        for (GridDhtLocalPartition part : ctx.topology().localPartitions()) {
            if (part.state() == GridDhtPartitionState.RENTING)
                entriesCnt += part.dataStore().fullSize();
        }

        return entriesCnt;
    }

    /** {@inheritDoc} */
    @Override public int getClusterOwningPartitionsCount() {
        return clusterPartitionsCountByState(GridDhtPartitionState.OWNING);
    }

    /** {@inheritDoc} */
    @Override public int getClusterMovingPartitionsCount() {
        return clusterPartitionsCountByState(GridDhtPartitionState.MOVING);
    }

    /**
     * Gets partitions allocation map with a given state.
     *
     * @param state State.
     * @return Partitions allocation map.
     */
    private Map<Integer, Set<String>> clusterPartitionsMapByState(GridDhtPartitionState state) {
        int parts = ctx.topology().partitions();

        GridDhtPartitionFullMap partFullMap = ctx.topology().partitionMap(false);

        Map<Integer, Set<String>> partsMap = new LinkedHashMap<>();

        for (int part = 0; part < parts; part++) {
            Set<String> partNodesSet = new HashSet<>();

            for (Map.Entry<UUID, GridDhtPartitionMap> entry : partFullMap.entrySet()) {
                if (entry.getValue().get(part) == state)
                    partNodesSet.add(entry.getKey().toString());
            }

            partsMap.put(part, partNodesSet);
        }

        return partsMap;
    }

    /** {@inheritDoc} */
    @Override public Map<Integer, Set<String>> getOwningPartitionsAllocationMap() {
        return clusterPartitionsMapByState(GridDhtPartitionState.OWNING);
    }

    /** {@inheritDoc} */
    @Override public Map<Integer, Set<String>> getMovingPartitionsAllocationMap() {
        return clusterPartitionsMapByState(GridDhtPartitionState.MOVING);
    }

    /** {@inheritDoc} */
    @Override public Map<Integer, List<String>> getAffinityPartitionsAssignmentMap() {
        AffinityAssignment assignment = ctx.affinity().cachedAffinity(AffinityTopologyVersion.NONE);

        int part = 0;

        Map<Integer, List<String>> assignmentMap = new LinkedHashMap<>();

        for (List<ClusterNode> partAssignment : assignment.assignment()) {
            List<String> partNodeIds = new ArrayList<>(partAssignment.size());

            for (ClusterNode node : partAssignment)
                partNodeIds.add(node.id().toString());

            assignmentMap.put(part, partNodeIds);

            part++;
        }

        return assignmentMap;
    }

    /** {@inheritDoc} */
    @Override public String getType() {
        CacheMode type = ctx.config().getCacheMode();

        return String.valueOf(type);
    }

    /** {@inheritDoc} */
    @Override public List<Integer> getPartitionIds() {
        List<GridDhtLocalPartition> parts = ctx.topology().localPartitions();

        List<Integer> partsRes = new ArrayList<>(parts.size());

        for (GridDhtLocalPartition part : parts)
            partsRes.add(part.id());

        return partsRes;
    }

    /** {@inheritDoc} */
    @Override public long getTotalAllocatedPages() {
        return groupPageAllocationTracker.totalAllocatedPages.longValue();
    }

    /** {@inheritDoc} */
    @Override public long getTotalAllocatedSize() {
        return getTotalAllocatedPages() * ctx.dataRegion().pageMemory().pageSize();
    }
}

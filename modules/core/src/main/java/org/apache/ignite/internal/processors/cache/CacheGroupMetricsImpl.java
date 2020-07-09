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
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.internal.pagemem.store.PageStore;
import org.apache.ignite.internal.processors.affinity.AffinityAssignment;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionFullMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.persistence.DataRegion;
import org.apache.ignite.internal.processors.cache.persistence.DataRegionMetricsImpl;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.internal.processors.metric.impl.AtomicLongMetric;
import org.apache.ignite.internal.processors.metric.impl.LongAdderMetric;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.spi.metric.LongMetric;

import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.cacheMetricsRegistryName;
import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.metricName;

/**
 * Cache group metrics.
 */
public class CacheGroupMetricsImpl {
    /** Cache group metrics prefix. */
    public static final String CACHE_GROUP_METRICS_PREFIX = "cacheGroups";

    /** Number of partitions need processed for finished indexes create or rebuilding. */
    private final AtomicLongMetric idxBuildCntPartitionsLeft;

    /** Cache group context. */
    private final CacheGroupContext ctx;

    /** */
    private final LongAdderMetric grpPageAllocationTracker;

    /** */
    private final LongMetric storageSize;

    /** */
    private final LongMetric sparseStorageSize;

    /** Number of local partitions initialized on current node. */
    private final AtomicLongMetric initLocalPartitionsNumber;

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

    /** */
    public CacheGroupMetricsImpl(CacheGroupContext ctx) {
        this.ctx = ctx;

        CacheConfiguration cacheCfg = ctx.config();

        DataStorageConfiguration dsCfg = ctx.shared().kernalContext().config().getDataStorageConfiguration();

        boolean persistentEnabled = !ctx.shared().kernalContext().clientNode() && CU.isPersistentCache(cacheCfg, dsCfg);

        MetricRegistry mreg = ctx.shared().kernalContext().metric().registry(metricGroupName());

        mreg.register("Caches", this::getCaches, List.class, null);

        storageSize = mreg.register("StorageSize",
            () -> persistentEnabled ? database().forGroupPageStores(ctx, PageStore::size) : 0,
            "Storage space allocated for group, in bytes.");

        sparseStorageSize = mreg.register("SparseStorageSize",
            () -> persistentEnabled ? database().forGroupPageStores(ctx, PageStore::getSparseSize) : 0,
            "Storage space allocated for group adjusted for possible sparsity, in bytes.");

        idxBuildCntPartitionsLeft = mreg.longMetric("IndexBuildCountPartitionsLeft",
            "Number of partitions need processed for finished indexes create or rebuilding.");

        initLocalPartitionsNumber = mreg.longMetric("InitializedLocalPartitionsNumber", "Number of local partitions initialized on current node.");

        DataRegion region = ctx.dataRegion();

        // On client node, region is null.
        if (region != null) {
            DataRegionMetricsImpl dataRegionMetrics = ctx.dataRegion().memoryMetrics();

            grpPageAllocationTracker =
                dataRegionMetrics.getOrAllocateGroupPageAllocationTracker(ctx.cacheOrGroupName());
        }
        else
            grpPageAllocationTracker = new LongAdderMetric("NO_OP", null);
    }

    /** Callback for initializing metrics after topology was initialized. */
    public void onTopologyInitialized() {
        MetricRegistry mreg = ctx.shared().kernalContext().metric().registry(metricGroupName());

        mreg.register("MinimumNumberOfPartitionCopies",
            this::getMinimumNumberOfPartitionCopies,
            "Minimum number of partition copies for all partitions of this cache group.");

        mreg.register("MaximumNumberOfPartitionCopies",
            this::getMaximumNumberOfPartitionCopies,
            "Maximum number of partition copies for all partitions of this cache group.");

        mreg.register("LocalNodeOwningPartitionsCount",
            this::getLocalNodeOwningPartitionsCount,
            "Count of partitions with state OWNING for this cache group located on this node.");

        mreg.register("LocalNodeMovingPartitionsCount",
            this::getLocalNodeMovingPartitionsCount,
            "Count of partitions with state MOVING for this cache group located on this node.");

        mreg.register("LocalNodeRentingPartitionsCount",
            this::getLocalNodeRentingPartitionsCount,
            "Count of partitions with state RENTING for this cache group located on this node.");

        mreg.register("LocalNodeRentingEntriesCount",
            this::getLocalNodeRentingEntriesCount,
            "Count of entries remains to evict in RENTING partitions located on this node for this cache group.");

        mreg.register("OwningPartitionsAllocationMap",
            this::getOwningPartitionsAllocationMap,
            Map.class,
            "Allocation map of partitions with state OWNING in the cluster.");

        mreg.register("MovingPartitionsAllocationMap",
            this::getMovingPartitionsAllocationMap,
            Map.class,
            "Allocation map of partitions with state MOVING in the cluster.");

        mreg.register("AffinityPartitionsAssignmentMap",
            this::getAffinityPartitionsAssignmentMap,
            Map.class,
            "Affinity partitions assignment map.");

        mreg.register("PartitionIds",
            this::getPartitionIds,
            List.class,
            "Local partition ids.");

        mreg.register("TotalAllocatedSize",
            this::getTotalAllocatedSize,
            "Total size of memory allocated for group, in bytes.");
    }

    /** */
    public long getIndexBuildCountPartitionsLeft() {
        return idxBuildCntPartitionsLeft.value();
    }

    /** Add number of partitions need processed for finished indexes create or rebuilding. */
    public void addIndexBuildCountPartitionsLeft(long idxBuildCntPartitionsLeft) {
        this.idxBuildCntPartitionsLeft.add(idxBuildCntPartitionsLeft);
    }

    /**
     * Decrement number of partitions need processed for finished indexes create or rebuilding.
     */
    public void decrementIndexBuildCountPartitionsLeft() {
        idxBuildCntPartitionsLeft.decrement();
    }

    /**
     * Increments number of local partitions initialized on current node.
     */
    public void incrementInitializedLocalPartitions() {
        initLocalPartitionsNumber.increment();
    }

    /**
     * Decrements number of local partitions initialized on current node.
     */
    public void decrementInitializedLocalPartitions() {
        initLocalPartitionsNumber.decrement();
    }

    /** */
    public int getGroupId() {
        return ctx.groupId();
    }

    /** */
    public String getGroupName() {
        return ctx.name();
    }

    /** */
    public List<String> getCaches() {
        List<String> caches = new ArrayList<>(ctx.caches().size());

        for (GridCacheContext cache : ctx.caches())
            caches.add(cache.name());

        Collections.sort(caches);

        return caches;
    }

    /** */
    public int getBackups() {
        return ctx.config().getBackups();
    }

    /** */
    public int getPartitions() {
        return ctx.topology().partitions();
    }

    /**
     * Calculates the number of partition copies for all partitions of this cache group and filter values by the
     * predicate.
     *
     * @param pred Predicate.
     */
    private int numberOfPartitionCopies(IntBiPredicate pred) {
        GridDhtPartitionFullMap partFullMap = ctx.topology().partitionMap(false);

        if (partFullMap == null)
            return 0;

        int parts = ctx.topology().partitions();

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

    /** */
    public int getMinimumNumberOfPartitionCopies() {
        return numberOfPartitionCopies(new IntBiPredicate() {
            @Override public boolean apply(int targetVal, int nextVal) {
                return nextVal < targetVal;
            }
        });
    }

    /** */
    public int getMaximumNumberOfPartitionCopies() {
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

    /** */
    public int getLocalNodeOwningPartitionsCount() {
        return localNodePartitionsCountByState(GridDhtPartitionState.OWNING);
    }

    /** */
    public int getLocalNodeMovingPartitionsCount() {
        return localNodePartitionsCountByState(GridDhtPartitionState.MOVING);
    }

    /** */
    public int getLocalNodeRentingPartitionsCount() {
        return localNodePartitionsCountByState(GridDhtPartitionState.RENTING);
    }

    /** */
    public long getLocalNodeRentingEntriesCount() {
        long entriesCnt = 0;

        for (GridDhtLocalPartition part : ctx.topology().localPartitions()) {
            if (part.state() == GridDhtPartitionState.RENTING)
                entriesCnt += part.dataStore().fullSize();
        }

        return entriesCnt;
    }

    /** */
    public int getClusterOwningPartitionsCount() {
        return clusterPartitionsCountByState(GridDhtPartitionState.OWNING);
    }

    /** */
    public int getClusterMovingPartitionsCount() {
        return clusterPartitionsCountByState(GridDhtPartitionState.MOVING);
    }

    /**
     * Gets partitions allocation map with a given state.
     *
     * @param state State.
     * @return Partitions allocation map.
     */
    private Map<Integer, Set<String>> clusterPartitionsMapByState(GridDhtPartitionState state) {
        GridDhtPartitionFullMap partFullMap = ctx.topology().partitionMap(false);

        if (partFullMap == null)
            return Collections.emptyMap();

        int parts = ctx.topology().partitions();

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

    /** */
    public Map<Integer, Set<String>> getOwningPartitionsAllocationMap() {
        return clusterPartitionsMapByState(GridDhtPartitionState.OWNING);
    }

    /** */
    public Map<Integer, Set<String>> getMovingPartitionsAllocationMap() {
        return clusterPartitionsMapByState(GridDhtPartitionState.MOVING);
    }

    /** */
    public Map<Integer, List<String>> getAffinityPartitionsAssignmentMap() {
        if (ctx.affinity().lastVersion().topologyVersion() < 0)
            return Collections.emptyMap();

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

    /** */
    public String getType() {
        CacheMode type = ctx.config().getCacheMode();

        return String.valueOf(type);
    }

    /** */
    public List<Integer> getPartitionIds() {
        List<GridDhtLocalPartition> parts = ctx.topology().localPartitions();

        List<Integer> partsRes = new ArrayList<>(parts.size());

        for (GridDhtLocalPartition part : parts)
            partsRes.add(part.id());

        return partsRes;
    }

    /** */
    public long getTotalAllocatedPages() {
        return ctx.shared().kernalContext().clientNode() ?
            0 :
            grpPageAllocationTracker.value();
    }

    /** */
    public long getTotalAllocatedSize() {
        return ctx.shared().kernalContext().clientNode() ?
            0 :
            getTotalAllocatedPages() * ctx.dataRegion().pageMemory().pageSize();
    }

    /** */
    public long getStorageSize() {
        return storageSize == null ? 0 : storageSize.value();
    }

    /** */
    public long getSparseStorageSize() {
        return sparseStorageSize == null ? 0 : sparseStorageSize.value();
    }

    /** Removes all metric for cache group. */
    public void remove() {
        if (ctx.shared().kernalContext().isStopping())
            return;

        if (ctx.config().getNearConfiguration() != null)
            ctx.shared().kernalContext().metric().remove(cacheMetricsRegistryName(ctx.config().getName(), true));

        ctx.shared().kernalContext().metric().remove(cacheMetricsRegistryName(ctx.config().getName(), false));

        ctx.shared().kernalContext().metric().remove(metricGroupName());
    }

    /**
     * @return Database.
     */
    private GridCacheDatabaseSharedManager database() {
        return (GridCacheDatabaseSharedManager)ctx.shared().database();
    }

    /** @return Metric group name. */
    private String metricGroupName() {
        return metricName(CACHE_GROUP_METRICS_PREFIX, ctx.cacheOrGroupName());
    }
}

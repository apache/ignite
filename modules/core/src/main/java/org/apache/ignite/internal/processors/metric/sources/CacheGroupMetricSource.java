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

package org.apache.ignite.internal.processors.metric.sources;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.internal.metric.IoStatisticsHolder;
import org.apache.ignite.internal.metric.IoStatisticsQueryHelper;
import org.apache.ignite.internal.pagemem.store.PageStore;
import org.apache.ignite.internal.processors.affinity.AffinityAssignment;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCachePreloader;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionDemander;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionFullMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.persistence.DataRegion;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.metric.MetricRegistryBuilder;
import org.apache.ignite.internal.processors.metric.impl.AtomicLongMetric;
import org.apache.ignite.internal.processors.metric.impl.LongAdderMetric;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.metric.LongMetric;

import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.metricName;

/**
 * Metric source that provides access to cache group metrics.
 */
public class CacheGroupMetricSource extends AbstractMetricSource<CacheGroupMetricSource.Holder> implements IoStatisticsHolder {
    /** Cache group metrics prefix. */
    public static final String CACHE_GROUP_METRICS_PREFIX = "cacheGroup";

    /** */
    public static final String PHYSICAL_READS = "physicalPageReads";

    /** */
    public static final String LOGICAL_READS = "logicalPageReads";

    /** Cache group context. */
    private final CacheGroupContext grpCtx;

    /**
     * Creates cache group metric source for given cache group context.
     *
     * @param grpCtx Cache group context.
     */
    public CacheGroupMetricSource(CacheGroupContext grpCtx) {
        super(metricName(CACHE_GROUP_METRICS_PREFIX, grpCtx.cacheOrGroupName()), grpCtx.shared().kernalContext());

        this.grpCtx = grpCtx;
    }

    /** {@inheritDoc} */
    @Override protected void init(MetricRegistryBuilder bldr, Holder hldr) {
        CacheConfiguration<?, ?> cacheCfg = grpCtx.config();

        DataStorageConfiguration dsCfg = ctx().config().getDataStorageConfiguration();

        boolean persistentEnabled = CU.isPersistentCache(cacheCfg, dsCfg);

        bldr.register("Caches", this::getCaches, List.class, null);

        bldr.longMetric("startTime", "Cache group start time.").value(U.currentTimeMillis());

        bldr.intMetric("grpId", "Cache group ID.").value(grpCtx.groupId());

        hldr.storageSize = bldr.register("StorageSize",
                () -> persistentEnabled ? databaseSharedManager().forGroupPageStores(grpCtx, PageStore::size) : 0,
                "Storage space allocated for group, in bytes.");

        hldr.sparseStorageSize = bldr.register("SparseStorageSize",
                () -> persistentEnabled ? databaseSharedManager().forGroupPageStores(grpCtx, PageStore::getSparseSize) : 0,
                "Storage space allocated for group adjusted for possible sparsity, in bytes.");

        hldr.idxBuildCntPartitionsLeft = bldr.longMetric("IndexBuildCountPartitionsLeft",
                "Number of partitions need processed for finished indexes create or rebuilding.");

        DataRegion region = grpCtx.dataRegion();

        String pref = CACHE_GROUP_METRICS_PREFIX + '.' + U.maskName(grpCtx.cacheOrGroupName()) + '.';

        // On client node, region is null.
        if (region != null) {
            DataRegionMetricSource dataRegionMetricSrc = grpCtx.dataRegion().metricSource();

            hldr.grpPageAllocationTracker =
                    dataRegionMetricSrc.getOrCreateGroupPageAllocationTracker(
                            pref,
                            grpCtx.groupId()
                    );
        }
        else
            hldr.grpPageAllocationTracker = new LongAdderMetric(pref + U.maskName(grpCtx.cacheOrGroupName()) + "TotalAllocatedPages", null);

        bldr.addMetric("TotalAllocatedPages", hldr.grpPageAllocationTracker);

        hldr.logicalReadCtr = bldr.longAdderMetric(LOGICAL_READS, null);

        hldr.physicalReadCtr = bldr.longAdderMetric(PHYSICAL_READS, null);

        bldr.register("MinimumNumberOfPartitionCopies",
                this::minimumNumberOfPartitionCopies,
                "Minimum number of partition copies for all partitions of this cache group.");

        bldr.register("MaximumNumberOfPartitionCopies",
                this::maximumNumberOfPartitionCopies,
                "Maximum number of partition copies for all partitions of this cache group.");

        bldr.register("LocalNodeOwningPartitionsCount",
                this::localNodeOwningPartitionsCount,
                "Count of partitions with state OWNING for this cache group located on this node.");

        bldr.register("LocalNodeMovingPartitionsCount",
                this::localNodeMovingPartitionsCount,
                "Count of partitions with state MOVING for this cache group located on this node.");

        bldr.register("LocalNodeRentingPartitionsCount",
                this::localNodeRentingPartitionsCount,
                "Count of partitions with state RENTING for this cache group located on this node.");

        bldr.register("LocalNodeRentingEntriesCount",
                this::localNodeRentingEntriesCount,
                "Count of entries remains to evict in RENTING partitions located on this node for this cache group.");

        //TODO: Should be removed in Apache Ignite 3.0. Map isn't metric. Consider another way for providing this info.
        bldr.register("OwningPartitionsAllocationMap",
                this::owningPartitionsAllocationMap,
                Map.class,
                "Allocation map of partitions with state OWNING in the cluster.");

        //TODO: Should be removed in Apache Ignite 3.0. Map isn't metric. Consider another way for providing this info.
        bldr.register("MovingPartitionsAllocationMap",
                this::movingPartitionsAllocationMap,
                Map.class,
                "Allocation map of partitions with state MOVING in the cluster.");

        //TODO: Should be removed in Apache Ignite 3.0. Map isn't metric. Consider another way for providing this info.
        bldr.register("AffinityPartitionsAssignmentMap",
                this::affinityPartitionsAssignmentMap,
                Map.class,
                "Affinity partitions assignment map.");

        //TODO: Should be removed in Apache Ignite 3.0. List isn't metric. Consider another way for providing this info.
        bldr.register("PartitionIds",
                this::partitionIds,
                List.class,
                "Local partition ids.");

        bldr.register("TotalAllocatedSize",
                this::totalAllocatedSize,
                "Total size of memory allocated for group, in bytes.");

        bldr.register("RebalancingPartitionsLeft",
                () -> {
                    GridDhtPartitionDemander.RebalanceFuture fut = rebalanceFuture();

                    return fut == null ? -1 : fut.partitionsLeft();
                },
                "The number of cache group partitions left to be rebalanced.");

        bldr.register("RebalancingReceivedKeys",
                () -> {
                    GridDhtPartitionDemander.RebalanceFuture fut = rebalanceFuture();

                    return fut == null ? -1 : fut.receivedKeys();
                },
                "The number of currently rebalanced keys for the whole cache group.");

        bldr.register("RebalancingReceivedBytes",
                () -> {
                    GridDhtPartitionDemander.RebalanceFuture fut = rebalanceFuture();

                    return fut == null ? -1 : fut.receivedBytes();
                },
                "The number of currently rebalanced bytes of this cache group.");

        bldr.register("RebalancingStartTime",
                () -> {
                    GridDhtPartitionDemander.RebalanceFuture fut = rebalanceFuture();

                    return fut == null ? -1 : fut.startTime();
                },
                "The time the first partition " +
                "demand message was sent. If there are no messages to send, the rebalancing time will be undefined.");

        bldr.register("RebalancingEndTime",
                () -> {
                    GridDhtPartitionDemander.RebalanceFuture fut = rebalanceFuture();

                    return fut == null ? -1 : fut.endTime();
                },
                "The time the rebalancing was completed. " +
                "If the rebalancing completed with an error, was cancelled, or the start time was undefined, " +
                "the rebalancing end time will be undefined.");

        bldr.register("RebalancingLastCancelledTime",
                () -> {
                    GridDhtPartitionDemander.RebalanceFuture fut = rebalanceFuture();

                    return fut == null ? -1 : fut.lastCancelledTime();
                },
                "The time the rebalancing " +
                "was completed with an error or was cancelled. If there were several such cases, the metric " +
                "stores the last time. The metric displays the value even if there is no rebalancing process.");
    }

    /**
     * Return cache group ID.
     *
     * @return Cache group ID.
     */
    public int groupId() {
        return grpCtx.groupId();
    }

    /** {@inheritDoc} */
    @Override public void trackLogicalRead(long pageAddr) {
        Holder hldr = holder();

        if (hldr != null) {
            int pageIoType = PageIO.getType(pageAddr);

            if (pageIoType == PageIO.T_DATA) {
                hldr.logicalReadCtr.increment();

                IoStatisticsQueryHelper.trackLogicalReadQuery(pageAddr);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void trackPhysicalAndLogicalRead(long pageAddr) {
        Holder hldr = holder();

        if (hldr != null) {
            int pageIoType = PageIO.getType(pageAddr);

            if (pageIoType == PageIO.T_DATA) {
                hldr.logicalReadCtr.increment();

                hldr.physicalReadCtr.increment();

                IoStatisticsQueryHelper.trackPhysicalAndLogicalReadQuery(pageAddr);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public long logicalReads() {
        Holder hldr = holder();

        return hldr != null ? hldr.logicalReadCtr.value() : 0;
    }

    /** {@inheritDoc} */
    @Override public long physicalReads() {
        Holder hldr = holder();

        return hldr != null ? hldr.physicalReadCtr.value() : 0;
    }

    /**
     * Returns number of partitions that must be processed for finish of indexes creation or rebuilding.
     *
     * @return Number of partitions that must be processed for finish of indexes creation or rebuilding.
     * @deprecated Should be removed in Apache Ignite 3.0 because client MBeans will be removed.
     */
    @Deprecated
    public long indexBuildCountPartitionsLeft() {
        Holder hldr = holder();

        return hldr != null ? hldr.idxBuildCntPartitionsLeft.value() : 0;
    }

    /**
     * Adds number of partitions need processed for finished indexes create or rebuilding.
     *
     * @param idxBuildCntPartitionsLeft Amount of partitions.
     */
    public void addIndexBuildCountPartitionsLeft(long idxBuildCntPartitionsLeft) {
        Holder hldr = holder();

        if (hldr != null)
            hldr.idxBuildCntPartitionsLeft.add(idxBuildCntPartitionsLeft);
    }

    /**
     * Decrements number of partitions that must be processed for finish of indexes creation or rebuilding.
     */
    public void decrementIndexBuildCountPartitionsLeft() {
        Holder hldr = holder();

        if (hldr != null)
            hldr.idxBuildCntPartitionsLeft.decrement();
    }

    /**
     * Returns total allocated pages for cache group.
     *
     * @return Total allocated pages for cache group.
     * @deprecated Should be removed in Apache Ignite 3.0 because client MBeans will be removed.
     */
    @Deprecated
    public long totalAllocatedPages() {
        Holder hldr = holder();

        return hldr != null ? hldr.grpPageAllocationTracker.value() : 0;
    }

    /**
     * Returns total size of memory in bytes allocated for cache group.
     *
     * @return Total size of memory in bytes allocated for cache group.
     * @deprecated Should be removed in Apache Ignite 3.0 because client MBeans will be removed.
     */
    @Deprecated
    public long totalAllocatedSize() {
        Holder hldr = holder();

        return hldr != null ? hldr.grpPageAllocationTracker.value() * grpCtx.dataRegion().pageMemory().pageSize() : 0;
    }

    /**
     * Returns storage space in bytes allocated for cache group.
     *
     * @return Storage space in bytes allocated for cache group.
     * @deprecated Should be removed in Apache Ignite 3.0 because client MBeans will be removed.
     */
    @Deprecated
    public long storageSize() {
        Holder hldr = holder();

        return hldr != null ? hldr.storageSize.value() : 0;
    }

    /**
     * Returns storage space in bytes allocated for cache group adjusted for possible sparsity.
     *
     * @return Storage space in bytes allocated for cache group adjusted for possible sparsity.
     * @deprecated Should be removed in Apache Ignite 3.0 because client MBeans will be removed.
     */
    @Deprecated
    public long sparseStorageSize() {
        Holder hldr = holder();

        return hldr !=null ? hldr.sparseStorageSize.value() : 0;
    }

    /**
     * Returns list of local partition IDs.
     *
     * @return List of local partition IDs.
     * @deprecated Should be removed in Apache Ignite 3.0. List isn't metric.
     * Consider another way for providing this info.
     */
    @Deprecated
    public List<Integer> partitionIds() {
        if (!enabled())
            return Collections.emptyList();

        List<GridDhtLocalPartition> parts = grpCtx.topology().localPartitions();

        List<Integer> partsRes = new ArrayList<>(parts.size());

        for (GridDhtLocalPartition part : parts)
            partsRes.add(part.id());

        return partsRes;
    }

    /**
     * Returns allocation map of partitions with state OWNING in the cluster.
     *
     * @return Map from partition number to set of nodes, where partition is located.
     * @deprecated Should be removed in Apache Ignite 3.0. Map isn't metric.
     * Consider another way for providing this info.
     */
    @Deprecated
    public Map<Integer, Set<String>> owningPartitionsAllocationMap() {
        if (!enabled())
            return Collections.emptyMap();

        return clusterPartitionsMapByState(GridDhtPartitionState.OWNING);
    }

    /**
     * Returns allocation map of partitions with state MOVING in the cluster.
     *
     * @return Map from partition number to set of nodes, where partition is located
     * @deprecated Should be removed in Apache Ignite 3.0. Map isn't metric.
     * Consider another way for providing this info.
     */
    @Deprecated
    public Map<Integer, Set<String>> movingPartitionsAllocationMap() {
        if (!enabled())
            return Collections.emptyMap();

        return clusterPartitionsMapByState(GridDhtPartitionState.MOVING);
    }

    /**
     * Returns affinity partitions assignment map.
     *
     * @return Map from partition number to list of nodes. The first node in this list is where the PRIMARY partition is
     * assigned, other nodes in the list is where the BACKUP partitions is assigned.
     * @deprecated Should be removed in Apache Ignite 3.0. Map isn't metric.
     * Consider another way for providing this info.
     */
    @Deprecated
    public Map<Integer, List<String>> affinityPartitionsAssignmentMap() {
        if (!enabled())
            return Collections.emptyMap();

        if (grpCtx.affinity().lastVersion().topologyVersion() < 0)
            return Collections.emptyMap();

        AffinityAssignment assignment = grpCtx.affinity().cachedAffinity(AffinityTopologyVersion.NONE);

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

    /**
     * Returns count of partitions with state OWNING for this cache group in the entire cluster.
     *
     * @return Count of partitions with state OWNING for this cache group in the entire cluster.
     * @deprecated Should be removed in Apache Ignite 3.0. Aggegated value but must be local.
     */
    @Deprecated
    public int clusterOwningPartitionsCount() {
        if (!enabled())
            return -1;

        return clusterPartitionsCountByState(GridDhtPartitionState.OWNING);
    }

    /**
     * Returns count of partitions with state MOVING for this cache group in the entire cluster.
     *
     * @return Count of partitions with state MOVING for this cache group in the entire cluster.
     * @deprecated Should be removed in Apache Ignite 3.0. Aggegated value but must be local.
     */
    @Deprecated
    public int clusterMovingPartitionsCount() {
        if (!enabled())
            return 0;

        return clusterPartitionsCountByState(GridDhtPartitionState.MOVING);
    }

    /**
     * Count of partitions with a given state in the entire cluster.
     *
     * @param state State.
     */
    private int clusterPartitionsCountByState(GridDhtPartitionState state) {
        GridDhtPartitionFullMap partFullMap = grpCtx.topology().partitionMap(true);

        int cnt = 0;

        for (UUID nodeId : partFullMap.keySet())
            cnt += nodePartitionsCountByState(nodeId, state);

        return cnt;
    }

    /**
     * Count of partitions with a given state on the node.
     *
     * @param nodeId Node id.
     * @param state State.
     */
    private int nodePartitionsCountByState(UUID nodeId, GridDhtPartitionState state) {
        int parts = grpCtx.topology().partitions();

        GridDhtPartitionMap partMap = grpCtx.topology().partitionMap(false).get(nodeId);

        int cnt = 0;

        for (int part = 0; part < parts; part++)
            if (partMap.get(part) == state)
                cnt++;

        return cnt;
    }

    /**
     * Returns count of entries remains to evict in RENTING partitions located on this node for this cache group.
     *
     * @return Count of entries remains to evict in RENTING partitions located on this node for this cache group.
     */
    public long localNodeRentingEntriesCount() {
        if (!enabled())
            return 0;

        long entriesCnt = 0;

        for (GridDhtLocalPartition part : grpCtx.topology().localPartitions()) {
            if (part.state() == GridDhtPartitionState.RENTING)
                entriesCnt += part.dataStore().fullSize();
        }

        return entriesCnt;
    }

    /**
     * Returns count of partitions with state OWNING for this cache group located on this node.
     *
     * @return Count of partitions with state OWNING for this cache group located on this node.
     */
    //TODO: Optimize for OWNING, MOVING and RENTING in order to avoid iterations over all partitions thrice.
    public int localNodeOwningPartitionsCount() {
        if (!enabled())
            return 0;

        return localNodePartitionsCountByState(GridDhtPartitionState.OWNING);
    }

    /**
     * Returns count of partitions with state MOVING for this cache group located on this node.
     *
     * @return count of partitions with state MOVING for this cache group located on this node.
     */
    //TODO: Optimize for OWNING, MOVING and RENTING in order to avoid iterations over all partitions thrice.
    public int localNodeMovingPartitionsCount() {
        if (!enabled())
            return 0;

        return localNodePartitionsCountByState(GridDhtPartitionState.MOVING);
    }

    /**
     * Returns count of partitions with state RENTING for this cache group located on this node.
     *
     * @return Count of partitions with state RENTING for this cache group located on this node.
     */
    //TODO: Optimize for OWNING, MOVING and RENTING in order to avoid iterations over all partitions thrice.
    public int localNodeRentingPartitionsCount() {
        if (!enabled())
            return 0;

        return localNodePartitionsCountByState(GridDhtPartitionState.RENTING);
    }

    /**
     * Calculates minimum number of partitions copies for all partitions of this cache group.
     *
     * @return Minimum number of partitions copies.
     */
    public int minimumNumberOfPartitionCopies() {
        if (!enabled())
            return 0;

        return numberOfPartitionCopies((targetVal, nextVal) -> nextVal < targetVal);
    }

    /**
     * Calculates maximum number of partitions copies for all partitions of this cache group.
     *
     * @return Maximum number of partitions copies.
     */
    public int maximumNumberOfPartitionCopies() {
        if (!enabled())
            return 0;

        return numberOfPartitionCopies((targetVal, nextVal) -> nextVal > targetVal);
    }

    /**
     * Gets partitions allocation map with a given state.
     *
     * @param state State.
     * @return Partitions allocation map.
     */
    private Map<Integer, Set<String>> clusterPartitionsMapByState(GridDhtPartitionState state) {
        if (!enabled())
            return Collections.emptyMap();

        GridDhtPartitionFullMap partFullMap = grpCtx.topology().partitionMap(false);

        if (partFullMap == null)
            return Collections.emptyMap();

        int parts = grpCtx.topology().partitions();

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

    /**
     * Count of partitions with a given state on the local node.
     *
     * @param state State.
     */
    private int localNodePartitionsCountByState(GridDhtPartitionState state) {
        int cnt = 0;

        for (GridDhtLocalPartition part : grpCtx.topology().localPartitions()) {
            if (part.state() == state)
                cnt++;
        }

        return cnt;
    }

    /**
     * Calculates the number of partition copies for all partitions of this cache group and filter values by the
     * predicate.
     *
     * @param pred Predicate.
     */
    private int numberOfPartitionCopies(IntBiPredicate pred) {
        GridDhtPartitionFullMap partFullMap = grpCtx.topology().partitionMap(false);

        if (partFullMap == null)
            return 0;

        int parts = grpCtx.topology().partitions();

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

    /**
     * @deprecated Should be removed in Apache Ignite 3.0. List isn't metric.
     */
    @Deprecated
    //TODO: It seems that it could be removed in Apache Ignite 2.8.
    private List<String> getCaches() {
        List<String> caches = new ArrayList<>(grpCtx.caches().size());

        for (GridCacheContext<?, ?> cache : grpCtx.caches())
            caches.add(cache.name());

        Collections.sort(caches);

        return caches;
    }

    /**
     * @return Database shared manager.
     */
    private GridCacheDatabaseSharedManager databaseSharedManager() {
        return (GridCacheDatabaseSharedManager) grpCtx.shared().database();
    }

    /** Returns rebalance future. */
    private GridDhtPartitionDemander.RebalanceFuture rebalanceFuture() {
        GridCachePreloader preloader = grpCtx.preloader();

        return preloader != null ? (GridDhtPartitionDemander.RebalanceFuture) preloader.rebalanceFuture() : null;
    }

    /** Interface describing a predicate of two integers. */
    @FunctionalInterface
    private interface IntBiPredicate {
        /**
         * Predicate body.
         *
         * @param targetVal Target value.
         * @param nextVal Next comparable value.
         */
        boolean apply(int targetVal, int nextVal);
    }

    /** {@inheritDoc} */
    @Override protected Holder createHolder() {
        return new Holder();
    }

    /** */
    protected static class Holder implements AbstractMetricSource.Holder<Holder> {
        /** Storage size. */
        private LongMetric storageSize;

        /** Sparse storage size. */
        private LongMetric sparseStorageSize;

        /** Number of partitions need processed for finished indexes create or rebuilding. */
        private AtomicLongMetric idxBuildCntPartitionsLeft;

        /** */
        private LongAdderMetric grpPageAllocationTracker;

        /** */
        private LongAdderMetric logicalReadCtr;

        /** */
        private LongAdderMetric physicalReadCtr;
    }
}

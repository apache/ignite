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

package org.apache.ignite.internal.processors.cache.distributed.dht.preloader;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cluster.BaselineNode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteFeatures;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.affinity.AffinityAssignment;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedManagerAdapter;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.jetbrains.annotations.NotNull;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_DISABLE_WAL_DURING_REBALANCING;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_FILE_REBALANCE_ENABLED;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_FILE_REBALANCE_THRESHOLD;
import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.configuration.IgniteConfiguration.DFLT_PDS_WAL_REBALANCE_THRESHOLD;
import static org.apache.ignite.internal.processors.cache.GridCacheUtils.UTILITY_CACHE_NAME;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.MOVING;

/**
 * DHT cache partition files preloader.
 */
public class IgnitePartitionPreloadManager extends GridCacheSharedManagerAdapter {
    /** */
    private final boolean fileRebalanceEnabled =
        IgniteSystemProperties.getBoolean(IGNITE_FILE_REBALANCE_ENABLED, true);

    /** */
    private final long fileRebalanceThreshold =
        IgniteSystemProperties.getLong(IGNITE_FILE_REBALANCE_THRESHOLD, DFLT_PDS_WAL_REBALANCE_THRESHOLD);

    /** Lock. */
    private final Lock lock = new ReentrantLock();

    /** Partition File rebalancing routine. */
    private volatile PartitionPreloadingRoutine partPreloadingRoutine;

    /** {@inheritDoc} */
    @Override protected void stop0(boolean cancel) {
        lock.lock();

        try {
            if (partPreloadingRoutine != null)
                partPreloadingRoutine.onDone(false);
        }
        finally {
            lock.unlock();
        }
    }

    /**
     * Initiates new partitions preload process from given {@code assignments}.
     *
     * @param rebalanceId Current rebalance id.
     * @param exchFut Exchange future.
     * @param assignments A map of cache assignments grouped by grpId.
     * @return Cache group identifiers with futures that will be completed when partitions are preloaded.
     */
    public Map<Integer, IgniteInternalFuture<GridDhtPreloaderAssignments>> preloadAsync(
        long rebalanceId,
        GridDhtPartitionsExchangeFuture exchFut,
        Map<Integer, GridDhtPreloaderAssignments> assignments
    ) {
        // Re-map assignments by node.
        Map<UUID, Map<Integer, Set<Integer>>> assignsByNode = new HashMap<>();
        Map<Integer, IgniteInternalFuture<GridDhtPreloaderAssignments>> futAssigns = new HashMap<>();

        for (Map.Entry<Integer, GridDhtPreloaderAssignments> e : assignments.entrySet()) {
            CacheGroupContext grp = cctx.cache().cacheGroup(e.getKey());
            GridDhtPreloaderAssignments assigns = e.getValue();

            GridDhtLocalPartition anyPart;

            if (F.isEmpty(assigns) || !supports(grp) ||
                (anyPart = F.first(grp.topology().currentLocalPartitions())) == null || anyPart.active()) {
                futAssigns.put(grp.groupId(), new GridFinishedFuture<>(assigns));

                continue;
            }

            for (Map.Entry<ClusterNode, GridDhtPartitionDemandMessage> e0 : assigns.entrySet()) {
                Map<Integer, Set<Integer>> grpAssigns =
                    assignsByNode.computeIfAbsent(e0.getKey().id(), v -> new HashMap<>());

                grpAssigns.put(grp.groupId(), e0.getValue().partitions().fullSet());
            }
        }

        if (!assignsByNode.isEmpty()) {
            lock.lock();

            try {
                if (isStopping())
                    return Collections.emptyMap();

                assert partPreloadingRoutine == null || partPreloadingRoutine.isDone();

                // Start new rebalance session.
                partPreloadingRoutine = new PartitionPreloadingRoutine(exchFut, cctx, rebalanceId, assignsByNode);

                Map<Integer, IgniteInternalFuture<GridDhtPreloaderAssignments>> futHistAssigns =
                    partPreloadingRoutine.startPartitionsPreloading();

                futAssigns.putAll(futHistAssigns);
            }
            finally {
                lock.unlock();
            }
        }

        return futAssigns;
    }

    /**
     * Callback on exchange done, should be invoked before initialize file page store.
     *
     * @param resVer Exchange result version.
     * @param grp Cache group.
     * @param cntrs Partition counters.
     * @param globalSizes Global partition sizes.
     * @param suppliers Historical suppliers.
     */
    public void onExchange(
        AffinityTopologyVersion resVer,
        CacheGroupContext grp,
        CachePartitionFullCountersMap cntrs,
        Map<Integer, Long> globalSizes,
        IgniteDhtPartitionHistorySuppliersMap suppliers
    ) {
        assert !cctx.kernalContext().clientNode() : "File preloader should not be created on the client node";

        PartitionPreloadingRoutine preloadRoutine = partPreloadingRoutine;

        // Abort the current parttition preloading if it is still in progress.
        if (preloadRoutine != null && !preloadRoutine.isDone())
            preloadRoutine.cancel();

        if (!supports(grp))
            return;

        boolean disable = filePreloadingApplicable(resVer, grp, cntrs, globalSizes, suppliers);

        // Should rebuild indexes if the last partition files preloading routine was incomplete.
        boolean rebuildIdx = false;

        // At this point, cache updates are queued, and we can safely
        // switch partitions to inactive mode and vice versa.
        for (GridDhtLocalPartition part : grp.topology().currentLocalPartitions()) {
            if (disable)
                part.disable();
            else if (part.enable() && cctx.pageStore().exists(grp.groupId(), part.id()))
                rebuildIdx = true;
        }

        if (rebuildIdx)
            cctx.database().rebuildIndexes(grp);
    }

    /**
     * Check whether file rebalancing is supported for the cache group.
     *
     * @param grp Cache group.
     * @param nodes List of Nodes.
     * @return {@code True} if file rebalancing is applicable for specified cache group and all nodes supports it.
     */
    public boolean supports(CacheGroupContext grp, @NotNull Collection<ClusterNode> nodes) {
        assert nodes != null && !nodes.isEmpty();

        if (!supports(grp))
            return false;

        if (!IgniteFeatures.allNodesSupports(nodes, IgniteFeatures.CACHE_PARTITION_FILE_REBALANCE))
            return false;

        Map<Integer, Long> globalSizes = grp.topology().globalPartSizes();

        if (globalSizes.isEmpty())
            return false;

        for (int p = 0; p < grp.affinity().partitions(); p++) {
            Long size = globalSizes.get(p);

            if (size != null && size > fileRebalanceThreshold)
                return true;
        }

        return false;
    }

    /**
     * Check whether file rebalancing is supported for the cache group.
     *
     * @param grp Cache group.
     * @return {@code True} if file rebalancing is applicable for specified cache group.
     */
    public boolean supports(CacheGroupContext grp) {
        if (grp == null || !fileRebalanceEnabled || !grp.persistenceEnabled() || grp.isLocal())
            return false;

        if (!IgniteSystemProperties.getBoolean(IGNITE_DISABLE_WAL_DURING_REBALANCING, true))
            return false;

        if (grp.config().getRebalanceDelay() == -1 || grp.config().getRebalanceMode() != CacheRebalanceMode.ASYNC)
            return false;

        // Do not rebalance system cache with files as they are not exists.
        assert grp.groupId() != CU.cacheId(UTILITY_CACHE_NAME) : "Should not preload utility cache partitions";

        for (GridCacheContext ctx : grp.caches()) {
            if (ctx.config().getAtomicityMode() == ATOMIC)
                return false;
        }

        return !grp.mvccEnabled();
    }

    /**
     * @param grp Group.
     * @return {@code True} If the last rebalance attempt was incomplete for specified cache group.
     */
    public boolean incompleteRebalance(CacheGroupContext grp) {
        PartitionPreloadingRoutine routine = partPreloadingRoutine;

        return routine != null && routine.isDone() && routine.remainingGroups().contains(grp.groupId());
    }

    /**
     * @param resVer Exchange result version.
     * @param grp Cache group.
     * @param cntrs Partition counters.
     * @param globalSizes Global partition sizes.
     * @param suppliers Historical suppliers.
     * @return {@code True} if file preloading is applicable for specified cache group.
     */
    private boolean filePreloadingApplicable(
        AffinityTopologyVersion resVer,
        CacheGroupContext grp,
        CachePartitionFullCountersMap cntrs,
        Map<Integer, Long> globalSizes,
        IgniteDhtPartitionHistorySuppliersMap suppliers
    ) {
        Collection<? extends BaselineNode> bltNodes = cctx.discovery().baselineNodes(resVer);

        if (bltNodes == null || !bltNodes.contains(cctx.localNode()))
            return false;

        if (grp.topology().stopping())
            return false;

        AffinityAssignment aff = grp.affinity().readyAffinity(resVer);

        assert aff != null;

        boolean hasApplicablePart = false;

        for (int p = 0; p < grp.affinity().partitions(); p++) {
            if (!aff.get(p).contains(cctx.localNode())) {
                // Should not start when a partition is evicting
                if (grp.topology().localPartition(p) != null)
                    return false;

                continue;
            }

            GridDhtLocalPartition part = grp.topology().localPartition(p);

            assert part != null : "grp=" + grp.cacheOrGroupName() + ", id=" + part.id();

            if (part.state() != MOVING)
                return false;

            if (part.dataStore().rowStore() != null || cctx.pageStore().exists(grp.groupId(), part.id()))
                return false;

            if (!hasApplicablePart) {
                Long partSize = globalSizes.get(p);

                if (partSize != null && partSize > fileRebalanceThreshold)
                    hasApplicablePart = true;
            }

            // Should have partition file supplier for all partitions to start file preloading.
            if (suppliers.getSupplier(grp.groupId(), p, cntrs.updateCounter(p)) == null)
                return false;
        }

        return hasApplicablePart;
    }
}

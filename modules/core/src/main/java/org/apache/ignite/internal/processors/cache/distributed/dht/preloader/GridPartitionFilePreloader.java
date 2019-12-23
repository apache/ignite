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

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteFeatures;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.NodeStoppingException;
import org.apache.ignite.internal.processors.affinity.AffinityAssignment;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedManagerAdapter;
import org.apache.ignite.internal.processors.cache.PartitionUpdateCounter;
import org.apache.ignite.internal.processors.cache.StateChangeRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.persistence.DbCheckpointListener;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheOffheapManager;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotListener;
import org.apache.ignite.internal.processors.cluster.BaselineTopologyHistoryItem;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.lang.IgniteInClosureX;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_PDS_FILE_REBALANCE_THRESHOLD;
import static org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion.NONE;
import static org.apache.ignite.internal.processors.cache.GridCacheUtils.UTILITY_CACHE_NAME;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.MOVING;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.OWNING;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.RENTING;

/**
 * DHT cache files preloader, manages partition files preloading routine.
 *
 * todo naming
 * GridCachePreloadSharedManager
 * GridPartitionFilePreloader
 * GridCachePartitionFilePreloader
 * GridFilePreloader
 * GridPartitionPreloader
 * GridSnapshotFilePreloader
 */
public class GridPartitionFilePreloader extends GridCacheSharedManagerAdapter {
    /** */
    private static final boolean FILE_REBALANCE_ENABLED = IgniteSystemProperties.getBoolean(
        IgniteSystemProperties.IGNITE_FILE_REBALANCE_ENABLED, true);

    /** */
    private static final long FILE_REBALANCE_THRESHOLD = IgniteSystemProperties.getLong(
        IGNITE_PDS_FILE_REBALANCE_THRESHOLD, 0);

    /** */
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    /** Checkpoint listener. */
    private final CheckpointListener cpLsnr = new CheckpointListener();

    /** */
    private volatile FileRebalanceFuture fileRebalanceFut = new FileRebalanceFuture();

    /**
     * @param ktx Kernal context.
     */
    public GridPartitionFilePreloader(GridKernalContext ktx) {
        assert CU.isPersistenceEnabled(ktx.config()) : "Persistence must be enabled to use file preloading";
    }

    /** {@inheritDoc} */
    @Override protected void start0() throws IgniteCheckedException {
        ((GridCacheDatabaseSharedManager)cctx.database()).addCheckpointListener(cpLsnr);

        cctx.snapshotMgr().addSnapshotListener(new PartitionSnapshotListener());
    }

    /** {@inheritDoc} */
    @Override protected void stop0(boolean cancel) {
        lock.writeLock().lock();

        try {
            ((GridCacheDatabaseSharedManager)cctx.database()).removeCheckpointListener(cpLsnr);

            fileRebalanceFut.onDone(false, new NodeStoppingException("Local node is stopping."), false);
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Callback on exchange done.
     *
     * @param exchFut Exchange future.
     */
    public void onExchangeDone(GridDhtPartitionsExchangeFuture exchFut) {
        assert !cctx.kernalContext().clientNode() : "File preloader should never be created on the client node";
        assert exchFut != null;

        if (!FILE_REBALANCE_ENABLED)
            return;

        GridDhtPartitionExchangeId exchId = exchFut.exchangeId();

        if (cctx.exchange().hasPendingExchange()) {
            if (log.isDebugEnabled())
                log.debug("Skipping rebalancing initialization exchange worker has pending exchange: " + exchId);

            return;
        }

        AffinityTopologyVersion rebTopVer = cctx.exchange().rebalanceTopologyVersion();

        FileRebalanceFuture rebFut = fileRebalanceFut;

        boolean forced = rebTopVer == NONE ||
            (rebFut.isDone() && (rebFut.result() == null || !rebFut.result())) || exchFut.localJoinExchange();

        Iterator<CacheGroupContext> itr = cctx.cache().cacheGroups().iterator();

        while (!forced && itr.hasNext()) {
            CacheGroupContext grp = itr.next();

            forced = exchFut.resetLostPartitionFor(grp.cacheOrGroupName()) ||
                grp.affinity().cachedVersions().contains(rebTopVer);
        }

        AffinityTopologyVersion lastAffChangeTopVer =
            cctx.exchange().lastAffinityChangedTopologyVersion(exchFut.topologyVersion());

        if (!forced && lastAffChangeTopVer.compareTo(rebTopVer) == 0) {
            assert lastAffChangeTopVer.compareTo(exchFut.topologyVersion()) != 0;

            if (log.isDebugEnabled())
                log.debug("Skipping file rebalancing initialization affinity not changed: " + exchId);

            return;
        }

        // Should interrupt current rebalance.
        // todo if memory cleanup in progress we should not wait for cleanup-future finish (on cancel)
        //      should somehow "re-set" the old-one cleanup future to new created rebalance future.
        if (!rebFut.isDone())
            rebFut.cancel();

        assert fileRebalanceFut.isDone();

        boolean locJoinBaselineChange = isLocalBaselineChange(exchFut);

        // At this point cache updates are queued and we can safely switch partitions to read-only mode.
        for (CacheGroupContext grp : cctx.cache().cacheGroups()) {
            if (!fileRebalanceSupported(grp))
                continue;

            Set<Integer> moving = detectMovingPartitions(grp, exchFut);

            if (!locJoinBaselineChange && !hasReadOnlyParts(grp)) {
                if (log.isDebugEnabled())
                    log.debug("File rebalancing skipped for group " + grp.cacheOrGroupName());

                continue;
            }

            if (moving != null && !moving.isEmpty() && log.isDebugEnabled())
                log.debug("Set READ-ONLY mode for cache=" + grp.cacheOrGroupName() + " parts=" + moving);

            // todo "global" partition size can change and file rebalance will not be applicable to it.
            //       add test case for specified scenario with global size change "on the fly".
            for (GridDhtLocalPartition part : grp.topology().currentLocalPartitions()) {
                // Partitions that no longer belong to current node should be evicted as usual.
                boolean toReadOnly = moving != null && moving.contains(part.id());

                if (part.dataStore().readOnly(toReadOnly)) {
                    // Should close grid cache data store - no updates expected.
                    ((GridCacheOffheapManager.GridCacheDataStore)part.dataStore().store(false)).close();
                }
            }
        }
    }

    private boolean hasReadOnlyParts(CacheGroupContext grp) {
        for (GridDhtLocalPartition part : grp.topology().currentLocalPartitions()) {
            if (part.dataStore().readOnly())
                return true;
        }

        return false;
    }

    private boolean isLocalBaselineChange(GridDhtPartitionsExchangeFuture exchFut) {
        if (exchFut.exchangeActions() == null)
            return false;

        StateChangeRequest req = exchFut.exchangeActions().stateChangeRequest();

        if (req == null)
            return false;

        BaselineTopologyHistoryItem prevBaseline = req.prevBaselineTopologyHistoryItem();

        if (prevBaseline == null)
            return false;

        return !prevBaseline.consistentIds().contains(cctx.localNode().consistentId());
    }

    private Set<Integer> detectMovingPartitions(CacheGroupContext grp, GridDhtPartitionsExchangeFuture exchFut) {
        AffinityAssignment aff = grp.affinity().readyAffinity(exchFut.topologyVersion());

        assert aff != null;

        CachePartitionFullCountersMap cntrsMap = grp.topology().fullUpdateCounters();

        Set<Integer> movingParts = new HashSet<>();

        Map<Integer, Long> globalSizes = grp.topology().globalPartSizes();

        boolean fatEnough = false;

        for (int p = 0; p < grp.affinity().partitions(); p++) {
            if (!aff.get(p).contains(cctx.localNode()))
                continue;

            if (!fatEnough && globalSizes.get(p) >= FILE_REBALANCE_THRESHOLD)
                fatEnough = true;

            GridDhtLocalPartition part = grp.topology().localPartition(p);

            if (part.state() == OWNING)
                return null;

            // Should have partition file supplier to start file rebalance.
            long cntr = cntrsMap.updateCounter(p);

            if (exchFut.partitionFileSupplier(grp.groupId(), p, cntr) == null)
                return null;

            // If partition is currently rented prevent destroy and start clearing process.
            // todo think about reserve/clear
            if (part.state() == RENTING)
                part.moving();

            assert part.state() == MOVING : "Unexpected partition state [cache=" + grp.cacheOrGroupName() +
                ", p=" + p + ", state=" + part.state() + "]";

            movingParts.add(p);
        }

        return fatEnough ? movingParts : null;
    }

    /** @deprecated used only for debugging, should be removed */
    @Deprecated
    public boolean isPreloading(int grpId) {
        return fileRebalanceFut.isPreloading(grpId);
    }

    /**
     * This method initiates new file rebalance process from given {@code assignments} by creating new file
     * rebalance future based on them. Cancels previous file rebalance future and sends rebalance started event (todo).
     * In case of delayed rebalance method schedules the new one with configured delay based on {@code lastExchangeFut}.
     *
     * @param assignsMap A map of cache assignments grouped by grpId.
     * @param rebalanceId Current rebalance id.
     * @return Runnable to execute the chain.
     */
    public Runnable addNodeAssignments(
        Map<Integer, GridDhtPreloaderAssignments> assignsMap,
        AffinityTopologyVersion topVer,
        long rebalanceId,
        GridDhtPartitionsExchangeFuture exchFut) {
        NavigableMap</**order*/Integer, Map<ClusterNode, Map</**grp*/Integer, Set<Integer>>>> nodeOrderAssignsMap =
            remapAssignments(assignsMap, exchFut);

        if (nodeOrderAssignsMap.isEmpty()) {
            if (log.isDebugEnabled())
                log.debug("Skipping file rebalancing due to empty assignments.");

            return null;
        }

        if (!cctx.kernalContext().grid().isRebalanceEnabled()) {
            if (log.isDebugEnabled())
                log.debug("Cancel partition file demand because rebalance disabled on current node.");

            return null;
        }

        if (log.isInfoEnabled())
            log.info("Starting file rebalancing");

        if (log.isTraceEnabled())
            log.trace(formatMappings(nodeOrderAssignsMap));

        // Start new rebalance session.
        FileRebalanceFuture rebFut = fileRebalanceFut;

        lock.writeLock().lock();

        try {
            if (!rebFut.isDone())
                rebFut.cancel();

            fileRebalanceFut = rebFut = new FileRebalanceFuture(cpLsnr, nodeOrderAssignsMap, topVer, cctx, rebalanceId, log, exchFut.exchangeId());

            FileRebalanceNodeRoutine lastFut = null;

            if (log.isInfoEnabled())
                log.info("Prepare the chain to demand assignments: " + nodeOrderAssignsMap);

            for (Map.Entry<Integer, Map<ClusterNode, Map<Integer, Set<Integer>>>> entry : nodeOrderAssignsMap.descendingMap().entrySet()) {
                Map<ClusterNode, Map<Integer, Set<Integer>>> descNodeMap = entry.getValue();

                int order = entry.getKey();

                for (Map.Entry<ClusterNode, Map<Integer, Set<Integer>>> assignEntry : descNodeMap.entrySet()) {
                    FileRebalanceNodeRoutine fut = new FileRebalanceNodeRoutine(cctx, fileRebalanceFut, log,
                        assignEntry.getKey(), order, rebalanceId, assignEntry.getValue(), topVer);

                    // todo seeems we don't need to track all futures through map, we should track only last
                    rebFut.add(order, fut);

                    if (lastFut != null) {
                        final FileRebalanceNodeRoutine lastFut0 = lastFut;

                        fut.listen(f -> {
                            try {
                                if (f.isCancelled())
                                    return;

                                if (log.isDebugEnabled())
                                    log.debug("Running next task, last future result is " + f.get());

                                if (f.get()) // Not cancelled.
                                    lastFut0.requestPartitions();
                                // todo check how this chain is cancelling
                            }
                            catch (IgniteCheckedException e) {
                                lastFut0.onDone(e);
                            }
                        });
                    }

                    lastFut = fut;
                }
            }

            cctx.kernalContext().getSystemExecutorService().submit(rebFut::clearPartitions);

            rebFut.listen(new IgniteInClosureX<IgniteInternalFuture<Boolean>>() {
                @Override public void applyx(IgniteInternalFuture<Boolean> fut0) throws IgniteCheckedException {
                    if (fut0.isCancelled()) {
                        log.info("File rebalance canceled [topVer=" + topVer + "]");

                        return;
                    }

                    if (log.isInfoEnabled())
                        log.info("The final persistence rebalance is done [result=" + fut0.get() + ']');
                }
            });

            return lastFut::requestPartitions;
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    public void printDiagnostic() {
        if (log.isInfoEnabled())
            log.info(debugInfo());
    }

    private String debugInfo() {
        StringBuilder buf = new StringBuilder("\n\nDiagnostic for file rebalancing [node=" + cctx.localNodeId() + ", finished=" + fileRebalanceFut.isDone() + ", failed=" + fileRebalanceFut.isFailed() +", cancelled=" + fileRebalanceFut.isCancelled() + "]");

        if (!fileRebalanceFut.isDone() || fileRebalanceFut.isCancelled() || fileRebalanceFut.isFailed())
            buf.append(fileRebalanceFut.toString());

        return buf.toString();
    }

    private String formatMappings(Map<Integer, Map<ClusterNode, Map<Integer, Set<Integer>>>> map) {
        StringBuilder buf = new StringBuilder("\nFile rebalancing mappings [node=" + cctx.localNodeId() + "]\n");

        for (Map.Entry<Integer, Map<ClusterNode, Map<Integer, Set<Integer>>>> entry : map.entrySet()) {
            buf.append("\torder=").append(entry.getKey()).append('\n');

            for (Map.Entry<ClusterNode, Map<Integer, Set<Integer>>> mapEntry : entry.getValue().entrySet()) {
                buf.append("\t\tnode=").append(mapEntry.getKey().id()).append('\n');

                for (Map.Entry<Integer, Set<Integer>> setEntry : mapEntry.getValue().entrySet()) {
                    buf.append("\t\t\tgrp=").append(cctx.cache().cacheGroup(setEntry.getKey()).cacheOrGroupName()).append('\n');

                    for (int p : setEntry.getValue())
                        buf.append("\t\t\t\tp=").append(p).append('\n');
                }

                buf.append('\n');
            }

            buf.append('\n');
        }

        return buf.toString();
    }

    /**
     * @param assignsMap The map of cache groups assignments to process.
     * @return The map of cache assignments <tt>[group_order, [node, [group_id, partitions]]]</tt>
     */
    private NavigableMap<Integer, Map<ClusterNode, Map<Integer, Set<Integer>>>> remapAssignments(
        Map<Integer, GridDhtPreloaderAssignments> assignsMap, GridDhtPartitionsExchangeFuture exchFut) {
        NavigableMap<Integer, Map<ClusterNode, Map<Integer, Set<Integer>>>> result = new TreeMap<>();

        for (Map.Entry<Integer, GridDhtPreloaderAssignments> grpEntry : assignsMap.entrySet()) {
            int grpId = grpEntry.getKey();

            CacheGroupContext grp = cctx.cache().cacheGroup(grpId);

            GridDhtPreloaderAssignments assigns = grpEntry.getValue();

            if (!fileRebalanceRequired(grp, assigns, exchFut))
                continue;

            int grpOrderNo = grp.config().getRebalanceOrder();

            result.putIfAbsent(grpOrderNo, new HashMap<>());

            for (Map.Entry<ClusterNode, GridDhtPartitionDemandMessage> grpAssigns : assigns.entrySet()) {
                ClusterNode node = grpAssigns.getKey();

                result.get(grpOrderNo).putIfAbsent(node, new HashMap<>());

                result.get(grpOrderNo)
                    .get(node)
                    .putIfAbsent(grpId,
                        grpAssigns.getValue()
                            .partitions()
                            .fullSet());
            }
        }

        return result;
    }

    /**
     * todo access
     * @param fut The future to check.
     * @return <tt>true</tt> if future can be processed.
     */
    boolean staleFuture(FileRebalanceNodeRoutine fut) {
        return fut == null || fut.isCancelled() || fut.isFailed() || fut.isDone() || topologyChanged(fut);
    }

    /**
     * @param fut Future.
     * @return {@code True} if rebalance topology version changed by exchange thread or force
     * reassing exchange occurs, see {@link RebalanceReassignExchangeTask} for details.
     */
    private boolean topologyChanged(FileRebalanceNodeRoutine fut) {
        return !cctx.exchange().rebalanceTopologyVersion().equals(fut.topologyVersion());
        // todo || fut != rebalanceFut; // Same topology, but dummy exchange forced because of missing partitions.
    }

    /**
     * Check whether file rebalancing is supported by the cache group.
     *
     * @param grp Cache group.
     * @param nodes List of Nodes.
     * @return {@code True} if file rebalancing is applicable for specified cache group and all nodes supports it.
     */
    public boolean fileRebalanceSupported(CacheGroupContext grp, Collection<ClusterNode> nodes) {
        assert nodes != null && !nodes.isEmpty();

        return fileRebalanceSupported(grp) &&
            IgniteFeatures.allNodesSupports(nodes, IgniteFeatures.CACHE_PARTITION_FILE_REBALANCE);
    }

    /**
     * Check whether file rebalancing is supported by the cache group.
     *
     * @param grp Cache group.
     * @return {@code True} if file rebalancing is applicable for specified cache group.
     */
    public boolean fileRebalanceSupported(CacheGroupContext grp) {
        if (!FILE_REBALANCE_ENABLED || !grp.persistenceEnabled() || grp.isLocal())
            return false;

        if (grp.config().getRebalanceDelay() == -1 || grp.config().getRebalanceMode() == CacheRebalanceMode.NONE)
            return false;

        // Do not rebalance system cache with files as they are not exists.
        if (grp.groupId() == CU.cacheId(UTILITY_CACHE_NAME))
            return false;

        if (grp.mvccEnabled())
            return false;

        if (grp.hasAtomicCaches())
            return false;

        // todo redundant check ?
        Map<Integer, Long> globalSizes = grp.topology().globalPartSizes();

        if (globalSizes.isEmpty())
            return false;

        for (int p = 0; p < grp.affinity().partitions(); p++) {
            Long size = globalSizes.get(p);

            if (size != null && size > FILE_REBALANCE_THRESHOLD)
                return true;
        }

        // Also should check the sizes of the local partitions.
        for (GridDhtLocalPartition part : grp.topology().currentLocalPartitions()) {
            if (part.fullSize() > FILE_REBALANCE_THRESHOLD)
                return true;
        }

        return false;
    }

    /**
     * Check whether file rebalancing is required for the cache group.
     *
     * @param grp The corresponding to assignments cache group context.
     * @param assignments Preloading assignments.
     * @return {@code True} if cache must be rebalanced by sending files.
     */
    public boolean fileRebalanceRequired(CacheGroupContext grp, GridDhtPreloaderAssignments assignments, GridDhtPartitionsExchangeFuture exchFut) {
        if (assignments == null || assignments.isEmpty()) {
            if (log.isDebugEnabled())
                log.debug("File rebalancing not required for group " + grp.cacheOrGroupName() + " due to empty assignments.");

            return false;
        }

        if (!fileRebalanceSupported(grp, assignments.keySet())) {
            if (log.isDebugEnabled())
                log.debug("File rebalancing not required for group " + grp.cacheOrGroupName() + " - not supported.");

            return false;
        }

        if (!hasReadOnlyParts(grp))
            return false;

        // onExchangeDone should create all partitions
        AffinityAssignment aff = grp.affinity().readyAffinity(exchFut.topologyVersion());

        CachePartitionFullCountersMap cntrsMap = grp.topology().fullUpdateCounters();

        // todo currentLocalPartitions?
        int parts = grp.affinity().partitions();

        for (int p = 0; p < parts; p++) {
            if (!aff.get(p).contains(cctx.localNode()))
                continue;

            GridDhtLocalPartition part = grp.topology().localPartition(p);

            if (part.state() == OWNING) {
                if (log.isDebugEnabled())
                    log.debug("File rebalancing not required for group " + grp.cacheOrGroupName() + " - we have owned partition in this group [p=" + part.id() + "]");

                return false;
            }

            assert part.state() == MOVING : "Unexpected partition state [cache=" + grp.cacheOrGroupName() +
                ", p=" + part.id() + ", state=" + part.state() + "]";

            if (exchFut.partitionFileSupplier(grp.groupId(), part.id(), cntrsMap.updateCounter(part.id())) == null) {
                if (log.isDebugEnabled())
                    log.debug("File rebalancing not required for group " + grp.cacheOrGroupName() + " - no supplier for part "+part.id());

                return false;
            }
        }

        Map<Integer, Long> globalSizes = grp.topology().globalPartSizes();

        boolean enoughData = false;

        // Enabling file rebalancing only when we have at least one big enough partition.
        for (Long partSize : globalSizes.values()) {
            if (partSize >= FILE_REBALANCE_THRESHOLD) {
                enoughData = true;

                break;
            }
        }

        if (!enoughData) {
            if (log.isDebugEnabled())
                log.debug("File rebalancing not required for group " + grp.cacheOrGroupName() + " - partitions too small");

            return false;
        }

        // For now mixed rebalancing modes are not supported.
        for (GridDhtPartitionDemandMessage msg : assignments.values()) {
            if (msg.partitions().hasHistorical())
                return false;
        }

        // todo for debug purposes only
        // todo rework this check
        for (int p = 0; p < parts; p++) {
            if (!aff.get(p).contains(cctx.localNode()))
                continue;

            GridDhtLocalPartition part = grp.topology().localPartition(p);

            assert part.dataStore().readOnly() :
                "Expected read-only partition [cache=" + grp.cacheOrGroupName() + ", p=" + part.id() + "]";
        }

        return true;
    }

    public IgniteInternalFuture<Map<Integer, Long>> switchPartitions(int grpId, Set<Integer> parts, IgniteInternalFuture fut) {
        final CacheGroupContext grp = cctx.cache().cacheGroup(grpId);

        GridFutureAdapter<Map<Integer, Long>> endFut = new GridFutureAdapter<>();

        cpLsnr.schedule(() -> {
            if (fut.isDone())
                return;

            Map<Integer, Long> resCntrs = new HashMap<>(U.capacity(parts.size()));

            Map<Integer, T2<PartitionUpdateCounter, PartitionUpdateCounter>> tempCntrs = new HashMap<>(U.capacity(parts.size()));

            // todo should be under cancel lock?
            for (Integer partId : parts) {
                GridDhtLocalPartition part = grp.topology().localPartition(partId);

                assert part.dataStore().readOnly() : "cache=" + grpId + " p=" + partId;

                // Save current counter.
                PartitionUpdateCounter readCntr = part.dataStore().store(true).partUpdateCounter();

                // Save current update counter.
                PartitionUpdateCounter snapshotCntr = part.dataStore().store(false).partUpdateCounter();

                part.readOnly(false);

                // Clear all on-heap entries.
                // todo something smarter and check large partition
                if (grp.sharedGroup()) {
                    for (GridCacheContext ctx : grp.caches())
                        part.entriesMap(ctx).map.clear();
                }
                else
                    part.entriesMap(null).map.clear();

                assert readCntr != snapshotCntr && snapshotCntr != null && readCntr != null : "grp=" +
                    grp.cacheOrGroupName() + ", p=" + partId + ", readCntr=" + readCntr + ", snapCntr=" + snapshotCntr;

                tempCntrs.put(partId, new T2<>(readCntr, snapshotCntr));
            }

            AffinityTopologyVersion infinTopVer = new AffinityTopologyVersion(Long.MAX_VALUE, 0);

            IgniteInternalFuture<?> partReleaseFut = cctx.partitionReleaseFuture(infinTopVer);

            // Operations that are in progress now will be lost and should be included in historical rebalancing.
            // These operations can update the old update counter or the new update counter, so the maximum applied
            // counter is used after all updates are completed.
            partReleaseFut.listen(c -> {
                    for (Map.Entry<Integer, T2<PartitionUpdateCounter, PartitionUpdateCounter>> entry : tempCntrs.entrySet()) {
                        int partId = entry.getKey();

                        PartitionUpdateCounter readCntr = entry.getValue().get1();
                        PartitionUpdateCounter snapshotCntr = entry.getValue().get2();

                        resCntrs.put(entry.getKey(), Math.max(readCntr.highestAppliedCounter(), snapshotCntr.highestAppliedCounter()));
                    }

                    endFut.onDone(resCntrs);
                }
            );
        });

        return endFut;
    }

    /**todo should be elimiaated (see comment about restorepartition) */
    public static class CheckpointListener implements DbCheckpointListener {
        /** Queue. */
        private final ConcurrentLinkedQueue<CheckpointTask> queue = new ConcurrentLinkedQueue<>();

        /** {@inheritDoc} */
        @Override public void onMarkCheckpointBegin(Context ctx) {
            Runnable r;

            while ((r = queue.poll()) != null)
                r.run();
        }

        /** {@inheritDoc} */
        @Override public void onCheckpointBegin(Context ctx) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void beforeCheckpointBegin(Context ctx) {
            // No-op.
        }

        /** */
        public void cancelAll() {
            ArrayList<CheckpointTask> tasks = new ArrayList<>(queue);

            queue.clear();

            for (CheckpointTask task : tasks)
                task.fut.onDone();
        }

        public IgniteInternalFuture<Void> schedule(final Runnable task) {
            CheckpointTask<Void> cpTask = new CheckpointTask<>(() -> {
                task.run();

                return null;
            });

            queue.offer(cpTask);

            return cpTask.fut;
        }

        /** */
        private static class CheckpointTask<R> implements Runnable {
            /** */
            final GridFutureAdapter<R> fut = new GridFutureAdapter<>();

            /** */
            final Callable<R> task;

            /** */
            CheckpointTask(Callable<R> task) {
                this.task = task;
            }

            /** {@inheritDoc} */
            @Override public void run() {
                try {
                    fut.onDone(task.call());
                }
                catch (Exception e) {
                    fut.onDone(e);
                }
            }
        }
    }

    /**
     * Partition snapshot listener.
     */
    private class PartitionSnapshotListener implements SnapshotListener {
        /** {@inheritDoc} */
        @Override public void onPartition(UUID nodeId, File file, int grpId, int partId) {
            fileRebalanceFut.onPartitionSnapshotReceived(nodeId, file, grpId, partId);
        }

        /** {@inheritDoc} */
        @Override public void onEnd(UUID rmtNodeId) {
            // No-op.
            // todo add assertion
        }

        /** {@inheritDoc} */
        @Override public void onException(UUID rmtNodeId, Throwable t) {
            log.error("Unable to create remote snapshot: " + t.getMessage(), t);
        }
    }
}

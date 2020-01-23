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
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteFutureCancelledCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.NodeStoppingException;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.PartitionUpdateCounter;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.persistence.DataRegion;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheOffheapManager;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMemoryEx;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.events.EventType.EVT_CACHE_REBALANCE_PART_LOADED;

/**
 * File rebalance routine.
 */
public class FileRebalanceRoutine extends GridFutureAdapter<Boolean> {
    /** Rebalance topology version. */
    private final AffinityTopologyVersion topVer;

    /** Unique (per demander) rebalance id. */
    private final long rebalanceId;

    /** Lock. */
    private final ReentrantLock lock = new ReentrantLock();

    /** Cache context. */
    private final GridCacheSharedContext cctx;

    /** Logger. */
    private final IgniteLogger log;

    /** Index rebuild future. */
    private final GridCompoundFuture idxRebuildFut = new GridCompoundFuture<>();

    /** Exchange ID. */
    private final GridDhtPartitionExchangeId exchId;

    /** Assignments ordered by cache rebalance priority and node. */
    private final Collection<Map<ClusterNode, Map<Integer, Set<Integer>>>> orderedAssgnments;

    /** Unique partition identifier with node identifier. */
    private final Map<Long, UUID> partsToNodes = new ConcurrentHashMap<>();

    /** The remaining groups with the number of partitions. */
    @GridToStringInclude
    private final Map<Integer, Integer> remaining = new ConcurrentHashMap<>();

    /** Count of partition snapshots received. */
    private final AtomicInteger receivedCnt = new AtomicInteger();

    /** Cache group with restored partition snapshots and HWM value of update counter. */
    @GridToStringInclude
    private final Map<Integer, Map<Integer, Long>> restored = new ConcurrentHashMap<>();

    /** Off-heap region clear tasks. */
    @GridToStringInclude
    private final Map<String, IgniteInternalFuture> offheapClearTasks = new ConcurrentHashMap<>();

    /** Snapshot future. */
    private volatile IgniteInternalFuture<Boolean> snapFut;

    /** Checkpoint listener. */
    private final Consumer<Runnable> cpLsnr;

    /** */
    public FileRebalanceRoutine() {
        this(null, null, null, null, 0, null);

        onDone(true);
    }

    /**
     * @param assigns Assigns.
     * @param startVer Topology version on which the rebalance started.
     * @param cctx Cache shared context.
     * @param exchId Exchange ID.
     * @param rebalanceId Rebalance ID
     * @param cpLsnr Checkpoint listener.
     */
    public FileRebalanceRoutine(
        Collection<Map<ClusterNode, Map<Integer, Set<Integer>>>> assigns,
        AffinityTopologyVersion startVer,
        GridCacheSharedContext cctx,
        GridDhtPartitionExchangeId exchId,
        long rebalanceId,
        Consumer<Runnable> cpLsnr
    ) {
        this.cctx = cctx;
        this.rebalanceId = rebalanceId;
        this.exchId = exchId;
        this.cpLsnr = cpLsnr;

        orderedAssgnments = assigns;
        topVer = startVer;
        log = cctx == null ? null : cctx.logger(getClass());
    }

    /**
     * Initialize and start partition preloading.
     */
    public void requestPartitionsSnapshot() {
        initialize();

        Set<Integer> requestedGroups = new GridConcurrentHashSet<>();

        cctx.kernalContext().getSystemExecutorService().submit(() -> {
            for (Map<ClusterNode, Map<Integer, Set<Integer>>> map : orderedAssgnments) {
                for (Map.Entry<ClusterNode, Map<Integer, Set<Integer>>> nodeAssigns : map.entrySet()) {
                    UUID nodeId = nodeAssigns.getKey().id();
                    Map<Integer, Set<Integer>> assigns = nodeAssigns.getValue();

                    try {
                        lock.lock();

                        try {
                            if (isDone())
                                return;

                            if (snapFut != null && (snapFut.isCancelled() || !snapFut.get()))
                                return;

                            Set<String> grps = new HashSet<>();

                            for (Integer grpId : assigns.keySet()) {
                                CacheGroupContext grp = cctx.cache().cacheGroup(grpId);

                                grps.add(grp.cacheOrGroupName());

                                if (!requestedGroups.contains(grpId)) {
                                    requestedGroups.add(grpId);

                                    grp.preloader().sendRebalanceStartedEvent(exchId.discoveryEvent());
                                }
                            }

                            U.log(log, "Preloading partition files [supplier=" + nodeId + ", groups=" + grps + "]");

                            snapFut = cctx.snapshotMgr().createRemoteSnapshot(nodeId, assigns);
                        }
                        finally {
                            lock.unlock();
                        }

                        snapFut.get();
                    }
                    catch (IgniteFutureCancelledCheckedException ignore) {
                        // No-op.
                    }
                    catch (IgniteCheckedException e) {
                        if (!isDone()) {
                            log.error(e.getMessage(), e);

                            onDone(e);

                            return;
                        }

                        if (log.isDebugEnabled())
                            log.debug("Stale error (ignore) " + e.getMessage());
                    }
                }
            }
        });
    }

    /**
     * Prepare to start rebalance routine.
     */
    private void initialize() {
        final Map<String, Set<Long>> regionToParts = new HashMap<>();

        lock.lock();

        try {
            for (Map<ClusterNode, Map<Integer, Set<Integer>>> map : orderedAssgnments) {
                for (Map.Entry<ClusterNode, Map<Integer, Set<Integer>>> mapEntry : map.entrySet()) {
                    UUID nodeId = mapEntry.getKey().id();

                    for (Map.Entry<Integer, Set<Integer>> entry : mapEntry.getValue().entrySet()) {
                        int grpId = entry.getKey();

                        CacheGroupContext grp = cctx.cache().cacheGroup(grpId);

                        String regName = cctx.cache().cacheGroup(grpId).dataRegion().config().getName();

                        Set<Long> regionParts = regionToParts.computeIfAbsent(regName, v -> new LinkedHashSet<>());

                        for (Integer partId : entry.getValue()) {
                            assert grp.topology().localPartition(partId).dataStore().readOnly() :
                                "cache=" + grp.cacheOrGroupName() + " p=" + partId;

                            long grpAndPart = uniquePartId(grpId, partId);

                            regionParts.add(grpAndPart);

                            partsToNodes.put(grpAndPart, nodeId);
                        }

                        Integer remainParts = remaining.get(grpId);

                        if (remainParts == null)
                            remainParts = 0;

                        remaining.put(grpId, remainParts + entry.getValue().size());
                    }
                }
            }

            // Start clearing off-heap regions.
            for (Map.Entry<String, Set<Long>> e : regionToParts.entrySet()) {
                String regionName = e.getKey();
                Set<Long> parts = e.getValue();

                DataRegion region = cctx.database().dataRegion(regionName);

                ClearRegionTask offheapClearTask = new ClearRegionTask(parts, region, cctx, log);

                offheapClearTasks.put(regionName, offheapClearTask.clearAsync());
            }
        }
        catch (IgniteCheckedException e) {
            onDone(e);
        }
        finally {
            lock.unlock();
        }
    }

    /**
     * @param nodeId Node ID.
     * @param file Partition snapshot file.
     * @param grpId Cache group ID.
     * @param partId Partition ID.
     */
    public void onPartitionSnapshotReceived(UUID nodeId, File file, int grpId, int partId) {
        try {
            awaitCleanupIfNeeded(grpId);

            if (isDone())
                return;

            CacheGroupContext grp = cctx.cache().cacheGroup(grpId);

            if (grp == null) {
                log.warning("Snapshot initialization skipped, cache group not found [grpId=" + grpId + "]");

                return;
            }

            grp.topology().localPartition(partId).initialize(file);

            grp.preloader().rebalanceEvent(partId, EVT_CACHE_REBALANCE_PART_LOADED, exchId.discoveryEvent());

            changePartitionMode(grpId, partId)
                .listen(f -> {
                    try {
                        if (!f.isCancelled())
                            onPartitionSnapshotRestored(grpId, partId, f.get());
                    }
                    catch (IgniteCheckedException e) {
                        log.error("Unable to restore partition snapshot [grpId=" + grpId + ", p=" + partId + "]");

                        onDone(e);
                    }
                });

            if (receivedCnt.incrementAndGet() == partsToNodes.size()) {
                // All partition files are received and it is necessary to force checkpoint
                // to complete file rebalancing process.
                U.log(log, "All partition files are received - triggering checkpoint to complete rebalancing.");

                cctx.database().wakeupForCheckpoint("Partition files preload complete.");
            }
        }
        catch (IOException | IgniteCheckedException e) {
            log.error("Unable to handle partition snapshot", e);

            onDone(e);
        }
    }

    /**
     * @param grpId Cache group ID.
     * @param partId Partition ID.
     * @param cntr The highest value of the update counter before this partition began to process updates.
     */
    private void onPartitionSnapshotRestored(int grpId, int partId, long cntr) {
        Integer partsCnt = remaining.get(grpId);

        assert partsCnt != null;

        Map<Integer, Long> cntrs = restored.computeIfAbsent(grpId, v-> new ConcurrentHashMap<>());

        cntrs.put(partId, cntr);

        if (partsCnt == cntrs.size() && remaining.remove(grpId) != null)
            onCacheGroupDone(grpId, cntrs);
    }

    /**
     * @param grpId Group ID.
     * @param maxCntrs Partition set with HWM update counter value for hstorical rebalance.
     */
    private void onCacheGroupDone(int grpId, Map<Integer, Long> maxCntrs) {
        CacheGroupContext grp = cctx.cache().cacheGroup(grpId);
        String grpName = grp.cacheOrGroupName();

        assert !grp.localWalEnabled() : "grp=" + grpName;

        Map<UUID, Map<Integer, T2<Long, Long>>> histAssignments = new HashMap<>();

        for (Map.Entry<Integer, Long> e : maxCntrs.entrySet()) {
            int partId = e.getKey();

            long initCntr = grp.topology().localPartition(partId).initialUpdateCounter();
            long maxCntr = e.getValue();

            assert maxCntr >= initCntr : "from=" + initCntr + ", to=" + maxCntr;

            if (initCntr != maxCntr) {
                UUID nodeId = partsToNodes.get(uniquePartId(grpId, partId));

                histAssignments.computeIfAbsent(nodeId, v -> new TreeMap<>()).put(partId, new T2<>(initCntr, maxCntr));

                continue;
            }

            if (log.isDebugEnabled())
                log.debug("No need for WAL rebalance [grp=" + grpName + ", p=" + partId + "]");
        }

        GridQueryProcessor qryProc = cctx.kernalContext().query();

        if (qryProc.moduleEnabled()) {
            U.log(log,"Starting index rebuild [grp=" + grpName + "]");

            for (GridCacheContext ctx : grp.caches()) {
                IgniteInternalFuture<?> fut = qryProc.rebuildIndexesFromHash(ctx);

                if (fut != null)
                    idxRebuildFut.add(fut);
            }
        }
        else
            U.log(log, "Skipping index rebuild [grp=" + grpName + "]");

        // Cache group file rebalancing is finished, historical rebalancing will send separate events.
        grp.preloader().sendRebalanceFinishedEvent(exchId.discoveryEvent());

        int remain = remaining.size();

        U.log(log, "Cache group files preload complete [grp=" + grpName + ", remainParts=" + remain + "]");

        if (histAssignments.isEmpty())
            cctx.walState().onGroupRebalanceFinished(grp.groupId(), topVer);
        else
            requestHistoricalRebalance(grp, histAssignments);

        if (remain == 0) {
            idxRebuildFut.markInitialized();

            onDone(true);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean cancel() {
        return onDone(false, null, true);
    }

    /** {@inheritDoc} */
    @Override protected boolean onDone(@Nullable Boolean res, @Nullable Throwable err, boolean cancel) {
        boolean nodeIsStopping = X.hasCause(err, NodeStoppingException.class);

        boolean done = super.onDone(res, nodeIsStopping ? null : err, nodeIsStopping || cancel);

        if (cancel || err != null && done) {
            lock.lock();

            try {
                U.log(log, "Cancelling file rebalancing [topVer=" + topVer + "]");

                if (snapFut != null && !snapFut.isDone()) {
                    if (log.isDebugEnabled())
                        log.debug("Cancelling snapshot creation [fut=" + snapFut + "]");

                    snapFut.cancel();
                }

                if (err == null) {
                    // Should await until off-heap cleanup is finished.
                    for (IgniteInternalFuture fut : offheapClearTasks.values()) {
                        if (!fut.isDone())
                            fut.get();
                    }
                }
            }
            catch (IgniteCheckedException e) {
                if (err != null)
                    e.addSuppressed(err);

                log.error("Failed to cancel file rebalancing.", e);
            }
            finally {
                lock.unlock();
            }
        }

        return done;
    }

    /**
     * @param grp Cache group.
     * @param assigns Assignments.
     */
    private void requestHistoricalRebalance(CacheGroupContext grp, Map<UUID, Map<Integer, T2<Long, Long>>> assigns) {
        GridDhtPreloaderAssignments grpAssigns = new GridDhtPreloaderAssignments(exchId, topVer);

        for (Map.Entry<UUID, Map<Integer, T2<Long, Long>>> entry : assigns.entrySet()) {
            ClusterNode node = cctx.discovery().node(entry.getKey());
            Map<Integer, T2<Long, Long>> nodeAssigns = entry.getValue();

            GridDhtPartitionDemandMessage msg = new GridDhtPartitionDemandMessage(rebalanceId, topVer, grp.groupId());

            for (Map.Entry<Integer, T2<Long, Long>> e : nodeAssigns.entrySet()) {
                int p = e.getKey();
                long from = e.getValue().get1();
                long to = e.getValue().get2();

                assert from != 0 && from <= to :
                    "grp=" + grp.cacheOrGroupName() + "p=" + p + ", from=" + from + ", to=" + to;

                if (log.isDebugEnabled())
                    log.debug("Requesting hstorical rebalancing [grp=" + grp.cacheOrGroupName() + ", p=" + p + ", from=" + from + ", to=" + to);

                msg.partitions().addHistorical(p, from, to, nodeAssigns.size());
            }

            grpAssigns.put(node, msg);
        }

        GridCompoundFuture<Boolean, Boolean> histFut = new GridCompoundFuture<>(CU.boolReducer());

        Runnable task = grp.preloader().addAssignments(grpAssigns, true, rebalanceId, null, histFut);

        cctx.kernalContext().getSystemExecutorService().submit(task);
    }

    /**
     * Schedule partition mode change to enable updates.
     *
     * @param grpId Cache group ID.
     * @param partId Partition ID.
     * @return Future that will be done when partition mode changed.
     */
    public IgniteInternalFuture<Long> changePartitionMode(int grpId, int partId) {
        GridFutureAdapter<Long> endFut = new GridFutureAdapter<>();

        cpLsnr.accept(() -> {
            if (isDone())
                return;

            lock.lock();

            try {
                final CacheGroupContext grp = cctx.cache().cacheGroup(grpId);

                // Cache was concurrently destroyed.
                if (grp == null)
                    return;

                GridDhtLocalPartition part = grp.topology().localPartition(partId);

                assert part.dataStore().readOnly() : "grpId=" + grpId + " p=" + partId;

                // Save current counter.
                PartitionUpdateCounter readCntr =
                    ((GridCacheOffheapManager.GridCacheDataStore)part.dataStore()).readOnlyPartUpdateCounter();

                // Save current update counter.
                PartitionUpdateCounter snapshotCntr = part.dataStore().partUpdateCounter();

                part.readOnly(false);

                AffinityTopologyVersion infinTopVer = new AffinityTopologyVersion(Long.MAX_VALUE, 0);

                IgniteInternalFuture<?> partReleaseFut = cctx.partitionReleaseFuture(infinTopVer);

                // Operations that are in progress now will be lost and should be included in historical rebalancing.
                // These operations can update the old update counter or the new update counter, so the maximum applied
                // counter is used after all updates are completed.
                partReleaseFut.listen(c -> {
                        long hwm = Math.max(readCntr.highestAppliedCounter(), snapshotCntr.highestAppliedCounter());

                        cctx.kernalContext().getSystemExecutorService().submit(() -> endFut.onDone(hwm));
                    }
                );
            }
            finally {
                lock.unlock();
            }
        });

        return endFut;
    }

    /**
     * Wait for region cleaning if necessary.
     *
     * @param grpId Cache group ID.
     * @throws IgniteCheckedException If the cleanup failed.
     */
    private void awaitCleanupIfNeeded(int grpId) throws IgniteCheckedException {
        try {
            CacheGroupContext grp = cctx.cache().cacheGroup(grpId);

            String region = grp.dataRegion().config().getName();

            IgniteInternalFuture clearTask = offheapClearTasks.get(region);

            if (clearTask.isCancelled()) {
                log.warning("Off-heap cleanup task has been cancelled [region=" + region + "]");

                return;
            }

            if (!clearTask.isDone() && log.isDebugEnabled())
                log.debug("Wait for region cleanup [grp=" + grp.cacheOrGroupName() + "]");
            else if (clearTask.error() != null) {
                log.warning("Off heap region was not cleared properly [region=" + region + "]", clearTask.error());

                onDone(clearTask.error());

                return;
            }

            clearTask.get();
        } catch (IgniteFutureCancelledCheckedException ignore) {
            // No-op.
        }
    }

    /**
     * @param grpId Cache group ID.
     * @param partId Partition ID.
     * @return Unique compound partition identifier.
     */
    private static long uniquePartId(int grpId, int partId) {
        return ((long)grpId << 32) + partId;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(FileRebalanceRoutine.class, this);
    }

    /** */
    private static class ClearRegionTask extends GridFutureAdapter {
        /** */
        private final Set<Long> parts;

        /** */
        private final DataRegion region;

        /** */
        private final GridCacheSharedContext cctx;

        /** */
        private final IgniteLogger log;

        /**
         * @param parts Parts.
         * @param region Region.
         * @param cctx Cache shared context.
         * @param log Logger.
         */
        public ClearRegionTask(Set<Long> parts, DataRegion region, GridCacheSharedContext cctx, IgniteLogger log) {
            this.parts = parts;
            this.region = region;
            this.cctx = cctx;
            this.log = log;
        }

        /**
         * Asynchronously clears off-heap memory region.
         */
        public IgniteInternalFuture clearAsync() {
            PageMemoryEx memEx = (PageMemoryEx)region.pageMemory();

            if (log.isDebugEnabled())
                log.debug("Off-heap clear task started [region=" + region.config().getName() + "]");

            memEx.clearAsync(
                (grp, pageId) -> parts.contains(uniquePartId(grp, PageIdUtils.partId(pageId))), true)
                .listen(c1 -> {
                    cctx.database().checkpointReadLock();

                    try {
                        if (log.isDebugEnabled())
                            log.debug("Off heap region cleared [region=" + region.config().getName() + "]");

                        invalidatePartitions(parts);

                        onDone();
                    }
                    catch (RuntimeException | IgniteCheckedException e) {
                        onDone(e);
                    }
                    finally {
                        cctx.database().checkpointReadUnlock();
                    }
                });

            return this;
        }

        /**
         * @param partSet Partition set.
         */
        private void invalidatePartitions(Set<Long> partSet) throws IgniteCheckedException {
            CacheGroupContext grp = null;
            int prevGrp = 0;

            for (long uniquePart : partSet) {
                int grpId = (int)(uniquePart >> 32);
                int partId = (int)uniquePart;

                if (prevGrp == 0|| prevGrp != grpId) {
                    grp = cctx.cache().cacheGroup(grpId);

                    prevGrp = grpId;
                }

                // Skip this group if it was stopped.
                if (grp == null)
                    continue;

                int tag = ((PageMemoryEx)grp.dataRegion().pageMemory()).invalidate(grpId, partId);

                ((FilePageStoreManager)cctx.pageStore()).getStore(grpId, partId).truncate(tag);

                if (log.isDebugEnabled())
                    log.debug("Parition truncated [grp=" + grp.cacheOrGroupName() + ", p=" + partId + "]");
            }
        }
    }
}

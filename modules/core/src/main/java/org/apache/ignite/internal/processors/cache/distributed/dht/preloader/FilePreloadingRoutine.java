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
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;
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
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.NodeStoppingException;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.PartitionUpdateCounter;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.persistence.DataRegion;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheOffheapManager;
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
 * Partition File rebalancing routine.
 */
public class FilePreloadingRoutine extends GridFutureAdapter<Boolean> {
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

    /** Exchange ID. */
    private final GridDhtPartitionExchangeId exchId;

    /** Assignments ordered by cache rebalance priority and node. */
    private final Collection<T2<UUID, Map<Integer, Set<Integer>>>> orderedAssgnments;

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

    /** Snapshot future. */
    private IgniteInternalFuture<Boolean> snapshotFut;

    /** Checkpoint listener. */
    private final Consumer<Runnable> cpLsnr;

    /**
     * Dummy constructor.
     */
    public FilePreloadingRoutine() {
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
    public FilePreloadingRoutine(
        Collection<T2<UUID, Map<Integer, Set<Integer>>>> assigns,
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
     * Initialize and start partitions preloading.
     */
    public void startPartitionsPreloading() {
        initialize();

        requestPartitionsSnapshot(orderedAssgnments.iterator(), new GridConcurrentHashSet<>(remaining.size()));
    }

    /**
     * Prepare to start rebalance routine.
     */
    private void initialize() {
        final Map<DataRegion, Set<Long>> regionToParts = new HashMap<>();

        for (T2<UUID, Map<Integer, Set<Integer>>> nodeAssigns : orderedAssgnments) {
            for (Map.Entry<Integer, Set<Integer>> grpAssigns : nodeAssigns.getValue().entrySet()) {
                int grpId = grpAssigns.getKey();
                Set<Integer> parts = grpAssigns.getValue();
                DataRegion region = cctx.cache().cacheGroup(grpId).dataRegion();

                Set<Long> regionParts = regionToParts.computeIfAbsent(region, v -> new LinkedHashSet<>());

                for (Integer partId : parts) {
                    long uniquePartId = uniquePartId(grpId, partId);

                    regionParts.add(uniquePartId);

                    partsToNodes.put(uniquePartId, nodeAssigns.getKey());
                }

                remaining.put(grpId, remaining.getOrDefault(grpId, 0) + parts.size());
            }
        }
    }

    /**
     * @param iter Iterator on node assignments.
     * @param groups Requested groups.
     */
    private void requestPartitionsSnapshot(Iterator<T2<UUID, Map<Integer, Set<Integer>>>> iter, Set<Integer> groups) {
        if (!iter.hasNext())
            return;

        T2<UUID, Map<Integer, Set<Integer>>> nodeAssigns = iter.next();

        UUID nodeId = nodeAssigns.get1();
        Map<Integer, Set<Integer>> assigns = nodeAssigns.get2();

        Set<String> currGroups = new HashSet<>();

        for (Integer grpId : assigns.keySet()) {
            CacheGroupContext grp = cctx.cache().cacheGroup(grpId);

            currGroups.add(grp.cacheOrGroupName());

            if (!groups.contains(grpId)) {
                groups.add(grpId);

                grp.preloader().sendRebalanceStartedEvent(exchId.discoveryEvent());
            }
        }

        lock.lock();

        try {
            if (isDone())
                return;

            U.log(log, "Preloading partition files [supplier=" + nodeId + ", groups=" + currGroups + "]");

            (snapshotFut = cctx.snapshotMgr().createRemoteSnapshot(nodeId, assigns)).listen(f -> {
                    try {
                        if (!f.isCancelled() && Boolean.TRUE.equals(f.get()))
                            requestPartitionsSnapshot(iter, groups);
                    }
                    catch (IgniteCheckedException e) {
                        if (onDone(e))
                            return;

                        if (log.isDebugEnabled())
                            log.debug("Stale error (ignored): " + e.getMessage());
                    }
                }
            );
        }
        finally {
            lock.unlock();
        }
    }

    /**
     * @return Set of identifiers of the remaining groups.
     */
    public Set<Integer> remainingGroups() {
        return remaining.keySet();
    }

    /**
     * @param nodeId Node ID.
     * @param file Partition snapshot file.
     * @param grpId Cache group ID.
     * @param partId Partition ID.
     */
    public void onPartitionSnapshotReceived(UUID nodeId, File file, int grpId, int partId) {
        try {
            if (isDone())
                return;

            CacheGroupContext grp = cctx.cache().cacheGroup(grpId);

            if (grp == null) {
                log.warning("Snapshot initialization skipped, cache group not found [grpId=" + grpId + "]");

                return;
            }

            grp.topology().localPartition(partId).initialize(file);

            grp.preloader().rebalanceEvent(partId, EVT_CACHE_REBALANCE_PART_LOADED, exchId.discoveryEvent());

            activatePartition(grpId, partId)
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

        Map<Integer, Long> cntrs = restored.computeIfAbsent(grpId, v -> new ConcurrentHashMap<>());

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
            for (GridCacheContext ctx : grp.caches()) {
                IgniteInternalFuture<?> fut = qryProc.rebuildIndexesFromHash(ctx);

                if (fut != null) {
                    U.log(log, "Starting index rebuild [cache=" + ctx.cache().name() + "]");

                    fut.listen(f -> log.info("Finished index rebuild [cache=" + ctx.cache().name() +
                        ", success=" + (!f.isCancelled() && f.error() == null) + "]"));
                }
            }
        }

        // Cache group file rebalancing is finished, historical rebalancing will send separate events.
        grp.preloader().sendRebalanceFinishedEvent(exchId.discoveryEvent());

        if (histAssignments.isEmpty())
            cctx.walState().onGroupRebalanceFinished(grp.groupId(), topVer);
        else
            requestHistoricalRebalance(grp, histAssignments);

        int remainGroupsCnt = remaining.size();

        U.log(log, "Completed" + (remainGroupsCnt == 0 ? " (final)" : "") +
            " cache group files preloading [grp=" + grpName + ", remain=" + remainGroupsCnt + "]");

        if (remainGroupsCnt == 0)
            onDone(true);
    }

    /** {@inheritDoc} */
    @Override public boolean cancel() {
        return onDone(false, null, true);
    }

    /** {@inheritDoc} */
    @Override protected boolean onDone(@Nullable Boolean res, @Nullable Throwable err, boolean cancel) {
        boolean nodeIsStopping = X.hasCause(err, NodeStoppingException.class);

        lock.lock();

        try {
            if (!super.onDone(res, nodeIsStopping ? null : err, nodeIsStopping || cancel))
                return false;

            // Dummy routine - no additional actions required.
            if (orderedAssgnments == null)
                return true;

            if (!isCancelled() && !isFailed()) {
                U.log(log, "The final persistence rebalance is done [result=" + res + ']');

                return true;
            }

            U.log(log, "Cancelling file rebalancing [topVer=" + topVer + "]");

            if (snapshotFut != null && !snapshotFut.isDone()) {
                if (log.isDebugEnabled())
                    log.debug("Cancelling snapshot creation [fut=" + snapshotFut + "]");

                snapshotFut.cancel();
            }

            if (isFailed()) {
                log.error("File rebalancing failed [topVer=" + topVer + "]", err);

                return true;
            }

            if (nodeIsStopping)
                return true;

            return true;

        }
        catch (IgniteCheckedException e) {
            if (err != null)
                e.addSuppressed(err);

            log.error("Failed to cancel file rebalancing.", e);
        }
        finally {
            lock.unlock();
        }

        return false;
    }

    /**
     * @param grp Cache group.
     * @param assigns Assignments.
     */
    private void requestHistoricalRebalance(CacheGroupContext grp, Map<UUID, Map<Integer, T2<Long, Long>>> assigns) {
        GridDhtPreloaderAssignments histAssigns = new GridDhtPreloaderAssignments(exchId, topVer);

        for (Map.Entry<UUID, Map<Integer, T2<Long, Long>>> nodeAssigns : assigns.entrySet()) {
            ClusterNode node = cctx.discovery().node(nodeAssigns.getKey());
            Map<Integer, T2<Long, Long>> grpAssigns = nodeAssigns.getValue();

            GridDhtPartitionDemandMessage msg = new GridDhtPartitionDemandMessage(rebalanceId, topVer, grp.groupId());

            for (Map.Entry<Integer, T2<Long, Long>> e : grpAssigns.entrySet()) {
                int p = e.getKey();
                long from = e.getValue().get1();
                long to = e.getValue().get2();
                String grpName = grp.cacheOrGroupName();

                assert from != 0 && from <= to : "grp=" + grpName + ", p=" + p + ", from=" + from + ", to=" + to;

                if (log.isDebugEnabled()) {
                    log.debug("Prepare for historical rebalancing [grp=" + grpName +
                        ", p=" + p +
                        ", from=" + from +
                        ", to=" + to + "]");
                }

                msg.partitions().addHistorical(p, from, to, grpAssigns.size());
            }

            histAssigns.put(node, msg);
        }

        GridCompoundFuture<Boolean, Boolean> histFut = new GridCompoundFuture<>(CU.boolReducer());

        Runnable task = grp.preloader().addAssignments(histAssigns, true, rebalanceId, null, histFut);

        cctx.kernalContext().getSystemExecutorService().submit(task);
    }

    /**
     * Schedule partition mode change to enable updates.
     *
     * @param grpId Cache group ID.
     * @param partId Partition ID.
     * @return Future that will be done when partition mode changed.
     */
    public IgniteInternalFuture<Long> activatePartition(int grpId, int partId) {
        GridFutureAdapter<Long> endFut = new GridFutureAdapter<Long>() {
            @Override public boolean cancel() {
                return onDone(null, null, true);
            }
        };

        cpLsnr.accept(() -> {
            lock.lock();

            try {
                if (isDone()) {
                    endFut.cancel();

                    return;
                }

                final CacheGroupContext grp = cctx.cache().cacheGroup(grpId);

                // Cache was concurrently destroyed.
                if (grp == null)
                    return;

                GridDhtLocalPartition part = grp.topology().localPartition(partId);

                assert !part.active() : "grpId=" + grpId + " p=" + partId;

                // Save current counter.
                PartitionUpdateCounter cntr =
                    ((GridCacheOffheapManager.GridCacheDataStore)part.dataStore()).inactivePartUpdateCounter();

                // Save current update counter.
                PartitionUpdateCounter snapshotCntr = part.dataStore().partUpdateCounter();

                part.enable();

                AffinityTopologyVersion infinTopVer = new AffinityTopologyVersion(Long.MAX_VALUE, 0);

                IgniteInternalFuture<?> partReleaseFut = cctx.partitionReleaseFuture(infinTopVer);

                // Operations that are in progress now will be lost and should be included in historical rebalancing.
                // These operations can update the old update counter or the new update counter, so the maximum applied
                // counter is used after all updates are completed.
                partReleaseFut.listen(c -> {
                        long hwm = Math.max(cntr.highestAppliedCounter(), snapshotCntr.highestAppliedCounter());

                        cctx.kernalContext().getSystemExecutorService().submit(() -> endFut.onDone(hwm));
                    }
                );
            }
            catch (IgniteCheckedException ignore) {
                assert false;
            }
            finally {
                lock.unlock();
            }
        });

        return endFut;
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
        return S.toString(FilePreloadingRoutine.class, this);
    }
}

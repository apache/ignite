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
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.PartitionUpdateCounter;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.persistence.DbCheckpointListener;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheOffheapManager;
import org.apache.ignite.internal.processors.cache.persistence.partstate.GroupPartitionId;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lifecycle.LifecycleAware;
import org.jetbrains.annotations.Nullable;

/**
 * Partition File preloading routine.
 */
public class PartitionPreloadingRoutine extends GridFutureAdapter<Boolean> {
    /** Rebalance topology version. */
    private final AffinityTopologyVersion topVer;

    /** Logger. */
    private final IgniteLogger log;

    /** Cache shared context. */
    private final GridCacheSharedContext cctx;

    /** Unique (per demander) rebalance id. */
    private final long rebalanceId;

    /** Lock. */
    private final ReentrantLock lock = new ReentrantLock();

    /** Partition restore handler. */
    private final PartitionRestoreHandler restoreHnd;

    /** Exchange ID. */
    private final GridDhtPartitionExchangeId exchId;

    /** Remaining nodes with groups and the number of partitions. */
    @GridToStringInclude
    private final Map<UUID, Map<Integer, Set<Integer>>> remaining;

    /** Count of partition snapshots received. */
    private final AtomicLong receivedCnt = new AtomicLong();

    /** Cache group with restored partition snapshots and HWM value of update counter mapped to node identifier. */
    @GridToStringInclude
    private final Map<Integer, Map<UUID, Map<Integer, Long>>> restored = new ConcurrentHashMap<>();

    /**
     * Cache group identifiers with historical assignments future that will be completed when partition files are
     * preloaded.
     */
    private final Map<Integer, GridFutureAdapter<GridDhtPreloaderAssignments>> futAssigns = new ConcurrentHashMap<>();

    /** Total number of partitions. */
    private final long totalPartitionsCnt;

    /** Snapshot future. */
    private IgniteInternalFuture<Void> snapshotFut;

    /**
     * @param exchFut Exchange future.
     * @param cctx Cache shared context.
     * @param rebalanceId Rebalance ID
     * @param assignments Assignments mapped by node ID.
     */
    public PartitionPreloadingRoutine(
        GridDhtPartitionsExchangeFuture exchFut,
        GridCacheSharedContext cctx,
        long rebalanceId,
        Map<UUID, Map<Integer, Set<Integer>>> assignments
    ) {
        long totalParts = 0;

        // Copy contents.
        Map<UUID, Map<Integer, Set<Integer>>> remaining0 = U.newHashMap(assignments.size());

        for (Map.Entry<UUID, Map<Integer, Set<Integer>>> nodeAssign : assignments.entrySet()) {
            Map<Integer, Set<Integer>> nodeAssign0 = new ConcurrentHashMap<>(nodeAssign.getValue().size());

            remaining0.put(nodeAssign.getKey(), nodeAssign0);

            for (Map.Entry<Integer, Set<Integer>> grpAssign : nodeAssign.getValue().entrySet()) {
                nodeAssign0.put(grpAssign.getKey(), new GridConcurrentHashSet<>(grpAssign.getValue()));
                futAssigns.put(grpAssign.getKey(), new GridFutureAdapter<>());

                totalParts += grpAssign.getValue().size();
            }
        }

        this.cctx = cctx;
        this.rebalanceId = rebalanceId;

        exchId = exchFut.exchangeId();
        topVer = exchFut.topologyVersion();
        log = cctx.kernalContext().log(getClass());
        totalPartitionsCnt = totalParts;
        remaining = Collections.unmodifiableMap(remaining0);
        restoreHnd = new PartitionRestoreHandler(cctx);
    }

    /**
     * Start partitions preloading.
     *
     * @return Cache group identifiers with futures that will be completed when partitions are preloaded.
     */
    public Map<Integer, IgniteInternalFuture<GridDhtPreloaderAssignments>> startPartitionsPreloading() {
        assert !remaining.isEmpty();

        restoreHnd.start();

        requestPartitionsSnapshot(remaining.entrySet().iterator());

        return Collections.unmodifiableMap(futAssigns);
    }

    /**
     * @param it Iterator on node assignments.
     */
    private void requestPartitionsSnapshot(Iterator<Map.Entry<UUID, Map<Integer, Set<Integer>>>> it) {
        if (!it.hasNext())
            return;

        Map.Entry<UUID, Map<Integer, Set<Integer>>> nodeAssigns = it.next();

        UUID nodeId = nodeAssigns.getKey();
        Map<Integer, Set<Integer>> assigns = nodeAssigns.getValue();

        Set<String> currGroups = new HashSet<>();

        for (Integer grpId : assigns.keySet()) {
            CacheGroupContext grp = cctx.cache().cacheGroup(grpId);

            currGroups.add(grp.cacheOrGroupName());
        }

        lock.lock();

        try {
            if (isDone())
                return;

            if (log.isInfoEnabled())
                log.info("Preloading partition files [supplier=" + nodeId + ", groups=" + currGroups + "]");

            assert snapshotFut == null || snapshotFut.isDone() : snapshotFut;

            (snapshotFut = cctx.snapshotMgr()
                .requestRemoteSnapshot(nodeId,
                    assigns,
                    (file, uniquePartId) -> onPartitionSnapshotReceived(nodeId, uniquePartId, file)))
                .chain(f -> {
                        if (!f.isCancelled() && f.error() == null)
                            requestPartitionsSnapshot(it);
                        else {
                            if (!onDone(f.error()) && log.isDebugEnabled())
                                log.debug("Stale error (ignored): " + f.error().getMessage());
                        }

                        return null;
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
        return futAssigns.keySet();
    }

    /**
     * @param nodeId Node id.
     * @param uniquePartId Pair of cache group ID with partition ID.
     * @param file Partition snapshot file.
     */
    public void onPartitionSnapshotReceived(UUID nodeId, GroupPartitionId uniquePartId, File file) {
        int grpId = uniquePartId.getGroupId();
        int partId = uniquePartId.getPartitionId();

        CacheGroupContext grp = cctx.cache().cacheGroup(grpId);

        if (grp == null) {
            log.warning("Partition snapshot initialization skipped, cache group not found [grpId=" + grpId + "]");

            return;
        }

        lock.lock();

        try {
            // Ensure that we are not stopping when getting checkpoint read lock.
            if (isDone())
                return;

            GridDhtLocalPartition part = grp.topology().localPartition(partId);

            assert part != null;

            IgniteInternalFuture<Long> restoreFut = restoreHnd.schedule(grp, part, file);

            restoreFut.listen(f -> {
                try {
                    if (!f.isCancelled() && !isDone())
                        onPartitionSnapshotRestored(nodeId, grpId, partId, f.get());
                }
                catch (IgniteCheckedException e) {
                    log.error("Unable to restore partition snapshot [grpId=" + grpId + ", p=" + partId + "]");

                    onDone(e);
                }
            });
        } catch (IgniteCheckedException e) {
            log.error("Unable to restore partition snapshot " +
                "[grp=" + grp.cacheOrGroupName() +
                ", p=" + partId +
                ", file=" + file + "]", e);

            onDone(e);
        } finally {
            lock.unlock();
        }

        if (receivedCnt.incrementAndGet() == totalPartitionsCnt) {
            if (log.isInfoEnabled())
                log.info("All partition files are received - triggering checkpoint to complete rebalancing.");

            cctx.database().wakeupForCheckpoint("Partition files preload complete.");
        }
    }

    /**
     * @param grpId Cache group ID.
     * @param partId Partition ID.
     * @param cntr The highest value of the update counter before this partition began to process updates.
     */
    private void onPartitionSnapshotRestored(UUID nodeId, int grpId, int partId, long cntr) {
        Map<Integer, Set<Integer>> grpParts = remaining.get(nodeId);

        assert grpParts != null : "nodeId=" + nodeId + ", grpId=" + grpId + ", p=" + partId;

        Set<Integer> parts = grpParts.get(grpId);

        boolean rmvd = parts.remove(partId);

        assert rmvd : "nodeId=" + nodeId + ", grpId=" + grpId + ", p=" + partId;

        Map<UUID, Map<Integer, Long>> grpCntrs = restored.computeIfAbsent(grpId, v -> new ConcurrentHashMap<>());

        grpCntrs.computeIfAbsent(nodeId, v -> new ConcurrentHashMap<>()).put(partId, cntr);

        GridFutureAdapter<GridDhtPreloaderAssignments> resFut;

        if (!parts.isEmpty() ||
            grpParts.remove(grpId) == null ||
            remaining.values().stream().map(Map::keySet).anyMatch(grps -> grps.contains(grpId)) ||
            (resFut = futAssigns.remove(grpId)) == null)
            return;

        CacheGroupContext grp = cctx.cache().cacheGroup(grpId);

        assert !grp.localWalEnabled() : "grp=" + grp.cacheOrGroupName();

        IgniteInternalFuture<?> idxFut = cctx.database().rebuildIndexes(grp);

        GridDhtPreloaderAssignments histAssignments = makeHistoricalAssignments(grp, grpCntrs);

        resFut.onDone(histAssignments);

        if (histAssignments.isEmpty())
            idxFut.listen(f -> cctx.walState().onGroupRebalanceFinished(grp.groupId(), topVer));

        boolean finalPreloading = futAssigns.isEmpty() && onDone(true);

        if (log.isInfoEnabled()) {
            log.info("Completed" + (finalPreloading ? " (final)" : "") +
                " partition files preloading [grp=" + grp.cacheOrGroupName() + "]");
        }
    }

    /**
     * Prepare assignments for historical rebalancing.
     *
     * @param grp Cache group.
     * @param cntrs Partition set with HWM update counter value for hstorical rebalance.
     * @return Partition to node assignments.
     */
    private GridDhtPreloaderAssignments makeHistoricalAssignments(
        CacheGroupContext grp,
        Map<UUID, Map<Integer, Long>> cntrs
    ) {
        GridDhtPreloaderAssignments histAssigns = new GridDhtPreloaderAssignments(exchId, topVer);

        int parts = grp.topology().partitions();

        for (Map.Entry<UUID, Map<Integer, Long>> e : cntrs.entrySet()) {
            ClusterNode node = cctx.discovery().node(e.getKey());

            assert node != null : e.getKey();

            Map<Integer, Long> orderedCntrs = new TreeMap<>(e.getValue());

            for (Map.Entry<Integer, Long> partCntr : orderedCntrs.entrySet()) {
                int partId = partCntr.getKey();

                long from = grp.topology().localPartition(partId).initialUpdateCounter();
                long to = partCntr.getValue();

                if (from == to)
                    continue;

                assert to > from : "from=" + from + ", to=" + to;

                GridDhtPartitionDemandMessage msg = histAssigns.
                    computeIfAbsent(node, v -> new GridDhtPartitionDemandMessage(rebalanceId, topVer, grp.groupId()));

                msg.partitions().addHistorical(partId, from, to, parts);
            }
        }

        return histAssigns;
    }

    /** {@inheritDoc} */
    @Override public boolean cancel() {
        return onDone(false, null, true);
    }

    /** {@inheritDoc} */
    @Override protected boolean onDone(@Nullable Boolean res, @Nullable Throwable err, boolean cancel) {
        lock.lock();

        try {
            if (!super.onDone(res, err, cancel))
                return false;

            if (!(cctx.database() instanceof GridCacheDatabaseSharedManager))
                return true;

            restoreHnd.stop();

            if (!isCancelled() && !isFailed())
                return true;

            if (log.isInfoEnabled())
                log.info("Cancelling File preloading [topVer=" + topVer + "]");

            if (snapshotFut != null && !snapshotFut.isDone()) {
                if (log.isDebugEnabled())
                    log.debug("Cancelling snapshot creation [fut=" + snapshotFut + "]");

                snapshotFut.cancel();
            }

            for (GridFutureAdapter fut : futAssigns.values())
                fut.onDone();

            if (isFailed())
                log.error("File preloading failed [topVer=" + topVer + "]", err);

            return true;
        }
        catch (IgniteCheckedException e) {
            if (err != null)
                e.addSuppressed(err);

            log.error("Failed to cancel File preloading.", e);
        }
        finally {
            lock.unlock();
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(PartitionPreloadingRoutine.class, this);
    }

    /**
     *
     */
    private static class PartitionRestoreHandler implements DbCheckpointListener, LifecycleAware {
        /** Cache shared context. */
        private final GridCacheSharedContext cctx;

        /** Lock. */
        private final ReentrantLock lock = new ReentrantLock();

        /** Checkpoint request queue. */
        private final Queue<Runnable> checkpointRequests = new ConcurrentLinkedQueue<>();

        /**
         * @param cctx Cache shared context.
         */
        private PartitionRestoreHandler(GridCacheSharedContext cctx) {
            this.cctx = cctx;
        }

        /** {@inheritDoc} */
        @Override public void start() {
            assert checkpointRequests.isEmpty();

            ((GridCacheDatabaseSharedManager)cctx.database()).addCheckpointListener(this);
        }

        /** {@inheritDoc} */
        @Override public void stop() {
            lock.lock();

            try {
                checkpointRequests.clear();

                ((GridCacheDatabaseSharedManager)cctx.database()).removeCheckpointListener(this);
            }
            finally {
                lock.unlock();
            }
        }

        /** {@inheritDoc} */
        @Override public void onMarkCheckpointBegin(Context ctx) {
            lock.lock();

            try {
                Runnable r;

                while ((r = checkpointRequests.poll()) != null)
                    r.run();
            }
            finally {
                lock.unlock();
            }
        }

        /**
         * Schedule partition snapshot hot restore.
         *
         * @param grp Cache group.
         * @param part Local partition.
         * @param file Snapshot file.
         * @return Future with max update counter as a result of restore.
         * @throws IgniteCheckedException If failed.
         */
        public IgniteInternalFuture<Long> schedule(
            CacheGroupContext grp,
            GridDhtLocalPartition part,
            File file
        ) throws IgniteCheckedException {
            GridFutureAdapter<Long> res = new GridFutureAdapter<>();

            cctx.pageStore().restore(grp.groupId(), part.id(), file);

            boolean initialized = part.dataStore().init();

            assert initialized;

            checkpointRequests.offer(() -> enablePartition(grp, part, res));

            return res;
        }

        /**
         * @param grp Cache group.
         * @param part Local partition.
         * @param res Future with max update counter as a result.
         */
        @SuppressWarnings("unchecked")
        private void enablePartition(CacheGroupContext grp, GridDhtLocalPartition part, GridFutureAdapter<Long> res) {
            if (grp.topology().stopping()) {
                res.onCancelled();

                return;
            }

            assert !part.active() : "grp=" + grp.cacheOrGroupName() + " p=" + part.id();

            // Save current counter.
            PartitionUpdateCounter cntr =
                ((GridCacheOffheapManager.GridCacheDataStore)part.dataStore()).inactivePartUpdateCounter();

            // Save current update counter.
            PartitionUpdateCounter snapshotCntr = part.dataStore().partUpdateCounter();

            part.enable();

            AffinityTopologyVersion infinTopVer = new AffinityTopologyVersion(Long.MAX_VALUE, 0);
            GridCompoundFuture partReleaseFut = new GridCompoundFuture();

            partReleaseFut.add(cctx.mvcc().finishAtomicUpdates(infinTopVer));
            partReleaseFut.add(cctx.mvcc().finishDataStreamerUpdates(infinTopVer));
            partReleaseFut.add(cctx.tm().finishLocalTxs(infinTopVer, null));

            partReleaseFut.markInitialized();

            // Local updates that are in progress now will be lost and should be included in historical rebalancing.
            // These operations can update the old update counter or the new update counter, so the maximum applied
            // counter is used after all updates are completed.
            partReleaseFut.listen(c -> {
                    long hwm = Math.max(cntr.highestAppliedCounter(), snapshotCntr.highestAppliedCounter());

                    cctx.kernalContext().getSystemExecutorService().submit(() -> res.onDone(hwm));
                }
            );
        }

        /** {@inheritDoc} */
        @Override public void onCheckpointBegin(Context ctx) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void beforeCheckpointBegin(Context ctx) {
            // No-op.
        }
    }
}

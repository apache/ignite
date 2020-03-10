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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
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
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.events.EventType.EVT_CACHE_REBALANCE_PART_LOADED;

/**
 * Partition File preloading routine.
 */
public class PartitionPreloadingRoutine extends GridFutureAdapter<Boolean> {
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

    /** Checkpoint listener. */
    private final CheckpointListener checkpointLsnr = new CheckpointListener();

    /** Exchange ID. */
    private final GridDhtPartitionExchangeId exchId;

    /** Remaining nodes with groups and the number of partitions. */
    @GridToStringInclude
    private final Map<UUID, Map<Integer, Set<Integer>>> remaining;

    /** Count of partition snapshots received. */
    private final AtomicInteger receivedCnt = new AtomicInteger();

    /** Cache group with restored partition snapshots and HWM value of update counter mapped to node identifier. */
    @GridToStringInclude
    private final Map<Integer, Map<UUID, Map<Integer, Long>>> restored = new ConcurrentHashMap<>();

    /** Cache group identifiers with futures that will be completed when partition files are preloaded. */
    private final Map<Integer, GridFutureAdapter<GridDhtPreloaderAssignments>> grpRoutines;

    /** Total number of partitions. */
    private final int totalPartitionsCnt;

    /** Snapshot future. */
    private IgniteInternalFuture<Boolean> snapshotFut;

    /**
     * @param assignments Assignments.
     * @param exchFut Exchange future.
     * @param cctx Cache shared context.
     * @param rebalanceId Rebalance ID
     */
    public PartitionPreloadingRoutine(
        Map<Integer, GridDhtPreloaderAssignments> assignments,
        GridDhtPartitionsExchangeFuture exchFut,
        GridCacheSharedContext cctx,
        long rebalanceId
    ) {
        // Re-map assignments by node.
        Map<UUID, Map<Integer, Set<Integer>>> assignsByNode = new ConcurrentHashMap<>();
        Map<Integer, GridFutureAdapter<GridDhtPreloaderAssignments>> routines = new HashMap<>();
        int totalParts = 0;

        for (Map.Entry<Integer, GridDhtPreloaderAssignments> e : assignments.entrySet()) {
            CacheGroupContext grp = cctx.cache().cacheGroup(e.getKey());
            GridDhtPreloaderAssignments assigns = e.getValue();

            GridDhtLocalPartition part = F.first(grp.topology().currentLocalPartitions());

            if (part == null || part.active() || assigns.isEmpty()) {
                GridFutureAdapter<GridDhtPreloaderAssignments> finished = new GridFutureAdapter<>();

                finished.onDone(assigns);

                routines.put(grp.groupId(), finished);

                continue;
            }

            for (Map.Entry<ClusterNode, GridDhtPartitionDemandMessage> e0 : assigns.entrySet()) {
                Map<Integer, Set<Integer>> grpAssigns =
                    assignsByNode.computeIfAbsent(e0.getKey().id(), v -> new ConcurrentHashMap<>());

                Set<Integer> parts = e0.getValue().partitions().fullSet();

                grpAssigns.put(grp.groupId(), new GridConcurrentHashSet<>(parts));
                routines.put(grp.groupId(), new GridFutureAdapter<>());

                totalParts  += parts.size();
            }
        }

        this.cctx = cctx;
        this.rebalanceId = rebalanceId;

        exchId = exchFut != null ? exchFut.exchangeId() : null;
        topVer = exchFut != null ? exchFut.topologyVersion() : null;
        log = cctx.kernalContext().log(getClass());
        totalPartitionsCnt = totalParts;
        grpRoutines = routines;
        remaining = assignsByNode;
    }

    /**
     * Start partitions preloading.
     *
     * @return Cache group identifiers with futures that will be completed when partitions are preloaded.
     */
    public Map<Integer, IgniteInternalFuture<GridDhtPreloaderAssignments>> startPartitionsPreloading() {
        if (!remaining.isEmpty()) {
            ((GridCacheDatabaseSharedManager)cctx.database()).addCheckpointListener(checkpointLsnr);

            requestPartitionsSnapshot(remaining.entrySet().iterator(), new GridConcurrentHashSet<>());
        }
        else // Nothing to do.
            onDone(true);

        return Collections.unmodifiableMap(new HashMap<>(grpRoutines));
    }

    /**
     * @param iter Iterator on node assignments.
     * @param groups Requested groups.
     */
    private void requestPartitionsSnapshot(Iterator<Map.Entry<UUID, Map<Integer, Set<Integer>>>> iter, Set<Integer> groups) {
        if (!iter.hasNext())
            return;

        Map.Entry<UUID, Map<Integer, Set<Integer>>> nodeAssigns = iter.next();

        UUID nodeId = nodeAssigns.getKey();
        Map<Integer, Set<Integer>> assigns = nodeAssigns.getValue();

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

            if (log.isInfoEnabled())
                log.info("Preloading partition files [supplier=" + nodeId + ", groups=" + currGroups + "]");

            assert snapshotFut == null || snapshotFut.isDone() : snapshotFut;

            (snapshotFut = cctx.snapshotMgr()
                .createRemoteSnapshot(nodeId,
                    assigns,
                    (file, pair) -> onPartitionSnapshotReceived(nodeId, file, pair.getGroupId(), pair.getPartitionId())))
                .chain(f -> {
                        try {
                            if (!f.isCancelled() && f.get())
                                requestPartitionsSnapshot(iter, groups);
                        }
                        catch (IgniteCheckedException e) {
                            if (!onDone(e) && log.isDebugEnabled())
                                log.debug("Stale error (ignored): " + e.getMessage());
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
        Set<Integer> grps = new HashSet<>();

        for (Map<Integer, ?> e : remaining.values())
            grps.addAll(e.keySet());

        return grps;
    }

    /**
     * @param nodeId Node ID.
     * @param file Partition snapshot file.
     * @param grpId Cache group ID.
     * @param partId Partition ID.
     */
    public void onPartitionSnapshotReceived(UUID nodeId, File file, int grpId, int partId) {
        CacheGroupContext grp = cctx.cache().cacheGroup(grpId);

        if (grp == null) {
            log.warning("Snapshot initialization skipped, cache group not found [grpId=" + grpId + "]");

            return;
        }

        initPartitionSnapshot(grp.topology().localPartition(partId), file);

        grp.preloader().rebalanceEvent(partId, EVT_CACHE_REBALANCE_PART_LOADED, exchId.discoveryEvent());

        activatePartition(grpId, partId)
            .listen(f -> {
                try {
                    if (!f.isCancelled())
                        onPartitionSnapshotRestored(nodeId, grpId, partId, f.get());
                }
                catch (IgniteCheckedException e) {
                    log.error("Unable to restore partition snapshot [grpId=" + grpId + ", p=" + partId + "]");

                    onDone(e);
                }
            });

        if (receivedCnt.incrementAndGet() == totalPartitionsCnt) {
            if (log.isInfoEnabled())
                log.info("All partition files are received - triggering checkpoint to complete rebalancing.");

            cctx.database().wakeupForCheckpoint("Partition files preload complete.");
        }
    }

    /**
     * @param part Partition.
     * @param file SNapshot file.
     */
    private void initPartitionSnapshot(GridDhtLocalPartition part, File file) {
        lock.lock();

        try {
            // Ensure that we are not stopping when getting checkpoint read lock.
            if (isDone())
                return;

            part.initialize(file);
        }
        catch (IOException | IgniteCheckedException e) {
            log.error("Unable to initialize partition snapshot [" +
                "grp=" + part.group().cacheOrGroupName() +
                ", p=" + part.id() +
                ", file=" + file + "]", e);

            onDone(e);
        }
        finally {
            lock.unlock();
        }
    }

    /**
     * @param grpId Cache group ID.
     * @param partId Partition ID.
     * @param cntr The highest value of the update counter before this partition began to process updates.
     */
    private void onPartitionSnapshotRestored(UUID nodeId, int grpId, int partId, long cntr) {
        Map<Integer, Set<Integer>> grpParts = remaining.get(nodeId);

        assert  grpParts != null : "nodeId=" + nodeId + ", grpId=" + grpId + ", p=" + partId;

        Set<Integer> parts = grpParts.get(grpId);

        boolean rmvd = parts.remove(partId);

        assert rmvd : "nodeId=" + nodeId + ", grpId=" + grpId + ", p=" + partId;

        Map<UUID, Map<Integer, Long>> grpCntrs = restored.computeIfAbsent(grpId, v -> new ConcurrentHashMap<>());

        grpCntrs.computeIfAbsent(nodeId, v -> new ConcurrentHashMap<>()).put(partId, cntr);

        if (parts.isEmpty() && grpParts.remove(grpId) != null && !remainingGroups().contains(grpId))
            onCacheGroupDone(grpId, grpCntrs);
    }

    /**
     * @param grpId Group ID.
     * @param maxCntrs Partition set with HWM update counter value for hstorical rebalance.
     */
    private void onCacheGroupDone(int grpId, Map<UUID, Map<Integer, Long>> maxCntrs) {
        GridFutureAdapter<GridDhtPreloaderAssignments> fut = grpRoutines.remove(grpId);

        if (fut == null)
            return;

        CacheGroupContext grp = cctx.cache().cacheGroup(grpId);
        String grpName = grp.cacheOrGroupName();

        assert !grp.localWalEnabled() : "grp=" + grpName;

        IgniteInternalFuture<?> idxFut = cctx.database().rebuildIndexes(grp);

        // Cache group File preloading is finished, historical rebalancing will send separate events.
        grp.preloader().sendRebalanceFinishedEvent(exchId.discoveryEvent());

        GridDhtPreloaderAssignments histAssignments = makeHistoricalAssignments(grp, maxCntrs);

        fut.onDone(histAssignments);

        if (histAssignments.isEmpty())
            idxFut.listen(f -> cctx.walState().onGroupRebalanceFinished(grp.groupId(), topVer));

        int remainGroupsCnt = remainingGroups().size();

        if (log.isInfoEnabled()) {
            log.info("Completed" + (remainGroupsCnt == 0 ? " (final)" : "") +
                " partition files preloading [grp=" + grpName + ", remaining=" + remainGroupsCnt + "]");
        }

        if (remainGroupsCnt == 0)
            onDone(true);
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

            ((GridCacheDatabaseSharedManager)cctx.database()).removeCheckpointListener(checkpointLsnr);

            if (!isCancelled() && !isFailed())
                return true;

            if (log.isInfoEnabled())
                log.info("Cancelling File preloading [topVer=" + topVer + "]");

            if (snapshotFut != null && !snapshotFut.isDone()) {
                if (log.isDebugEnabled())
                    log.debug("Cancelling snapshot creation [fut=" + snapshotFut + "]");

                snapshotFut.cancel();
            }

            for (GridFutureAdapter fut : grpRoutines.values())
                fut.onDone();

            if (isFailed()) {
                log.error("File preloading failed [topVer=" + topVer + "]", err);

                return true;
            }

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

        checkpointLsnr.schedule(() -> {
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

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(PartitionPreloadingRoutine.class, this);
    }

    /** */
    private static class CheckpointListener implements DbCheckpointListener {
        /** Checkpoint requests queue. */
        private final Queue<Runnable> requests = new ConcurrentLinkedQueue<>();

        /** {@inheritDoc} */
        @Override public void onMarkCheckpointBegin(Context ctx) {
            Runnable r;

            while ((r = requests.poll()) != null)
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

        /**
         * @param task Task to execute.
         */
        public void schedule(Runnable task) {
            requests.offer(task);
        }
    }
}

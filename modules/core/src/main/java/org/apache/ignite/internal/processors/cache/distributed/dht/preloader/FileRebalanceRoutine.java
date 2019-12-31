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
import java.nio.file.Files;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteFutureCancelledCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.NodeStoppingException;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStore;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMemoryEx;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.events.EventType.EVT_CACHE_REBALANCE_PART_LOADED;
import static org.apache.ignite.internal.pagemem.PageIdAllocator.INDEX_PARTITION;

public class FileRebalanceRoutine extends GridFutureAdapter<Boolean> {
    /** */
    private static final long MAX_MEM_CLEANUP_TIMEOUT = 60_000;

    /** */
    private final GridPartitionFilePreloader.CheckpointListener cpLsnr;

    /** */
    private final AffinityTopologyVersion topVer;

    /** */
    private final long rebalanceId;

    /** */
    private final ReentrantLock cancelLock = new ReentrantLock();

    /** */
    private final GridCacheSharedContext cctx;

    /** */
    private final IgniteLogger log;

    /** Index rebuild future. */
    private final GridCompoundFuture idxRebuildFut = new GridCompoundFuture<>();

    /** */
    private final GridDhtPartitionExchangeId exchId;

    /** */
    private final Collection<Map<ClusterNode, Map<Integer, Set<Integer>>>> orderedAssgnments;

    /** */
    private final Map<Long, UUID> partsToNodes = new HashMap<>();

    /** */
    private final Map<Integer, Set<Integer>> remaining = new ConcurrentHashMap<>();

    /** */
    private final Map<Integer, AtomicInteger> received = new ConcurrentHashMap<>();

    /** */
    private final Map<String, GridFutureAdapter> regions = new HashMap<>();

    /** */
    private final Map<String, Set<Long>> regionToParts = new HashMap<>();

    /** */
    private volatile IgniteInternalFuture<Boolean> snapFut;

    /** */
    public FileRebalanceRoutine() {
        this(null, null, null, null, 0, null, null);

        onDone(true);
    }

    /**
     * @param lsnr Checkpoint listener.
     * @param exchId Exchange ID.
     */
    public FileRebalanceRoutine(
        GridPartitionFilePreloader.CheckpointListener lsnr,
        Collection<Map<ClusterNode, Map</** group */Integer, Set</** part */Integer>>>> assigns,
        AffinityTopologyVersion startVer,
        GridCacheSharedContext cctx,
        long rebalanceId,
        IgniteLogger log,
        GridDhtPartitionExchangeId exchId) {
        cpLsnr = lsnr;
        topVer = startVer;

        this.log = log;
        this.cctx = cctx;
        this.rebalanceId = rebalanceId;
        this.exchId = exchId;

        orderedAssgnments = assigns;

        initialize(assigns);
    }

    /** @deprecated used only for debugging, should be removed */
    @Deprecated
    boolean isPreloading(int grpId) {
        return remaining.containsKey(grpId) && !isDone();
    }

    /**
     * Initialize rebalancing mappings.
     *
     * @param assignments Assignments.
     */
    private void initialize(Collection<Map<ClusterNode, Map<Integer, Set<Integer>>>> assignments) {
        if (assignments == null)
            return;

        cancelLock.lock();

        try {
            for (Map<ClusterNode, Map<Integer, Set<Integer>>> map : assignments) {
                for (Map.Entry<ClusterNode, Map<Integer, Set<Integer>>> mapEntry : map.entrySet()) {
                    UUID nodeId = mapEntry.getKey().id();

                    for (Map.Entry<Integer, Set<Integer>> entry : mapEntry.getValue().entrySet()) {
                        int grpId = entry.getKey();

                        CacheGroupContext grp = cctx.cache().cacheGroup(grpId);

                        String regName = cctx.cache().cacheGroup(grpId).dataRegion().config().getName();

                        Set<Long> regionParts = regionToParts.computeIfAbsent(regName, v -> new HashSet<>());

                        Set<Integer> allPartitions = remaining.computeIfAbsent(grpId, v -> new GridConcurrentHashSet<>());

                        for (Integer partId : entry.getValue()) {
                            assert grp.topology().localPartition(partId).dataStore().readOnly() :
                                "cache=" + grp.cacheOrGroupName() + " p=" + partId;

                            long grpAndPart = ((long)grpId << 32) + partId;

                            regionParts.add(grpAndPart);

                            partsToNodes.put(grpAndPart, nodeId);

                            allPartitions.add(partId);
                        }
                    }
                }
            }

            for (Map.Entry<String, Set<Long>> e : regionToParts.entrySet())
                regions.put(e.getKey(), new GridFutureAdapter());
        }
        finally {
            cancelLock.unlock();
        }
    }

    /** */
    public AffinityTopologyVersion topologyVersion() {
        return topVer;
    }

    /** {@inheritDoc} */
    @Override public boolean cancel() {
        return onDone(false, null, true);
    }

    /** {@inheritDoc} */
    @Override protected boolean onDone(@Nullable Boolean res, @Nullable Throwable err, boolean cancel) {
        boolean nodeIsStopping = X.hasCause(err, NodeStoppingException.class);

        if (cancel || err != null) {
            cancelLock.lock();

            try {
                synchronized (this) {
                    if (isDone())
                        return true;

                    if (log.isInfoEnabled())
                        log.info("Cancelling file rebalancing.");

                    cpLsnr.cancelAll();

                    if (err == null) {
                        for (IgniteInternalFuture fut : regions.values()) {
                            if (!fut.isDone())
                                fut.get(MAX_MEM_CLEANUP_TIMEOUT);
                        }
                    }

                    if (snapFut != null && !snapFut.isDone()) {
                        if (log.isDebugEnabled())
                            log.debug("Cancelling snapshot creation [fut=" + snapFut + "]");

                        snapFut.cancel();
                    }

                    if (log.isDebugEnabled() && !idxRebuildFut.isDone() && !idxRebuildFut.futures().isEmpty()) {
                        log.debug("Index rebuild is still in progress, cancelling.");

                        idxRebuildFut.cancel();
                    }

                    for (Integer grpId : remaining.keySet()) {
                        CacheGroupContext grp = cctx.cache().cacheGroup(grpId);

                        if (grp != null)
                            grp.preloader().sendRebalanceFinishedEvent(exchId.discoveryEvent());
                    }
                }
            }
            catch (IgniteCheckedException e) {
                if (err != null)
                    e.addSuppressed(err);

                log.error("Failed to cancel file rebalancing.", e);
            }
            finally {
                cancelLock.unlock();
            }
        }

        return super.onDone(res, nodeIsStopping ? null : err, nodeIsStopping || cancel);
    }

    private void onCacheGroupDone(int grpId, Map<Integer, Long> maxCntrs) {
        Set<Integer> parts = remaining.remove(grpId);

        if (parts == null)
            return;

        CacheGroupContext grp = cctx.cache().cacheGroup(grpId);

        assert !grp.localWalEnabled() : "grp=" + grp.cacheOrGroupName();

        Map<UUID, Map<Integer, T2<Long, Long>>> histAssignments = new HashMap<>();

        for (Map.Entry<Integer, Long> e : maxCntrs.entrySet()) {
            int partId = e.getKey();

            long initCntr = grp.topology().localPartition(partId).initialUpdateCounter();
            long maxCntr = e.getValue();

            assert maxCntr >= initCntr : "from=" + initCntr + ", to=" + maxCntr;

            if (initCntr != maxCntr) {
                long uniquePartId = ((long)grpId << 32) + partId;

                UUID nodeId = partsToNodes.get(uniquePartId);

                histAssignments.computeIfAbsent(nodeId, v -> new TreeMap<>()).put(partId, new T2<>(initCntr, maxCntr));

                continue;
            }

            if (log.isDebugEnabled())
                log.debug("No need for WAL rebalance [grp=" + grp.cacheOrGroupName() + ", p=" + partId + "]");
        }

        GridQueryProcessor qryProc = cctx.kernalContext().query();

        if (qryProc.moduleEnabled()) {
            if (log.isInfoEnabled())
                log.info("Starting index rebuild for cache group: " + grp.cacheOrGroupName());

            for (GridCacheContext ctx : grp.caches()) {
                IgniteInternalFuture<?> fut = qryProc.rebuildIndexesFromHash(ctx);

                if (fut != null)
                    idxRebuildFut.add(fut);
            }
        }
        else
        if (log.isInfoEnabled())
            log.info("Skipping index rebuild for cache group: " + grp.cacheOrGroupName());

        // Cache group file rebalancing is finished.
        // todo historical rebalancing will send separate events
        grp.preloader().sendRebalanceFinishedEvent(exchId.discoveryEvent());

        if (histAssignments.isEmpty()) {
            log.info("File rebalancing complete [group=" + grp.cacheOrGroupName() + "]");

            assert !grp.localWalEnabled() : "WAL shoud be disabled for file rebalancing [grp=" + grp.cacheOrGroupName() + "]";

            cctx.walState().onGroupRebalanceFinished(grp.groupId(), topVer);
        }
        else
            requestHistoricalRebalance(grp, histAssignments);

        if (remaining.isEmpty()) {
            idxRebuildFut.markInitialized();

            onDone(true);
        }
    }

    private void requestHistoricalRebalance(CacheGroupContext grp, Map<UUID, Map<Integer, T2<Long, Long>>> assigns) {
        GridDhtPreloaderAssignments grpAssigns = new GridDhtPreloaderAssignments(exchId, topVer);

        for (Map.Entry<UUID, Map<Integer, T2<Long, Long>>> entry : assigns.entrySet()) {
            ClusterNode node = cctx.discovery().node(entry.getKey());
            Map<Integer, T2<Long, Long>> nodeAssigns = entry.getValue();

            GridDhtPartitionDemandMessage msg = new GridDhtPartitionDemandMessage(rebalanceId, topVer, grp.groupId());

            for (Map.Entry<Integer, T2<Long, Long>> e : nodeAssigns.entrySet())
                msg.partitions().addHistorical(e.getKey(), e.getValue().get1(), e.getValue().get2(), nodeAssigns.size());

            grpAssigns.put(node, msg);
        }

        // todo investigate waliterator troubles
        try {
            U.sleep(500);
        } catch (IgniteInterruptedCheckedException e) {
            log.warning(e.getMessage(), e);
        }

        GridCompoundFuture<Boolean, Boolean> histFut = new GridCompoundFuture<>(CU.boolReducer());

        Runnable task = grp.preloader().addAssignments(grpAssigns, true, rebalanceId, null, histFut);

        cctx.kernalContext().getSystemExecutorService().submit(task);
    }

    public void requestPartitionsSnapshot() {
        // todo should we send start event only when we starting to preload specified group?
        for (Integer grpId : remaining.keySet())
            cctx.cache().cacheGroup(grpId).preloader().sendRebalanceStartedEvent(exchId.discoveryEvent());

        cctx.kernalContext().getSystemExecutorService().submit(() -> {
            for (Map<ClusterNode, Map<Integer, Set<Integer>>> map : orderedAssgnments) {
                for (Map.Entry<ClusterNode, Map<Integer, Set<Integer>>> nodeAssigns : map.entrySet()) {
                    UUID nodeId = nodeAssigns.getKey().id();
                    Map<Integer, Set<Integer>> assigns = nodeAssigns.getValue();

                    try {
                        cancelLock.lock();

                        try {
                            if (isDone())
                                return;

                            if (snapFut != null && (snapFut.isCancelled() || !snapFut.get()))
                                break;

                            snapFut = cctx.snapshotMgr().createRemoteSnapshot(nodeId, assigns);

                            if (log.isInfoEnabled())
                                log.info("Start partitions preloading [from=" + nodeId + "]");

                            if (log.isDebugEnabled())
                                log.debug("Current state: " + this);
                        }
                        finally {
                            cancelLock.unlock();
                        }

                        // todo
                        snapFut.get();
                    }
                    catch (IgniteFutureCancelledCheckedException ignore) {
                        // No-op.
                    }
                    catch (IgniteCheckedException e) {
                        log.error(e.getMessage(), e);

                        onDone(e);
                    }
                }
            }
        });
    }

    /**
     *
     */
    public void clearPartitions() {
        if (isDone())
            return;

        cancelLock.lock();

        try {
            for (Map.Entry<String, Set<Long>> entry : regionToParts.entrySet()) {
                String region = entry.getKey();

                Set<Long> parts = entry.getValue();

                GridFutureAdapter fut = regions.get(region);

                PageMemoryEx memEx = (PageMemoryEx)cctx.database().dataRegion(region).pageMemory();

                if (log.isDebugEnabled())
                    log.debug("Cleaning up region " + region);

                // Eviction of partition  should be prevented while cleanup is in progress.
                reservePartitions(parts);

                memEx.clearAsync(
                    (grp, pageId) -> parts.contains(((long)grp << 32) + PageIdUtils.partId(pageId)), true)
                    .listen(c1 -> {
                        cctx.database().checkpointReadLock();

                        try {
                            if (log.isDebugEnabled())
                                log.debug("Off heap region cleared [node=" + cctx.localNodeId() + ", region=" + region + "]");

                            invalidatePartitions(parts);

                            fut.onDone();
                        }
                        catch (RuntimeException | IgniteCheckedException e) {
                            fut.onDone(e);

                            onDone(e);
                        }
                        finally {
                            cctx.database().checkpointReadUnlock();

                            releasePartitions(parts);
                        }
                    });
            }
        }
        catch (RuntimeException | IgniteCheckedException e) {
            onDone(e);
        }
        finally {
            cancelLock.unlock();
        }
    }

    private void invalidatePartitions(Set<Long> partSet) throws IgniteCheckedException {
        for (long partGrp : partSet) {
            int grpId = (int)(partGrp >> 32);
            int partId = (int)partGrp;

            CacheGroupContext grp = cctx.cache().cacheGroup(grpId);

            int tag = ((PageMemoryEx)grp.dataRegion().pageMemory()).invalidate(grpId, partId);

            ((FilePageStoreManager)cctx.pageStore()).getStore(grpId, partId).truncate(tag);

            if (log.isDebugEnabled())
                log.debug("Parition truncated [grp=" + cctx.cache().cacheGroup(grpId).cacheOrGroupName() + ", p=" + partId + "]");
        }
    }

    private void reservePartitions(Set<Long> partSet) {
        for (long entry : partSet)
            localPartition(entry).reserve();
    }

    private void releasePartitions(Set<Long> partSet) {
        for (long entry : partSet)
            localPartition(entry).release();
    }

    private GridDhtLocalPartition localPartition(long globalPartId) {
        int grpId = (int)(globalPartId >> 32);
        int partId = (int)globalPartId;

        // todo remove
        assert partId != INDEX_PARTITION;

        GridDhtLocalPartition part = cctx.cache().cacheGroup(grpId).topology().localPartition(partId);

        assert part != null : "groupId=" + grpId + ", p=" + partId;

        return part;
    }

    /**
     * Wait for region cleaning if necessary.
     *
     * @param grpId Group ID.
     * @throws IgniteCheckedException If the cleanup failed.
     */
    private void awaitCleanupIfNeeded(int grpId) throws IgniteCheckedException {
        try {
            CacheGroupContext grp = cctx.cache().cacheGroup(grpId);

            IgniteInternalFuture fut = regions.get(grp.dataRegion().config().getName());

            if (fut.isCancelled()) {
                log.info("The cleaning task has been canceled.");

                return;
            }

            if (!fut.isDone() && log.isDebugEnabled())
                log.debug("Wait for region cleanup [grp=" + grp + "]");

            fut.get();
        } catch (IgniteFutureCancelledCheckedException ignore) {
            // No-op.
        }
    }

    public void onPartitionSnapshotReceived(UUID nodeId, File file, int grpId, int partId) {
        assert file != null;

        if (log.isTraceEnabled()) {
            log.trace("Processing partition snapshot [grp=" + cctx.cache().cacheGroup(grpId) +
                ", p=" + partId + ", path=" + file + "]");
        }

        if (isDone())
            return;

        try {
            awaitCleanupIfNeeded(grpId);

            if (isDone())
                return;

            reinitPartition(grpId, partId, file);

            AtomicInteger receivedCntr = received.computeIfAbsent(grpId, cntr -> new AtomicInteger());

            int receivedCnt = receivedCntr.incrementAndGet();

            Set<Integer> parts = remaining.get(grpId);

            if (receivedCnt != parts.size())
                return;

            cctx.filePreloader().switchPartitions(grpId, parts, this)
                .listen(fut -> {
                    try {
                        Map<Integer, Long> cntrs = fut.get();

                        assert cntrs != null;

                        cctx.kernalContext().closure().runLocalSafe(() -> onCacheGroupDone(grpId, cntrs));
                    }
                    catch (IgniteCheckedException e) {
                        log.error("Unable to restore partition snapshot [cache=" +
                            cctx.cache().cacheGroup(grpId) + ", p=" + partId + "]", e);

                        onDone(e);
                    }
                });
        }
        catch (IOException | IgniteCheckedException e) {
            log.error("Unable to handle partition snapshot", e);

            onDone(e);
        }
    }

    private void reinitPartition(int grpId, int partId, File src) throws IOException, IgniteCheckedException {
        FilePageStore pageStore = ((FilePageStore)((FilePageStoreManager)cctx.pageStore()).getStore(grpId, partId));

        File dest = new File(pageStore.getFileAbsolutePath());

        if (log.isDebugEnabled()) {
            log.debug("Moving downloaded partition file [from=" + src +
                " , to=" + dest + " , size=" + src.length() + "]");
        }

        assert !cctx.pageStore().exists(grpId, partId) : "Partition file exists [cache=" +
            cctx.cache().cacheGroup(grpId).cacheOrGroupName() + ", p=" + partId + "]";

        Files.move(src.toPath(), dest.toPath());

        CacheGroupContext grp = cctx.cache().cacheGroup(grpId);

        GridDhtLocalPartition part = grp.topology().localPartition(partId);

        part.dataStore().store(false).reinit();

        grp.preloader().rebalanceEvent(partId, EVT_CACHE_REBALANCE_PART_LOADED, exchId.discoveryEvent());
    }

    // todo
    /** {@inheritDoc} */
    @Override public String toString() {
        StringBuilder buf = new StringBuilder();

        buf.append("\n\tReceived: " + received);
        buf.append("\n\tRemainng: " + remaining);

        if (!snapFut.isDone())
            buf.append("\n\tSnapshot: " + snapFut.toString());

        buf.append("\n\tMemory regions:\n");

        for (Map.Entry<String, GridFutureAdapter> entry : regions.entrySet())
            buf.append("\t\t" + entry.getKey() + " finished=" + entry.getValue().isDone() + ", failed=" + entry.getValue().isFailed() + "\n");

        if (!isDone())
            buf.append("\n\tIndex future fnished=").append(idxRebuildFut.isDone()).append(" failed=").append(idxRebuildFut.isFailed()).append(" futs=").append(idxRebuildFut.futures()).append('\n');

        return buf.toString();
    }
}

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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteFutureCancelledCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMemoryEx;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.pagemem.PageIdAllocator.INDEX_PARTITION;

public class FileRebalanceFuture extends GridFutureAdapter<Boolean> {
    /** */
    private final Map<T2<Integer, UUID>, FileRebalanceNodeRoutine> futs = new HashMap<>();

    /** */
    private final GridPartitionFilePreloader.CheckpointListener cpLsnr;

    /** */
    private final Map<Integer, Set<Integer>> allPartsMap = new HashMap<>();

    /** */
    private final Map<Integer, Set<UUID>> allGroupsMap = new ConcurrentHashMap<>();

    /** */
    private final AffinityTopologyVersion topVer;

    /** */
    private final long rebalanceId;

    /** */
    private final Map<String, GridFutureAdapter> regions = new HashMap<>();

    /** */
    private final ReentrantLock cancelLock = new ReentrantLock();

    /** */
    private final GridCacheSharedContext cctx;

    /** */
    private final IgniteLogger log;

    /** */
    private final Map<String, Set<Long>> regionToParts = new HashMap<>();

    /** */
    private final Map<Integer, GridDhtPreloaderAssignments> historicalAssignments = new ConcurrentHashMap<>();

    /** Index rebuild future. */
    private final GridCompoundFuture idxFuture = new GridCompoundFuture<>();

    /** */
    public FileRebalanceFuture() {
        this(null, null, null, null, 0, null);

        onDone(true);
    }

    /**
     * @param lsnr Checkpoint listener.
     */
    public FileRebalanceFuture(
        GridPartitionFilePreloader.CheckpointListener lsnr,
        NavigableMap</** order */Integer, Map<ClusterNode, Map</** group */Integer, Set</** part */Integer>>>> assignsMap,
        AffinityTopologyVersion startVer,
        GridCacheSharedContext cctx,
        long rebalanceId,
        IgniteLogger log
    ) {
        cpLsnr = lsnr;
        topVer = startVer;

        this.log = log;
        this.cctx = cctx;
        this.rebalanceId = rebalanceId;

        // The dummy future does not require initialization.
        if (assignsMap != null)
            initialize(assignsMap);
    }

    /** @deprecated used only for debugging, should be removed */
    @Deprecated
    boolean isPreloading(int grpId) {
        return allGroupsMap.containsKey(grpId) && !isDone();
    }

    /**
     * Initialize rebalancing mappings.
     *
     * @param assignments Assignments.
     */
    private synchronized void initialize(NavigableMap<Integer, Map<ClusterNode, Map<Integer, Set<Integer>>>> assignments) {
        assert assignments != null;
        assert !assignments.isEmpty();

        // todo redundant?
        cancelLock.lock();

        try {
            for (Map<ClusterNode, Map<Integer, Set<Integer>>> map : assignments.values()) {
                for (Map.Entry<ClusterNode, Map<Integer, Set<Integer>>> mapEntry : map.entrySet()) {
                    UUID nodeId = mapEntry.getKey().id();

                    for (Map.Entry<Integer, Set<Integer>> entry : mapEntry.getValue().entrySet()) {
                        int grpId = entry.getKey();

                        allGroupsMap.computeIfAbsent(grpId, v -> new GridConcurrentHashSet<>()).add(nodeId);

                        CacheGroupContext grp = cctx.cache().cacheGroup(grpId);

                        String regName = cctx.cache().cacheGroup(grpId).dataRegion().config().getName();

                        Set<Long> regionParts = regionToParts.computeIfAbsent(regName, v -> new HashSet<>());

                        Set<Integer> allPartitions = allPartsMap.computeIfAbsent(grpId, v -> new HashSet<>());

                        for (Integer partId : entry.getValue()) {
                            assert grp.topology().localPartition(partId).dataStore().readOnly() :
                                "cache=" + grp.cacheOrGroupName() + " p=" + partId;

                            regionParts.add(((long)grpId << 32) + partId);

                            allPartitions.add(partId);
                        }

                        regionParts.add(((long)grpId << 32) + INDEX_PARTITION);
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

    public synchronized void add(int order, FileRebalanceNodeRoutine fut) {
        T2<Integer, UUID> k = new T2<>(order, fut.nodeId());

        futs.put(k, fut);
    }

    // todo add/get should be consistent (ORDER or GROUP_ID arg)
    public synchronized FileRebalanceNodeRoutine nodeRoutine(int grpId, UUID nodeId) {
        int order = cctx.cache().cacheGroup(grpId).config().getRebalanceOrder();

        T2<Integer, UUID> k = new T2<>(order, nodeId);

        return futs.get(k);
    }

    /** {@inheritDoc} */
    @Override public boolean cancel() {
        return onDone(false, null, true);
    }

    /** {@inheritDoc} */
    @Override protected boolean onDone(@Nullable Boolean res, @Nullable Throwable err, boolean cancel) {
        if (cancel || err != null) {
            cancelLock.lock();

            try {
                synchronized (this) {
                    if (isDone())
                        return true;

                    if (log.isInfoEnabled())
                        log.info("Cancelling file rebalancing.");

                    cpLsnr.cancelAll();

                    for (IgniteInternalFuture fut : regions.values()) {
                        if (!fut.isDone())
                            fut.get();
                    }

                    // todo eliminate ConcurrentModification
                    for (FileRebalanceNodeRoutine fut : new HashMap<>(futs).values()) {
                        if (!fut.isDone())
                            fut.cancel();
                    }

                    futs.clear();

                    if (!idxFuture.isDone()) {
                        if (log.isDebugEnabled())
                            log.debug("Cancelling index rebuild");

                        idxFuture.cancel();
                    }
                }
            }
            catch (IgniteCheckedException e) {
                log.error("Failed to cancel file rebalancing.", e);
            }
            finally {
                cancelLock.unlock();
            }
        }

        return super.onDone(res, err, cancel);
    }

    public void onCacheGroupDone(int grpId, UUID nodeId, GridDhtPartitionDemandMessage msg) {
        Set<UUID> remainingNodes = allGroupsMap.get(grpId);

        boolean rmvd = remainingNodes.remove(nodeId);

        assert rmvd : "Duplicate remove " + nodeId;

        if (msg.partitions().hasHistorical()) {
            GridDhtPartitionExchangeId exchId = cctx.exchange().lastFinishedFuture().exchangeId();

            historicalAssignments.computeIfAbsent(grpId, v -> new GridDhtPreloaderAssignments(exchId, topVer)).put(cctx.discovery().node(nodeId), msg);
        }

        if (remainingNodes.isEmpty() && allGroupsMap.remove(grpId) != null) {
            CacheGroupContext grp = cctx.cache().cacheGroup(grpId);

            // rebuildIndexes
            // todo should be combined with existsing mechanics - can conflict with same index rebuild at the same time
            GridQueryProcessor qryProc = cctx.kernalContext().query();

            if (qryProc.moduleEnabled()) {
                if (log.isInfoEnabled())
                    log.info("Starting index rebuild for cache group: " + grp.cacheOrGroupName());

                cancelLock.lock();
                try {
                    for (GridCacheContext ctx : grp.caches()) {
                        IgniteInternalFuture<?> fut = qryProc.rebuildIndexesFromHash(ctx);

                        if (fut != null)
                            idxFuture.add(fut);
                    }

                } finally {
                    cancelLock.unlock();
                }
            }

            GridDhtPreloaderAssignments assigns = historicalAssignments.remove(grpId);

            if (assigns != null) {
                GridCompoundFuture<Boolean, Boolean> histFut = new GridCompoundFuture<>(CU.boolReducer());

                Runnable task = grp.preloader().addAssignments(assigns, true, rebalanceId, null, histFut);

                // todo do we need to run it async
                cctx.kernalContext().getSystemExecutorService().submit(task);

                return;
            }

            CacheGroupContext gctx = cctx.cache().cacheGroup(grpId);

            log.info("Rebalancing complete [group=" + gctx.cacheOrGroupName() + "]");

            if (gctx.localWalEnabled())
                cctx.exchange().scheduleResendPartitions();
            else
                cctx.walState().onGroupRebalanceFinished(gctx.groupId(), topVer);
        }
    }

    public synchronized void onNodeDone(FileRebalanceNodeRoutine fut, Boolean res, Throwable err, boolean cancel) {
        if (err != null || cancel) {
            onDone(res, err, cancel);

            return;
        }

        GridFutureAdapter<Boolean> rmvdFut = futs.remove(new T2<>(fut.order(), fut.nodeId()));

        assert rmvdFut != null && rmvdFut.isDone() : rmvdFut;

//        if (futs.isEmpty()) {
//            histFut.listen(c -> {
//                onDone(true);
//            });
//        }

        if (futs.isEmpty()) {
            cancelLock.lock();

            try {
                if (!idxFuture.initialized()) {
                    idxFuture.markInitialized();

                    idxFuture.listen(clo -> {
                        onDone(true);
                    });
                }
            } finally {
                cancelLock.unlock();
            }
        }
    }

    /**
     * Switch all rebalanced partitions to read-only mode and start evicting.
     */
    public void clearPartitions() {
        if (isDone()) {
            if (log.isDebugEnabled())
                log.debug("Cancelling clear and invalidation");

            return;
        }

        cancelLock.lock();

        try {
            for (Map.Entry<String, Set<Long>> entry : regionToParts.entrySet()) {
                String region = entry.getKey();

                Set<Long> parts = entry.getValue();

                GridFutureAdapter fut = regions.get(region);

                PageMemoryEx memEx = (PageMemoryEx)cctx.database().dataRegion(region).pageMemory();

                if (log.isDebugEnabled())
                    log.debug("Cleaning up region " + region);

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

    private void invalidatePartitions(Set<Long> partitionSet) throws IgniteCheckedException {
        for (long partGrp : partitionSet) {
            int grpId = (int)(partGrp >> 32);
            int partId = (int)partGrp;

            CacheGroupContext grp = cctx.cache().cacheGroup(grpId);

            int tag = ((PageMemoryEx)grp.dataRegion().pageMemory()).invalidate(grpId, partId);

            ((FilePageStoreManager)cctx.pageStore()).getStore(grpId, partId).truncate(tag);

            if (log.isDebugEnabled())
                log.debug("Parition truncated [grp=" + cctx.cache().cacheGroup(grpId).cacheOrGroupName() + ", p=" + partId + "]");

//            // todo close on switch as others
//            if (partId == INDEX_PARTITION) {
//                ((GridCacheOffheapManager.GridCacheDataStore)((IgniteCacheOffheapManagerImpl)grp.offheap()).dataStore(INDEX_PARTITION)).close();
//            }
        }
    }

    private void reservePartitions(Set<Long> partitionSet) {
        for (long entry : partitionSet) {
            GridDhtLocalPartition part = getPartition(entry);

            if (part != null)
                part.reserve();
        }
    }

    private void releasePartitions(Set<Long> partitionSet) {
        for (long entry : partitionSet) {
            GridDhtLocalPartition part = getPartition(entry);

            if (part != null)
                part.release();
        }
    }

    private GridDhtLocalPartition getPartition(long entry) {
        int grpId = (int)(entry >> 32);
        int partId = (int)entry;

        if (partId == INDEX_PARTITION)
            return null;

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
    public void awaitCleanupIfNeeded(int grpId) throws IgniteCheckedException {
        CacheGroupContext grp = cctx.cache().cacheGroup(grpId);

        IgniteInternalFuture fut = regions.get(grp.dataRegion().config().getName());

        if (fut.isCancelled()) {
            log.info("The cleaning task has been canceled.");

            return;
        }

        if (!fut.isDone() && log.isDebugEnabled())
            log.debug("Wait cleanup [grp=" + grp + "]");

        try {
            fut.get();
        } catch (IgniteFutureCancelledCheckedException ignore) {
            // No-op.
        }
    }

    // todo
    /** {@inheritDoc} */
    @Override public String toString() {
        StringBuilder buf = new StringBuilder();

        buf.append("\n\tNode routines:\n");

        for (FileRebalanceNodeRoutine fut : futs.values())
            buf.append("\t\t" + fut.toString() + "\n");

        buf.append("\n\tMemory regions:\n");

        for (Map.Entry<String, GridFutureAdapter> entry : regions.entrySet())
            buf.append("\t\t" + entry.getKey() + " finished=" + entry.getValue().isDone() + ", failed=" + entry.getValue().isFailed() + "\n");

        if (!isDone())
            buf.append("\n\tIndex future fnished=").append(idxFuture.isDone()).append(" failed=").append(idxFuture.isFailed()).append(" futs=").append(idxFuture.futures()).append('\n');

        return buf.toString();
    }
}

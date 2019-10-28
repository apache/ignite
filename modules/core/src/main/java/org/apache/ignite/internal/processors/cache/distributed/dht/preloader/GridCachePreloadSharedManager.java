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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteFeatures;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedManagerAdapter;
import org.apache.ignite.internal.processors.cache.PartitionUpdateCounter;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.persistence.DataRegion;
import org.apache.ignite.internal.processors.cache.persistence.DbCheckpointListener;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStore;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMemoryEx;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotListener;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.lang.IgniteInClosureX;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.GridCacheUtils.UTILITY_CACHE_NAME;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.MOVING;

/**
 * todo naming
 * GridCachePartitionFilePreloader
 * GridFilePreloader
 * GridPartitionPreloader
 * GridSnapshotFilePreloader
 */
public class GridCachePreloadSharedManager extends GridCacheSharedManagerAdapter {
    /** */
    public static final String REBALANCE_CP_REASON = "Rebalance has been scheduled [grps=%s]";

    /** */
    private static final Runnable NO_OP = () -> {};

    /** todo */
    private static final boolean presistenceRebalanceEnabled = IgniteSystemProperties.getBoolean(
        IgniteSystemProperties.IGNITE_PERSISTENCE_REBALANCE_ENABLED, false);

    /** */
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    /** Checkpoint listener. */
    private final CheckpointListener cpLsnr = new CheckpointListener();

    /** */
    private volatile FileRebalanceFuture fileRebalanceFut = new FileRebalanceFuture();

    /**
     * @param ktx Kernal context.
     */
    public GridCachePreloadSharedManager(GridKernalContext ktx) {
        assert CU.isPersistenceEnabled(ktx.config()) :
            "Persistence must be enabled to preload any of cache partition files";
    }

    public boolean persistenceRebalanceApplicable() {
        return !cctx.kernalContext().clientNode() &&
            CU.isPersistenceEnabled(cctx.kernalContext().config()) &&
            cctx.isRebalanceEnabled();
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

            fileRebalanceFut.cancel();
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    // todo the result assignment should be equal to generate assignments
    public void onExchangeDone(GridDhtPartitionExchangeId exchId) {
        for (CacheGroupContext grp : cctx.cache().cacheGroups()) {
            if (!grp.dataRegion().config().isPersistenceEnabled() || CU.isUtilityCache(grp.cacheOrGroupName()))
                continue;

            for (GridDhtLocalPartition part : grp.topology().currentLocalPartitions())
                if (part.state() == MOVING)
                    part.readOnly(true);
        }
    }

    /**
     * This method initiates new file rebalance process from given {@code assignments} by creating new file
     * rebalance future based on them. Cancels previous file rebalance future and sends rebalance started event (todo).
     * In case of delayed rebalance method schedules the new one with configured delay based on {@code lastExchangeFut}.
     *
     * @param assignsMap A map of cache assignments grouped by grpId.
     * @param force {@code true} if must cancel previous rebalance.
     * @param rebalanceId Current rebalance id.
     * @return Runnable to execute the chain.
     */
    public Runnable addNodeAssignments(
        Map<Integer, GridDhtPreloaderAssignments> assignsMap,
        AffinityTopologyVersion topVer,
        boolean force,
        long rebalanceId) {
        NavigableMap<Integer, Map<ClusterNode, Map<Integer, Set<Integer>>>> nodeOrderAssignsMap =
            sliceNodeCacheAssignments(assignsMap);

        if (nodeOrderAssignsMap.isEmpty())
            return NO_OP;

        // Start new rebalance session.
        FileRebalanceFuture rebFut = fileRebalanceFut;

        lock.writeLock().lock();

        try {
            if (!rebFut.isDone())
                rebFut.cancel();

            fileRebalanceFut = rebFut = new FileRebalanceFuture(cpLsnr, assignsMap, topVer, cctx, log);

            FileRebalanceNodeFuture rqFut = null;
            Runnable rq = NO_OP;

            if (log.isInfoEnabled())
                log.info("Prepare the chain to demand assignments: " + nodeOrderAssignsMap);

            for (Map.Entry<Integer, Map<ClusterNode, Map<Integer, Set<Integer>>>> entry : nodeOrderAssignsMap.descendingMap().entrySet()) {
                Map<ClusterNode, Map<Integer, Set<Integer>>> descNodeMap = entry.getValue();

                int order = entry.getKey();

                for (Map.Entry<ClusterNode, Map<Integer, Set<Integer>>> assignEntry : descNodeMap.entrySet()) {
                    FileRebalanceNodeFuture fut = new FileRebalanceNodeFuture(cctx, fileRebalanceFut, log, assignEntry.getKey(),
                        order, rebalanceId, assignEntry.getValue(), topVer);

                    rebFut.add(order, fut);

                    final Runnable nextRq0 = rq;
                    final FileRebalanceNodeFuture rqFut0 = rqFut;

//                }
//                    else {

                    if (rqFut0 != null) {
                        // xxxxFut = xxxFut; // The first seen rebalance node.
                        fut.listen(f -> {
                            try {
                                if (log.isDebugEnabled())
                                    log.debug("Running next task, last future result is " + f.get());

                                if (f.get()) // Not cancelled.
                                    nextRq0.run();
                                // todo check how this chain is cancelling
                            }
                            catch (IgniteCheckedException e) {
                                rqFut0.onDone(e);
                            }
                        });
                    }

                    rq = fut::requestPartitions;
                    rqFut = fut;
                }
            }

            cctx.kernalContext().getSystemExecutorService().submit(rebFut::clearPartitions);

            rebFut.listen(new IgniteInClosureX<IgniteInternalFuture<Boolean>>() {
                @Override public void applyx(IgniteInternalFuture<Boolean> fut0) throws IgniteCheckedException {
                    if (fut0.isCancelled()) {
                        log.info("File rebalance canceled");

                        return;
                    }

                    if (log.isInfoEnabled())
                        log.info("The final persistence rebalance is done [result=" + fut0.get() + ']');
                }
            });

            return rq;
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * @param assignsMap The map of cache groups assignments to process.
     * @return The map of cache assignments <tt>[group_order, [node, [group_id, partitions]]]</tt>
     */
    private NavigableMap<Integer, Map<ClusterNode, Map<Integer, Set<Integer>>>> sliceNodeCacheAssignments(
        Map<Integer, GridDhtPreloaderAssignments> assignsMap) {
        NavigableMap<Integer, Map<ClusterNode, Map<Integer, Set<Integer>>>> result = new TreeMap<>();

        for (Map.Entry<Integer, GridDhtPreloaderAssignments> grpEntry : assignsMap.entrySet()) {
            int grpId = grpEntry.getKey();

            CacheGroupContext grp = cctx.cache().cacheGroup(grpId);

            GridDhtPreloaderAssignments assigns = grpEntry.getValue();

            if (fileRebalanceRequired(grp, assigns)) {
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
        }

        return result;
    }

    /**
     * @param fut The future to check.
     * @return <tt>true</tt> if future can be processed.
     */
    private boolean staleFuture(FileRebalanceNodeFuture fut) {
        return fut == null || fut.isCancelled() || fut.isFailed() || fut.isDone() || topologyChanged(fut);
    }

    /**
     * @param grp The corresponding to assignments cache group context.
     * @param assignments Preloading assignments.
     * @return {@code True} if cache must be rebalanced by sending files.
     */
    public boolean fileRebalanceRequired(CacheGroupContext grp, GridDhtPreloaderAssignments assignments) {
        return FileRebalanceSupported(grp, assignments) &&
            grp.config().getRebalanceDelay() != -1 &&
            grp.config().getRebalanceMode() != CacheRebalanceMode.NONE;
    }

    /**
     * @param grp The corresponding to assignments cache group context.
     * @param assignments Preloading assignments.
     * @return {@code True} if cache might be rebalanced by sending cache partition files.
     */
    public boolean FileRebalanceSupported(CacheGroupContext grp, GridDhtPreloaderAssignments assignments) {
        if (assignments.keySet() == null || assignments.keySet().isEmpty())
            return false;

        // Do not rebalance system cache with files as they are not exists.
        if (grp.groupId() == CU.cacheId(UTILITY_CACHE_NAME))
            return false;

        if (grp.mvccEnabled())
            return false;

        Map<Integer, Long> globalSizes = grp.topology().globalPartSizes();

        assert !globalSizes.isEmpty() : grp.cacheOrGroupName();

        boolean notEnoughData = true;

        for (Long partSize : globalSizes.values()) {
            if (partSize > 0) {
                notEnoughData = false;

                break;
            }
        }

        if (notEnoughData)
            return false;

        if (!presistenceRebalanceEnabled ||
            !grp.persistenceEnabled() ||
            !IgniteFeatures.allNodesSupports(assignments.keySet(), IgniteFeatures.CACHE_PARTITION_FILE_REBALANCE))
            return false;

        for (GridDhtPartitionDemandMessage msg : assignments.values()) {
            if (msg.partitions().hasHistorical())
                return false;
        }

        return true;
    }

    /**
     * Restore partition on new file. Partition should be completely destroyed before restore it with new file.
     *
     * @param grpId Group id.
     * @param partId Partition number.
     * @param partFile New partition file on the same filesystem.
     * @param fut
     * @return Future that will be completed when partition will be fully re-initialized. The future result is the HWM
     * value of update counter in read-only partition.
     * @throws IgniteCheckedException If file store for specified partition doesn't exists or partition file cannot be
     * moved.
     */
    public IgniteInternalFuture<T2<Long, Long>> restorePartition(int grpId, int partId,
        File partFile,
        FileRebalanceNodeFuture fut) throws IgniteCheckedException {
        if (topologyChanged(fut))
            return null;

        FilePageStore pageStore = ((FilePageStore)((FilePageStoreManager)cctx.pageStore()).getStore(grpId, partId));

        File dest = new File(pageStore.getFileAbsolutePath());

        if (log.isDebugEnabled())
            log.debug("Moving downloaded partition file: " + partFile + " --> " + dest + "  (size=" + partFile.length() + ")");

        try {
            Files.move(partFile.toPath(), dest.toPath());
        }
        catch (IOException e) {
            throw new IgniteCheckedException("Unable to move file [source=" + partFile + ", target=" + dest + "]", e);
        }

        GridDhtLocalPartition part = cctx.cache().cacheGroup(grpId).topology().localPartition(partId);

        part.dataStore().store(false).reinit();

        GridFutureAdapter<T2<Long, Long>> endFut = new GridFutureAdapter<>();

        cpLsnr.schedule(() -> {
            if (topologyChanged(fut))
                return null;

            // Save current update counter.
            PartitionUpdateCounter maxCntr = part.dataStore().partUpdateCounter();

            assert maxCntr != null;

            assert cctx.pageStore().exists(grpId, partId) : "File doesn't exist [grpId=" + grpId + ", p=" + partId + "]";

            part.readOnly(false);

            // Clear all on heap entries.
            // todo something smarter
            // todo check on large partition
            part.entriesMap(null).map.clear();

            PartitionUpdateCounter minCntr = part.dataStore().partUpdateCounter();

            assert minCntr != null;
            assert minCntr.get() != 0 : "grpId=" + grpId + ", p=" + partId + ", fullSize=" + part.dataStore().fullSize();

            AffinityTopologyVersion infinTopVer = new AffinityTopologyVersion(Long.MAX_VALUE, 0);

            IgniteInternalFuture<?> partReleaseFut = cctx.partitionReleaseFuture(infinTopVer);

            // Operations that are in progress now will be lost and should be included in historical rebalancing.
            // These operations can update the old update counter or the new update counter, so the maximum applied
            // counter is used after all updates are completed.
            // todo Consistency check fails sometimes for ATOMIC cache.
            partReleaseFut.listen(c ->
                endFut.onDone(
                    new T2<>(minCntr.get(), Math.max(maxCntr.highestAppliedCounter(), minCntr.highestAppliedCounter()))
                )
            );

            return null;
        });

        return endFut;
    }

    /**
     * @param fut Future.
     * @return {@code True} if rebalance topology version changed by exchange thread or force
     * reassing exchange occurs, see {@link RebalanceReassignExchangeTask} for details.
     */
    private boolean topologyChanged(FileRebalanceNodeFuture fut) {
        return !cctx.exchange().rebalanceTopologyVersion().equals(fut.topVer);
        // todo || fut != rebalanceFut; // Same topology, but dummy exchange forced because of missing partitions.
    }

    /** */
    private static class CheckpointListener implements DbCheckpointListener {
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
            return schedule(() -> {
                task.run();

                return null;
            });
        }

        public <R> IgniteInternalFuture<R> schedule(final Callable<R> task) {
            return schedule(new CheckpointTask<>(task));
        }

        private <R> IgniteInternalFuture<R> schedule(CheckpointTask<R> task) {
            queue.offer(task);

            return task.fut;
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
        @Override public void onPartition(UUID nodeId, String snpName, File file, int grpId, int partId) {
            FileRebalanceNodeFuture fut = fileRebalanceFut.nodeRoutine(grpId, nodeId);

            if (staleFuture(fut) || !snpName.equals(fut.snapName)) {
                if (log.isDebugEnabled())
                    log.debug("Cancel partitions download due to stale rebalancing future [current snapshot=" + snpName + ", fut=" + fut);

                // todo
                file.delete();

                return;
//                // todo how cancel current download
//                throw new IgniteException("Cancel partitions download due to stale rebalancing future.");
            }

            try {
                fileRebalanceFut.awaitCleanupIfNeeded(grpId);

                IgniteInternalFuture<T2<Long, Long>> restoreFut = restorePartition(grpId, partId, file, fut);

                // todo
                if (topologyChanged(fut)) {
                    log.info("Cancel partitions download due to topology changes.");

                    file.delete();

                    fut.cancel();

                    throw new IgniteException("Cancel partitions download due to topology changes.");
                }

                restoreFut.listen(f -> {
                    try {
                        T2<Long, Long> cntrs = f.get();

                        assert cntrs != null;

                        cctx.kernalContext().closure().runLocalSafe(() -> {
                            fut.onPartitionRestored(grpId, partId, cntrs.get1(), cntrs.get2());
                        });
                    }
                    catch (IgniteCheckedException e) {
                        log.error("Unable to restore partition snapshot [cache=" +
                            cctx.cache().cacheGroup(grpId) + ", p=" + partId, e);

                        fut.onDone(e);
                    }
                });
            }
            catch (IgniteCheckedException e) {
                log.error("Unable to handle partition snapshot", e);

                fut.onDone(e);
            }
        }

        /** {@inheritDoc} */
        @Override public void onEnd(UUID rmtNodeId, String snpName) {

        }

        /** {@inheritDoc} */
        @Override public void onException(UUID rmtNodeId, String snpName, Throwable t) {
            log.error("Unable to create remote snapshot " + snpName, t);

            fileRebalanceFut.onDone(t);
        }
    }

    /** */
    private static class FileRebalanceFuture extends GridFutureAdapter<Boolean> {
        /** */
        private final Map<T2<Integer, UUID>, FileRebalanceNodeFuture> futMap = new HashMap<>();

        /** */
        private final CheckpointListener cpLsnr;

        /** */
        private final Map<Integer, Set<Integer>> allPartsMap = new HashMap<>();

        /** */
        private final Map<Integer, Set<UUID>> allGroupsMap = new ConcurrentHashMap<>();

        /** */
        private final AffinityTopologyVersion topVer;

        /** */
        private final Map<String, PageMemCleanupTask> regions = new HashMap<>();

        /** */
        private final ReentrantLock cancelLock = new ReentrantLock();

        /** */
        private final GridCacheSharedContext cctx;

        /** */
        private final IgniteLogger log;

        /** */
        public FileRebalanceFuture() {
            this(null, null, null, null, null);

            onDone(true);
        }

        /**
         * @param lsnr Checkpoint listener.
         */
        public FileRebalanceFuture(
            CheckpointListener lsnr,
            Map<Integer, GridDhtPreloaderAssignments> assignsMap,
            AffinityTopologyVersion startVer,
            GridCacheSharedContext cctx,
            IgniteLogger log
        ) {
            cpLsnr = lsnr;
            topVer = startVer;

            this.log = log;
            this.cctx = cctx;

            initialize(assignsMap);
        }

        /**
         * Initialize rebalancing mappings.
         *
         * @param assignments Assignments.
         */
        private synchronized void initialize(Map<Integer, GridDhtPreloaderAssignments> assignments) {
            if (assignments == null || assignments.isEmpty())
                return;

            Map<String, Set<Long>> regionToParts = new HashMap<>();

            // todo redundant?
            cancelLock.lock();

            try {
                for (Map.Entry<Integer, GridDhtPreloaderAssignments> entry : assignments.entrySet()) {
                    int grpId = entry.getKey();
                    GridDhtPreloaderAssignments assigns = entry.getValue();

                    Set<UUID> nodes = allGroupsMap.computeIfAbsent(grpId, v -> new GridConcurrentHashSet<>());

                    CacheGroupContext grp = cctx.cache().cacheGroup(grpId);

                    if (!cctx.filePreloader().fileRebalanceRequired(grp, assigns))
                        continue;

                    String regName = cctx.cache().cacheGroup(grpId).dataRegion().config().getName();

                    Set<Long> regionParts = regionToParts.computeIfAbsent(regName, v -> new HashSet<>());

                    Set<Integer> allPartitions = allPartsMap.computeIfAbsent(grpId, v -> new HashSet<>());

                    for (Map.Entry<ClusterNode, GridDhtPartitionDemandMessage> e : assigns.entrySet()) {
                        GridDhtPartitionDemandMessage msg = e.getValue();
                        ClusterNode node = e.getKey();

                        nodes.add(node.id());

                        Set<Integer> parttitions = msg.partitions().fullSet();

                        for (Integer partId : parttitions) {
                            regionParts.add(((long)grpId << 32) + partId);

                            allPartitions.add(partId);
                        }
                    }
                }

                for (Map.Entry<String, Set<Long>> e : regionToParts.entrySet())
                    regions.put(e.getKey(), new PageMemCleanupTask(e.getKey(), e.getValue()));
            }
            finally {
                cancelLock.unlock();
            }
        }

        public synchronized void add(int order, FileRebalanceNodeFuture fut) {
            T2<Integer, UUID> k = new T2<>(order, fut.node.id());

            futMap.put(k, fut);
        }

        // todo add/get should be consistent (ORDER or GROUP_ID arg)
        public synchronized FileRebalanceNodeFuture nodeRoutine(int grpId, UUID nodeId) {
            int order = cctx.cache().cacheGroup(grpId).config().getRebalanceOrder();

            T2<Integer, UUID> k = new T2<>(order, nodeId);

            return futMap.get(k);
        }

        /** {@inheritDoc} */
        @Override public boolean cancel() {
            return onDone(false, null, true);
        }

        /** {@inheritDoc} */
        @Override protected boolean onDone(@Nullable Boolean res, @Nullable Throwable err, boolean cancel) {
            if (cancel) {
                cancelLock.lock();

                try {
                    synchronized (this) {
                        if (isDone())
                            return true;

                        if (log.isInfoEnabled())
                            log.info("Cancel file rebalancing.");

                        cpLsnr.cancelAll();

                        for (IgniteInternalFuture fut : regions.values()) {
                            if (!fut.isDone())
                                fut.cancel();
                        }

                        for (FileRebalanceNodeFuture fut : futMap.values()) {
                            if (!cctx.filePreloader().staleFuture(fut))
                                fut.cancel();
                        }

                        futMap.clear();

//                        cctx.database().checkpointReadLock();
//
//                        try {
//                            for (Map.Entry<Integer, Set<Integer>> e : allPartsMap.entrySet()) {
//                                int grpId = e.getKey();
//
//                                CacheGroupContext grp = cctx.cache().cacheGroup(grpId);
//
//                                if (grp == null)
//                                    continue;
//
//                                for (int partId : e.getValue()) {
//                                    if (grp != null) {
//                                        GridDhtLocalPartition part = grp.topology().localPartition(partId);
//
//                                        CacheDataStoreEx store = part.dataStore();
//
//                                        if (!cctx.pageStore().exists(grpId, partId)) {
//                                            cctx.pageStore().ensure(grpId, partId);
//
//                                            store.reinit();
//
//                                            log.info(">xxx> init grp=" + grpId + " p=" + partId);
//                                        }
//
//                                        if (store.readOnly())
//                                            store.readOnly(false);
//                                    }
//                                }
//                            }
//                        } finally {
//                            cctx.database().checkpointReadUnlock();
//                        }
                    }
                }
                catch (IgniteCheckedException e) {
                    e.printStackTrace();
                }
                finally {
                    cancelLock.unlock();
                }
            }

            return super.onDone(res, err, cancel);
        }

        public void onNodeGroupDone(int grpId, UUID nodeId, boolean historical) {
            Set<UUID> remainingNodes = allGroupsMap.get(grpId);

            boolean rmvd = remainingNodes.remove(nodeId);

            assert rmvd : "Duplicate remove " + nodeId;

            if (remainingNodes.isEmpty() && allGroupsMap.remove(grpId) != null && !historical) {
                CacheGroupContext gctx = cctx.cache().cacheGroup(grpId);

                log.info("Rebalancing complete [group=" + gctx.cacheOrGroupName() + "]");

                if (gctx.localWalEnabled())
                    cctx.exchange().scheduleResendPartitions();
                else
                    cctx.walState().onGroupRebalanceFinished(gctx.groupId(), topVer);
            }
        }

        public synchronized void onNodeDone(FileRebalanceNodeFuture fut, Boolean res, Throwable err, boolean cancel) {
            if (err != null || cancel) {
                onDone(res, err, cancel);

                return;
            }

            GridFutureAdapter<Boolean> rmvdFut = futMap.remove(new T2<>(fut.order(), fut.nodeId()));

            assert rmvdFut != null && rmvdFut.isDone() : rmvdFut;

            if (futMap.isEmpty())
                onDone(true);
        }

        /**
         * Switch all rebalanced partitions to read-only mode and start evicting.
         */
        private void clearPartitions() {
//            IgniteInternalFuture<Void> switchFut = cpLsnr.schedule(() -> {
//                for (Map.Entry<Integer, Set<Integer>> e : allPartsMap.entrySet()) {
//                    CacheGroupContext grp = cctx.cache().cacheGroup(e.getKey());
//
//                    if (log.isDebugEnabled())
//                        log.debug("switch partitions [cache=" + grp.cacheOrGroupName() + "]");
//
//                    for (Integer partId : e.getValue()) {
//                        GridDhtLocalPartition part = grp.topology().localPartition(partId);
//
//                        // todo reinit just set update counter from delegate
//                        part.dataStore().store(true).reinit();
//
//                        if (part.readOnly())
//                            continue;
//
//                        part.readOnly(true);
//                    }
//                }
//            });

            for (Map.Entry<Integer, Set<Integer>> e : allPartsMap.entrySet()) {
                CacheGroupContext grp = cctx.cache().cacheGroup(e.getKey());

                for (Integer partId : e.getValue()) {
                    assert grp.topology().localPartition(partId).dataStore().readOnly();

                    grp.topology().localPartition(partId).dataStore().store(true).reinit();
                }
            }



//            try {
//                if (!switchFut.isDone())
//                    cctx.database().wakeupForCheckpoint(String.format(REBALANCE_CP_REASON, allPartsMap.keySet()));
//
//                switchFut.get();
//            }
//            catch (IgniteCheckedException e) {
//                log.error(e.getMessage(), e);
//
//                onDone(e);
//
//                return;
//            }

            if (isDone()) {
                if (log.isDebugEnabled())
                    log.debug("Cacncelling clear and invalidation");

                return;
            }

            for (Map.Entry<Integer, Set<Integer>> e : allPartsMap.entrySet()) {
                int grpId = e.getKey();

                CacheGroupContext grp = cctx.cache().cacheGroup(grpId);

                if (log.isDebugEnabled())
                    log.debug("Clearing partitions [cache=" + grp.cacheOrGroupName() + "]");

                for (Integer partId : e.getValue()) {
                    GridDhtLocalPartition part = grp.topology().localPartition(partId);

                    part.clearAsync();

                    part.onClearFinished(c -> {
                        cancelLock.lock();

//                        cctx.database().checkpointReadLock();

                        try {
                            if (isDone()) {
                                if (log.isDebugEnabled())
                                    log.debug("Cacncel pagemem invalidation grp=" + grpId + ", p=" + partId + ", rebalance canceled topVer="+this.topVer.topologyVersion() + "." + topVer.minorTopologyVersion());

                                return;
                            }

//                            if (log.isDebugEnabled())
//                                log.debug("Invalidate grp=" + grpId + ", p=" + partId);
//
//                            int tag = ((PageMemoryEx)grp.dataRegion().pageMemory()).invalidate(grpId, partId);
//
//                            ((FilePageStoreManager)cctx.pageStore()).getStore(grpId, partId).truncate(tag);

                            PageMemCleanupTask task = regions.get(grp.dataRegion().config().getName());

                            task.onPartitionCleared();
                        }
                        catch (IgniteCheckedException ex) {
                            onDone(ex);
                        }
                        finally {
//                            cctx.database().checkpointReadUnlock();
                            cancelLock.unlock();
                        }
                    });
                }
            }
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

            if (fut.isCancelled())
                throw new IgniteCheckedException("The cleaning task has been canceled.");

            if (!fut.isDone() && log.isDebugEnabled())
                log.debug("Wait cleanup [cache=" + grp + "]");

            fut.get();
        }

        private class PageMemCleanupTask extends GridFutureAdapter {
            private final Set<Long> parts;

            private final AtomicInteger evictedCntr;

            private final String name;

            public PageMemCleanupTask(String regName, Set<Long> remainingParts) {
                name = regName;
                parts = remainingParts;
                evictedCntr = new AtomicInteger();
            }

            /** {@inheritDoc} */
            @Override public boolean cancel() {
                return onDone(null, null, true);
            }

            public void onPartitionCleared() throws IgniteCheckedException {
                if (isCancelled())
                    return;

                int evictedCnt = evictedCntr.incrementAndGet();

                assert evictedCnt <= parts.size();

                if (log.isDebugEnabled())
                    log.debug("Partition cleared [cleared=" + evictedCnt + ", remaining=" + (parts.size() - evictedCnt) + "]");

                if (evictedCnt == parts.size()) {
                    DataRegion region = cctx.database().dataRegion(name);

                    cctx.database().checkpointReadLock();
                    cancelLock.lock();

                    try {
                        if (isCancelled())
                            return;

                        for (long partGrp : parts) {
                            int grpId = (int)(partGrp >> 32);
                            int partId = (int)partGrp;

                            CacheGroupContext grp = cctx.cache().cacheGroup(grpId);

                            int tag = ((PageMemoryEx)grp.dataRegion().pageMemory()).invalidate(grpId, partId);

                            ((FilePageStoreManager)cctx.pageStore()).getStore(grpId, partId).truncate(tag);

                            if (log.isDebugEnabled())
                                log.debug("Truncated grp=" + cctx.cache().cacheGroup(grpId).cacheOrGroupName() + ", p=" + partId);
                        }

                        PageMemoryEx memEx = (PageMemoryEx)region.pageMemory();

                        if (log.isDebugEnabled())
                            log.debug("Clearing region: " + name);

                        memEx.clearAsync(
                            (grp, pageId) -> {
//                                if (isCancelled())
//                                    return false;

                                return parts.contains(((long)grp << 32) + PageIdUtils.partId(pageId));
                            }, true)
                            .listen(c1 -> {
                                // todo misleading should be reformulate
                                if (log.isDebugEnabled())
                                    log.debug("Off heap region cleared [node=" + cctx.localNodeId() + ", region=" + name + "]");

                                onDone();
                            });

                        log.info("Await pagemem cleanup");

                        get();
                    } finally {
                        cancelLock.unlock();

                        cctx.database().checkpointReadUnlock();
                    }
                }
            }
        }
    }

    /** */
    private static class FileRebalanceNodeFuture extends GridFutureAdapter<Boolean> {
        /** Context. */
        protected GridCacheSharedContext cctx;

        /** Logger. */
        protected IgniteLogger log;

        /** */
        private long rebalanceId;

        /** */
        @GridToStringInclude
        private Map<Integer, Set<Integer>> assigns;

        /** */
        private AffinityTopologyVersion topVer;

        /** */
        private Map<Integer, Set<Integer>> remaining;

        /** */
        private Map<Integer, Set<HistoryDesc>> remainingHist;

        /** {@code True} if the initial demand request has been sent. */
        private AtomicBoolean initReq = new AtomicBoolean();

        /** */
        private final ClusterNode node;

        /** */
        private final FileRebalanceFuture mainFut;

        /** Cache group rebalance order. */
        private final int rebalanceOrder;

        /** Node snapshot name. */
        private volatile String snapName;

        /**
         * Default constructor for the dummy future.
         */
        public FileRebalanceNodeFuture() {
            this(null, null, null, null, 0, 0, Collections.emptyMap(), null);

            onDone();
        }

        /**
         * @param node Supplier node.
         * @param rebalanceId Rebalance id.
         * @param assigns Map of assignments to request from remote.
         * @param topVer Topology version.
         */
        public FileRebalanceNodeFuture(
            GridCacheSharedContext cctx,
            FileRebalanceFuture mainFut,
            IgniteLogger log,
            ClusterNode node,
            int rebalanceOrder,
            long rebalanceId,
            Map<Integer, Set<Integer>> assigns,
            AffinityTopologyVersion topVer
        ) {
            this.cctx = cctx;
            this.mainFut = mainFut;
            this.log = log;
            this.node = node;
            this.rebalanceOrder = rebalanceOrder;
            this.rebalanceId = rebalanceId;
            this.assigns = assigns;
            this.topVer = topVer;

            remaining = new ConcurrentHashMap<>(assigns.size());
            remainingHist = new ConcurrentHashMap<>(assigns.size());

            for (Map.Entry<Integer, Set<Integer>> entry : assigns.entrySet()) {
                Set<Integer> parts = entry.getValue();
                int grpId = entry.getKey();

                assert !remaining.containsKey(grpId);

                remaining.put(grpId, new GridConcurrentHashSet<>(entry.getValue()));
            }
        }

        /**
         * @return Rebalancing order.
         */
        public int order() {
            return rebalanceOrder;
        }

        /**
         * @return Supplier node ID.
         */
        public UUID nodeId() {
            return node.id();
        }

        /** {@inheritDoc} */
        @Override public boolean cancel() {
            return onDone(false, null, true);
        }

        /**
         * @param grpId Cache group id to search.
         * @param partId Cache partition to remove;
         */
        public void onPartitionRestored(int grpId, int partId, long min, long max) {
            Set<Integer> parts = remaining.get(grpId);

            assert parts != null : "Invalid group identifier: " + grpId;

            remainingHist.computeIfAbsent(grpId, v -> new ConcurrentSkipListSet<>())
                .add(new HistoryDesc(partId, min, max));

            if (log.isDebugEnabled()) {
                log.debug("Partition done [grp=" + cctx.cache().cacheGroup(grpId).cacheOrGroupName() +
                    ", p=" + partId + ", remaining=" + parts.size() + "]");
            }

            boolean rmvd = parts.remove(partId);

            assert rmvd : "Partition not found: " + partId;

            if (parts.isEmpty())
                onGroupRestored(grpId);
        }

        private void onGroupRestored(int grpId) {
            Set<Integer> parts = remaining.remove(grpId);

            if (parts == null)
                return;

            Set<HistoryDesc> histParts = remainingHist.remove(grpId);

            assert histParts.size() == assigns.get(grpId).size() : "expect=" + assigns.get(grpId).size() + ", actual=" + histParts.size();

            CacheGroupContext grp = cctx.cache().cacheGroup(grpId);

            GridDhtPartitionDemandMessage msg = new GridDhtPartitionDemandMessage(rebalanceId, topVer, grpId);

            for (HistoryDesc desc : histParts) {
                assert desc.toCntr >= desc.fromCntr : "from=" + desc.fromCntr + ", to=" + desc.toCntr;

                if (desc.fromCntr != desc.toCntr) {
                    if (log.isDebugEnabled()) {
                        log.debug("Prepare to request historical rebalancing [cache=" + grp.cacheOrGroupName() + ", p=" +
                            desc.partId + ", from=" + desc.fromCntr + ", to=" + desc.toCntr + "]");
                    }

                    msg.partitions().addHistorical(desc.partId, desc.fromCntr, desc.toCntr, histParts.size());

                    continue;
                }

                log.debug("Skipping historical rebalancing [p=" +
                    desc.partId + ", from=" + desc.fromCntr + ", to=" + desc.toCntr + "]");

                // No historical rebalancing required  -can own partition.
                if (grp.localWalEnabled()) {
                    boolean owned = grp.topology().own(grp.topology().localPartition(desc.partId));

                    assert owned : "part=" + desc.partId + ", grp=" + grp.cacheOrGroupName();
                }
            }

            if (!msg.partitions().hasHistorical()) {
                mainFut.onNodeGroupDone(grpId, nodeId(), false);

                if (remaining.isEmpty() && !isDone())
                    onDone(true);

                return;
            }

            GridDhtPartitionExchangeId exchId = cctx.exchange().lastFinishedFuture().exchangeId();

            GridDhtPreloaderAssignments assigns = new GridDhtPreloaderAssignments(exchId, topVer);

            assigns.put(node, msg);

            GridCompoundFuture<Boolean, Boolean> forceFut = new GridCompoundFuture<>(CU.boolReducer());

            Runnable cur = grp.preloader().addAssignments(assigns,
                true,
                rebalanceId,
                null,
                forceFut);

            if (log.isDebugEnabled())
                log.debug("Triggering historical rebalancing [node=" + node.id() + ", group=" + grp.cacheOrGroupName() + "]");

            cur.run();

            forceFut.markInitialized();

            forceFut.listen(c -> {
                try {
                    mainFut.onNodeGroupDone(grpId, nodeId(), true);

                    if (forceFut.get() && remaining.isEmpty())
                        onDone(true);
                    else
                        cancel();
                }
                catch (IgniteCheckedException e) {
                    onDone(e);
                }
            });
        }

        /** {@inheritDoc} */
        @Override public synchronized boolean onDone(@Nullable Boolean res, @Nullable Throwable err, boolean cancel) {
            if (isDone())
                return false;

            boolean r = super.onDone(res, err, cancel);

            mainFut.onNodeDone(this, res, err, cancel);

            return r;
        }

        /**
         * Request a remote snapshot of partitions.
         */
        public void requestPartitions() {
            try {
                if (log.isInfoEnabled())
                    log.info("Start partitions preloading [from=" + node.id() + ", fut=" + this + ']');

                snapName = cctx.snapshotMgr().createRemoteSnapshot(node.id(), assigns);
            }
            catch (IgniteCheckedException e) {
                log.error("Unable to create remote snapshot [from=" + node.id() + ", assigns=" + assigns + "]", e);

                onDone(e);
            }
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(FileRebalanceNodeFuture.class, this);
        }

        private static class HistoryDesc implements Comparable {
            /** Partition id. */
            final int partId;

            /** From counter. */
            final long fromCntr;

            /** To counter. */
            final long toCntr;

            public HistoryDesc(int partId, long fromCntr, long toCntr) {
                this.partId = partId;
                this.fromCntr = fromCntr;
                this.toCntr = toCntr;
            }

            @Override public int compareTo(@NotNull Object o) {
                HistoryDesc otherDesc = (HistoryDesc)o;

                if (partId > otherDesc.partId)
                    return 1;

                if (partId < otherDesc.partId)
                    return -1;

                return 0;
            }
        }
    }
}

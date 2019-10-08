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
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.ignite.IgniteCheckedException;
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
import org.apache.ignite.internal.processors.cache.GridCacheGroupIdMessage;
import org.apache.ignite.internal.processors.cache.GridCacheIdMessage;
import org.apache.ignite.internal.processors.cache.GridCacheMessage;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedManagerAdapter;
import org.apache.ignite.internal.processors.cache.PartitionUpdateCounter;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.persistence.DbCheckpointListener;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.ReadOnlyGridCacheDataStore;
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
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.GridTopic.TOPIC_REBALANCE;
import static org.apache.ignite.internal.processors.cache.GridCacheUtils.UTILITY_CACHE_NAME;

/** */
public class GridCachePreloadSharedManager extends GridCacheSharedManagerAdapter {
    /** */
    public static final String REBALANCE_CP_REASON = "Rebalance has been scheduled [grps=%s]";

    /** */
    private static final Runnable NO_OP = () -> {};

    /** */
    public static final int REBALANCE_TOPIC_IDX = 0;

    /** todo */
    private static final boolean presistenceRebalanceEnabled = IgniteSystemProperties.getBoolean(
        IgniteSystemProperties.IGNITE_PERSISTENCE_REBALANCE_ENABLED, false);

    /** */
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    /** Checkpoint listener. */
    private final CheckpointListener cpLsnr = new CheckpointListener();

    /** */
//    private volatile FileRebalanceSingleNodeFuture headFut = new FileRebalanceSingleNodeFuture();

    private volatile FileRebalanceFuture mainFut = new FileRebalanceFuture();

    /**
     * @param ktx Kernal context.
     */
    public GridCachePreloadSharedManager(GridKernalContext ktx) {
        assert CU.isPersistenceEnabled(ktx.config()) :
            "Persistence must be enabled to preload any of cache partition files";
    }

    /**
     * @return The Rebalance topic to communicate with.
     */
    public static Object rebalanceThreadTopic() {
        return TOPIC_REBALANCE.topic("Rebalance", REBALANCE_TOPIC_IDX);
    }

    public boolean persistenceRebalanceApplicable() {
        return !cctx.kernalContext().clientNode() &&
            CU.isPersistenceEnabled(cctx.kernalContext().config()) &&
            cctx.isRebalanceEnabled();
    }

    /** {@inheritDoc} */
    @Override protected void start0() throws IgniteCheckedException {
        ((GridCacheDatabaseSharedManager)cctx.database()).addCheckpointListener(cpLsnr);

        cctx.snapshotMgr().addSnapshotListener(new RebalanceSnapshotListener());
    }

    /** {@inheritDoc} */
    @Override protected void stop0(boolean cancel) {
        lock.writeLock().lock();

        try {
            ((GridCacheDatabaseSharedManager)cctx.database()).removeCheckpointListener(cpLsnr);

            mainFut.cancel();
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * @param assignsMap A map of cache assignments grouped by grpId.
     * @param force {@code true} if must cancel previous rebalance.
     * @param rebalanceId Current rebalance id.
     * @return Runnable to execute the chain.
     */
    public Runnable addNodeAssignments(
        Map<Integer, GridDhtPreloaderAssignments> assignsMap,
        AffinityTopologyVersion topVer,
        boolean force,
        long rebalanceId
    ) {
        U.dumpStack(cctx.localNodeId() + ">>> add assignments");

        NavigableMap<Integer, Map<ClusterNode, Map<Integer, Set<Integer>>>> nodeOrderAssignsMap =
            sliceNodeCacheAssignments(assignsMap);

        if (nodeOrderAssignsMap.isEmpty())
            return NO_OP;

        // Start new rebalance session.
        FileRebalanceFuture mainFut0 = mainFut;

        lock.writeLock().lock();

        try {
            if (!mainFut0.isDone())
                mainFut0.cancel();

            mainFut0 = mainFut = new FileRebalanceFuture(cpLsnr, assignsMap, topVer);

            FileRebalanceSingleNodeFuture rqFut = null;
            Runnable rq = NO_OP;

            if (log.isInfoEnabled())
                log.info("Prepare the chain to demand assignments: " + nodeOrderAssignsMap);

            // Clear the previous rebalance futures if exists.
//            futMap.clear();

            for (Map.Entry<Integer, Map<ClusterNode, Map<Integer, Set<Integer>>>> entry : nodeOrderAssignsMap.descendingMap().entrySet()) {
                Map<ClusterNode, Map<Integer, Set<Integer>>> descNodeMap = entry.getValue();

                int order = entry.getKey();

                for (Map.Entry<ClusterNode, Map<Integer, Set<Integer>>> assignEntry : descNodeMap.entrySet()) {
                    FileRebalanceSingleNodeFuture rebFut = new FileRebalanceSingleNodeFuture(cctx, mainFut, log, assignEntry.getKey(),
                        order, rebalanceId, assignEntry.getValue(), topVer);

                    mainFut0.add(order, rebFut);

                    final Runnable nextRq0 = rq;
                    final FileRebalanceSingleNodeFuture rqFut0 = rqFut;

//                }
//                    else {

                    if (rqFut0 != null) {
                        // headFut = rebFut; // The first seen rebalance node.
                        rebFut.listen(f -> {
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

                    rq = requestNodePartitions(assignEntry.getKey(), rebFut);
                    rqFut = rebFut;
                }
            }

            // todo should be invoked in separated thread
            mainFut.enableReadOnlyMode();

            mainFut0.listen(new IgniteInClosureX<IgniteInternalFuture<Boolean>>() {
                @Override public void applyx(IgniteInternalFuture<Boolean> fut0) throws IgniteCheckedException {
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
     * @param node Clustre node to send inital demand message to.
     * @param rebFut The future to handle demand request.
     */
    private Runnable requestNodePartitions(
        ClusterNode node,
        FileRebalanceSingleNodeFuture rebFut
    ) {
        return new Runnable() {
            @Override public void run() {
                if (staleFuture(rebFut))
                    return;

                if (log.isInfoEnabled())
                    log.info("Start partitions preloading [from=" + node.id() + ", fut=" + rebFut + ']');

                final Map<Integer, Set<Integer>> assigns = rebFut.assigns;

                try {
                    if (rebFut.initReq.compareAndSet(false, true)) {
                        if (log.isDebugEnabled())
                            log.debug("Prepare demand batch message [rebalanceId=" + rebFut.rebalanceId + "]");

//                        GridPartitionBatchDemandMessage msg0 =
//                            new GridPartitionBatchDemandMessage(rebFut.rebalanceId,
//                                rebFut.topVer,
//                                assigns.entrySet()
//                                    .stream()
//                                    .collect(Collectors.toMap(Map.Entry::getKey,
//                                        e -> GridIntList.valueOf(e.getValue()))));

                        cctx.snapshotMgr().createRemoteSnapshot(node.id(), assigns);

                        rebFut.listen(c -> {
                            // todo remove snapshot listener
                            ///cctx.snapshotMgr().
                        });

//                        futMap.put(node.id(), rebFut);

//                        cctx.gridIO().sendToCustomTopic(node, rebalanceThreadTopic(), msg0, SYSTEM_POOL);

                        if (log.isDebugEnabled())
                            log.debug("Demand message is sent to partition supplier [node=" + node.id() + "]");
                    }
                }
                catch (IgniteCheckedException e) {
                    U.error(log, "Error sending request for demanded cache partitions", e);

//                    rebFut.onDone(e);

                    mainFut.onDone(e);
                }
            }
        };
    }

    /**
     * @param assignsMap The map of cache groups assignments to process.
     * @return The map of cache assignments <tt>[group_order, [node, [group_id, partitions]]]</tt>
     */
    private NavigableMap<Integer, Map<ClusterNode, Map<Integer, Set<Integer>>>> sliceNodeCacheAssignments(
        Map<Integer, GridDhtPreloaderAssignments> assignsMap
    ) {
        NavigableMap<Integer, Map<ClusterNode, Map<Integer, Set<Integer>>>> result = new TreeMap<>();

        for (Map.Entry<Integer, GridDhtPreloaderAssignments> grpEntry : assignsMap.entrySet()) {
            int grpId = grpEntry.getKey();

            CacheGroupContext grp = cctx.cache().cacheGroup(grpId);

            GridDhtPreloaderAssignments assigns = grpEntry.getValue();

            if (fileRebalanceRequired(grp, assigns.keySet())) {
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
    private boolean staleFuture(GridFutureAdapter<?> fut) {
        return fut == null || fut.isCancelled() || fut.isFailed() || fut.isDone();
    }

    /**
     * @param grp The corresponding to assignments cache group context.
     * @param nodes Assignment nodes for specified cache group.
     * @return {@code True} if cache must be rebalanced by sending files.
     */
    public boolean fileRebalanceRequired(CacheGroupContext grp, Collection<ClusterNode> nodes) {
        return FileRebalanceSupported(grp, nodes) &&
            grp.config().getRebalanceDelay() != -1 &&
            grp.config().getRebalanceMode() != CacheRebalanceMode.NONE;
    }

    /**
     * @param grp The corresponding to assignments cache group context.
     * @param nodes Assignment nodes for specified cache group.
     * @return {@code True} if cache might be rebalanced by sending cache partition files.
     */
    public boolean FileRebalanceSupported(CacheGroupContext grp, Collection<ClusterNode> nodes) {
        if (nodes == null || nodes.isEmpty())
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

        return presistenceRebalanceEnabled &&
            grp.persistenceEnabled() &&
            IgniteFeatures.allNodesSupports(nodes, IgniteFeatures.CACHE_PARTITION_FILE_REBALANCE);
    }

    /**
     * @param fut Exchange future.
     */
    public void onExchangeDone(GridDhtPartitionsExchangeFuture fut) {
        // todo switch to read-only mode after first exchange
        //System.out.println(cctx.localNodeId() + " >xxx> process onExchangeDone");

//        if (!mainFut.isDone() && fut.topologyVersion().equals(mainFut.topVer)) {
//            mainFut.switchAllPartitions();
//        }
//        else {
//            U.dumpStack(cctx.localNodeId() + " skip onExchange done=" + mainFut.isDone() + ", topVer="+fut.topologyVersion() +", rebVer="+mainFut.topVer +", equals="+fut.topologyVersion().equals(mainFut.topVer));
//        }

        // switch partitions without exchange
    }

    /**
     * Get partition restore future.
     *
     * @param msg Message.
     * @return Partition restore future or {@code null} if no partition currently restored.
     */
    public IgniteInternalFuture partitionRestoreFuture(UUID nodeId, GridCacheMessage msg) {
        if (!(msg instanceof GridCacheGroupIdMessage) && !(msg instanceof GridCacheIdMessage))
            return null;

        return mainFut.lockMessagesFuture(null, -1, -1);
    }

    /**
     * Completely destroy the partition without changing its state.
     *
     * @param part Partition to destroy.
     * @return Future that will be completed after removing the partition file.
     */
    private IgniteInternalFuture<Boolean> destroyPartitionAsync(GridDhtLocalPartition part) {
        GridFutureAdapter<Boolean> fut = new GridFutureAdapter<>();

        part.clearAsync();

        part.onClearFinished(c -> {
            //todo should prevent any removes on DESTROYED partition.
            ReadOnlyGridCacheDataStore store = (ReadOnlyGridCacheDataStore)part.dataStore().store(true);

            store.disableRemoves();

            try {
                part.group().offheap().destroyCacheDataStore(part.dataStore()).listen(f -> {
                        try {
                            fut.onDone(f.get());
                        }
                        catch (IgniteCheckedException e) {
                            fut.onDone(e);
                        }
                    }
                );
            }
            catch (IgniteCheckedException e) {
                fut.onDone(e);
            }
        });

        return fut;
    }

    /**
     * Restore partition on new file. Partition should be completely destroyed before restore it with new file.
     *
     * @param grpId Group id.
     * @param partId Partition number.
     * @param fsPartFile New partition file on the same filesystem.
     * @return Future that will be completed when partition will be fully re-initialized. The future result is the HWM
     * value of update counter in read-only partition.
     * @throws IgniteCheckedException If file store for specified partition doesn't exists or partition file cannot be
     * moved.
     */
    private IgniteInternalFuture<T2<Long, Long>> restorePartition(
        int grpId,
        int partId,
        File fsPartFile,
        IgniteInternalFuture destroyFut
    ) throws IgniteCheckedException {
        CacheGroupContext ctx = cctx.cache().cacheGroup(grpId);

        if (!destroyFut.isDone()) {
            if (log.isDebugEnabled())
                log.debug("Await partition destroy [grp=" + grpId + ", partId=" + partId + "]");

            destroyFut.get();
        }

        File dst = new File(getStorePath(grpId, partId));

        if (log.isInfoEnabled())
            log.info("Moving downloaded partition file: " + fsPartFile + " --> " + dst);

        try {
            Files.move(fsPartFile.toPath(), dst.toPath());
        }
        catch (IOException e) {
            // todo FileAlreadyExistsException -> retry ?
            throw new IgniteCheckedException("Unable to move file from " + fsPartFile + " to " + dst, e);
        }

        // Reinitialize file store afte rmoving partition file.
        cctx.pageStore().ensure(grpId, partId);

        return cpLsnr.schedule(() -> {
            // Save current update counter.
            PartitionUpdateCounter maxCntr = ctx.topology().localPartition(partId).dataStore().partUpdateCounter();

            // Replacing partition and cache data store with the new one.
            // After this operation all on-heap cached entries should be cleaned.
            // At this point all partition updates are queued.
            // File page store should be reinitialized.
            assert cctx.pageStore().exists(grpId, partId) : "File doesn't exist [grpId=" + grpId + ", p=" + partId + "]";

            GridDhtLocalPartition part = ctx.topology().forceCreatePartition(partId, true);

            // Switching to new datastore.
            part.readOnly(false);

            maxCntr.finalizeUpdateCounters();

            return new T2<>(part.updateCounter(), maxCntr.get());
        });
    }

    /**
     * Get partition file path.
     *
     * @param grpId Group ID.
     * @param partId Partition ID.
     * @return Absolute partition file path
     * @throws IgniteCheckedException If cache or partition with the given ID was not created.
     */
    private String getStorePath(int grpId, int partId) throws IgniteCheckedException {
        return ((FilePageStore)((FilePageStoreManager)cctx.pageStore()).getStore(grpId, partId)).getFileAbsolutePath();
    }

    /**
     * @param fut Future.
     * @return {@code True} if rebalance topology version changed by exchange thread or force
     * reassing exchange occurs, see {@link RebalanceReassignExchangeTask} for details.
     */
    private boolean topologyChanged(FileRebalanceSingleNodeFuture fut) {
        return !cctx.exchange().rebalanceTopologyVersion().equals(fut.topVer);
        // todo || fut != rebalanceFut; // Same topology, but dummy exchange forced because of missing partitions.
    }

    public void reserveHistoryForFilePreloading(GridDhtPartitionExchangeId exchId,
        GridDhtPartitionsExchangeFuture exchangeFut) {

    }

    /** */
    private static class CheckpointListener implements DbCheckpointListener {
        /** Queue. */
        private final ConcurrentLinkedQueue<Runnable> queue = new ConcurrentLinkedQueue<>();

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
            queue.clear();
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

    private class RebalanceSnapshotListener implements SnapshotListener {
        @Override public void onPartition(UUID nodeId, String snpName, File file, int grpId, int partId) {
            FileRebalanceSingleNodeFuture fut = mainFut.nodeRoutine(grpId, nodeId);

            // todo should track rebalanceId by snpName
            if (staleFuture(fut)) { //  || mainFut.isCancelled()
                if (log.isInfoEnabled())
                    log.info("Removing staled file [nodeId=" + nodeId + ", file=" + file + "]");

                file.delete();

                return;
            }

            IgniteInternalFuture evictFut = fut.evictionFuture(grpId);

            try {
                // todo should lock only on checkpoint
                mainFut.lockMessaging(nodeId, grpId, partId);

                IgniteInternalFuture<T2<Long, Long>> switchFut = restorePartition(grpId, partId, file, evictFut);

                switchFut.listen(f -> {
                    try {
                        T2<Long, Long> cntrs = f.get();

                        assert cntrs != null;

                        cctx.kernalContext().closure().runLocalSafe(() -> {
                            fut.onPartitionRestored(grpId, partId, cntrs.get1(), cntrs.get2());
                        });
                    }
                    catch (IgniteCheckedException e) {
                        fut.onDone(e);
                    }
                });
            }
            catch (IgniteCheckedException e) {
                fut.onDone(e);
            }
        }

        @Override public void onEnd(UUID rmtNodeId, String snpName) {

        }

        @Override public void onException(UUID rmtNodeId, String snpName, Throwable t) {
            log.error("Unable to create remote snapshot " + snpName, t);

            mainFut.onDone(t);
        }
    }

    /** */
    private class FileRebalanceFuture extends GridFutureAdapter<Boolean> {
        /** */
        private final Map<T2<Integer, UUID>, FileRebalanceSingleNodeFuture> futMap = new HashMap<>();

        /** */
        private final CheckpointListener cpLsnr;

        /** */
        private final Map<Integer, Set<Integer>> allPartsMap = new HashMap<>();

        /** */
        private final Map<Integer, Set<UUID>> allGroupsMap = new ConcurrentHashMap<>();

        /** */
        private final AffinityTopologyVersion topVer;

        /** */
        private final AtomicReference<GridFutureAdapter> switchFutRef = new AtomicReference<>();

        /** */
        private final Map<String, PageMemCleanupTask> cleanupRegions = new HashMap<>();

        public FileRebalanceFuture() {
            this(null, null, null);

            onDone(true);
        }

        /**
         * @param lsnr Checkpoint listener.
         */
        public FileRebalanceFuture(CheckpointListener lsnr, Map<Integer, GridDhtPreloaderAssignments> assignsMap, AffinityTopologyVersion startVer) {
            cpLsnr = lsnr;
            topVer = startVer;

            initialize(assignsMap);
        }

        /**
         * Initialize rebalancing mappings.
         *
         * @param assignments Assignments.
         */
        private void initialize(Map<Integer, GridDhtPreloaderAssignments> assignments) {
            if (assignments == null || assignments.isEmpty())
                return;

            Map<String, Set<Long>> regionToParts = new HashMap<>();

            for (Map.Entry<Integer, GridDhtPreloaderAssignments> entry : assignments.entrySet()) {
                int grpId = entry.getKey();
                GridDhtPreloaderAssignments assigns = entry.getValue();

                Set<UUID> nodes = allGroupsMap.computeIfAbsent(grpId, v -> new GridConcurrentHashSet<>());

                CacheGroupContext grp = cctx.cache().cacheGroup(grpId);

                if (!fileRebalanceRequired(grp, assigns.keySet()))
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
                cleanupRegions.put(e.getKey(), new PageMemCleanupTask(e.getKey(), e.getValue()));
        }

        public synchronized void add(int order, FileRebalanceSingleNodeFuture fut) {
            T2<Integer, UUID> k = new T2<>(order, fut.node.id());

            futMap.put(k, fut);
        }

        // todo add/get should be consistent (ORDER or GROUP_ID arg)
        public synchronized FileRebalanceSingleNodeFuture nodeRoutine(int grpId, UUID nodeId) {
            int order = cctx.cache().cacheGroup(grpId).config().getRebalanceOrder();

            T2<Integer, UUID> k = new T2<>(order, nodeId);

            return futMap.get(k);
        }

        /** {@inheritDoc} */
        @Override public synchronized boolean cancel() {
            cpLsnr.cancelAll();

            for (FileRebalanceSingleNodeFuture fut : futMap.values())
                fut.cancel();

            futMap.clear();

            return onDone(false, null, true);
        }

        public IgniteInternalFuture lockMessagesFuture(UUID nodeId, int grpId, int partId) {
            // todo we don't care from where request is coming - we should
            //      lock partition for all updates! nodeId is redundant
            // FileRebalanceSingleNodeFuture currFut = futMap.get(nodeId);

            // todo how to get partition and group
            // return staleFuture(currFut) ? null : currFut.switchFut(-1, -1);

            return switchFutRef.get();
        }

        public void lockMessaging(UUID nodeId, Integer grpId, Integer partId) {
            switchFutRef.compareAndSet(null, new GridFutureAdapter());
        }

        public boolean unlockMessaging() {
            GridFutureAdapter fut = switchFutRef.get();

            if (fut != null && switchFutRef.compareAndSet(fut, null)) {
                fut.onDone();

                return true;
            }

            return false;
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
                    cctx.walState().onGroupRebalanceFinished(gctx.groupId(), mainFut.topVer);
            }
        }

        public synchronized void onNodeDone(FileRebalanceSingleNodeFuture fut, Boolean res, Throwable err, boolean cancel) {
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
         * Switch all rebalanced partitions to read-only mode.
         */
        private void enableReadOnlyMode() {
            IgniteInternalFuture<Void> switchFut = cpLsnr.schedule(() -> {
                for (Map.Entry<Integer, Set<Integer>> e : allPartsMap.entrySet()) {
                    CacheGroupContext grp = cctx.cache().cacheGroup(e.getKey());

                    for (Integer partId : e.getValue()) {
                        GridDhtLocalPartition part = grp.topology().localPartition(partId);

                        if (part.readOnly())
                            continue;

                        part.readOnly(true);
                    }
                }
            });

            if (log.isDebugEnabled())
                log.debug("Await partition switch: " + allPartsMap);

            try {
                if (!switchFut.isDone())
                    cctx.database().wakeupForCheckpoint(String.format(REBALANCE_CP_REASON, allPartsMap.keySet()));

                switchFut.get();
            }
            catch (IgniteCheckedException e) {
                onDone(e);

                // todo throw exception?
                return;
            }

            for (Map.Entry<Integer, Set<Integer>> e : allPartsMap.entrySet()) {
                int grpId = e.getKey();

                CacheGroupContext gctx = cctx.cache().cacheGroup(grpId);

                for (Integer partId : e.getValue()) {
                    GridDhtLocalPartition part = gctx.topology().localPartition(partId);

                    if (log.isDebugEnabled())
                        log.debug("Add destroy future for partition " + part.id());

                    destroyPartitionAsync(part).listen(fut -> {
                        try {
                            if (!fut.get())
                                throw new IgniteCheckedException("Partition was not destroyed " +
                                    "properly [grp=" + gctx.cacheOrGroupName() + ", p=" + part.id() + "]");

                            boolean exists = gctx.shared().pageStore().exists(grpId, part.id());

                            assert !exists : "File exists [grp=" + gctx.cacheOrGroupName() + ", p=" + part.id() + "]";

                            onPartitionEvicted(grpId, partId);
                        }
                        catch (IgniteCheckedException ex) {
                            onDone(ex);
                        }
                    });
                }
            }
        }

        private void onPartitionEvicted(int grpId, int partId) throws IgniteCheckedException {
            CacheGroupContext gctx = cctx.cache().cacheGroup(grpId);

            String regName = gctx.dataRegion().config().getName();

            PageMemCleanupTask pageMemFut = cleanupRegions.get(regName);

            pageMemFut.cleanupMemory();
        }

        public IgniteInternalFuture evictionFuture(int grpId) {
            String regName = cctx.cache().cacheGroup(grpId).dataRegion().config().getName();

            return cleanupRegions.get(regName);
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

            public void cleanupMemory() throws IgniteCheckedException {
                int evictedCnt = evictedCntr.incrementAndGet();

                assert evictedCnt <= parts.size();

                if (evictedCnt == parts.size()) {
                    ((PageMemoryEx)cctx.database().dataRegion(name).pageMemory())
                        .clearAsync(
                            (grp, pageId) ->
                                parts.contains(((long)grp << 32) + PageIdUtils.partId(pageId)), true)
                        .listen(c1 -> {
                            if (log.isDebugEnabled())
                                log.debug("Eviction is done [region=" + name + "]");

                            onDone();
                        });
                }
            }
        }
    }

    /** */
    private static class FileRebalanceSingleNodeFuture extends GridFutureAdapter<Boolean> {
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

        /** */
        private final int rebalanceOrder;

        /**
         * Default constructor for the dummy future.
         */
        public FileRebalanceSingleNodeFuture() {
            this(null, null, null, null, 0, 0, Collections.emptyMap(), null);

            onDone();
        }

        /**
         * @param node Supplier node.
         * @param rebalanceId Rebalance id.
         * @param assigns Map of assignments to request from remote.
         * @param topVer Topology version.
         */
        public FileRebalanceSingleNodeFuture(
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

            boolean rmvd = parts.remove(partId);

            assert rmvd : "Partition not found: " + partId;

            remainingHist.computeIfAbsent(grpId, v -> new ConcurrentSkipListSet<>())
                .add(new HistoryDesc(partId, min, max));

            if (log.isDebugEnabled()) {
                log.debug("Partition done [grp=" + cctx.cache().cacheGroup(grpId).cacheOrGroupName() +
                    ", p=" + partId + ", remaining=" + parts.size() + "]");
            }

            if (parts.isEmpty()) {
                mainFut.unlockMessaging();

                onGroupRestored(grpId);
            }
        }

        private void onGroupRestored(int grpId) {
            if (remaining.remove(grpId) == null)
                return;

            Set<HistoryDesc> parts0 = remainingHist.remove(grpId);

            CacheGroupContext grp = cctx.cache().cacheGroup(grpId);

            GridDhtPartitionDemandMessage msg = new GridDhtPartitionDemandMessage(rebalanceId, topVer, grpId);

            for (HistoryDesc desc : parts0) {
                assert desc.toCntr >= desc.fromCntr : "from=" + desc.fromCntr + ", to=" + desc.toCntr;

                if (desc.fromCntr != desc.toCntr) {
                    msg.partitions().addHistorical(desc.partId, desc.fromCntr, desc.toCntr, parts0.size());

                    continue;
                }

                if (log.isDebugEnabled()) {
                    log.debug("Prepare to request historical rebalancing [p=" +
                        desc.partId + ", from=" + desc.fromCntr + ", to=" + desc.toCntr + "]");
                }

                // No historical rebalancing required  -can own partition.
                if (grp.localWalEnabled()) {
                    boolean owned = grp.topology().own(grp.topology().localPartition(desc.partId));

                    assert owned : "part=" + desc.partId + ", grp=" + grp.cacheOrGroupName();
                }
            }

            if (!msg.partitions().hasHistorical()) {
                mainFut.onNodeGroupDone(grpId, nodeId(), false);

                if (remaining.isEmpty())
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
        public boolean onDone(@Nullable Boolean res, @Nullable Throwable err, boolean cancel) {
            boolean r = super.onDone(res, err, cancel);

            mainFut.onNodeDone(this, res, err, cancel);

            return r;
        }

        public IgniteInternalFuture evictionFuture(int grpId) {
            IgniteInternalFuture fut = mainFut.evictionFuture(grpId);

            assert fut != null;

            return fut;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(FileRebalanceSingleNodeFuture.class, this);
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

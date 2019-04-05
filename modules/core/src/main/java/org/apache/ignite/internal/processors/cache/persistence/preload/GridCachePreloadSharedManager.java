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

package org.apache.ignite.internal.processors.cache.persistence.preload;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteFeatures;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.managers.communication.GridIoChannelListener;
import org.apache.ignite.internal.processors.affinity.AffinityAssignment;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheDataStoreEx;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedManagerAdapter;
import org.apache.ignite.internal.processors.cache.IgniteCacheOffheapManager;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionDemandMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPreloaderAssignments;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.util.GridIntList;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.lang.IgniteInClosureX;
import org.apache.ignite.internal.util.nio.channel.IgniteSocketChannel;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;

import static org.apache.ignite.internal.GridTopic.TOPIC_REBALANCE;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.SYSTEM_POOL;
import static org.apache.ignite.internal.processors.cache.GridCacheUtils.UTILITY_CACHE_NAME;
import static org.apache.ignite.internal.processors.cache.persistence.preload.PartitionUploadManager.persistenceRebalanceApplicable;

/**
 *
 */
public class GridCachePreloadSharedManager extends GridCacheSharedManagerAdapter {
    /** */
    private static final String REBALANCE_CP_REASON = "Rebalance has been scheduled [grps=%s]";

    /** */
    private static final Runnable NO_OP = () -> {};

    /** */
    public static final int REBALANCE_TOPIC_IDX = 0;

    /** */
    private final boolean presistenceRebalanceEnabled;

    /** */
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    /** */
    private final ConcurrentMap<UUID, RebalanceDownloadFuture> futMap = new ConcurrentHashMap<>();

    /** */
    private volatile RebalanceDownloadFuture headFut = new RebalanceDownloadFuture();

    /** */
    private GridCacheDatabaseSharedManager dbMgr;

    /** */
    private PartitionDownloadManager downloadMgr;

    /** */
    private PartitionUploadManager uploadMgr;

    /** */
    private PartitionSwitchModeManager switchMgr;

    /** */
    private PartitionStorePumpManager pumpMgr;

    /**
     * @param ktx Kernal context.
     */
    public GridCachePreloadSharedManager(GridKernalContext ktx) {
        assert CU.isPersistenceEnabled(ktx.config()) :
            "Persistence must be enabled to preload any of cache partition files";

        downloadMgr = new PartitionDownloadManager(ktx);
        uploadMgr = new PartitionUploadManager(ktx);
        pumpMgr = new PartitionStorePumpManager(ktx);

        presistenceRebalanceEnabled = IgniteSystemProperties.getBoolean(
            IgniteSystemProperties.IGNITE_PERSISTENCE_REBALANCE_ENABLED, false);
    }

    /**
     * @return The Rebalance topic to communicate with.
     */
    static Object rebalanceThreadTopic() {
        return TOPIC_REBALANCE.topic("Rebalance", REBALANCE_TOPIC_IDX);
    }

    /**
     * @param fut The future to check.
     * @return <tt>true</tt> if future can be processed.
     */
    static boolean staleFuture(GridFutureAdapter<?> fut) {
        return fut == null || fut.isCancelled() || fut.isFailed() || fut.isDone();
    }

    /** {@inheritDoc} */
    @Override protected void start0() throws IgniteCheckedException {
        dbMgr = ((GridCacheDatabaseSharedManager) cctx.database());

        downloadMgr.start0(cctx);
        uploadMgr.start0(cctx);
        pumpMgr.start0(cctx);

        ((GridCacheDatabaseSharedManager) cctx.database()).addCheckpointListener(
            switchMgr = new PartitionSwitchModeManager(cctx));

        if (persistenceRebalanceApplicable(cctx)) {
            // Register channel listeners for the rebalance thread.
            cctx.gridIO().addChannelListener(rebalanceThreadTopic(), new GridIoChannelListener() {
                @Override public void onChannelCreated(UUID nodeId, IgniteSocketChannel channel) {
                    final RebalanceDownloadFuture fut0 = futMap.get(nodeId);

                    if (staleFuture(fut0))
                        return;

                    lock.readLock().lock();

                    try {
                        onChannelCreated0(nodeId, channel, fut0);
                    }
                    finally {
                        lock.readLock().unlock();
                    }
                }
            });
        }
    }

    /** {@inheritDoc} */
    @Override protected void stop0(boolean cancel) {
        lock.writeLock().lock();

        try {
            downloadMgr.stop0(cancel);
            uploadMgr.stop0(cancel);
            pumpMgr.stop0(cancel);

            ((GridCacheDatabaseSharedManager)cctx.database()).removeCheckpointListener(switchMgr);

            for (RebalanceDownloadFuture rebFut : futMap.values())
                rebFut.cancel();

            futMap.clear();

            cctx.gridIO().removeChannelListener(rebalanceThreadTopic(), null);
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * @param nodeId The remote node id.
     * @param channel A blocking socket channel to handle rebalance partitions.
     * @param rebFut The future of assignments handling.
     */
    private void onChannelCreated0(
        UUID nodeId,
        IgniteSocketChannel channel,
        RebalanceDownloadFuture rebFut
    ) {
        assert rebFut.nodeId.equals(nodeId);
        assert lock.getReadHoldCount() > 0;

        if (staleFuture(rebFut))
            return;

        U.log(log, "The channel created. Start download of partition files [channel=" + channel +
            ", remote=" + nodeId + ']');

        try {
            downloadMgr.onChannelCreated0(nodeId, channel, rebFut.nodeAssigns, rebFut.topVer, rebFut);
        }
        catch (Throwable t) {
            // TODO hande by failure handler
            log.error("The error occurred on the demander node during processing channel creation", t);

            throw t;
        }
    }

    /**
     * @param nodeId The source node id.
     * @param grp The cache group context.
     * @param part Completely downloaded partition file.
     */
    void onPartitionDownloaded(UUID nodeId, CacheGroupContext grp, GridDhtLocalPartition part) {
        assert lock.getReadHoldCount() > 0;

        final RebalanceDownloadFuture fut0 = futMap.get(nodeId);

        if (staleFuture(fut0))
            return;

        assert part.dataStoreMode() == CacheDataStoreEx.StorageMode.LOG_ONLY;

        IgniteCacheOffheapManager.CacheDataStore store = part.dataStore(CacheDataStoreEx.StorageMode.FULL);

        // Re-init data store at first access to it.
        store.updateCounter();

        GridFutureAdapter<?> catchFut = pumpMgr.registerPumpSource(part.dataStoreCatchLog());

        // TODO Rebuild indexes by partition
        IgniteInternalFuture<?> rebuildFut = dbMgr.rebuildIndexesOnDemand(cctx.cacheContext(grp.groupId()),
            p -> p.id() == part.id(),
            true);

        if (rebuildFut == null)
            rebuildFut = new GridFinishedFuture<>();

        rebuildFut.listen(rf0 -> U.log(log, "Rebuild indexex finished [grpId=" + grp.groupId() + ", partId=" + part.id() + ']'));

        GridCompoundFuture rebuilCatchfut = new GridCompoundFuture();

        rebuilCatchfut.add(rebuildFut);
        rebuilCatchfut.add(catchFut);

        rebuilCatchfut.markInitialized();

        rebuilCatchfut.listen(f -> {
            // TODO switch mode when the next checkpoint finished.
            U.log(log, "The partition will be swithed to the FULL mode: " + part);

            Map<Integer, Set<Integer>> parts = new HashMap<>();

            parts.put(grp.groupId(), new HashSet<>(Collections.singletonList(part.id())));

            switchPartitionsMode(CacheDataStoreEx.StorageMode.FULL, parts).listen(
                new IgniteInClosureX<IgniteInternalFuture<Boolean>>() {
                    @Override public void applyx(IgniteInternalFuture<Boolean> f0) throws IgniteCheckedException {
                        U.log(log, "The partition file will be owned by node: " + part);

                        // TODO Register owning partition listener here to own it on checkpoint done
                        // There is no need to check grp.localWalEnabled() as for the partition
                        // file transfer process it has no meaning. We always apply this partiton
                        // without any records to the WAL.
                        boolean isOwned = grp.topology().own(part);

                        assert isOwned : "Partition must be owned: " + part;

                        // TODO Send EVT_CACHE_REBALANCE_PART_LOADED
                    }
                });
        });
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

            if (cctx.preloadMgr().partitionRebalanceRequired(grp, grpEntry.getValue())) {
                int grpOrderNo = grp.config().getRebalanceOrder();

                result.putIfAbsent(grpOrderNo, new HashMap<>());

                for (Map.Entry<ClusterNode, GridDhtPartitionDemandMessage> grpAssigns : grpEntry.getValue().entrySet()) {
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
        NavigableMap<Integer, Map<ClusterNode, Map<Integer, Set<Integer>>>> nodeOrderAssignsMap =
            sliceNodeCacheAssignments(assignsMap);

        if (nodeOrderAssignsMap.isEmpty())
            return NO_OP;

        // Start new rebalance session.
        final RebalanceDownloadFuture headFut0 = headFut;

        if (!headFut0.isDone())
            headFut0.cancel();

        // TODO Start eviction.
        // Assume that the partition tag will be changed on eviction process finished,
        // so we will have no additional writes (via writeInternal method) to current
        // MOVING partition if checkpoint thread occures. So the current partition file
        // can be easily replaced with the new one received from the socket.

        lock.writeLock().lock();

        try {
            RebalanceDownloadFuture rqFut = null;
            Runnable rq = NO_OP;

            U.log(log, "Prepare the chain to demand assignments: " + nodeOrderAssignsMap);

            // Clear the previous rebalance futures if exists.
            futMap.clear();

            for (Map<ClusterNode, Map<Integer, Set<Integer>>> descNodeMap : nodeOrderAssignsMap.descendingMap().values()) {
                for (Map.Entry<ClusterNode, Map<Integer, Set<Integer>>> assignEntry : descNodeMap.entrySet()) {
                    RebalanceDownloadFuture rebFut = new RebalanceDownloadFuture(assignEntry.getKey().id(), rebalanceId,
                        assignEntry.getValue(), topVer);

                    final Runnable nextRq0 = rq;
                    final RebalanceDownloadFuture rqFut0 = rqFut;

                    if (rqFut0 == null)
                        headFut = rebFut; // The first seen rebalance node.
                    else {
                        rebFut.listen(f -> {
                            try {
                                if (f.get()) // Not cancelled.
                                    nextRq0.run();
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

            headFut.listen(new IgniteInClosureX<IgniteInternalFuture<Boolean>>() {
                @Override public void applyx(IgniteInternalFuture<Boolean> fut0) throws IgniteCheckedException {
                    if (fut0.get())
                        U.log(log, "The final persistence rebalance future is done [result=" + fut0.isDone() + ']');
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
        RebalanceDownloadFuture rebFut
    ) {
        return new Runnable() {
            @Override public void run() {
                if (staleFuture(rebFut))
                    return;

                U.log(log, "Start partitions preloading [from=" + node.id() + ", fut=" + rebFut + ']');

                final Map<Integer, Set<Integer>> assigns = rebFut.nodeAssigns;

                IgniteInternalFuture<Boolean> switchFut = cctx.preloadMgr()
                    .switchPartitionsMode(CacheDataStoreEx.StorageMode.LOG_ONLY, assigns);

                switchFut.listen(new IgniteInClosure<IgniteInternalFuture>() {
                    @Override public void apply(IgniteInternalFuture fut) {
                        try {
                            if (rebFut.initReq.compareAndSet(false, true)) {
                                GridPartitionBatchDemandMessage msg0 =
                                    new GridPartitionBatchDemandMessage(rebFut.rebalanceId,
                                        rebFut.topVer,
                                        assigns.entrySet()
                                            .stream()
                                            .collect(Collectors.toMap(Map.Entry::getKey,
                                                e -> GridIntList.getAsIntList(e.getValue()))));

                                futMap.put(node.id(), rebFut);

                                cctx.gridIO().sendToCustomTopic(node, rebalanceThreadTopic(), msg0, SYSTEM_POOL);
                            }
                        }
                        catch (IgniteCheckedException e) {
                            U.error(log, "Error sending request for demanded cache partitions", e);

                            rebFut.onDone(e);

                            futMap.remove(node.id());
                        }
                    }
                });

                // This is an optional step. The request will be completed on the next checkpoint occurs.
                if (!switchFut.isDone())
                    dbMgr.wakeupForCheckpoint(String.format(REBALANCE_CP_REASON, assigns.keySet()));
            }
        };
    }

    /**
     * @return {@code True} if rebalance via sending partitions files enabled. Default <tt>false</tt>.
     */
    public boolean isPresistenceRebalanceEnabled() {
        return presistenceRebalanceEnabled;
    }

    /**
     * @param grp The corresponding to assignments cache group context.
     * @param assigns A generated cache assignments in a cut of cache group [grpId, [nodeId, parts]].
     * @return {@code True} if cache might be rebalanced by sending cache partition files.
     */
    public boolean rebalanceByPartitionSupported(CacheGroupContext grp, GridDhtPreloaderAssignments assigns) {
        if (assigns == null || assigns.isEmpty())
            return false;

        return rebalanceByPartitionSupported(grp, assigns.keySet());
    }

    /**
     * @param grp The corresponding to assignments cache group context.
     * @param topVer Topology versions to calculate assignmets at.
     * @return {@code True} if cache might be rebalanced by sending cache partition files.
     */
    public boolean rebalanceByPartitionSupported(CacheGroupContext grp, AffinityTopologyVersion topVer) {
        AffinityAssignment aff = grp.affinity().cachedAffinity(topVer);

        // All of affinity nodes must support to new persistence rebalance feature.
        List<ClusterNode> affNodes =  aff.idealAssignment().stream()
                .flatMap(List::stream)
                .collect(Collectors.toList());

        return rebalanceByPartitionSupported(grp, affNodes);
    }

    /**
     * @param grp The corresponding to assignments cache group context.
     * @param nodes The list of nodes to check ability of file transferring.
     * @return {@code True} if cache might be rebalanced by sending cache partition files.
     */
    private boolean rebalanceByPartitionSupported(CacheGroupContext grp, Collection<ClusterNode> nodes) {
        // Do not rebalance system cache with files as they are not exists.
        if (grp.groupId() == CU.cacheId(UTILITY_CACHE_NAME))
            return false;

        if (grp.mvccEnabled())
            return false;

        return presistenceRebalanceEnabled &&
            grp.persistenceEnabled() &&
            IgniteFeatures.allNodesSupports(nodes, IgniteFeatures.CACHE_PARTITION_FILE_REBALANCE);
    }

    /**
     * @param grp The corresponding to assignments cache group context.
     * @param assigns A generated cache assignments in a cut of cache group [grpId, [nodeId, parts]].
     * @return {@code True} if cache must be rebalanced by sending files.
     */
    public boolean partitionRebalanceRequired(CacheGroupContext grp, GridDhtPreloaderAssignments assigns) {
        return rebalanceByPartitionSupported(grp, assigns) &&
            grp.config().getRebalanceDelay() != -1 &&
            grp.config().getRebalanceMode() != CacheRebalanceMode.NONE;
    }

    /**
     * @return The instantiated download manager.
     */
    public PartitionDownloadManager download() {
        return downloadMgr;
    }

    /**
     * @return The instantiated upload mamanger.
     */
    public PartitionUploadManager upload() {
        return uploadMgr;
    }

    /**
     * @return The cache data storage pump manager.
     */
    public PartitionStorePumpManager pump() {
        return pumpMgr;
    }

    /**
     * @return The switch mode manager.
     */
    public PartitionSwitchModeManager switcher() {
        return switchMgr;
    }

    /**
     * @param mode The storage mode to switch to.
     * @param parts The set of partitions to change storage mode.
     * @return The future which will be completed when request is done.
     */
    public IgniteInternalFuture<Boolean> switchPartitionsMode(
        CacheDataStoreEx.StorageMode mode,
        Map<Integer, Set<Integer>> parts
    ) {
        return switchMgr.offerSwitchRequest(mode, parts);
    }

    /**
     *
     */
    private class RebalanceDownloadFuture extends GridFutureAdapter<Boolean> {
        /** */
        private UUID nodeId;

        /** */
        private long rebalanceId;

        /** */
        @GridToStringInclude
        private Map<Integer, Set<Integer>> nodeAssigns;

        /** */
        private AffinityTopologyVersion topVer;

        /** */
        private Map<Integer, Set<Integer>> remaining;

        /** {@code True} if the initial demand request has been sent. */
        private AtomicBoolean initReq = new AtomicBoolean();

        /**
         * Default constructor for the dummy future.
         */
        public RebalanceDownloadFuture() {
            onDone();
        }

        /**
         * @param nodeId The remote nodeId.
         * @param nodeAssigns Map of assignments to request from remote.
         */
        public RebalanceDownloadFuture(
            UUID nodeId,
            long rebalanceId,
            Map<Integer, Set<Integer>> nodeAssigns,
            AffinityTopologyVersion topVer
        ) {
            this.nodeId = nodeId;
            this.rebalanceId = rebalanceId;
            this.nodeAssigns = nodeAssigns;
            this.topVer = topVer;

            remaining = U.newHashMap(nodeAssigns.size());

            for (Map.Entry<Integer, Set<Integer>> grpPartEntry : nodeAssigns.entrySet())
                remaining.putIfAbsent(grpPartEntry.getKey(), new HashSet<>(grpPartEntry.getValue()));
        }

        /** {@inheritDoc} */
        @Override public boolean cancel() {
            return onCancelled();
        }

        /** */
        public synchronized void onCompleteSuccess() {
            assert remaining.isEmpty();

            U.log(log, "Partitions have been scheduled to resend. Files have been transferred " +
                "[from=" + nodeId + ", to=" + cctx.localNodeId() + ']');

            // Late affinity assignment
            cctx.exchange().scheduleResendPartitions();

            onDone(true);
        }

        /**
         * @param grpId Cache group id to search.
         * @param partId Cache partition to remove;
         * @throws IgniteCheckedException If fails.
         */
        public synchronized void markPartitionDone(int grpId, int partId) throws IgniteCheckedException {
            Set<Integer> parts = remaining.get(grpId);

            if (parts == null)
                throw new IgniteCheckedException("Partition index incorrect [grpId=" + grpId + ", partId=" + partId + ']');

            boolean success = parts.remove(partId);

            assert success : "Partition not found: " + partId;

            if (parts.isEmpty())
                remaining.remove(grpId);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(RebalanceDownloadFuture.class, this);
        }
    }
}

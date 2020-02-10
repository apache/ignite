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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
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
import org.apache.ignite.internal.processors.cache.StateChangeRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.persistence.DbCheckpointListener;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStore;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotListener;
import org.apache.ignite.internal.processors.cluster.BaselineTopologyHistoryItem;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.NotNull;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_DISABLE_WAL_DURING_REBALANCING;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_FILE_REBALANCE_ENABLED;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_PDS_FILE_REBALANCE_THRESHOLD;
import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion.NONE;
import static org.apache.ignite.internal.processors.cache.GridCacheUtils.UTILITY_CACHE_NAME;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.MOVING;

/**
 * DHT cache partition files preloader.
 */
public class IgnitePartitionPreloadManager extends GridCacheSharedManagerAdapter {
    /** */
    private final boolean fileRebalanceEnabled =
        IgniteSystemProperties.getBoolean(IGNITE_FILE_REBALANCE_ENABLED, true);

    /** */
    private final long fileRebalanceThreshold =
        IgniteSystemProperties.getLong(IGNITE_PDS_FILE_REBALANCE_THRESHOLD, 0);

    /** Lock. */
    private final Lock lock = new ReentrantLock();

    /** Checkpoint listener. */
    private final CheckpointListener checkpointLsnr = new CheckpointListener();

    /** Partition File rebalancing routine. */
    private volatile FileRebalanceRoutine fileRebalanceRoutine = new FileRebalanceRoutine();

    /**
     * @param ktx Kernal context.
     */
    public IgnitePartitionPreloadManager(GridKernalContext ktx) {
        assert CU.isPersistenceEnabled(ktx.config()) : "Persistence must be enabled to use file preloading";
    }

    /** {@inheritDoc} */
    @Override protected void start0() throws IgniteCheckedException {
        ((GridCacheDatabaseSharedManager)cctx.database()).addCheckpointListener(checkpointLsnr);

        cctx.snapshotMgr().addSnapshotListener(new PartitionSnapshotListener());
    }

    /** {@inheritDoc} */
    @Override protected void stop0(boolean cancel) {
        lock.lock();

        try {
            ((GridCacheDatabaseSharedManager)cctx.database()).removeCheckpointListener(checkpointLsnr);

            fileRebalanceRoutine.onDone(false, new NodeStoppingException("Local node is stopping."), false);
        }
        finally {
            lock.unlock();
        }
    }

    /**
     * Callback on exchange done.
     *
     * @param exchFut Exchange future.
     */
    public void beforeTopologyUpdate(
        AffinityTopologyVersion resVer,
        GridDhtPartitionsExchangeFuture exchFut,
        GridDhtPartitionsFullMessage msg
    ) {
        assert !cctx.kernalContext().clientNode() : "File preloader should never be created on the client node";
        assert exchFut != null;

        if (!fileRebalanceEnabled)
            return;

        GridDhtPartitionExchangeId exchId = exchFut.exchangeId();

        if (cctx.exchange().hasPendingExchange()) {
            if (log.isDebugEnabled())
                log.debug("Skipping rebalancing initialization exchange worker has pending exchange: " + exchId);

            return;
        }

        AffinityTopologyVersion rebTopVer = cctx.exchange().rebalanceTopologyVersion();

        FileRebalanceRoutine rebRoutine = fileRebalanceRoutine;

        boolean forced = rebTopVer == NONE || exchFut.localJoinExchange() ||
            (rebRoutine.isCancelled() || rebRoutine.isFailed() || (rebRoutine.isDone() && !rebRoutine.result()));

        Iterator<CacheGroupContext> itr = cctx.cache().cacheGroups().iterator();

        while (!forced && itr.hasNext()) {
            CacheGroupContext grp = itr.next();

            forced = exchFut.resetLostPartitionFor(grp.cacheOrGroupName()) ||
                grp.affinity().cachedVersions().contains(rebTopVer);
        }

        AffinityTopologyVersion lastAffChangeTopVer =
            cctx.exchange().lastAffinityChangedTopologyVersion(resVer);

        if (!forced && lastAffChangeTopVer.compareTo(rebTopVer) == 0) {
            assert lastAffChangeTopVer.compareTo(resVer) != 0;

            if (log.isDebugEnabled())
                log.debug("Skipping file rebalancing initialization affinity not changed: " + exchId);

            return;
        }

        // Abort the current rebalancing procedure if it is still in progress
        if (!rebRoutine.isDone())
            rebRoutine.cancel();

        assert fileRebalanceRoutine.isDone();

        boolean locJoinBaselineChange = isLocalBaselineChange(exchFut);

        // At this point, cache updates are queued, and we can safely
        // switch partitions to inactive mode and vice versa.
        for (CacheGroupContext grp : cctx.cache().cacheGroups()) {
            if (!supports(grp))
                continue;

            boolean hasIdleParttition = false;

            if (!locJoinBaselineChange) {
                if (log.isDebugEnabled())
                    log.debug("File rebalancing skipped [grp=" + grp.cacheOrGroupName() + "]");

                if (!(hasIdleParttition = hasIdleParttition(grp)))
                    continue;
            }

            boolean disable = !hasIdleParttition && fileRebalanceApplicable(grp, exchFut, resVer, msg);


            for (GridDhtLocalPartition part : grp.topology().currentLocalPartitions()) {
                if (disable) {
                    // todo only for debugging
                    try {
                        assert !cctx.pageStore().exists(grp.groupId(), part.id());
                    } catch (IgniteCheckedException ignore) {
                        // No-op.
                    }

                    part.disable();
                }
                else
                    part.enable();
            }

//            log.info("hasIdleParttition = " + hasIdleParttition);

            if (hasIdleParttition && cctx.kernalContext().query().moduleEnabled()) {
                for (GridCacheContext ctx : grp.caches()) {
                    IgniteInternalFuture<?> fut = cctx.kernalContext().query().rebuildIndexesFromHash(ctx);

                    if (fut != null) {
                        U.log(log,"Starting index rebuild [cache=" + ctx.cache().name() + "]");

                        fut.listen(f -> log.info("Finished index rebuild [cache=" + ctx.cache().name() +
                            ", success=" + (!f.isCancelled() && f.error() == null) + "]"));
                    }
                }
            }
        }
    }

    /**
     * Callback on exchange done.
     *
     * @param exchFut Exchange future.
     */
    public void onExchangeDone0(GridDhtPartitionsExchangeFuture exchFut) {
        assert !cctx.kernalContext().clientNode() : "File preloader should never be created on the client node";
        assert exchFut != null;

        if (!fileRebalanceEnabled)
            return;

        GridDhtPartitionExchangeId exchId = exchFut.exchangeId();

        if (cctx.exchange().hasPendingExchange()) {
            if (log.isDebugEnabled())
                log.debug("Skipping rebalancing initialization exchange worker has pending exchange: " + exchId);

            return;
        }

        AffinityTopologyVersion rebTopVer = cctx.exchange().rebalanceTopologyVersion();

        FileRebalanceRoutine rebRoutine = fileRebalanceRoutine;

        boolean forced = rebTopVer == NONE || exchFut.localJoinExchange() ||
            (rebRoutine.isCancelled() || rebRoutine.isFailed() || (rebRoutine.isDone() && !rebRoutine.result()));

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

        // Abort the current rebalancing procedure if it is still in progress
        if (!rebRoutine.isDone())
            rebRoutine.cancel();

        assert fileRebalanceRoutine.isDone();

        boolean locJoinBaselineChange = isLocalBaselineChange(exchFut);

        // At this point, cache updates are queued, and we can safely
        // switch partitions to inactive mode and vice versa.
        for (CacheGroupContext grp : cctx.cache().cacheGroups()) {
            if (!supports(grp))
                continue;

            boolean hasIdleParttition = false;

            if (!locJoinBaselineChange && !required(grp)) {
                if (log.isDebugEnabled())
                    log.debug("File rebalancing skipped [grp=" + grp.cacheOrGroupName() + "]");

                if (!(hasIdleParttition = hasIdleParttition(grp)))
                    continue;
            }

            boolean disable = !hasIdleParttition && fileRebalanceApplicable(grp, exchFut, exchFut.topologyVersion(), null);

            for (GridDhtLocalPartition part : grp.topology().currentLocalPartitions()) {
                if (disable)
                    part.disable();
                else
                    part.enable();
            }

            if (hasIdleParttition && cctx.kernalContext().query().moduleEnabled()) {
                for (GridCacheContext ctx : grp.caches()) {
                    IgniteInternalFuture<?> fut = cctx.kernalContext().query().rebuildIndexesFromHash(ctx);

                    if (fut != null) {
                        U.log(log,"Starting index rebuild [cache=" + ctx.cache().name() + "]");

                        fut.listen(f -> log.info("Finished index rebuild [cache=" + ctx.cache().name() +
                            ", success=" + (!f.isCancelled() && f.error() == null) + "]"));
                    }
                }
            }
        }
    }

    /**
     * This method initiates new file rebalance process from given {@code assignments} by creating new file
     * rebalance future based on them. Cancels previous file rebalance future and sends rebalance started event.
     * In case of delayed rebalance method schedules the new one with configured delay based on {@code lastExchangeFut}.
     *
     * @param topVer Current topology version.
     * @param rebalanceId Current rebalance id.
     * @param exchFut Exchange future.
     * @param assignments A map of cache assignments grouped by grpId.
     * @return Runnable to execute the chain.
     */
    public Runnable addNodeAssignments(
        AffinityTopologyVersion topVer,
        long rebalanceId,
        GridDhtPartitionsExchangeFuture exchFut,
        Map<CacheGroupContext, GridDhtPreloaderAssignments> assignments
    ) {
        Collection<T2<UUID, Map<Integer, Set<Integer>>>> orderedAssigns = reorderAssignments(assignments);

        if (orderedAssigns.isEmpty()) {
            if (log.isDebugEnabled())
                log.debug("Skipping file rebalancing due to empty assignments.");

            return null;
        }

        if (!cctx.kernalContext().grid().isRebalanceEnabled()) {
            if (log.isDebugEnabled())
                log.debug("Cancel partition file demand because rebalance disabled on current node.");

            return null;
        }

        FileRebalanceRoutine rebRoutine = fileRebalanceRoutine;

        lock.lock();

        try {
            if (!rebRoutine.isDone())
                rebRoutine.cancel();

            // Start new rebalance session.
            fileRebalanceRoutine = rebRoutine = new FileRebalanceRoutine(orderedAssigns, topVer, cctx,
                exchFut.exchangeId(), rebalanceId, checkpointLsnr::schedule);

            return rebRoutine::startPartitionsPreloading;
        }
        finally {
            lock.unlock();
        }
    }

    /**
     * Check whether file rebalancing is supported for the cache group.
     *
     * @param grp Cache group.
     * @param nodes List of Nodes.
     * @return {@code True} if file rebalancing is applicable for specified cache group and all nodes supports it.
     */
    public boolean supports(CacheGroupContext grp, @NotNull Collection<ClusterNode> nodes) {
        assert nodes != null && !nodes.isEmpty();

        if (!supports(grp))
            return false;

        if (!IgniteFeatures.allNodesSupports(nodes, IgniteFeatures.CACHE_PARTITION_FILE_REBALANCE))
            return false;

        Map<Integer, Long> globalSizes = grp.topology().globalPartSizes();

        if (globalSizes.isEmpty())
            return false;

        for (int p = 0; p < grp.affinity().partitions(); p++) {
            Long size = globalSizes.get(p);

            if (size != null && size > fileRebalanceThreshold)
                return true;
        }

        return false;
    }

    /**
     * Check whether file rebalancing is supported for the cache group.
     *
     * @param grp Cache group.
     * @return {@code True} if file rebalancing is applicable for specified cache group.
     */
    public boolean supports(CacheGroupContext grp) {
        if (!fileRebalanceEnabled || !grp.persistenceEnabled() || grp.isLocal())
            return false;

        if (!IgniteSystemProperties.getBoolean(IGNITE_DISABLE_WAL_DURING_REBALANCING, true))
            return false;

        if (grp.config().getRebalanceDelay() == -1 || grp.config().getRebalanceMode() != CacheRebalanceMode.ASYNC)
            return false;

        // Do not rebalance system cache with files as they are not exists.
        if (grp.groupId() == CU.cacheId(UTILITY_CACHE_NAME))
            return false;

        for (GridCacheContext ctx : grp.caches()) {
            if (ctx.config().getAtomicityMode() == ATOMIC)
                return false;
        }

        return !grp.mvccEnabled();
    }

    /**
     * @param grp Cache group.
     * @return {@code True} if file rebalancing required for the specified group.
     */
    public boolean required(CacheGroupContext grp) {
        if (!supports(grp))
            return false;

        boolean required = false;

        // File rebalancing should start only if all partitions are in inactive mode.
        for (GridDhtLocalPartition part : grp.topology().currentLocalPartitions()) {
            if (part.active())
                return false;

            required = true;
        }

        return required;
    }

    /**
     * @param grp Cache group.
     * @return {@code True} if cache group has at least one inactive partition.
     */
    private boolean hasIdleParttition(CacheGroupContext grp) {
        for (GridDhtLocalPartition part : grp.topology().currentLocalPartitions()) {
            if (!part.active())
                return true;
        }

        return false;
    }

    /**
     * @param exchFut Exchange future.
     * @return {@code True} if the cluster baseline was changed by local node join.
     */
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

    /**
     * @param grp Cache group.
     * @param exchFut Exchange future.
     */
    private boolean fileRebalanceApplicable(CacheGroupContext grp, GridDhtPartitionsExchangeFuture exchFut, AffinityTopologyVersion resVer, GridDhtPartitionsFullMessage msg) {
        AffinityAssignment aff = grp.affinity().readyAffinity(resVer);

        assert aff != null;

        CachePartitionFullCountersMap cntrs = msg != null ? msg.partitionUpdateCounters(grp.groupId(), grp.topology().partitions()) : grp.topology().fullUpdateCounters();

        Map<Integer, Long> globalSizes = msg != null ? msg.partitionSizes(cctx).get(grp.groupId()) : grp.topology().globalPartSizes();;

        boolean hasApplicablePart = false;

        for (int p = 0; p < grp.affinity().partitions(); p++) {
            if (!aff.get(p).contains(cctx.localNode())) {
                if (grp.topology().localPartition(p) != null) {
                    if (log.isDebugEnabled()) {
                        log.debug("Detected partition evitction, file rebalancing skipped [grp=" +
                            grp.cacheOrGroupName() + ", p=" + p + "]");
                    }

                    return false;
                }

                continue;
            }

            if (!hasApplicablePart) {
                Long partSize = globalSizes.get(p);

                if (partSize != null && partSize > fileRebalanceThreshold)
                    hasApplicablePart = true;
            }

            if (grp.topology().localPartition(p).state() != MOVING) {
                log.info("part not moving");

                return false;
            }

            // Should have partition file supplier to start file rebalancing.
            if (exchFut.partitionHistorySupplier(grp.groupId(), p, cntrs.updateCounter(p)) == null) {
                log.info("no supplier grp="+grp.cacheOrGroupName() + " p="+p + " cntr="+cntrs.updateCounter(p));

                return false;
            }
        }

        log.info("hasApplicablePart="+ hasApplicablePart);

        return hasApplicablePart;
    }

    /**
     * @param assignsMap The map of cache groups assignments to preload.
     * @return Collection of cache assignments sorted by rebalance order and grouped by node.
     */
    private List<T2<UUID, Map<Integer, Set<Integer>>>> reorderAssignments(
        Map<CacheGroupContext, GridDhtPreloaderAssignments> assignsMap
    ) {
        Map<Integer, Map<ClusterNode, Map<Integer, Set<Integer>>>> sorted = new TreeMap<>();

        for (Map.Entry<CacheGroupContext, GridDhtPreloaderAssignments> e : assignsMap.entrySet()) {
            CacheGroupContext grp = e.getKey();
            GridDhtPreloaderAssignments assigns = e.getValue();

            assert required(grp);

            int order = grp.config().getRebalanceOrder();

            Map<ClusterNode, Map<Integer, Set<Integer>>> nodeAssigns = sorted.computeIfAbsent(order, v -> new HashMap<>());

            for (Map.Entry<ClusterNode, GridDhtPartitionDemandMessage> e0 : assigns.entrySet()) {
                Map<Integer, Set<Integer>> grpAssigns = nodeAssigns.computeIfAbsent(e0.getKey(), v -> new HashMap<>());

                grpAssigns.put(grp.groupId(), e0.getValue().partitions().fullSet());
            }
        }

        List<T2<UUID, Map<Integer, Set<Integer>>>> ordered = new ArrayList<>(8);

        for (Map<ClusterNode, Map<Integer, Set<Integer>>> nodeAssigns : sorted.values()) {
            for (Map.Entry<ClusterNode, Map<Integer, Set<Integer>>>  e : nodeAssigns.entrySet())
                ordered.add(new T2<>(e.getKey().id(), e.getValue()));
        }

        return ordered;
    }

    /**
     * Partition snapshot listener.
     */
    private class PartitionSnapshotListener implements SnapshotListener {
        /** {@inheritDoc} */
        @Override public void onPartition(UUID nodeId, File file, int grpId, int partId) {
            fileRebalanceRoutine.onPartitionSnapshotReceived(nodeId, file, grpId, partId);
        }

        /** {@inheritDoc} */
        @Override public void onEnd(UUID rmtNodeId) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void onException(UUID rmtNodeId, Throwable t) {
            log.error("Unable to receive partitions [rmtNode=" + rmtNodeId + ", msg=" + t.getMessage() + "]", t);
        }
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

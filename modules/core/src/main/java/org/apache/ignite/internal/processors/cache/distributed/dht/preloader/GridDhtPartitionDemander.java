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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.processors.affinity.AffinityAssignment;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheEntryInfoCollection;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.CacheMetricsImpl;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.GridCacheEntryInfo;
import org.apache.ignite.internal.processors.cache.GridCacheEntryRemovedException;
import org.apache.ignite.internal.processors.cache.GridCacheMvccEntryInfo;
import org.apache.ignite.internal.processors.cache.GridCachePartitionExchangeManager;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtInvalidPartitionException;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionTopology;
import org.apache.ignite.internal.processors.cache.mvcc.txlog.TxState;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.internal.processors.timeout.GridTimeoutObject;
import org.apache.ignite.internal.processors.timeout.GridTimeoutObjectAdapter;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.lang.IgniteInClosureX;
import org.apache.ignite.internal.util.lang.IgnitePredicateX;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.spi.IgniteSpiException;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.events.EventType.EVT_CACHE_REBALANCE_OBJECT_LOADED;
import static org.apache.ignite.events.EventType.EVT_CACHE_REBALANCE_PART_LOADED;
import static org.apache.ignite.events.EventType.EVT_CACHE_REBALANCE_STARTED;
import static org.apache.ignite.events.EventType.EVT_CACHE_REBALANCE_STOPPED;
import static org.apache.ignite.internal.processors.cache.CacheGroupMetricsImpl.CACHE_GROUP_METRICS_PREFIX;
import static org.apache.ignite.internal.processors.cache.GridCacheUtils.TTL_ETERNAL;
import static org.apache.ignite.internal.processors.cache.IgniteCacheOffheapManagerImpl.PRELOAD_SIZE_UNDER_CHECKPOINT_LOCK;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.MOVING;
import static org.apache.ignite.internal.processors.dr.GridDrType.DR_NONE;
import static org.apache.ignite.internal.processors.dr.GridDrType.DR_PRELOAD;
import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.metricName;

/**
 * Thread pool for requesting partitions from other nodes and populating local cache.
 */
public class GridDhtPartitionDemander {
    /** */
    private final GridCacheSharedContext<?, ?> ctx;

    /** */
    private final CacheGroupContext grp;

    /** */
    private final IgniteLogger log;

    /** Future for preload mode {@link CacheRebalanceMode#SYNC}. */
    @GridToStringInclude
    private final GridFutureAdapter syncFut = new GridFutureAdapter();

    /** Rebalance future. */
    @GridToStringInclude
    private volatile RebalanceFuture rebalanceFut;

    /** Last timeout object. */
    private AtomicReference<GridTimeoutObject> lastTimeoutObj = new AtomicReference<>();

    /** Last exchange future. */
    private volatile GridDhtPartitionsExchangeFuture lastExchangeFut;

    /** Rebalancing last cancelled time. */
    private final AtomicLong lastCancelledTime = new AtomicLong(-1);

    /**
     * @param grp Ccahe group.
     */
    public GridDhtPartitionDemander(CacheGroupContext grp) {
        assert grp != null;

        this.grp = grp;

        ctx = grp.shared();

        log = ctx.logger(getClass());

        boolean enabled = grp.rebalanceEnabled() && !ctx.kernalContext().clientNode();

        rebalanceFut = new RebalanceFuture(); //Dummy.

        if (!enabled) {
            // Calling onDone() immediately since preloading is disabled.
            rebalanceFut.onDone(true);
            syncFut.onDone();
        }

        Map<Integer, Object> tops = new HashMap<>();

        String metricGroupName = metricName(CACHE_GROUP_METRICS_PREFIX, grp.cacheOrGroupName());

        MetricRegistry mreg = grp.shared().kernalContext().metric().registry(metricGroupName);

        mreg.register("RebalancingPartitionsLeft", () -> rebalanceFut.partitionsLeft.get(),
            "The number of cache group partitions left to be rebalanced.");

        mreg.register("RebalancingPartitionsTotal",
            () -> rebalanceFut.partitionsTotal,
            "The total number of cache group partitions to be rebalanced.");

        mreg.register("RebalancingReceivedKeys", () -> rebalanceFut.receivedKeys.get(),
            "The number of currently rebalanced keys for the whole cache group.");

        mreg.register("RebalancingReceivedBytes", () -> rebalanceFut.receivedBytes.get(),
            "The number of currently rebalanced bytes of this cache group.");

        mreg.register("RebalancingStartTime", () -> rebalanceFut.startTime, "The time the first partition " +
            "demand message was sent. If there are no messages to send, the rebalancing time will be undefined.");

        mreg.register("RebalancingEndTime", () -> rebalanceFut.endTime, "The time the rebalancing was " +
            "completed. If the rebalancing completed with an error, was cancelled, or the start time was undefined, " +
            "the rebalancing end time will be undefined.");

        mreg.register("RebalancingLastCancelledTime", () -> lastCancelledTime.get(), "The time the " +
            "rebalancing was completed with an error or was cancelled. If there were several such cases, the metric " +
            "stores the last time. The metric displays the value even if there is no rebalancing process.");
    }

    /**
     * Start.
     */
    void start() {
        // No-op.
    }

    /**
     * Stop.
     */
    void stop() {
        try {
            rebalanceFut.cancel();
        }
        catch (Exception ignored) {
            rebalanceFut.onDone(false);
        }

        lastExchangeFut = null;

        lastTimeoutObj.set(null);

        syncFut.onDone();
    }

    /**
     * @return Future for {@link CacheRebalanceMode#SYNC} mode.
     */
    IgniteInternalFuture<?> syncFuture() {
        return syncFut;
    }

    /**
     * @return Rebalance future.
     */
    IgniteInternalFuture<Boolean> rebalanceFuture() {
        return rebalanceFut;
    }

    /**
     * @return Rebalance future.
     */
    IgniteInternalFuture<Boolean> forceRebalance() {
        GridTimeoutObject obj = lastTimeoutObj.getAndSet(null);

        if (obj != null)
            ctx.time().removeTimeoutObject(obj);

        final GridDhtPartitionsExchangeFuture exchFut = lastExchangeFut;

        if (exchFut != null) {
            if (log.isDebugEnabled())
                log.debug("Forcing rebalance event for future: " + exchFut);

            final GridFutureAdapter<Boolean> fut = new GridFutureAdapter<>();

            exchFut.listen(new CI1<IgniteInternalFuture<AffinityTopologyVersion>>() {
                @Override public void apply(IgniteInternalFuture<AffinityTopologyVersion> t) {
                    IgniteInternalFuture<Boolean> fut0 = ctx.exchange().forceRebalance(exchFut.exchangeId());

                    fut0.listen(new IgniteInClosure<IgniteInternalFuture<Boolean>>() {
                        @Override public void apply(IgniteInternalFuture<Boolean> future) {
                            try {
                                fut.onDone(future.get());
                            }
                            catch (Exception e) {
                                fut.onDone(e);
                            }
                        }
                    });
                }
            });

            return fut;
        }
        else if (log.isDebugEnabled())
            log.debug("Ignoring force rebalance request (no topology event happened yet).");

        return new GridFinishedFuture<>(true);
    }

    /**
     * Sets last exchange future.
     *
     * @param lastFut Last future to set.
     */
    void onTopologyChanged(GridDhtPartitionsExchangeFuture lastFut) {
        lastExchangeFut = lastFut;
    }

    /**
     * @return Collection of supplier nodes. Value {@code empty} means rebalance already finished.
     */
    Collection<UUID> remainingNodes() {
        return rebalanceFut.remainingNodes();
    }

    /**
     * This method initiates new rebalance process from given {@code assignments} by creating new rebalance
     * future based on them. Cancels previous rebalance future and sends rebalance started event.
     * In case of delayed rebalance method schedules the new one with configured delay based on {@code lastExchangeFut}.
     *
     * @param assignments Assignments to process.
     * @param force {@code True} if preload request by {@link ForceRebalanceExchangeTask}.
     * @param rebalanceId Rebalance id generated from exchange thread.
     * @param next A next rebalance routine in chain.
     * @param forcedRebFut External future for forced rebalance.
     * @param compatibleRebFut Future for waiting for compatible rebalances.
     *
     * @return Rebalancing future or {@code null} to exclude an assignment from a chain.
     */
    @Nullable RebalanceFuture addAssignments(
        final GridDhtPreloaderAssignments assignments,
        boolean force,
        long rebalanceId,
        final RebalanceFuture next,
        @Nullable final GridCompoundFuture<Boolean, Boolean> forcedRebFut,
        GridCompoundFuture<Boolean, Boolean> compatibleRebFut
    ) {
        if (log.isDebugEnabled())
            log.debug("Adding partition assignments: " + assignments);

        assert force == (forcedRebFut != null);

        long delay = grp.config().getRebalanceDelay();

        if ((delay == 0 || force) && assignments != null) {
            final RebalanceFuture oldFut = rebalanceFut;

            if (assignments.cancelled()) { // Pending exchange.
                if (log.isDebugEnabled())
                    log.debug("Rebalancing skipped due to cancelled assignments.");

                return null;
            }

            if (assignments.isEmpty()) { // Nothing to rebalance.
                if (log.isDebugEnabled())
                    log.debug("Rebalancing skipped due to empty assignments.");

                if (oldFut.isInitial())
                    oldFut.onDone(true);
                else if (!oldFut.isDone())
                    oldFut.tryCancel();

                ((GridFutureAdapter)grp.preloader().syncFuture()).onDone();

                return null;
            }

            if (!force && (!oldFut.isDone() || oldFut.result()) && oldFut.compatibleWith(assignments)) {
                if (!oldFut.isDone())
                    compatibleRebFut.add(oldFut);

                return null;
            }

            final RebalanceFuture fut = new RebalanceFuture(grp, assignments, log, rebalanceId, next, lastCancelledTime);

            if (!grp.localWalEnabled()) {
                fut.listen(new IgniteInClosureX<IgniteInternalFuture<Boolean>>() {
                    @Override public void applyx(IgniteInternalFuture<Boolean> future) throws IgniteCheckedException {
                        if (future.get())
                            ctx.walState().onGroupRebalanceFinished(grp.groupId());
                    }
                });
            }

            if (!oldFut.isInitial())
                oldFut.tryCancel();
            else
                fut.listen(f -> oldFut.onDone(f.result()));

            // Make sure partitions scheduled for full rebalancing are first cleared.
            if (grp.persistenceEnabled()) {
                for (Map.Entry<ClusterNode, GridDhtPartitionDemandMessage> e : assignments.entrySet()) {
                    for (Integer partId : e.getValue().partitions().fullSet()) {
                        GridDhtLocalPartition part = grp.topology().localPartition(partId);

                        if (part != null && part.state() == MOVING)
                            part.clearAsync();
                    }
                }
            }

            if (forcedRebFut != null)
                forcedRebFut.add(fut);

            rebalanceFut = fut;

            for (final GridCacheContext cctx : grp.caches()) {
                if (cctx.statisticsEnabled()) {
                    final CacheMetricsImpl metrics = cctx.cache().metrics0();

                    metrics.clearRebalanceCounters();

                    for (GridDhtPartitionDemandMessage msg : assignments.values()) {
                        for (Integer partId : msg.partitions().fullSet())
                            metrics.onRebalancingKeysCountEstimateReceived(grp.topology().globalPartSizes().get(partId));

                        CachePartitionPartialCountersMap histMap = msg.partitions().historicalMap();

                        for (int i = 0; i < histMap.size(); i++) {
                            long from = histMap.initialUpdateCounterAt(i);
                            long to = histMap.updateCounterAt(i);

                            metrics.onRebalancingKeysCountEstimateReceived(to - from);
                        }
                    }

                    metrics.startRebalance(0);
                }
            }

            fut.sendRebalanceStartedEvent();

            return fut;
        }
        else if (delay > 0) {
            for (GridCacheContext cctx : grp.caches()) {
                if (cctx.statisticsEnabled()) {
                    final CacheMetricsImpl metrics = cctx.cache().metrics0();

                    metrics.startRebalance(delay);
                }
            }

            GridTimeoutObject obj = lastTimeoutObj.get();

            if (obj != null)
                ctx.time().removeTimeoutObject(obj);

            final GridDhtPartitionsExchangeFuture exchFut = lastExchangeFut;

            assert exchFut != null : "Delaying rebalance process without topology event.";

            obj = new GridTimeoutObjectAdapter(delay) {
                @Override public void onTimeout() {
                    exchFut.listen(new CI1<IgniteInternalFuture<AffinityTopologyVersion>>() {
                        @Override public void apply(IgniteInternalFuture<AffinityTopologyVersion> f) {
                            ctx.exchange().forceRebalance(exchFut.exchangeId());
                        }
                    });
                }
            };

            lastTimeoutObj.set(obj);

            ctx.time().addTimeoutObject(obj);
        }

        return null;
    }

    /**
     * Enqueues supply message.
     */
    public void registerSupplyMessage(final UUID nodeId, final GridDhtPartitionSupplyMessage supplyMsg, final Runnable r) {
        final RebalanceFuture fut = rebalanceFut;

        if (fut.isActual(supplyMsg.rebalanceId())) {
            boolean historical = false;

            for (Integer p : supplyMsg.infos().keySet()) {
                fut.queued.get(p).increment();

                if (fut.historical.contains(p))
                    historical = true;
            }

            if (historical) // Can not be reordered.
                ctx.kernalContext().getStripedRebalanceExecutorService().execute(r, Math.abs(nodeId.hashCode()));
            else // Can be reordered.
                ctx.kernalContext().getRebalanceExecutorService().execute(r);
        }
    }

    /**
     * Handles supply message from {@code nodeId} with specified {@code topicId}.
     *
     * Supply message contains entries to populate rebalancing partitions.
     *
     * There is a cyclic process:
     * Populate rebalancing partitions with entries from Supply message.
     * If not all partitions specified in {@link #rebalanceFut} were rebalanced or marked as missed
     * send new Demand message to request next batch of entries.
     *
     * @param nodeId Node id.
     * @param supplyMsg Supply message.
     */
    public void handleSupplyMessage(
        final UUID nodeId,
        final GridDhtPartitionSupplyMessage supplyMsg
    ) {
        AffinityTopologyVersion topVer = supplyMsg.topologyVersion();

        final RebalanceFuture fut = rebalanceFut;

        fut.cancelLock.readLock().lock();

        try {
            if (fut.isDone()) {
                if (log.isDebugEnabled())
                    log.debug("Supply message ignored (rebalance completed) [" + demandRoutineInfo(nodeId, supplyMsg) + "]");

                return;
            }

            ClusterNode node = ctx.node(nodeId);

            if (node == null) {
                if (log.isDebugEnabled())
                    log.debug("Supply message ignored (supplier has left cluster) [" + demandRoutineInfo(nodeId, supplyMsg) + "]");

                return;
            }

            // Topology already changed (for the future that supply message based on).
            if (!fut.isActual(supplyMsg.rebalanceId())) {
                if (log.isDebugEnabled())
                    log.debug("Supply message ignored (topology changed) [" + demandRoutineInfo(nodeId, supplyMsg) + "]");

                return;
            }

            if (log.isDebugEnabled())
                log.debug("Received supply message [" + demandRoutineInfo(nodeId, supplyMsg) + "]");

            // Check whether there were error during supply message unmarshalling process.
            if (supplyMsg.classError() != null) {
                U.warn(log, "Rebalancing from node cancelled [" + demandRoutineInfo(nodeId, supplyMsg) + "]" +
                    ". Supply message couldn't be unmarshalled: " + supplyMsg.classError());

                fut.cancel(nodeId);

                return;
            }

            // Check whether there were error during supplying process.
            if (supplyMsg.error() != null) {
                U.warn(log, "Rebalancing from node cancelled [" + demandRoutineInfo(nodeId, supplyMsg) + "]" +
                    "]. Supplier has failed with error: " + supplyMsg.error());

                fut.cancel(nodeId);

                return;
            }

            final GridDhtPartitionTopology top = grp.topology();

            rebalanceFut.receivedBytes.addAndGet(supplyMsg.messageSize());

            if (grp.sharedGroup()) {
                for (GridCacheContext cctx : grp.caches()) {
                    if (cctx.statisticsEnabled()) {
                        long keysCnt = supplyMsg.keysForCache(cctx.cacheId());

                        if (keysCnt != -1)
                            cctx.cache().metrics0().onRebalancingKeysCountEstimateReceived(keysCnt);

                        // Can not be calculated per cache.
                        cctx.cache().metrics0().onRebalanceBatchReceived(supplyMsg.messageSize());
                    }
                }
            }
            else {
                GridCacheContext cctx = grp.singleCacheContext();

                if (cctx.statisticsEnabled()) {
                    if (supplyMsg.estimatedKeysCount() != -1)
                        cctx.cache().metrics0().onRebalancingKeysCountEstimateReceived(supplyMsg.estimatedKeysCount());

                    cctx.cache().metrics0().onRebalanceBatchReceived(supplyMsg.messageSize());
                }
            }

            try {
                AffinityAssignment aff = grp.affinity().cachedAffinity(topVer);

                // Preload.
                for (Map.Entry<Integer, CacheEntryInfoCollection> e : supplyMsg.infos().entrySet()) {
                    int p = e.getKey();

                    if (aff.get(p).contains(ctx.localNode())) {
                        GridDhtLocalPartition part;

                        try {
                            part = top.localPartition(p, topVer, true);
                        }
                        catch (GridDhtInvalidPartitionException err) {
                            assert !topVer.equals(top.lastTopologyChangeVersion());

                            if (log.isDebugEnabled()) {
                                log.debug("Failed to get partition for rebalancing [" +
                                    "grp=" + grp.cacheOrGroupName() +
                                    ", err=" + err +
                                    ", p=" + p +
                                    ", topVer=" + topVer +
                                    ", lastTopVer=" + top.lastTopologyChangeVersion() + ']');
                            }

                            continue;
                        }

                        assert part != null;

                        boolean last = supplyMsg.last().containsKey(p);

                        if (part.state() == MOVING) {
                            boolean reserved = part.reserve();

                            assert reserved : "Failed to reserve partition [igniteInstanceName=" +
                                ctx.igniteInstanceName() + ", grp=" + grp.cacheOrGroupName() + ", part=" + part + ']';

                            part.beforeApplyBatch(last);

                            try {
                                Iterator<GridCacheEntryInfo> infos = e.getValue().infos().iterator();

                                try {
                                    if (grp.mvccEnabled())
                                        mvccPreloadEntries(topVer, node, p, infos);
                                    else
                                        preloadEntries(topVer, p, infos);
                                }
                                catch (GridDhtInvalidPartitionException ignored) {
                                    if (log.isDebugEnabled())
                                        log.debug("Partition became invalid during rebalancing (will ignore): " + p);
                                }

                                fut.processed.get(p).increment();

                                // If message was last for this partition, then we take ownership.
                                if (last)
                                    ownPartition(fut, p, nodeId, supplyMsg);
                            }
                            finally {
                                part.release();
                            }
                        }
                        else {
                            if (last)
                                fut.partitionDone(nodeId, p, false);

                            if (log.isDebugEnabled())
                                log.debug("Skipping rebalancing partition (state is not MOVING): " +
                                    "[" + demandRoutineInfo(nodeId, supplyMsg) + ", p=" + p + "]");
                        }
                    }
                    else {
                        fut.partitionDone(nodeId, p, false);

                        if (log.isDebugEnabled())
                            log.debug("Skipping rebalancing partition (affinity changed): " +
                                "[" + demandRoutineInfo(nodeId, supplyMsg) + ", p=" + p + "]");
                    }
                }

                // Only request partitions based on latest topology version.
                for (Integer miss : supplyMsg.missed()) {
                    if (aff.get(miss).contains(ctx.localNode()))
                        fut.partitionMissed(nodeId, miss);
                }

                for (Integer miss : supplyMsg.missed())
                    fut.partitionDone(nodeId, miss, false);

                GridDhtPartitionDemandMessage d = new GridDhtPartitionDemandMessage(
                    supplyMsg.rebalanceId(),
                    supplyMsg.topologyVersion(),
                    grp.groupId());

                d.timeout(grp.preloader().timeout());

                if (!fut.isDone()) {
                    // Send demand message.
                    try {
                        ctx.io().sendOrderedMessage(node, d.topic(),
                            d.convertIfNeeded(node.version()), grp.ioPolicy(), grp.preloader().timeout());

                        if (log.isDebugEnabled())
                            log.debug("Send next demand message [" + demandRoutineInfo(nodeId, supplyMsg) + "]");
                    }
                    catch (ClusterTopologyCheckedException e) {
                        if (log.isDebugEnabled())
                            log.debug("Supplier has left [" + demandRoutineInfo(nodeId, supplyMsg) +
                                ", errMsg=" + e.getMessage() + ']');
                    }
                }
                else {
                    if (log.isDebugEnabled())
                        log.debug("Will not request next demand message [" + demandRoutineInfo(nodeId, supplyMsg) +
                            ", rebalanceFuture=" + fut + "]");
                }
            }
            catch (IgniteSpiException | IgniteCheckedException e) {
                fut.cancel(nodeId);

                LT.error(log, e, "Error during rebalancing [" + demandRoutineInfo(nodeId, supplyMsg) +
                    ", err=" + e + ']');
            }
        }
        finally {
            fut.cancelLock.readLock().unlock();
        }
    }

    /**
     * Owns the partition recursively.
     */
    protected void ownPartition(
        final RebalanceFuture fut,
        int p,
        final UUID nodeId,
        final GridDhtPartitionSupplyMessage supplyMsg) {
        if (fut.isDone() || !fut.isActual(supplyMsg.rebalanceId()))
            return;

        long queued = fut.queued.get(p).sum();
        long processed = fut.processed.get(p).sum();

        if (processed == queued) {
            fut.partitionDone(nodeId, p, true);

            if (log.isDebugEnabled())
                log.debug("Finished rebalancing partition: " +
                    "[" + demandRoutineInfo(nodeId, supplyMsg) + ", p=" + p + "]");
        }
        else {
            if (log.isDebugEnabled())
                log.debug("Retrying partition owning: " +
                    "[" + demandRoutineInfo(nodeId, supplyMsg) + ", p=" + p +
                    ", processed=" + processed + ", queued=" + queued + "]");

            ctx.kernalContext().getRebalanceExecutorService().execute(() -> ownPartition(fut, p, nodeId, supplyMsg));
        }
    }

    /**
     * Adds mvcc entries with theirs history to partition p.
     *
     * @param node Node which sent entry.
     * @param p Partition id.
     * @param infos Entries info for preload.
     * @param topVer Topology version.
     * @throws IgniteInterruptedCheckedException If interrupted.
     */
    private void mvccPreloadEntries(AffinityTopologyVersion topVer, ClusterNode node, int p,
        Iterator<GridCacheEntryInfo> infos) throws IgniteCheckedException {
        if (!infos.hasNext())
            return;

        List<GridCacheMvccEntryInfo> entryHist = new ArrayList<>();

        GridCacheContext cctx = grp.sharedGroup() ? null : grp.singleCacheContext();

        // Loop through all received entries and try to preload them.
        while (infos.hasNext() || !entryHist.isEmpty()) {
            ctx.database().checkpointReadLock();

            try {
                for (int i = 0; i < PRELOAD_SIZE_UNDER_CHECKPOINT_LOCK; i++) {
                    boolean hasMore = infos.hasNext();

                    assert hasMore || !entryHist.isEmpty();

                    GridCacheMvccEntryInfo entry = null;

                    boolean flushHistory;

                    if (hasMore) {
                        entry = (GridCacheMvccEntryInfo)infos.next();

                        GridCacheMvccEntryInfo prev = entryHist.isEmpty() ? null : entryHist.get(0);

                        flushHistory = prev != null && ((grp.sharedGroup() && prev.cacheId() != entry.cacheId())
                            || !prev.key().equals(entry.key()));
                    }
                    else
                        flushHistory = true;

                    if (flushHistory) {
                        assert !entryHist.isEmpty();

                        int cacheId = entryHist.get(0).cacheId();

                        if (grp.sharedGroup() && (cctx == null || cacheId != cctx.cacheId())) {
                            assert cacheId != CU.UNDEFINED_CACHE_ID;

                            cctx = grp.shared().cacheContext(cacheId);
                        }

                        if (cctx != null) {
                            mvccPreloadEntry(cctx, node, entryHist, topVer, p);

                            rebalanceFut.receivedKeys.incrementAndGet();

                            updateGroupMetrics();
                        }

                        if (!hasMore)
                            return;

                        entryHist.clear();
                    }

                    entryHist.add(entry);
                }
            }
            finally {
                ctx.database().checkpointReadUnlock();
            }
        }
    }

    /**
     * Adds entries to partition p.
     *
     * @param topVer Topology version.
     * @param p Partition id.
     * @param infos Entries info for preload.
     * @throws IgniteCheckedException If failed.
     */
    private void preloadEntries(AffinityTopologyVersion topVer, int p,
        Iterator<GridCacheEntryInfo> infos) throws IgniteCheckedException {

        grp.offheap().storeEntries(p, infos, new IgnitePredicateX<CacheDataRow>() {
            @Override public boolean applyx(CacheDataRow row) throws IgniteCheckedException {
                return preloadEntry(row, topVer);
            }
        });
    }

    /**
     * Adds {@code entry} to partition {@code p}.
     *
     * @param row Data row.
     * @param topVer Topology version.
     * @return {@code True} if the initial value was set for the specified cache entry.
     * @throws IgniteInterruptedCheckedException If interrupted.
     */
    private boolean preloadEntry(CacheDataRow row, AffinityTopologyVersion topVer) throws IgniteCheckedException {
        assert !grp.mvccEnabled();
        assert ctx.database().checkpointLockIsHeldByThread();

        rebalanceFut.receivedKeys.incrementAndGet();

        updateGroupMetrics();

        GridCacheContext cctx = grp.sharedGroup() ? ctx.cacheContext(row.cacheId()) : grp.singleCacheContext();

        if (cctx == null)
            return false;

        cctx = cctx.isNear() ? cctx.dhtCache().context() : cctx;

        GridCacheEntryEx cached = cctx.cache().entryEx(row.key(), topVer);

        try {
            if (log.isTraceEnabled()) {
                log.trace("Rebalancing key [key=" + cached.key() + ", part=" + cached.partition() +
                    ", grpId=" + grp.groupId() + ']');
            }

            assert row.expireTime() >= 0 : row.expireTime();

            if (cached.initialValue(
                row.value(),
                row.version(),
                null,
                null,
                TxState.NA,
                TxState.NA,
                TTL_ETERNAL,
                row.expireTime(),
                true,
                topVer,
                cctx.isDrEnabled() ? DR_PRELOAD : DR_NONE,
                false,
                row
            )) {
                cached.touch(); // Start tracking.

                if (cctx.events().isRecordable(EVT_CACHE_REBALANCE_OBJECT_LOADED) && !cached.isInternal())
                    cctx.events().addEvent(cached.partition(), cached.key(), cctx.localNodeId(), null,
                        null, null, EVT_CACHE_REBALANCE_OBJECT_LOADED, row.value(), true, null,
                        false, null, null, null, true);

                return true;
            }
            else {
                cached.touch(); // Start tracking.

                if (log.isTraceEnabled())
                    log.trace("Rebalancing entry is already in cache (will ignore) [key=" + cached.key() +
                        ", part=" + cached.partition() + ']');
            }
        }
        catch (GridCacheEntryRemovedException ignored) {
            if (log.isTraceEnabled())
                log.trace("Entry has been concurrently removed while rebalancing (will ignore) [key=" +
                    cached.key() + ", part=" + cached.partition() + ']');
        }
        catch (IgniteInterruptedCheckedException e) {
            throw e;
        }
        catch (IgniteCheckedException e) {
            throw new IgniteCheckedException("Failed to cache rebalanced entry (will stop rebalancing) [" +
                "key=" + row.key() + ", part=" + row.partition() + ']', e);
        }

        return false;
    }

    /**
     * Adds mvcc {@code entry} with it's history to partition {@code p}.
     *
     * @param cctx Cache context.
     * @param from Node which sent entry.
     * @param history Mvcc entry history.
     * @param topVer Topology version.
     * @param p Partition id.
     * @return {@code True} if the initial value was set for the specified cache entry.
     * @throws IgniteInterruptedCheckedException If interrupted.
     */
    private boolean mvccPreloadEntry(
        GridCacheContext cctx,
        ClusterNode from,
        List<GridCacheMvccEntryInfo> history,
        AffinityTopologyVersion topVer,
        int p
    ) throws IgniteCheckedException {
        assert ctx.database().checkpointLockIsHeldByThread();
        assert !history.isEmpty();

        GridCacheMvccEntryInfo info = history.get(0);

        assert info.key() != null;

        try {
            GridCacheEntryEx cached = null;

            try {
                cached = cctx.cache().entryEx(info.key(), topVer);

                if (log.isTraceEnabled())
                    log.trace("Rebalancing key [key=" + info.key() + ", part=" + p + ", node=" + from.id() + ']');

                if (cached.mvccPreloadEntry(history)) {
                    cached.touch(); // Start tracking.

                    if (cctx.events().isRecordable(EVT_CACHE_REBALANCE_OBJECT_LOADED) && !cached.isInternal())
                        cctx.events().addEvent(cached.partition(), cached.key(), cctx.localNodeId(), null,
                            null, null, EVT_CACHE_REBALANCE_OBJECT_LOADED, null, true, null,
                            false, null, null, null, true);

                    return true;
                }
                else {
                    cached.touch(); // Start tracking.

                    if (log.isTraceEnabled())
                        log.trace("Rebalancing entry is already in cache (will ignore) [key=" + cached.key() +
                            ", part=" + p + ']');
                }
            }
            catch (GridCacheEntryRemovedException ignored) {
                if (log.isTraceEnabled())
                    log.trace("Entry has been concurrently removed while rebalancing (will ignore) [key=" +
                        cached.key() + ", part=" + p + ']');
            }
        }
        catch (IgniteInterruptedCheckedException | ClusterTopologyCheckedException e) {
            throw e;
        }
        catch (IgniteCheckedException e) {
            throw new IgniteCheckedException("Failed to cache rebalanced entry (will stop rebalancing) [local=" +
                ctx.localNode() + ", node=" + from.id() + ", key=" + info.key() + ", part=" + p + ']', e);
        }

        return false;
    }

    /**
     * String representation of demand routine.
     *
     * @param supplier Supplier.
     * @param supplyMsg Supply message.
     */
    private String demandRoutineInfo(UUID supplier, GridDhtPartitionSupplyMessage supplyMsg) {
        return "grp=" + grp.cacheOrGroupName() + ", topVer=" + supplyMsg.topologyVersion() + ", supplier=" + supplier;
    }

    /**
     * Update rebalancing metrics.
     */
    private void updateGroupMetrics() {
        // TODO: IGNITE-11330: Update metrics for touched cache only.
        // Due to historical rebalancing "EstimatedRebalancingKeys" metric is currently calculated for the whole cache
        // group (by partition counters), so "RebalancedKeys" and "RebalancingKeysRate" is calculated in the same way.
        for (GridCacheContext cctx0 : grp.caches()) {
            if (cctx0.statisticsEnabled())
                cctx0.cache().metrics0().onRebalanceKeyReceived();
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtPartitionDemander.class, this);
    }

    /**
     * Internal states of rebalance future.
     */
    private enum RebalanceFutureState {
        /** Init. */
        INIT,

        /** Started. */
        STARTED,

        /** Marked as cancelled. */
        MARK_CANCELLED,
    }

    /**
     *
     */
    public static class RebalanceFuture extends GridFutureAdapter<Boolean> {
        /** State updater. */
        private static final AtomicReferenceFieldUpdater<RebalanceFuture, RebalanceFutureState> STATE_UPD =
            AtomicReferenceFieldUpdater.newUpdater(RebalanceFuture.class, RebalanceFutureState.class, "state");

        /** */
        private final GridCacheSharedContext<?, ?> ctx;

        /** Internal state. */
        private volatile RebalanceFutureState state = RebalanceFutureState.INIT;

        /** */
        private final CacheGroupContext grp;

        /** */
        private final IgniteLogger log;

        /** Remaining. */
        private final Map<UUID, IgniteDhtDemandedPartitionsMap> remaining = new HashMap<>();

        /** Missed. */
        private final Map<UUID, Collection<Integer>> missed = new HashMap<>();

        /** Exchange ID. */
        @GridToStringExclude
        private final GridDhtPartitionExchangeId exchId;

        /** Topology version. */
        private final AffinityTopologyVersion topVer;

        /** Unique (per demander) rebalance id. */
        private final long rebalanceId;

        /** The number of rebalance routines. */
        private final long routines;

        /** Used to order rebalance cancellation and supply message processing, they should not overlap.
         * Otherwise partition clearing could start on still rebalancing partition resulting in eviction of
         * partition in OWNING state. */
        private final ReentrantReadWriteLock cancelLock;

        /** Entries batches queued. */
        private final Map<Integer, LongAdder> queued = new HashMap<>();

        /** Entries batches processed. */
        private final Map<Integer, LongAdder> processed = new HashMap<>();

        /** Historical rebalance set. */
        private final Set<Integer> historical = new HashSet<>();

        /** Rebalanced bytes count. */
        private final AtomicLong receivedBytes = new AtomicLong(0);

        /** Rebalanced keys count. */
        private final AtomicLong receivedKeys = new AtomicLong(0);

        /** The number of cache group partitions left to be rebalanced. */
        private final AtomicLong partitionsLeft = new AtomicLong(0);

        /** The number of cache group partitions total to be rebalanced. */
        private final int partitionsTotal;

        /** Rebalancing start time. */
        private volatile long startTime = -1;

        /** Rebalancing end time. */
        private volatile long endTime = -1;

        /** Rebalancing last cancelled time. */
        private final AtomicLong lastCancelledTime;

        /** Next future in chain. */
        private final RebalanceFuture next;

        /** Assigment. */
        private final GridDhtPreloaderAssignments assignments;

        /** Partitions which have been scheduled for rebalance from specific supplier. */
        private final Map<ClusterNode, Set<Integer>> rebalancingParts;

        /**
         * @param grp Cache group.
         * @param assignments Assignments.
         * @param log Logger.
         * @param rebalanceId Rebalance id.
         * @param next Next rebalance future.
         * @param lastCancelledTime Cancelled time.
         */
        RebalanceFuture(
            CacheGroupContext grp,
            GridDhtPreloaderAssignments assignments,
            IgniteLogger log,
            long rebalanceId,
            RebalanceFuture next,
            AtomicLong lastCancelledTime) {
            assert assignments != null;

            rebalancingParts = U.newHashMap(assignments.size());
            this.assignments = assignments;
            exchId = assignments.exchangeId();
            topVer = assignments.topologyVersion();
            this.next = next;

            this.lastCancelledTime = lastCancelledTime;

            assignments.forEach((k, v) -> {
                assert v.partitions() != null :
                    "Partitions are null [grp=" + grp.cacheOrGroupName() + ", fromNode=" + k.id() + "]";

                remaining.put(k.id(), v.partitions());

                partitionsLeft.addAndGet(v.partitions().size());

                rebalancingParts.put(k, new HashSet<Integer>(v.partitions().size()) {{
                    addAll(v.partitions().historicalSet());
                    addAll(v.partitions().fullSet());
                }});

                historical.addAll(v.partitions().historicalSet());

                Stream.concat(v.partitions().historicalSet().stream(), v.partitions().fullSet().stream())
                    .forEach(
                        p -> {
                            queued.put(p, new LongAdder());
                            processed.put(p, new LongAdder());
                        });
            });

            this.routines = remaining.size();

            partitionsTotal = rebalancingParts.values().stream().mapToInt(Set::size).sum();
            this.grp = grp;
            this.log = log;
            this.rebalanceId = rebalanceId;

            ctx = grp.shared();

            cancelLock = new ReentrantReadWriteLock();
        }

        /**
         * Dummy future. Will be done by real one.
         */
        RebalanceFuture() {
            this.rebalancingParts = null;
            this.partitionsTotal = 0;
            this.assignments = null;
            this.exchId = null;
            this.topVer = null;
            this.ctx = null;
            this.grp = null;
            this.log = null;
            this.rebalanceId = -1;
            this.routines = 0;
            this.cancelLock = new ReentrantReadWriteLock();
            this.next = null;
            this.lastCancelledTime = new AtomicLong();
        }

        /**
         * Asynchronously sends initial demand messages formed from {@code assignments} and initiates supply-demand processes.
         *
         * For each node participating in rebalance process method distributes set of partitions for that node to several stripes (topics).
         * It means that each stripe containing a subset of partitions can be processed in parallel.
         * The number of stripes are controlled by {@link IgniteConfiguration#getRebalanceThreadPoolSize()} property.
         *
         * Partitions that can be rebalanced using only WAL are called historical, others are called full.
         *
         * Before sending messages, method awaits partitions clearing for full partitions.
         */
        public void requestPartitions() {
            if (!STATE_UPD.compareAndSet(this, RebalanceFutureState.INIT, RebalanceFutureState.STARTED)) {
                cancel();

                return;
            }

            if (!ctx.kernalContext().grid().isRebalanceEnabled()) {
                if (log.isTraceEnabled())
                    log.trace("Cancel partition demand because rebalance disabled on current node.");

                cancel();

                return;
            }

            if (isDone()) {
                assert !result() : "Rebalance future was done, but partitions never requested [grp="
                    + grp.cacheOrGroupName() + ", topVer=" + topologyVersion() + "]";

                return;
            }

            final CacheConfiguration cfg = grp.config();

            for (Map.Entry<ClusterNode, GridDhtPartitionDemandMessage> e : assignments.entrySet()) {
                final ClusterNode node = e.getKey();

                GridDhtPartitionDemandMessage d = e.getValue();

                final IgniteDhtDemandedPartitionsMap parts;

                synchronized (this) { // Synchronized to prevent consistency issues in case of parallel cancellation.
                    if (isDone())
                        return;

                    if (startTime == -1)
                        startTime = System.currentTimeMillis();

                    parts = remaining.get(node.id());

                    U.log(log, "Prepared rebalancing [grp=" + grp.cacheOrGroupName()
                        + ", mode=" + cfg.getRebalanceMode() + ", supplier=" + node.id() + ", partitionsCount=" + parts.size()
                        + ", topVer=" + topologyVersion() + "]");
                }

                if (!parts.isEmpty()) {
                    d.rebalanceId(rebalanceId);
                    d.timeout(grp.preloader().timeout());

                    IgniteInternalFuture<?> clearAllFuture = clearFullPartitions(this, d.partitions().fullSet());

                    // Start rebalancing after clearing full partitions is finished.
                    clearAllFuture.listen(f -> ctx.kernalContext().closure().runLocalSafe(() -> {
                        if (isDone())
                            return;

                        try {
                            if (log.isInfoEnabled())
                                log.info("Starting rebalance routine [" + grp.cacheOrGroupName() +
                                    ", topVer=" + topologyVersion() +
                                    ", supplier=" + node.id() +
                                    ", fullPartitions=" + S.compact(parts.fullSet()) +
                                    ", histPartitions=" + S.compact(parts.historicalSet()) + "]");

                            ctx.io().sendOrderedMessage(node, d.topic(),
                                d.convertIfNeeded(node.version()), grp.ioPolicy(), d.timeout());

                            // Cleanup required in case partitions demanded in parallel with cancellation.
                            synchronized (this) {
                                if (isDone())
                                    cleanupRemoteContexts(node.id());
                            }
                        }
                        catch (IgniteCheckedException e1) {
                            ClusterTopologyCheckedException cause = e1.getCause(ClusterTopologyCheckedException.class);

                            if (cause != null)
                                log.warning("Failed to send initial demand request to node. " + e1.getMessage());
                            else
                                log.error("Failed to send initial demand request to node.", e1);

                            cancel();
                        }
                        catch (Throwable th) {
                            log.error("Runtime error caught during initial demand request sending.", th);

                            cancel();
                        }
                    }, true));
                }
            }
        }

        /**
         * Creates future which will be completed when all {@code fullPartitions} are cleared.
         *
         * @param fut Rebalance future.
         * @param fullPartitions Set of full partitions need to be cleared.
         * @return Future which will be completed when given partitions are cleared.
         */
        private IgniteInternalFuture<?> clearFullPartitions(RebalanceFuture fut, Set<Integer> fullPartitions) {
            final GridFutureAdapter clearAllFuture = new GridFutureAdapter();

            if (fullPartitions.isEmpty()) {
                clearAllFuture.onDone();

                return clearAllFuture;
            }

            for (GridCacheContext cctx : grp.caches()) {
                if (cctx.statisticsEnabled()) {
                    final CacheMetricsImpl metrics = cctx.cache().metrics0();

                    metrics.rebalanceClearingPartitions(fullPartitions.size());
                }
            }

            final AtomicInteger clearingPartitions = new AtomicInteger(fullPartitions.size());

            for (int partId : fullPartitions) {
                if (fut.isDone()) {
                    clearAllFuture.onDone();

                    return clearAllFuture;
                }

                GridDhtLocalPartition part = grp.topology().localPartition(partId);

                if (part != null && part.state() == MOVING) {
                    part.onClearFinished(f -> {
                        if (!fut.isDone()) {
                            // Cancel rebalance if partition clearing was failed.
                            if (f.error() != null) {
                                for (GridCacheContext cctx : grp.caches()) {
                                    if (cctx.statisticsEnabled()) {
                                        final CacheMetricsImpl metrics = cctx.cache().metrics0();

                                        metrics.rebalanceClearingPartitions(0);
                                    }
                                }

                                log.error("Unable to await partition clearing " + part, f.error());

                                fut.cancel();

                                clearAllFuture.onDone(f.error());
                            }
                            else {
                                int remaining = clearingPartitions.decrementAndGet();

                                for (GridCacheContext cctx : grp.caches()) {
                                    if (cctx.statisticsEnabled()) {
                                        final CacheMetricsImpl metrics = cctx.cache().metrics0();

                                        metrics.rebalanceClearingPartitions(remaining);
                                    }
                                }

                                if (log.isDebugEnabled())
                                    log.debug("Partition is ready for rebalance [grp=" + grp.cacheOrGroupName()
                                        + ", p=" + part.id() + ", remaining=" + remaining + "]");

                                if (remaining == 0)
                                    clearAllFuture.onDone();
                            }
                        }
                        else
                            clearAllFuture.onDone();
                    });
                }
                else {
                    int remaining = clearingPartitions.decrementAndGet();

                    for (GridCacheContext cctx : grp.caches()) {
                        if (cctx.statisticsEnabled()) {
                            final CacheMetricsImpl metrics = cctx.cache().metrics0();

                            metrics.rebalanceClearingPartitions(remaining);
                        }
                    }

                    if (remaining == 0)
                        clearAllFuture.onDone();
                }
            }

            return clearAllFuture;
        }

        /** {@inheritDoc} */
        @Override public boolean onDone(@Nullable Boolean res, @Nullable Throwable err) {
            if (super.onDone(res, err)) {
                if (next != null)
                    next.requestPartitions(); // Go to next item in chain everything if it exists.

                return true;
            }

            return false;
        }

        /**
         * @return Topology version.
         */
        public AffinityTopologyVersion topologyVersion() {
            return topVer;
        }

        /**
         * @param rebalanceId Rebalance id.
         * @return true in case future created for specified {@code rebalanceId}, false in other case.
         */
        private boolean isActual(long rebalanceId) {
            return this.rebalanceId == rebalanceId;
        }

        /**
         * @return Is initial (created at demander creation).
         */
        public boolean isInitial() {
            return topVer == null;
        }

        /**
         * Cancel future or mark for cancel {@code RebalanceFutureState#MARK_CANCELLED}.
         */
        private void tryCancel() {
            if (STATE_UPD.compareAndSet(this, RebalanceFutureState.INIT, RebalanceFutureState.MARK_CANCELLED))
                return;

            cancel();
        }

        /**
         * Cancels this future.
         *
         * @return {@code True}.
         */
        @Override public boolean cancel() {
            // Cancel lock is needed only for case when some message might be on the fly while rebalancing is
            // cancelled.
            cancelLock.writeLock().lock();

            try {
                synchronized (this) {
                    if (isDone())
                        return true;

                    U.log(log, "Cancelled rebalancing from all nodes [grp=" + grp.cacheOrGroupName() +
                        ", topVer=" + topologyVersion() + "]");

                    if (!ctx.kernalContext().isStopping()) {
                        for (UUID nodeId : remaining.keySet())
                            cleanupRemoteContexts(nodeId);
                    }

                    remaining.clear();

                    checkIsDone(true /* cancelled */);
                }

                return true;
            }
            finally {
                cancelLock.writeLock().unlock();
            }
        }

        /** {@inheritDoc} */
        @Override public boolean onDone(@Nullable Boolean res, @Nullable Throwable err, boolean cancel) {
            assert !cancel : "RebalanceFuture cancel is not supported. Use res = false.";

            boolean byThisCall = super.onDone(res, err, cancel);
            boolean isCancelled = res == Boolean.FALSE || isFailed();

            if (byThisCall) {
                if (isCancelled)
                    lastCancelledTime.accumulateAndGet(System.currentTimeMillis(), Math::max);
                else if (startTime != -1)
                    endTime = System.currentTimeMillis();
            }

            return byThisCall;
        }

        /**
         * @param nodeId Node id.
         */
        private synchronized void cancel(UUID nodeId) {
            if (isDone())
                return;

            U.log(log, ("Cancelled rebalancing [grp=" + grp.cacheOrGroupName() +
                ", supplier=" + nodeId + ", topVer=" + topologyVersion() + ']'));

            cleanupRemoteContexts(nodeId);

            remaining.remove(nodeId);

            onDone(false); // Finishing rebalance future as non completed.

            checkIsDone(); // But will finish syncFuture only when other nodes are preloaded or rebalancing cancelled.
        }

        /**
         * @param nodeId Node id.
         * @param p Partition id.
         */
        private synchronized void partitionMissed(UUID nodeId, int p) {
            if (isDone())
                return;

            missed.computeIfAbsent(nodeId, k -> new HashSet<>());

            missed.get(nodeId).add(p);
        }

        /**
         * @param nodeId Node id.
         */
        private void cleanupRemoteContexts(UUID nodeId) {
            ClusterNode node = ctx.discovery().node(nodeId);

            if (node == null)
                return;

            GridDhtPartitionDemandMessage d = new GridDhtPartitionDemandMessage(
                // Negative number of id signals that supply context
                // with the same positive id must be cleaned up at the supply node.
                -rebalanceId,
                this.topologyVersion(),
                grp.groupId());

            d.timeout(grp.preloader().timeout());

            try {
                Object rebalanceTopic = GridCachePartitionExchangeManager.rebalanceTopic(0);

                ctx.io().sendOrderedMessage(node, rebalanceTopic,
                    d.convertIfNeeded(node.version()), grp.ioPolicy(), grp.preloader().timeout());
            }
            catch (IgniteCheckedException ignored) {
                if (log.isDebugEnabled())
                    log.debug("Failed to send failover context cleanup request to node " + nodeId);
            }
        }

        /**
         * @param nodeId Node id.
         * @param p Partition number.
         */
        private synchronized void partitionDone(UUID nodeId, int p, boolean updateState) {
            if (updateState && grp.localWalEnabled())
                grp.topology().own(grp.topology().localPartition(p));

            if (isDone())
                return;

            if (grp.eventRecordable(EVT_CACHE_REBALANCE_PART_LOADED))
                rebalanceEvent(p, EVT_CACHE_REBALANCE_PART_LOADED, exchId.discoveryEvent());

            IgniteDhtDemandedPartitionsMap parts = remaining.get(nodeId);

            assert parts != null : "Remaining not found [grp=" + grp.cacheOrGroupName() + ", fromNode=" + nodeId +
                ", part=" + p + "]";

            boolean rmvd = parts.remove(p);

            assert rmvd : "Partition already done [grp=" + grp.cacheOrGroupName() + ", fromNode=" + nodeId +
                ", part=" + p + ", left=" + parts + "]";

                if (rmvd)
                    partitionsLeft.decrementAndGet();

            if (parts.isEmpty()) {
                int remainingRoutines = remaining.size() - 1;

                U.log(log, "Completed " + ((remainingRoutines == 0 ? "(final) " : "") +
                    "rebalancing [grp=" + grp.cacheOrGroupName() +
                    ", supplier=" + nodeId +
                    ", topVer=" + topologyVersion() +
                    ", progress=" + (routines - remainingRoutines) + "/" + routines + "]"));

                remaining.remove(nodeId);
            }

            checkIsDone();
        }

        /**
         * @param part Partition.
         * @param type Type.
         * @param discoEvt Discovery event.
         */
        private void rebalanceEvent(int part, int type, DiscoveryEvent discoEvt) {
            assert discoEvt != null;

            grp.addRebalanceEvent(part, type, discoEvt.eventNode(), discoEvt.type(), discoEvt.timestamp());
        }

        /**
         * @param type Type.
         * @param discoEvt Discovery event.
         */
        private void rebalanceEvent(int type, DiscoveryEvent discoEvt) {
            rebalanceEvent(-1, type, discoEvt);
        }

        /**
         *
         */
        private void checkIsDone() {
            checkIsDone(false);
        }

        /**
         * @param cancelled Is cancelled.
         */
        private void checkIsDone(boolean cancelled) {
            if (remaining.isEmpty()) {
                sendRebalanceFinishedEvent();

                if (log.isInfoEnabled())
                    log.info("Completed rebalance future: " + this);

                if (log.isDebugEnabled())
                    log.debug("Partitions have been scheduled to resend [reason=" +
                        "Rebalance is done, grp=" + grp.cacheOrGroupName() + "]");

                if (!cancelled)
                    ctx.exchange().scheduleResendPartitions();

                Collection<Integer> m = new HashSet<>();

                for (Map.Entry<UUID, Collection<Integer>> e : missed.entrySet()) {
                    if (e.getValue() != null && !e.getValue().isEmpty())
                        m.addAll(e.getValue());
                }

                if (!m.isEmpty()) {
                    U.log(log, "Reassigning partitions that were missed [parts=" + m +
                        ", grpId=" + grp.groupId() +
                        ", grpName=" + grp.cacheOrGroupName() +
                        ", topVer=" + topVer + ']');

                    onDone(false); // Finished but has missed partitions, will force dummy exchange

                    ctx.exchange().forceReassign(exchId);

                    return;
                }

                if (!cancelled && !grp.preloader().syncFuture().isDone())
                    ((GridFutureAdapter)grp.preloader().syncFuture()).onDone();

                onDone(!cancelled);
            }
        }

        /**
         * @return Collection of supplier nodes. Value {@code empty} means rebalance already finished.
         */
        private synchronized Collection<UUID> remainingNodes() {
            return remaining.keySet();
        }

        /**
         *
         */
        private void sendRebalanceStartedEvent() {
            if (grp.eventRecordable(EVT_CACHE_REBALANCE_STARTED))
                rebalanceEvent(EVT_CACHE_REBALANCE_STARTED, exchId.discoveryEvent());
        }

        /**
         *
         */
        private void sendRebalanceFinishedEvent() {
            if (grp.eventRecordable(EVT_CACHE_REBALANCE_STOPPED))
                rebalanceEvent(EVT_CACHE_REBALANCE_STOPPED, exchId.discoveryEvent());
        }

        /**
         * @param otherAssignments Newest assigmnets.
         *
         * @return {@code True} when future compared with other, {@code False} otherwise.
         */
        public boolean compatibleWith(GridDhtPreloaderAssignments otherAssignments) {
            if (isInitial() || ((GridDhtPreloader)grp.preloader()).disableRebalancingCancellationOptimization())
                return false;

            if (ctx.exchange().lastAffinityChangedTopologyVersion(topVer).equals(
                ctx.exchange().lastAffinityChangedTopologyVersion(otherAssignments.topologyVersion()))) {
                if (log.isDebugEnabled())
                    log.debug("Rebalancing is forced on the same topology [grp="
                        + grp.cacheOrGroupName() + ", " + "top=" + topVer + ']');

                return false;
            }

            if (otherAssignments.affinityReassign()) {
                if (log.isDebugEnabled())
                    log.debug("Some of owned partitions were reassigned through coordinator [grp="
                        + grp.cacheOrGroupName() + ", " + "init=" + topVer + " ,other=" + otherAssignments.topologyVersion() + ']');

                return false;
            }

            Set<Integer> p0 = new HashSet<>();
            Set<Integer> p1 = new HashSet<>();

            // Not compatible if a supplier has left.
            for (ClusterNode node : rebalancingParts.keySet()) {
                if (!grp.cacheObjectContext().kernalContext().discovery().alive(node))
                    return false;
            }

            for (Set<Integer> partitions : rebalancingParts.values())
                p0.addAll(partitions);

            for (GridDhtPartitionDemandMessage message : otherAssignments.values()) {
                p1.addAll(message.partitions().fullSet());
                p1.addAll(message.partitions().historicalSet());
            }

            // Not compatible if not a subset.
            if (!p0.containsAll(p1))
                return false;

            p1 = Stream.concat(grp.affinity().cachedAffinity(otherAssignments.topologyVersion())
                .primaryPartitions(ctx.localNodeId()).stream(), grp.affinity()
                .cachedAffinity(otherAssignments.topologyVersion()).backupPartitions(ctx.localNodeId()).stream())
                .collect(Collectors.toSet());

            NavigableSet<AffinityTopologyVersion> toCheck = grp.affinity().cachedVersions()
                .headSet(otherAssignments.topologyVersion(), false);

            if (!toCheck.contains(topVer)) {
                log.warning("History is not enough for checking compatible last rebalance, new rebalance started " +
                    "[grp=" + grp.cacheOrGroupName() + ", lastTop=" + topVer + ']');

                return false;
            }

            for (AffinityTopologyVersion previousTopVer : toCheck.descendingSet()) {
                if (previousTopVer.before(topVer))
                    break;

                if (!ctx.exchange().lastAffinityChangedTopologyVersion(previousTopVer).equals(previousTopVer))
                    continue;

                p0 = Stream.concat(grp.affinity().cachedAffinity(previousTopVer).primaryPartitions(ctx.localNodeId()).stream(),
                    grp.affinity().cachedAffinity(previousTopVer).backupPartitions(ctx.localNodeId()).stream())
                    .collect(Collectors.toSet());

                // Not compatible if owners are different.
                if (!p0.equals(p1))
                    return false;
            }

            return true;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(RebalanceFuture.class, this);
        }
    }
}

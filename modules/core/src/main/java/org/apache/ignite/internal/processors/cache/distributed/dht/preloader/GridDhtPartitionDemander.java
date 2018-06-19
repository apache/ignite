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
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
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
import org.apache.ignite.internal.processors.cache.GridCachePartitionExchangeManager;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtInvalidPartitionException;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionTopology;
import org.apache.ignite.internal.processors.timeout.GridTimeoutObject;
import org.apache.ignite.internal.processors.timeout.GridTimeoutObjectAdapter;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.lang.IgniteInClosureX;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.spi.IgniteSpiException;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.events.EventType.EVT_CACHE_REBALANCE_OBJECT_LOADED;
import static org.apache.ignite.events.EventType.EVT_CACHE_REBALANCE_PART_LOADED;
import static org.apache.ignite.events.EventType.EVT_CACHE_REBALANCE_STARTED;
import static org.apache.ignite.events.EventType.EVT_CACHE_REBALANCE_STOPPED;
import static org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionState.MOVING;
import static org.apache.ignite.internal.processors.dr.GridDrType.DR_NONE;
import static org.apache.ignite.internal.processors.dr.GridDrType.DR_PRELOAD;

/**
 * Thread pool for requesting partitions from other nodes and populating local cache.
 */
@SuppressWarnings("NonConstantFieldWithUpperCaseName")
public class GridDhtPartitionDemander {
    /** */
    private final GridCacheSharedContext<?, ?> ctx;

    /** */
    private final CacheGroupContext grp;

    /** */
    private final IgniteLogger log;

    /** Preload predicate. */
    private IgnitePredicate<GridCacheEntryInfo> preloadPred;

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

    /** Cached rebalance topics. */
    private final Map<Integer, Object> rebalanceTopics;

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

        for (int idx = 0; idx < grp.shared().kernalContext().config().getRebalanceThreadPoolSize(); idx++)
            tops.put(idx, GridCachePartitionExchangeManager.rebalanceTopic(idx));

        rebalanceTopics = tops;
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
     * Sets preload predicate for demand pool.
     *
     * @param preloadPred Preload predicate.
     */
    void preloadPredicate(IgnitePredicate<GridCacheEntryInfo> preloadPred) {
        this.preloadPred = preloadPred;
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
     * @param fut Future.
     * @return {@code True} if topology changed.
     */
    private boolean topologyChanged(RebalanceFuture fut) {
        return
            !grp.affinity().lastVersion().equals(fut.topologyVersion()) || // Topology already changed.
                fut != rebalanceFut; // Same topology, but dummy exchange forced because of missing partitions.
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
     * Initiates new rebalance process from given {@code assignments}.
     * If previous rebalance is not finished method cancels it.
     * In case of delayed rebalance method schedules new with configured delay.
     *
     * @param assignments Assignments.
     * @param force {@code True} if dummy reassign.
     * @param rebalanceId Rebalance id.
     * @param next Runnable responsible for cache rebalancing start.
     * @param forcedRebFut External future for forced rebalance.
     * @return Rebalancing runnable.
     */
    Runnable addAssignments(
        final GridDhtPreloaderAssignments assignments,
        boolean force,
        long rebalanceId,
        final Runnable next,
        @Nullable final GridCompoundFuture<Boolean, Boolean> forcedRebFut
    ) {
        if (log.isDebugEnabled())
            log.debug("Adding partition assignments: " + assignments);

        assert force == (forcedRebFut != null);

        long delay = grp.config().getRebalanceDelay();

        if ((delay == 0 || force) && assignments != null) {
            final RebalanceFuture oldFut = rebalanceFut;

            final RebalanceFuture fut = new RebalanceFuture(grp, assignments, log, rebalanceId);

            if (!grp.localWalEnabled())
                fut.listen(new IgniteInClosureX<IgniteInternalFuture<Boolean>>() {
                    @Override public void applyx(IgniteInternalFuture<Boolean> future) throws IgniteCheckedException {
                        if (future.get())
                            ctx.walState().onGroupRebalanceFinished(grp.groupId(), assignments.topologyVersion());
                    }
                });

            if (!oldFut.isInitial())
                oldFut.cancel();
            else
                fut.listen(f -> oldFut.onDone(f.result()));

            if (forcedRebFut != null)
                forcedRebFut.add(fut);

            rebalanceFut = fut;

            for (final GridCacheContext cctx : grp.caches()) {
                if (cctx.statisticsEnabled()) {
                    final CacheMetricsImpl metrics = cctx.cache().metrics0();

                    metrics.clearRebalanceCounters();

                    for (GridDhtPartitionDemandMessage msg : assignments.values()) {
                        for (Integer partId : msg.partitions().fullSet()) {
                            metrics.onRebalancingKeysCountEstimateReceived(grp.topology().globalPartSizes().get(partId));
                        }

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

            if (assignments.cancelled()) { // Pending exchange.
                if (log.isDebugEnabled())
                    log.debug("Rebalancing skipped due to cancelled assignments.");

                fut.onDone(false);

                fut.sendRebalanceFinishedEvent();

                return null;
            }

            if (assignments.isEmpty()) { // Nothing to rebalance.
                if (log.isDebugEnabled())
                    log.debug("Rebalancing skipped due to empty assignments.");

                fut.onDone(true);

                ((GridFutureAdapter)grp.preloader().syncFuture()).onDone();

                fut.sendRebalanceFinishedEvent();

                return null;
            }

            return () -> {
                if (next != null)
                    fut.listen(f -> {
                        try {
                            if (f.get()) // Not cancelled.
                                next.run(); // Starts next cache rebalancing (according to the order).
                        }
                        catch (IgniteCheckedException e) {
                            if (log.isDebugEnabled())
                                log.debug(e.getMessage());
                        }
                    });

                requestPartitions(fut, assignments);
            };
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
     * Asynchronously sends initial demand messages formed from {@code assignments} and initiates supply-demand processes.
     *
     * For each node participating in rebalance process method distributes set of partitions for that node to several stripes (topics).
     * It means that each stripe containing a subset of partitions can be processed in parallel.
     * The number of stripes are controlled by {@link IgniteConfiguration#getRebalanceThreadPoolSize()} property.
     *
     * Partitions that can be rebalanced using only WAL are called historical, others are called full.
     *
     * Before sending messages, method awaits partitions clearing for full partitions.
     *
     * @param fut Rebalance future.
     * @param assignments Assignments.
     */
    private void requestPartitions(final RebalanceFuture fut, GridDhtPreloaderAssignments assignments) {
        assert fut != null;

        if (topologyChanged(fut)) {
            fut.cancel();

            return;
        }

        if (!ctx.kernalContext().grid().isRebalanceEnabled()) {
            if (log.isDebugEnabled())
                log.debug("Cancel partition demand because rebalance disabled on current node.");

            fut.cancel();

            return;
        }

        synchronized (fut) { // Synchronized to prevent consistency issues in case of parallel cancellation.
            if (fut.isDone())
                return;

            // Must add all remaining node before send first request, for avoid race between add remaining node and
            // processing response, see checkIsDone(boolean).
            for (Map.Entry<ClusterNode, GridDhtPartitionDemandMessage> e : assignments.entrySet()) {
                UUID nodeId = e.getKey().id();

                IgniteDhtDemandedPartitionsMap parts = e.getValue().partitions();

                assert parts != null : "Partitions are null [grp=" + grp.cacheOrGroupName() + ", fromNode=" + nodeId + "]";

                fut.remaining.put(nodeId, new T2<>(U.currentTimeMillis(), parts));
            }
        }

        final CacheConfiguration cfg = grp.config();

        for (Map.Entry<ClusterNode, GridDhtPartitionDemandMessage> e : assignments.entrySet()) {
            final ClusterNode node = e.getKey();

            GridDhtPartitionDemandMessage d = e.getValue();

            final IgniteDhtDemandedPartitionsMap parts;
            synchronized (fut) { // Synchronized to prevent consistency issues in case of parallel cancellation.
                if (fut.isDone())
                    break;

                parts = fut.remaining.get(node.id()).get2();

                U.log(log, "Starting rebalancing [grp=" + grp.cacheOrGroupName()
                        + ", mode=" + cfg.getRebalanceMode() + ", fromNode=" + node.id() + ", partitionsCount=" + parts.size()
                        + ", topology=" + fut.topologyVersion() + ", rebalanceId=" + fut.rebalanceId + "]");
            }

            int totalStripes = ctx.gridConfig().getRebalanceThreadPoolSize();

            int stripes = totalStripes;

            final List<IgniteDhtDemandedPartitionsMap> stripePartitions = new ArrayList<>(stripes);
            for (int i = 0; i < stripes; i++)
                stripePartitions.add(new IgniteDhtDemandedPartitionsMap());

            // Reserve one stripe for historical partitions.
            if (parts.hasHistorical()) {
                stripePartitions.set(stripes - 1, new IgniteDhtDemandedPartitionsMap(parts.historicalMap(), null));

                if (stripes > 1)
                    stripes--;
            }

            // Distribute full partitions across other stripes.
            Iterator<Integer> it = parts.fullSet().iterator();
            for (int i = 0; it.hasNext(); i++)
                stripePartitions.get(i % stripes).addFull(it.next());

            for (int stripe = 0; stripe < totalStripes; stripe++) {
                if (!stripePartitions.get(stripe).isEmpty()) {
                    // Create copy of demand message with new striped partitions map.
                    final GridDhtPartitionDemandMessage demandMsg = d.withNewPartitionsMap(stripePartitions.get(stripe));

                    demandMsg.topic(rebalanceTopics.get(stripe));
                    demandMsg.rebalanceId(fut.rebalanceId);
                    demandMsg.timeout(cfg.getRebalanceTimeout());

                    final int topicId = stripe;

                    IgniteInternalFuture<?> clearAllFuture = clearFullPartitions(fut, demandMsg.partitions().fullSet());

                    // Start rebalancing after clearing full partitions is finished.
                    clearAllFuture.listen(f -> ctx.kernalContext().closure().runLocalSafe(() -> {
                        if (fut.isDone())
                            return;

                        try {
                            ctx.io().sendOrderedMessage(node, rebalanceTopics.get(topicId),
                                demandMsg.convertIfNeeded(node.version()), grp.ioPolicy(), demandMsg.timeout());

                            // Cleanup required in case partitions demanded in parallel with cancellation.
                            synchronized (fut) {
                                if (fut.isDone())
                                    fut.cleanupRemoteContexts(node.id());
                            }

                            if (log.isDebugEnabled())
                                log.debug("Requested rebalancing [from node=" + node.id() + ", listener index=" +
                                    topicId + " " + demandMsg.rebalanceId() + ", partitions count=" + stripePartitions.get(topicId).size() +
                                    " (" + stripePartitions.get(topicId).partitionsList() + ")]");
                        }
                        catch (IgniteCheckedException e1) {
                            ClusterTopologyCheckedException cause = e1.getCause(ClusterTopologyCheckedException.class);

                            if (cause != null)
                                log.warning("Failed to send initial demand request to node. " + e1.getMessage());
                            else
                                log.error("Failed to send initial demand request to node.", e1);

                            fut.cancel();
                        }
                        catch (Throwable th) {
                            log.error("Runtime error caught during initial demand request sending.", th);

                            fut.cancel();
                        }
                    }, true));
                }
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
                                log.debug("Remaining clearing partitions [grp=" + grp.cacheOrGroupName()
                                    + ", remaining=" + remaining + "]");

                            if (remaining == 0)
                                clearAllFuture.onDone();
                        }
                    }
                    else {
                        clearAllFuture.onDone();
                    }
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

                if (log.isDebugEnabled())
                    log.debug("Remaining clearing partitions [grp=" + grp.cacheOrGroupName()
                        + ", remaining=" + remaining + "]");

                if (remaining == 0)
                    clearAllFuture.onDone();
            }
        }

        return clearAllFuture;
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
     * @param topicId Topic id.
     * @param nodeId Node id.
     * @param supply Supply message.
     */
    public void handleSupplyMessage(
        int topicId,
        final UUID nodeId,
        final GridDhtPartitionSupplyMessage supply
    ) {
        AffinityTopologyVersion topVer = supply.topologyVersion();

        final RebalanceFuture fut = rebalanceFut;

        ClusterNode node = ctx.node(nodeId);

        if (node == null)
            return;

        if (topologyChanged(fut)) // Topology already changed (for the future that supply message based on).
            return;

        if (!fut.isActual(supply.rebalanceId())) {
            // Current future have another rebalance id.
            // Supple message based on another future.
            return;
        }

        if (log.isDebugEnabled())
            log.debug("Received supply message [grp=" + grp.cacheOrGroupName() + ", msg=" + supply + ']');

        // Check whether there were class loading errors on unmarshal
        if (supply.classError() != null) {
            U.warn(log, "Rebalancing from node cancelled [grp=" + grp.cacheOrGroupName() + ", node=" + nodeId +
                "]. Class got undeployed during preloading: " + supply.classError());

            fut.cancel(nodeId);

            return;
        }

        final GridDhtPartitionTopology top = grp.topology();

        if (grp.sharedGroup()) {
            for (GridCacheContext cctx : grp.caches()) {
                if (cctx.statisticsEnabled()) {
                    long keysCnt = supply.keysForCache(cctx.cacheId());

                    if (keysCnt != -1)
                        cctx.cache().metrics0().onRebalancingKeysCountEstimateReceived(keysCnt);

                    // Can not be calculated per cache.
                    cctx.cache().metrics0().onRebalanceBatchReceived(supply.messageSize());
                }
            }
        }
        else {
            GridCacheContext cctx = grp.singleCacheContext();

            if (cctx.statisticsEnabled()) {
                if (supply.estimatedKeysCount() != -1)
                    cctx.cache().metrics0().onRebalancingKeysCountEstimateReceived(supply.estimatedKeysCount());

                cctx.cache().metrics0().onRebalanceBatchReceived(supply.messageSize());
            }
        }

        try {
            AffinityAssignment aff = grp.affinity().cachedAffinity(topVer);

            ctx.database().checkpointReadLock();

            try {
                // Preload.
                for (Map.Entry<Integer, CacheEntryInfoCollection> e : supply.infos().entrySet()) {
                    int p = e.getKey();

                    if (aff.get(p).contains(ctx.localNode())) {
                        GridDhtLocalPartition part = top.localPartition(p, topVer, true);

                        assert part != null;

                        boolean last = supply.last().containsKey(p);

                        if (part.state() == MOVING) {
                            boolean reserved = part.reserve();

                            assert reserved : "Failed to reserve partition [igniteInstanceName=" +
                                ctx.igniteInstanceName() + ", grp=" + grp.cacheOrGroupName() + ", part=" + part + ']';

                            part.lock();

                            try {
                                // Loop through all received entries and try to preload them.
                                for (GridCacheEntryInfo entry : e.getValue().infos()) {
                                    if (!preloadEntry(node, p, entry, topVer)) {
                                        if (log.isDebugEnabled())
                                            log.debug("Got entries for invalid partition during " +
                                                "preloading (will skip) [p=" + p + ", entry=" + entry + ']');

                                        break;
                                    }

                                    for (GridCacheContext cctx : grp.caches()) {
                                        if (cctx.statisticsEnabled())
                                            cctx.cache().metrics0().onRebalanceKeyReceived();
                                    }
                                }

                                // If message was last for this partition,
                                // then we take ownership.
                                if (last) {
                                    fut.partitionDone(nodeId, p, true);

                                    if (log.isDebugEnabled())
                                        log.debug("Finished rebalancing partition: " + part);
                                }
                            }
                            finally {
                                part.unlock();
                                part.release();
                            }
                        }
                        else {
                            if (last)
                                fut.partitionDone(nodeId, p, false);

                            if (log.isDebugEnabled())
                                log.debug("Skipping rebalancing partition (state is not MOVING): " + part);
                        }
                    }
                    else {
                        fut.partitionDone(nodeId, p, false);

                        if (log.isDebugEnabled())
                            log.debug("Skipping rebalancing partition (it does not belong on current node): " + p);
                    }
                }
            }
            finally {
                ctx.database().checkpointReadUnlock();
            }

            // Only request partitions based on latest topology version.
            for (Integer miss : supply.missed()) {
                if (aff.get(miss).contains(ctx.localNode()))
                    fut.partitionMissed(nodeId, miss);
            }

            for (Integer miss : supply.missed())
                fut.partitionDone(nodeId, miss, false);

            GridDhtPartitionDemandMessage d = new GridDhtPartitionDemandMessage(
                supply.rebalanceId(),
                supply.topologyVersion(),
                grp.groupId());

            d.timeout(grp.config().getRebalanceTimeout());

            d.topic(rebalanceTopics.get(topicId));

            if (!topologyChanged(fut) && !fut.isDone()) {
                // Send demand message.
                try {
                    ctx.io().sendOrderedMessage(node, rebalanceTopics.get(topicId),
                        d.convertIfNeeded(node.version()), grp.ioPolicy(), grp.config().getRebalanceTimeout());
                }
                catch (ClusterTopologyCheckedException e) {
                    if (log.isDebugEnabled()) {
                        log.debug("Node left during rebalancing [grp=" + grp.cacheOrGroupName() +
                            ", node=" + node.id() + ", msg=" + e.getMessage() + ']');
                    }
                }
            }
        }
        catch (IgniteSpiException | IgniteCheckedException e) {
            LT.error(log, e, "Error during rebalancing [grp=" + grp.cacheOrGroupName() +
                ", srcNode=" + node.id() +
                ", err=" + e + ']');
        }
    }

    /**
     * Adds {@code entry} to partition {@code p}.
     *
     * @param from Node which sent entry.
     * @param p Partition id.
     * @param entry Preloaded entry.
     * @param topVer Topology version.
     * @return {@code False} if partition has become invalid during preloading.
     * @throws IgniteInterruptedCheckedException If interrupted.
     */
    private boolean preloadEntry(
        ClusterNode from,
        int p,
        GridCacheEntryInfo entry,
        AffinityTopologyVersion topVer
    ) throws IgniteCheckedException {
        assert ctx.database().checkpointLockIsHeldByThread();

        try {
            GridCacheEntryEx cached = null;

            try {
                GridCacheContext cctx = grp.sharedGroup() ? ctx.cacheContext(entry.cacheId()) : grp.singleCacheContext();

                cached = cctx.dhtCache().entryEx(entry.key());

                if (log.isDebugEnabled())
                    log.debug("Rebalancing key [key=" + entry.key() + ", part=" + p + ", node=" + from.id() + ']');

                if (preloadPred == null || preloadPred.apply(entry)) {
                    if (cached.initialValue(
                        entry.value(),
                        entry.version(),
                        entry.ttl(),
                        entry.expireTime(),
                        true,
                        topVer,
                        cctx.isDrEnabled() ? DR_PRELOAD : DR_NONE,
                        false
                    )) {
                        cctx.evicts().touch(cached, topVer); // Start tracking.

                        if (cctx.events().isRecordable(EVT_CACHE_REBALANCE_OBJECT_LOADED) && !cached.isInternal())
                            cctx.events().addEvent(cached.partition(), cached.key(), cctx.localNodeId(),
                                (IgniteUuid)null, null, EVT_CACHE_REBALANCE_OBJECT_LOADED, entry.value(), true, null,
                                false, null, null, null, true);
                    }
                    else {
                        cctx.evicts().touch(cached, topVer); // Start tracking.

                        if (log.isDebugEnabled())
                            log.debug("Rebalancing entry is already in cache (will ignore) [key=" + cached.key() +
                                ", part=" + p + ']');
                    }
                }
                else if (log.isDebugEnabled())
                    log.debug("Rebalance predicate evaluated to false for entry (will ignore): " + entry);
            }
            catch (GridCacheEntryRemovedException ignored) {
                if (log.isDebugEnabled())
                    log.debug("Entry has been concurrently removed while rebalancing (will ignore) [key=" +
                        cached.key() + ", part=" + p + ']');
            }
            catch (GridDhtInvalidPartitionException ignored) {
                if (log.isDebugEnabled())
                    log.debug("Partition became invalid during rebalancing (will ignore): " + p);

                return false;
            }
        }
        catch (IgniteInterruptedCheckedException e) {
            throw e;
        }
        catch (IgniteCheckedException e) {
            throw new IgniteCheckedException("Failed to cache rebalanced entry (will stop rebalancing) [local=" +
                ctx.localNode() + ", node=" + from.id() + ", key=" + entry.key() + ", part=" + p + ']', e);
        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtPartitionDemander.class, this);
    }

    /**
     *
     */
    public static class RebalanceFuture extends GridFutureAdapter<Boolean> {
        /** */
        private final GridCacheSharedContext<?, ?> ctx;

        /** */
        private final CacheGroupContext grp;

        /** */
        private final IgniteLogger log;

        /** Remaining. T2: startTime, partitions */
        private final Map<UUID, T2<Long, IgniteDhtDemandedPartitionsMap>> remaining = new HashMap<>();

        /** Missed. */
        private final Map<UUID, Collection<Integer>> missed = new HashMap<>();

        /** Exchange ID. */
        @GridToStringExclude
        private final GridDhtPartitionExchangeId exchId;

        /** Topology version. */
        private final AffinityTopologyVersion topVer;

        /** Unique (per demander) rebalance id. */
        private final long rebalanceId;

        /**
         * @param grp Cache group.
         * @param assignments Assignments.
         * @param log Logger.
         * @param rebalanceId Rebalance id.
         */
        RebalanceFuture(
            CacheGroupContext grp,
            GridDhtPreloaderAssignments assignments,
            IgniteLogger log,
            long rebalanceId) {
            assert assignments != null;

            exchId = assignments.exchangeId();
            topVer = assignments.topologyVersion();

            this.grp = grp;
            this.log = log;
            this.rebalanceId = rebalanceId;

            ctx = grp.shared();
        }

        /**
         * Dummy future. Will be done by real one.
         */
        RebalanceFuture() {
            this.exchId = null;
            this.topVer = null;
            this.ctx = null;
            this.grp = null;
            this.log = null;
            this.rebalanceId = -1;
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
        private boolean isInitial() {
            return topVer == null;
        }

        /**
         * Cancels this future.
         *
         * @return {@code True}.
         */
        @Override public boolean cancel() {
            synchronized (this) {
                if (isDone())
                    return true;

                U.log(log, "Cancelled rebalancing from all nodes [topology=" + topologyVersion() + ']');

                if (!ctx.kernalContext().isStopping()) {
                    for (UUID nodeId : remaining.keySet())
                        cleanupRemoteContexts(nodeId);
                }

                remaining.clear();

                checkIsDone(true /* cancelled */);
            }

            return true;
        }

        /**
         * @param nodeId Node id.
         */
        private void cancel(UUID nodeId) {
            synchronized (this) {
                if (isDone())
                    return;

                U.log(log, ("Cancelled rebalancing [cache=" + grp.cacheOrGroupName() +
                    ", fromNode=" + nodeId + ", topology=" + topologyVersion() +
                    ", time=" + (U.currentTimeMillis() - remaining.get(nodeId).get1()) + " ms]"));

                cleanupRemoteContexts(nodeId);

                remaining.remove(nodeId);

                onDone(false); // Finishing rebalance future as non completed.

                checkIsDone(); // But will finish syncFuture only when other nodes are preloaded or rebalancing cancelled.
            }
        }

        /**
         * @param nodeId Node id.
         * @param p Partition id.
         */
        private void partitionMissed(UUID nodeId, int p) {
            synchronized (this) {
                if (isDone())
                    return;

                missed.computeIfAbsent(nodeId, k -> new HashSet<>());

                missed.get(nodeId).add(p);
            }
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

            d.timeout(grp.config().getRebalanceTimeout());

            try {
                for (int idx = 0; idx < ctx.gridConfig().getRebalanceThreadPoolSize(); idx++) {
                    d.topic(GridCachePartitionExchangeManager.rebalanceTopic(idx));

                    ctx.io().sendOrderedMessage(node, GridCachePartitionExchangeManager.rebalanceTopic(idx),
                        d.convertIfNeeded(node.version()), grp.ioPolicy(), grp.config().getRebalanceTimeout());
                }
            }
            catch (IgniteCheckedException ignored) {
                if (log.isDebugEnabled())
                    log.debug("Failed to send failover context cleanup request to node");
            }
        }

        /**
         * @param nodeId Node id.
         * @param p Partition number.
         */
        private void partitionDone(UUID nodeId, int p, boolean updateState) {
            synchronized (this) {
                if (updateState && grp.localWalEnabled())
                    grp.topology().own(grp.topology().localPartition(p));

                if (isDone())
                    return;

                if (grp.eventRecordable(EVT_CACHE_REBALANCE_PART_LOADED))
                    rebalanceEvent(p, EVT_CACHE_REBALANCE_PART_LOADED, exchId.discoveryEvent());

                T2<Long, IgniteDhtDemandedPartitionsMap> t = remaining.get(nodeId);

                assert t != null : "Remaining not found [grp=" + grp.cacheOrGroupName() + ", fromNode=" + nodeId +
                    ", part=" + p + "]";

                IgniteDhtDemandedPartitionsMap parts = t.get2();

                boolean rmvd = parts.remove(p);

                assert rmvd : "Partition already done [grp=" + grp.cacheOrGroupName() + ", fromNode=" + nodeId +
                    ", part=" + p + ", left=" + parts + "]";

                if (parts.isEmpty()) {
                    U.log(log, "Completed " + ((remaining.size() == 1 ? "(final) " : "") +
                            "rebalancing [fromNode=" + nodeId +
                            ", cacheOrGroup=" + grp.cacheOrGroupName() +
                            ", topology=" + topologyVersion() +
                        ", time=" + (U.currentTimeMillis() - t.get1()) + " ms]"));

                    remaining.remove(nodeId);
                }

                checkIsDone();
            }
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

                ctx.exchange().scheduleResendPartitions();

                Collection<Integer> m = new HashSet<>();

                for (Map.Entry<UUID, Collection<Integer>> e : missed.entrySet()) {
                    if (e.getValue() != null && !e.getValue().isEmpty())
                        m.addAll(e.getValue());
                }

                if (!m.isEmpty()) {
                    U.log(log, ("Reassigning partitions that were missed: " + m));

                    onDone(false); //Finished but has missed partitions, will force dummy exchange

                    ctx.exchange().forceReassign(exchId);

                    return;
                }

                if (!cancelled && !grp.preloader().syncFuture().isDone())
                    ((GridFutureAdapter)grp.preloader().syncFuture()).onDone();

                onDone(!cancelled);
            }
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

        /** {@inheritDoc} */
        public String toString() {
            return S.toString(RebalanceFuture.class, this);
        }
    }
}

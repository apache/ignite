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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.processors.affinity.AffinityAssignment;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheEntryInfoCollection;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
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
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.T2;
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
     * Force Rebalance.
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
                    IgniteInternalFuture<Boolean> fut0 = ctx.exchange().forceRebalance(exchFut);

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
     * @param assigns Assignments.
     * @param force {@code True} if dummy reassign.
     * @param cnt Counter.
     * @param next Runnable responsible for cache rebalancing start.
     * @param forcedRebFut External future for forced rebalance.
     * @return Rebalancing runnable.
     */
    Runnable addAssignments(final GridDhtPreloaderAssignments assigns,
        boolean force,
        int cnt,
        final Runnable next,
        @Nullable final GridCompoundFuture<Boolean, Boolean> forcedRebFut) {
        if (log.isDebugEnabled())
            log.debug("Adding partition assignments: " + assigns);

        assert force == (forcedRebFut != null);

        long delay = grp.config().getRebalanceDelay();

        if (delay == 0 || force) {
            final RebalanceFuture oldFut = rebalanceFut;

            final RebalanceFuture fut = new RebalanceFuture(grp, assigns, log, cnt);

            if (!oldFut.isInitial())
                oldFut.cancel();
            else {
                fut.listen(new CI1<IgniteInternalFuture<Boolean>>() {
                    @Override public void apply(IgniteInternalFuture<Boolean> fut) {
                        oldFut.onDone(fut.result());
                    }
                });
            }

            if (forcedRebFut != null)
                forcedRebFut.add(fut);

            rebalanceFut = fut;

            fut.sendRebalanceStartedEvent();

            if (assigns.cancelled()) { // Pending exchange.
                if (log.isDebugEnabled())
                    log.debug("Rebalancing skipped due to cancelled assignments.");

                fut.onDone(false);

                fut.sendRebalanceFinishedEvent();

                return null;
            }

            if (assigns.isEmpty()) { // Nothing to rebalance.
                if (log.isDebugEnabled())
                    log.debug("Rebalancing skipped due to empty assignments.");

                fut.onDone(true);

                ((GridFutureAdapter)grp.preloader().syncFuture()).onDone();

                fut.sendRebalanceFinishedEvent();

                return null;
            }

            return new Runnable() {
                @Override public void run() {
                    if (next != null)
                        fut.listen(new CI1<IgniteInternalFuture<Boolean>>() {
                            @Override public void apply(IgniteInternalFuture<Boolean> f) {
                                try {
                                    if (f.get()) // Not cancelled.
                                        next.run(); // Starts next cache rebalancing (according to the order).
                                }
                                catch (IgniteCheckedException e) {
                                    if (log.isDebugEnabled())
                                        log.debug(e.getMessage());
                                }
                            }
                        });

                    requestPartitions(fut, assigns);
                }
            };
        }
        else if (delay > 0) {
            GridTimeoutObject obj = lastTimeoutObj.get();

            if (obj != null)
                ctx.time().removeTimeoutObject(obj);

            final GridDhtPartitionsExchangeFuture exchFut = lastExchangeFut;

            assert exchFut != null : "Delaying rebalance process without topology event.";

            obj = new GridTimeoutObjectAdapter(delay) {
                @Override public void onTimeout() {
                    exchFut.listen(new CI1<IgniteInternalFuture<AffinityTopologyVersion>>() {
                        @Override public void apply(IgniteInternalFuture<AffinityTopologyVersion> f) {
                            ctx.exchange().forceRebalance(exchFut);
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
     * @param fut Rebalance future.
     * @param assigns Assignments.
     * @throws IgniteCheckedException If failed.
     */
    private void requestPartitions(final RebalanceFuture fut, GridDhtPreloaderAssignments assigns) {
        assert fut != null;

        if (topologyChanged(fut)) {
            fut.cancel();

            return;
        }

        synchronized (fut) { // Synchronized to prevent consistency issues in case of parallel cancellation.
            if (fut.isDone())
                return;

            // Must add all remaining node before send first request, for avoid race between add remaining node and
            // processing response, see checkIsDone(boolean).
            for (Map.Entry<ClusterNode, GridDhtPartitionDemandMessage> e : assigns.entrySet()) {
                UUID nodeId = e.getKey().id();

                Collection<Integer> parts= e.getValue().partitions();

                assert parts != null : "Partitions are null [grp=" + grp.cacheOrGroupName() + ", fromNode=" + nodeId + "]";

                fut.remaining.put(nodeId, new T2<>(U.currentTimeMillis(), parts));
            }
        }

        final CacheConfiguration cfg = grp.config();

        int lsnrCnt = ctx.gridConfig().getRebalanceThreadPoolSize();

        for (Map.Entry<ClusterNode, GridDhtPartitionDemandMessage> e : assigns.entrySet()) {
            final ClusterNode node = e.getKey();

            final Collection<Integer> parts = fut.remaining.get(node.id()).get2();

            GridDhtPartitionDemandMessage d = e.getValue();

            U.log(log, "Starting rebalancing [mode=" + cfg.getRebalanceMode() +
                ", fromNode=" + node.id() + ", partitionsCount=" + parts.size() +
                ", topology=" + fut.topologyVersion() + ", updateSeq=" + fut.updateSeq + "]");

            final List<Set<Integer>> sParts = new ArrayList<>(lsnrCnt);

            for (int cnt = 0; cnt < lsnrCnt; cnt++)
                sParts.add(new HashSet<Integer>());

            Iterator<Integer> it = parts.iterator();

            int cnt = 0;

            while (it.hasNext())
                sParts.get(cnt++ % lsnrCnt).add(it.next());

            for (cnt = 0; cnt < lsnrCnt; cnt++) {
                if (!sParts.get(cnt).isEmpty()) {
                    // Create copy.
                    final GridDhtPartitionDemandMessage initD = createDemandMessage(d, sParts.get(cnt));

                    initD.topic(rebalanceTopics.get(cnt));
                    initD.updateSequence(fut.updateSeq);
                    initD.timeout(cfg.getRebalanceTimeout());

                    final int finalCnt = cnt;

                    ctx.kernalContext().closure().runLocalSafe(new Runnable() {
                        @Override public void run() {
                            try {
                                if (!fut.isDone()) {
                                    ctx.io().sendOrderedMessage(node,
                                        rebalanceTopics.get(finalCnt), initD, grp.ioPolicy(), initD.timeout());

                                    // Cleanup required in case partitions demanded in parallel with cancellation.
                                    synchronized (fut) {
                                        if (fut.isDone())
                                            fut.cleanupRemoteContexts(node.id());
                                    }

                                    if (log.isDebugEnabled())
                                        log.debug("Requested rebalancing [from node=" + node.id() + ", listener index=" +
                                            finalCnt + ", partitions count=" + sParts.get(finalCnt).size() +
                                            " (" + partitionsList(sParts.get(finalCnt)) + ")]");
                                }
                            }
                            catch (IgniteCheckedException e) {
                                ClusterTopologyCheckedException cause = e.getCause(ClusterTopologyCheckedException.class);

                                if (cause != null)
                                    log.warning("Failed to send initial demand request to node. " + e.getMessage());
                                else
                                    log.error("Failed to send initial demand request to node.", e);

                                fut.cancel();
                            }
                            catch (Throwable th) {
                                log.error("Runtime error caught during initial demand request sending.", th);

                                fut.cancel();
                            }
                        }
                    }, /*system pool*/true);
                }
            }
        }
    }

    /**
     * @param old Old message.
     * @param parts Partitions to demand.
     * @return New demand message.
     */
    private GridDhtPartitionDemandMessage createDemandMessage(GridDhtPartitionDemandMessage old,
        Collection<Integer> parts) {
        Map<Integer, Long> partCntrs = null;

        for (Integer part : parts) {
            try {
                if (ctx.database().persistenceEnabled()) {
                    if (partCntrs == null)
                        partCntrs = new HashMap<>(parts.size(), 1.0f);

                    GridDhtLocalPartition p = grp.topology().localPartition(part, old.topologyVersion(), false);

                    partCntrs.put(part, p.initialUpdateCounter());
                }
            }
            catch (GridDhtInvalidPartitionException ignore) {
                // Skip this partition.
            }
        }

        return new GridDhtPartitionDemandMessage(old, parts, partCntrs);
    }

    /**
     * @param c Partitions.
     * @return String representation of partitions list.
     */
    private String partitionsList(Collection<Integer> c) {
        List<Integer> s = new ArrayList<>(c);

        Collections.sort(s);

        StringBuilder sb = new StringBuilder();

        int start = -1;

        int prev = -1;

        Iterator<Integer> sit = s.iterator();

        while (sit.hasNext()) {
            int p = sit.next();

            if (start == -1) {
                start = p;
                prev = p;
            }

            if (prev < p - 1) {
                sb.append(start);

                if (start != prev)
                    sb.append("-").append(prev);

                sb.append(", ");

                start = p;
            }

            if (!sit.hasNext()) {
                sb.append(start);

                if (start != p)
                    sb.append("-").append(p);
            }

            prev = p;
        }

        return sb.toString();
    }

    /**
     * @param idx Index.
     * @param id Node id.
     * @param supply Supply.
     */
    public void handleSupplyMessage(
        int idx,
        final UUID id,
        final GridDhtPartitionSupplyMessage supply
    ) {
        AffinityTopologyVersion topVer = supply.topologyVersion();

        final RebalanceFuture fut = rebalanceFut;

        ClusterNode node = ctx.node(id);

        if (node == null)
            return;

        if (!fut.isActual(supply.updateSequence())) // Current future have another update sequence.
            return; // Supple message based on another future.

        if (topologyChanged(fut)) // Topology already changed (for the future that supply message based on).
            return;

        if (log.isDebugEnabled())
            log.debug("Received supply message: " + supply);

        // Check whether there were class loading errors on unmarshal
        if (supply.classError() != null) {
            U.warn(log, "Rebalancing from node cancelled [node=" + id +
                "]. Class got undeployed during preloading: " + supply.classError());

            fut.cancel(id);

            return;
        }

        final GridDhtPartitionTopology top = grp.topology();

        try {
            AffinityAssignment aff = grp.affinity().cachedAffinity(topVer);

            // Preload.
            for (Map.Entry<Integer, CacheEntryInfoCollection> e : supply.infos().entrySet()) {
                int p = e.getKey();

                if (aff.get(p).contains(ctx.localNode())) {
                    GridDhtLocalPartition part = top.localPartition(p, topVer, true);

                    assert part != null;

                    boolean last = supply.last().contains(p);

                    if (part.state() == MOVING) {
                        boolean reserved = part.reserve();

                        assert reserved : "Failed to reserve partition [igniteInstanceName=" +
                            ctx.igniteInstanceName() + ", grp=" + grp.cacheOrGroupName() + ", part=" + part + ']';

                        part.lock();

                        try {
                            // Loop through all received entries and try to preload them.
                            for (GridCacheEntryInfo entry : e.getValue().infos()) {
                                if (!part.preloadingPermitted(entry.key(), entry.version())) {
                                    if (log.isDebugEnabled())
                                        log.debug("Preloading is not permitted for entry due to " +
                                            "evictions [key=" + entry.key() +
                                            ", ver=" + entry.version() + ']');

                                    continue;
                                }

                                if (!preloadEntry(node, p, entry, topVer)) {
                                    if (log.isDebugEnabled())
                                        log.debug("Got entries for invalid partition during " +
                                            "preloading (will skip) [p=" + p + ", entry=" + entry + ']');

                                    break;
                                }
                            }

                            // If message was last for this partition,
                            // then we take ownership.
                            if (last) {
                                top.own(part);

                                fut.partitionDone(id, p);

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
                            fut.partitionDone(id, p);

                        if (log.isDebugEnabled())
                            log.debug("Skipping rebalancing partition (state is not MOVING): " + part);
                    }
                }
                else {
                    fut.partitionDone(id, p);

                    if (log.isDebugEnabled())
                        log.debug("Skipping rebalancing partition (it does not belong on current node): " + p);
                }
            }

            // Only request partitions based on latest topology version.
            for (Integer miss : supply.missed()) {
                if (aff.get(miss).contains(ctx.localNode()))
                    fut.partitionMissed(id, miss);
            }

            for (Integer miss : supply.missed())
                fut.partitionDone(id, miss);

            GridDhtPartitionDemandMessage d = new GridDhtPartitionDemandMessage(
                supply.updateSequence(),
                supply.topologyVersion(),
                grp.groupId());

            d.timeout(grp.config().getRebalanceTimeout());

            d.topic(rebalanceTopics.get(idx));

            if (!topologyChanged(fut) && !fut.isDone()) {
                // Send demand message.
                ctx.io().sendOrderedMessage(node, rebalanceTopics.get(idx),
                    d, grp.ioPolicy(), grp.config().getRebalanceTimeout());
            }
        }
        catch (IgniteCheckedException e) {
            if (log.isDebugEnabled())
                log.debug("Node left during rebalancing [node=" + node.id() +
                    ", msg=" + e.getMessage() + ']');
        }
        catch (IgniteSpiException e) {
            if (log.isDebugEnabled())
                log.debug("Failed to send message to node (current node is stopping?) [node=" + node.id() +
                    ", msg=" + e.getMessage() + ']');
        }
    }

    /**
     * @param pick Node picked for preloading.
     * @param p Partition.
     * @param entry Preloaded entry.
     * @param topVer Topology version.
     * @return {@code False} if partition has become invalid during preloading.
     * @throws IgniteInterruptedCheckedException If interrupted.
     */
    private boolean preloadEntry(
        ClusterNode pick,
        int p,
        GridCacheEntryInfo entry,
        AffinityTopologyVersion topVer
    ) throws IgniteCheckedException {
        ctx.database().checkpointReadLock();

        try {
            GridCacheEntryEx cached = null;

            try {
                GridCacheContext cctx = grp.sharedGroup() ? ctx.cacheContext(entry.cacheId()) : grp.singleCacheContext();

                cached = cctx.dhtCache().entryEx(entry.key());

                if (log.isDebugEnabled())
                    log.debug("Rebalancing key [key=" + entry.key() + ", part=" + p + ", node=" + pick.id() + ']');

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
                ctx.localNode() + ", node=" + pick.id() + ", key=" + entry.key() + ", part=" + p + ']', e);
        }
        finally {
            ctx.database().checkpointReadUnlock();
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
        private final Map<UUID, T2<Long, Collection<Integer>>> remaining = new HashMap<>();

        /** Missed. */
        private final Map<UUID, Collection<Integer>> missed = new HashMap<>();

        /** Exchange future. */
        @GridToStringExclude
        private final GridDhtPartitionsExchangeFuture exchFut;

        /** Topology version. */
        private final AffinityTopologyVersion topVer;

        /** Unique (per demander) sequence id. */
        private final long updateSeq;

        /**
         * @param grp Cache group.
         * @param assigns Assigns.
         * @param log Logger.
         * @param updateSeq Update sequence.
         */
        RebalanceFuture(
            CacheGroupContext grp,
            GridDhtPreloaderAssignments assigns,
            IgniteLogger log,
            long updateSeq) {
            assert assigns != null;

            exchFut = assigns.exchangeFuture();
            topVer = assigns.topologyVersion();

            this.grp = grp;
            this.log = log;
            this.updateSeq = updateSeq;

            ctx= grp.shared();
        }

        /**
         * Dummy future. Will be done by real one.
         */
        RebalanceFuture() {
            this.exchFut = null;
            this.topVer = null;
            this.ctx = null;
            this.grp = null;
            this.log = null;
            this.updateSeq = -1;
        }

        /**
         * @return Topology version.
         */
        public AffinityTopologyVersion topologyVersion() {
            return topVer;
        }

        /**
         * @param updateSeq Update sequence.
         * @return true in case future created for specified updateSeq, false in other case.
         */
        private boolean isActual(long updateSeq) {
            return this.updateSeq == updateSeq;
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
         * @param p P.
         */
        private void partitionMissed(UUID nodeId, int p) {
            synchronized (this) {
                if (isDone())
                    return;

                if (missed.get(nodeId) == null)
                    missed.put(nodeId, new HashSet<Integer>());

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
                -1/* remove supply context signal */,
                this.topologyVersion(),
                grp.groupId());

            d.timeout(grp.config().getRebalanceTimeout());

            try {
                for (int idx = 0; idx < ctx.gridConfig().getRebalanceThreadPoolSize(); idx++) {
                    d.topic(GridCachePartitionExchangeManager.rebalanceTopic(idx));

                    ctx.io().sendOrderedMessage(node, GridCachePartitionExchangeManager.rebalanceTopic(idx),
                        d, grp.ioPolicy(), grp.config().getRebalanceTimeout());
                }
            }
            catch (IgniteCheckedException ignored) {
                if (log.isDebugEnabled())
                    log.debug("Failed to send failover context cleanup request to node");
            }
        }

        /**
         * @param nodeId Node id.
         * @param p P.
         */
        private void partitionDone(UUID nodeId, int p) {
            synchronized (this) {
                if (isDone())
                    return;

                if (grp.eventRecordable(EVT_CACHE_REBALANCE_PART_LOADED))
                    rebalanceEvent(p, EVT_CACHE_REBALANCE_PART_LOADED, exchFut.discoveryEvent());

                T2<Long, Collection<Integer>> t = remaining.get(nodeId);

                assert t != null : "Remaining not found [grp=" + grp.name() + ", fromNode=" + nodeId +
                    ", part=" + p + "]";

                Collection<Integer> parts = t.get2();

                boolean rmvd = parts.remove(p);

                assert rmvd : "Partition already done [grp=" + grp.name() + ", fromNode=" + nodeId +
                    ", part=" + p + ", left=" + parts + "]";

                if (parts.isEmpty()) {
                    U.log(log, "Completed " + ((remaining.size() == 1 ? "(final) " : "") +
                        "rebalancing [fromNode=" + nodeId + ", topology=" + topologyVersion() +
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

                if (log.isDebugEnabled())
                    log.debug("Completed rebalance future: " + this);

                ctx.exchange().scheduleResendPartitions();

                Collection<Integer> m = new HashSet<>();

                for (Map.Entry<UUID, Collection<Integer>> e : missed.entrySet()) {
                    if (e.getValue() != null && !e.getValue().isEmpty())
                        m.addAll(e.getValue());
                }

                if (!m.isEmpty()) {
                    U.log(log, ("Reassigning partitions that were missed: " + m));

                    onDone(false); //Finished but has missed partitions, will force dummy exchange

                    ctx.exchange().forceDummyExchange(true, exchFut);

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
                rebalanceEvent(EVT_CACHE_REBALANCE_STARTED, exchFut.discoveryEvent());
        }

        /**
         *
         */
        private void sendRebalanceFinishedEvent() {
            if (grp.eventRecordable(EVT_CACHE_REBALANCE_STOPPED))
                rebalanceEvent(EVT_CACHE_REBALANCE_STOPPED, exchFut.discoveryEvent());
        }

        /** {@inheritDoc} */
        public String toString() {
            return S.toString(RebalanceFuture.class, this);
        }
    }
}

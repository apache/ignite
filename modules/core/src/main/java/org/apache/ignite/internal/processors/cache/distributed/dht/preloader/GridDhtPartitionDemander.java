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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheEntryInfoCollection;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.GridCacheEntryInfo;
import org.apache.ignite.internal.processors.cache.GridCacheEntryRemovedException;
import org.apache.ignite.internal.processors.cache.GridCachePartitionExchangeManager;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtInvalidPartitionException;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionTopology;
import org.apache.ignite.internal.processors.timeout.GridTimeoutObject;
import org.apache.ignite.internal.processors.timeout.GridTimeoutObjectAdapter;
import org.apache.ignite.internal.util.GridLeanSet;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.CI2;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.spi.IgniteSpiException;
import org.jetbrains.annotations.Nullable;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.events.EventType.EVT_CACHE_REBALANCE_OBJECT_LOADED;
import static org.apache.ignite.events.EventType.EVT_CACHE_REBALANCE_PART_LOADED;
import static org.apache.ignite.events.EventType.EVT_CACHE_REBALANCE_STARTED;
import static org.apache.ignite.events.EventType.EVT_CACHE_REBALANCE_STOPPED;
import static org.apache.ignite.internal.GridTopic.TOPIC_CACHE;
import static org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionState.MOVING;
import static org.apache.ignite.internal.processors.dr.GridDrType.DR_NONE;
import static org.apache.ignite.internal.processors.dr.GridDrType.DR_PRELOAD;

/**
 * Thread pool for requesting partitions from other nodes and populating local cache.
 */
@SuppressWarnings("NonConstantFieldWithUpperCaseName")
public class GridDhtPartitionDemander {
    /** */
    private final GridCacheContext<?, ?> cctx;

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

    /** Demand lock. */
    @Deprecated//Backward compatibility. To be removed in future.
    private final ReadWriteLock demandLock;

    /** DemandWorker index. */
    @Deprecated//Backward compatibility. To be removed in future.
    private final AtomicInteger dmIdx = new AtomicInteger();

    /** DemandWorker. */
    @Deprecated//Backward compatibility. To be removed in future.
    private volatile DemandWorker worker;

    /** Cached rebalance topics. */
    private final Map<Integer, Object> rebalanceTopics;

    /**
     * Started event sent.
     * Make sense for replicated cache only.
     */
    private final AtomicBoolean startedEvtSent = new AtomicBoolean();

    /**
     * Stopped event sent.
     * Make sense for replicated cache only.
     */
    private final AtomicBoolean stoppedEvtSent = new AtomicBoolean();

    /**
     * @param cctx Cctx.
     * @param demandLock Demand lock.
     */
    public GridDhtPartitionDemander(GridCacheContext<?, ?> cctx, ReadWriteLock demandLock) {
        assert cctx != null;

        this.cctx = cctx;
        this.demandLock = demandLock;

        log = cctx.logger(getClass());

        boolean enabled = cctx.rebalanceEnabled() && !cctx.kernalContext().clientNode();

        rebalanceFut = new RebalanceFuture();//Dummy.

        if (!enabled) {
            // Calling onDone() immediately since preloading is disabled.
            rebalanceFut.onDone(true);
            syncFut.onDone();
        }

        Map<Integer, Object> tops = new HashMap<>();

        for (int idx = 0; idx < cctx.gridConfig().getRebalanceThreadPoolSize(); idx++)
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

        DemandWorker dw = worker;

        if (dw != null)
            dw.cancel();

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
            cctx.time().removeTimeoutObject(obj);

        final GridDhtPartitionsExchangeFuture exchFut = lastExchangeFut;

        if (exchFut != null) {
            if (log.isDebugEnabled())
                log.debug("Forcing rebalance event for future: " + exchFut);

            final GridFutureAdapter<Boolean> fut = new GridFutureAdapter<>();

            exchFut.listen(new CI1<IgniteInternalFuture<AffinityTopologyVersion>>() {
                @Override public void apply(IgniteInternalFuture<AffinityTopologyVersion> t) {
                    IgniteInternalFuture<Boolean> fut0 = cctx.shared().exchange().forceRebalance(exchFut);

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
            !cctx.affinity().affinityTopologyVersion().equals(fut.topologyVersion()) || // Topology already changed.
                fut != rebalanceFut; // Same topology, but dummy exchange forced because of missing partitions.
    }

    /**
     * @param part Partition.
     * @param type Type.
     * @param discoEvt Discovery event.
     */
    private void preloadEvent(int part, int type, DiscoveryEvent discoEvt) {
        assert discoEvt != null;

        cctx.events().addPreloadEvent(part, type, discoEvt.eventNode(), discoEvt.type(), discoEvt.timestamp());
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
        @Nullable final GridFutureAdapter<Boolean> forcedRebFut) {
        if (log.isDebugEnabled())
            log.debug("Adding partition assignments: " + assigns);

        assert force == (forcedRebFut != null);

        long delay = cctx.config().getRebalanceDelay();

        if (delay == 0 || force) {
            final RebalanceFuture oldFut = rebalanceFut;

            final RebalanceFuture fut = new RebalanceFuture(assigns, cctx, log, startedEvtSent, stoppedEvtSent, cnt);

            if (!oldFut.isInitial())
                oldFut.cancel();
            else {
                fut.listen(new CI1<IgniteInternalFuture<Boolean>>() {
                    @Override public void apply(IgniteInternalFuture<Boolean> fut) {
                        oldFut.onDone(fut.result());
                    }
                });
            }

             if (forcedRebFut != null) {
                fut.listen(new CI1<IgniteInternalFuture<Boolean>>() {
                    @Override public void apply(IgniteInternalFuture<Boolean> future) {
                        try {
                            forcedRebFut.onDone(future.get());
                        }
                        catch (Exception e) {
                            forcedRebFut.onDone(e);
                        }
                    }
                });
            }

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

                ((GridFutureAdapter)cctx.preloader().syncFuture()).onDone();

                fut.sendRebalanceFinishedEvent();

                return null;
            }

            return new Runnable() {
                @Override public void run() {
                    try {
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

                        if (th instanceof Error)
                            throw th;
                    }
                }
            };
        }
        else if (delay > 0) {
            GridTimeoutObject obj = lastTimeoutObj.get();

            if (obj != null)
                cctx.time().removeTimeoutObject(obj);

            final GridDhtPartitionsExchangeFuture exchFut = lastExchangeFut;

            assert exchFut != null : "Delaying rebalance process without topology event.";

            obj = new GridTimeoutObjectAdapter(delay) {
                @Override public void onTimeout() {
                    exchFut.listen(new CI1<IgniteInternalFuture<AffinityTopologyVersion>>() {
                        @Override public void apply(IgniteInternalFuture<AffinityTopologyVersion> f) {
                            cctx.shared().exchange().forceRebalance(exchFut);
                        }
                    });
                }
            };

            lastTimeoutObj.set(obj);

            cctx.time().addTimeoutObject(obj);
        }

        return null;
    }

    /**
     * @param fut Future.
     * @param assigns Assignments.
     * @throws IgniteCheckedException If failed.
     * @return Partitions were requested.
     */
    private void requestPartitions(
        RebalanceFuture fut,
        GridDhtPreloaderAssignments assigns
    ) throws IgniteCheckedException {
        if (topologyChanged(fut)) {
            fut.cancel();

            return;
        }

        for (Map.Entry<ClusterNode, GridDhtPartitionDemandMessage> e : assigns.entrySet()) {
            final ClusterNode node = e.getKey();

            GridDhtPartitionDemandMessage d = e.getValue();

            fut.appendPartitions(node.id(), d.partitions()); //Future preparation.
        }

        for (Map.Entry<ClusterNode, GridDhtPartitionDemandMessage> e : assigns.entrySet()) {
            final ClusterNode node = e.getKey();

            final CacheConfiguration cfg = cctx.config();

            final Collection<Integer> parts = fut.remaining.get(node.id()).get2();

            GridDhtPartitionDemandMessage d = e.getValue();

            //Check remote node rebalancing API version.
            if (node.version().compareTo(GridDhtPreloader.REBALANCING_VER_2_SINCE) >= 0) {
                U.log(log, "Starting rebalancing [mode=" + cfg.getRebalanceMode() +
                    ", fromNode=" + node.id() + ", partitionsCount=" + parts.size() +
                    ", topology=" + fut.topologyVersion() + ", updateSeq=" + fut.updateSeq + "]");

                int lsnrCnt = cctx.gridConfig().getRebalanceThreadPoolSize();

                List<Set<Integer>> sParts = new ArrayList<>(lsnrCnt);

                for (int cnt = 0; cnt < lsnrCnt; cnt++)
                    sParts.add(new HashSet<Integer>());

                Iterator<Integer> it = parts.iterator();

                int cnt = 0;

                while (it.hasNext())
                    sParts.get(cnt++ % lsnrCnt).add(it.next());

                for (cnt = 0; cnt < lsnrCnt; cnt++) {
                    if (!sParts.get(cnt).isEmpty()) {
                        // Create copy.
                        GridDhtPartitionDemandMessage initD = new GridDhtPartitionDemandMessage(d, sParts.get(cnt));

                        initD.topic(rebalanceTopics.get(cnt));
                        initD.updateSequence(fut.updateSeq);
                        initD.timeout(cctx.config().getRebalanceTimeout());

                        synchronized (fut) {
                            if (!fut.isDone()) {
                                // Future can be already cancelled at this moment and all failovers happened.
                                // New requests will not be covered by failovers.
                                cctx.io().sendOrderedMessage(node,
                                    rebalanceTopics.get(cnt), initD, cctx.ioPolicy(), initD.timeout());
                            }
                        }

                        if (log.isDebugEnabled())
                            log.debug("Requested rebalancing [from node=" + node.id() + ", listener index=" +
                                cnt + ", partitions count=" + sParts.get(cnt).size() +
                                " (" + partitionsList(sParts.get(cnt)) + ")]");
                    }
                }
            }
            else {
                U.log(log, "Starting rebalancing (old api) [cache=" + cctx.name() +
                    ", mode=" + cfg.getRebalanceMode() +
                    ", fromNode=" + node.id() +
                    ", partitionsCount=" + parts.size() +
                    ", topology=" + fut.topologyVersion() +
                    ", updateSeq=" + fut.updateSeq + "]");

                d.timeout(cctx.config().getRebalanceTimeout());
                d.workerId(0);//old api support.

                worker = new DemandWorker(dmIdx.incrementAndGet(), fut);

                worker.run(node, d);
            }
        }
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
        final GridDhtPartitionSupplyMessageV2 supply
    ) {
        AffinityTopologyVersion topVer = supply.topologyVersion();

        final RebalanceFuture fut = rebalanceFut;

        ClusterNode node = cctx.node(id);

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

        final GridDhtPartitionTopology top = cctx.dht().topology();

        try {
            // Preload.
            for (Map.Entry<Integer, CacheEntryInfoCollection> e : supply.infos().entrySet()) {
                int p = e.getKey();

                if (cctx.affinity().partitionLocalNode(p, topVer)) {
                    GridDhtLocalPartition part = top.localPartition(p, topVer, true);

                    assert part != null;

                    boolean last = supply.last().contains(p);

                    if (part.state() == MOVING) {
                        boolean reserved = part.reserve();

                        assert reserved : "Failed to reserve partition [gridName=" +
                            cctx.gridName() + ", cacheName=" + cctx.namex() + ", part=" + part + ']';

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
                if (cctx.affinity().partitionLocalNode(miss, topVer))
                    fut.partitionMissed(id, miss);
            }

            for (Integer miss : supply.missed())
                fut.partitionDone(id, miss);

            GridDhtPartitionDemandMessage d = new GridDhtPartitionDemandMessage(
                supply.updateSequence(), supply.topologyVersion(), cctx.cacheId());

            d.timeout(cctx.config().getRebalanceTimeout());

            d.topic(rebalanceTopics.get(idx));

            if (!topologyChanged(fut) && !fut.isDone()) {
                // Send demand message.
                cctx.io().sendOrderedMessage(node, rebalanceTopics.get(idx),
                    d, cctx.ioPolicy(), cctx.config().getRebalanceTimeout());
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
        try {
            GridCacheEntryEx cached = null;

            try {
                cached = cctx.dht().entryEx(entry.key());

                if (log.isDebugEnabled())
                    log.debug("Rebalancing key [key=" + entry.key() + ", part=" + p + ", node=" + pick.id() + ']');

                if (cctx.dht().isIgfsDataCache() &&
                    cctx.dht().igfsDataSpaceUsed() > cctx.dht().igfsDataSpaceMax()) {
                    LT.error(log, null, "Failed to rebalance IGFS data cache (IGFS space size exceeded maximum " +
                        "value, will ignore rebalance entries)");

                    if (cached.markObsoleteIfEmpty(null))
                        cached.context().cache().removeEntry(cached);

                    return true;
                }

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
                        if (cctx.isSwapOrOffheapEnabled())
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
                cctx.nodeId() + ", node=" + pick.id() + ", key=" + entry.key() + ", part=" + p + ']', e);
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
        private static final long serialVersionUID = 1L;

        /** Should EVT_CACHE_REBALANCE_STARTED event be sent or not. */
        private final AtomicBoolean startedEvtSent;

        /** Should EVT_CACHE_REBALANCE_STOPPED event be sent or not. */
        private final AtomicBoolean stoppedEvtSent;

        /** */
        private final GridCacheContext<?, ?> cctx;

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
         * @param assigns Assigns.
         * @param cctx Context.
         * @param log Logger.
         * @param startedEvtSent Start event sent flag.
         * @param stoppedEvtSent Stop event sent flag.
         * @param updateSeq Update sequence.
         */
        RebalanceFuture(GridDhtPreloaderAssignments assigns,
            GridCacheContext<?, ?> cctx,
            IgniteLogger log,
            AtomicBoolean startedEvtSent,
            AtomicBoolean stoppedEvtSent,
            long updateSeq) {
            assert assigns != null;

            this.exchFut = assigns.exchangeFuture();
            this.topVer = assigns.topologyVersion();
            this.cctx = cctx;
            this.log = log;
            this.startedEvtSent = startedEvtSent;
            this.stoppedEvtSent = stoppedEvtSent;
            this.updateSeq = updateSeq;
        }

        /**
         * Dummy future. Will be done by real one.
         */
        public RebalanceFuture() {
            this.exchFut = null;
            this.topVer = null;
            this.cctx = null;
            this.log = null;
            this.startedEvtSent = null;
            this.stoppedEvtSent = null;
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
         * @param nodeId Node id.
         * @param parts Parts.
         */
        private void appendPartitions(UUID nodeId, Collection<Integer> parts) {
            synchronized (this) {
                assert parts != null : "Partitions are null [cache=" + cctx.name() + ", fromNode=" + nodeId + "]";

                remaining.put(nodeId, new T2<>(U.currentTimeMillis(), parts));
            }
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

                if (!cctx.kernalContext().isStopping()) {
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

                U.log(log, ("Cancelled rebalancing [cache=" + cctx.name() +
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
            ClusterNode node = cctx.discovery().node(nodeId);

            if (node == null)
                return;

            //Check remote node rebalancing API version.
            if (node.version().compareTo(GridDhtPreloader.REBALANCING_VER_2_SINCE) >= 0) {
                GridDhtPartitionDemandMessage d = new GridDhtPartitionDemandMessage(
                    -1/* remove supply context signal */, this.topologyVersion(), cctx.cacheId());

                d.timeout(cctx.config().getRebalanceTimeout());

                try {
                    for (int idx = 0; idx < cctx.gridConfig().getRebalanceThreadPoolSize(); idx++) {
                        d.topic(GridCachePartitionExchangeManager.rebalanceTopic(idx));

                        cctx.io().sendOrderedMessage(node, GridCachePartitionExchangeManager.rebalanceTopic(idx),
                            d, cctx.ioPolicy(), cctx.config().getRebalanceTimeout());
                    }
                }
                catch (IgniteCheckedException ignored) {
                    if (log.isDebugEnabled())
                        log.debug("Failed to send failover context cleanup request to node");
                }
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

                if (cctx.events().isRecordable(EVT_CACHE_REBALANCE_PART_LOADED))
                    preloadEvent(p, EVT_CACHE_REBALANCE_PART_LOADED,
                        exchFut.discoveryEvent());

                T2<Long, Collection<Integer>> t = remaining.get(nodeId);

                assert t != null : "Remaining not found [cache=" + cctx.name() + ", fromNode=" + nodeId +
                    ", part=" + p + "]";

                Collection<Integer> parts = t.get2();

                boolean rmvd = parts.remove(p);

                assert rmvd : "Partition already done [cache=" + cctx.name() + ", fromNode=" + nodeId +
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
        private void preloadEvent(int part, int type, DiscoveryEvent discoEvt) {
            assert discoEvt != null;

            cctx.events().addPreloadEvent(part, type, discoEvt.eventNode(), discoEvt.type(), discoEvt.timestamp());
        }

        /**
         * @param type Type.
         * @param discoEvt Discovery event.
         */
        private void preloadEvent(int type, DiscoveryEvent discoEvt) {
            preloadEvent(-1, type, discoEvt);
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

                cctx.shared().exchange().scheduleResendPartitions();

                Collection<Integer> m = new HashSet<>();

                for (Map.Entry<UUID, Collection<Integer>> e : missed.entrySet()) {
                    if (e.getValue() != null && !e.getValue().isEmpty())
                        m.addAll(e.getValue());
                }

                if (!m.isEmpty()) {
                    U.log(log, ("Reassigning partitions that were missed: " + m));

                    onDone(false); //Finished but has missed partitions, will force dummy exchange

                    cctx.shared().exchange().forceDummyExchange(true, exchFut);

                    return;
                }

                if (!cancelled && !cctx.preloader().syncFuture().isDone())
                    ((GridFutureAdapter)cctx.preloader().syncFuture()).onDone();

                onDone(!cancelled);
            }
        }

        /**
         *
         */
        private void sendRebalanceStartedEvent() {
            if (cctx.events().isRecordable(EVT_CACHE_REBALANCE_STARTED) &&
                (!cctx.isReplicated() || !startedEvtSent.get())) {
                preloadEvent(EVT_CACHE_REBALANCE_STARTED, exchFut.discoveryEvent());

                startedEvtSent.set(true);
            }
        }

        /**
         *
         */
        private void sendRebalanceFinishedEvent() {
            if (cctx.events().isRecordable(EVT_CACHE_REBALANCE_STOPPED) &&
                (!cctx.isReplicated() || !stoppedEvtSent.get())) {
                preloadEvent(EVT_CACHE_REBALANCE_STOPPED, exchFut.discoveryEvent());

                stoppedEvtSent.set(true);
            }
        }

        /** {@inheritDoc} */
        public String toString() {
            return S.toString(RebalanceFuture.class, this);
        }
    }

    /**
     * Supply message wrapper.
     */
    @Deprecated//Backward compatibility. To be removed in future.
    private static class SupplyMessage {
        /** Sender ID. */
        private UUID sndId;

        /** Supply message. */
        private GridDhtPartitionSupplyMessage supply;

        /**
         * Dummy constructor.
         */
        private SupplyMessage() {
            // No-op.
        }

        /**
         * @param sndId Sender ID.
         * @param supply Supply message.
         */
        SupplyMessage(UUID sndId, GridDhtPartitionSupplyMessage supply) {
            this.sndId = sndId;
            this.supply = supply;
        }

        /**
         * @return Sender ID.
         */
        UUID senderId() {
            return sndId;
        }

        /**
         * @return Message.
         */
        GridDhtPartitionSupplyMessage supply() {
            return supply;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(SupplyMessage.class, this);
        }
    }

    /**
     *
     */
    @Deprecated//Backward compatibility. To be removed in future.
    private class DemandWorker {
        /** Worker ID. */
        private int id;

        /** Partition-to-node assignments. */
        private final LinkedBlockingDeque<GridDhtPreloaderAssignments> assignQ = new LinkedBlockingDeque<>();

        /** Message queue. */
        private final LinkedBlockingDeque<SupplyMessage> msgQ =
            new LinkedBlockingDeque<>();

        /** Counter. */
        private long cntr;

        /** Hide worker logger and use cache logger instead. */
        private IgniteLogger log = GridDhtPartitionDemander.this.log;

        /** */
        private volatile RebalanceFuture fut;

        /**
         * @param id Worker ID.
         * @param fut Rebalance future.
         */
        private DemandWorker(int id, RebalanceFuture fut) {
            assert id >= 0;

            this.id = id;
            this.fut = fut;
        }

        /**
         * @param msg Message.
         */
        private void addMessage(SupplyMessage msg) {
            msgQ.offer(msg);
        }

        /**
         * @param deque Deque to poll from.
         * @param time Time to wait.
         * @return Polled item.
         * @throws InterruptedException If interrupted.
         */
        @Nullable private <T> T poll(BlockingQueue<T> deque, long time) throws InterruptedException {
            return deque.poll(time, MILLISECONDS);
        }

        /**
         * @param idx Unique index for this topic.
         * @return Topic for partition.
         */
        public Object topic(long idx) {
            return TOPIC_CACHE.topic(cctx.namexx(), cctx.nodeId(), id, idx);
        }

        /** */
        public void cancel() {
            msgQ.clear();

            msgQ.offer(new SupplyMessage(null, null));
        }

        /**
         * @param node Node to demand from.
         * @param topVer Topology version.
         * @param d Demand message.
         * @param exchFut Exchange future.
         * @throws InterruptedException If interrupted.
         * @throws ClusterTopologyCheckedException If node left.
         * @throws IgniteCheckedException If failed to send message.
         */
        private void demandFromNode(
            ClusterNode node,
            final AffinityTopologyVersion topVer,
            GridDhtPartitionDemandMessage d,
            GridDhtPartitionsExchangeFuture exchFut
        ) throws InterruptedException, IgniteCheckedException {
            GridDhtPartitionTopology top = cctx.dht().topology();

            cntr++;

            d.topic(topic(cntr));
            d.workerId(id);

            if (fut.isDone() || topologyChanged(fut))
                return;

            cctx.io().addOrderedHandler(d.topic(), new CI2<UUID, GridDhtPartitionSupplyMessage>() {
                @Override public void apply(UUID nodeId, GridDhtPartitionSupplyMessage msg) {
                    addMessage(new SupplyMessage(nodeId, msg));
                }
            });

            try {
                boolean retry;

                // DoWhile.
                // =======
                do {
                    retry = false;

                    // Create copy.
                    d = new GridDhtPartitionDemandMessage(d, fut.remaining.get(node.id()).get2());

                    long timeout = cctx.config().getRebalanceTimeout();

                    d.timeout(timeout);

                    if (log.isDebugEnabled())
                        log.debug("Sending demand message [node=" + node.id() + ", demand=" + d + ']');

                    // Send demand message.
                    cctx.io().send(node, d, cctx.ioPolicy());

                    // While.
                    // =====
                    while (!fut.isDone() && !topologyChanged(fut)) {
                        SupplyMessage s = poll(msgQ, timeout);

                        // If timed out.
                        if (s == null) {
                            if (msgQ.isEmpty()) { // Safety check.
                                U.warn(log, "Timed out waiting for partitions to load, will retry in " + timeout +
                                    " ms (you may need to increase 'networkTimeout' or 'rebalanceBatchSize'" +
                                    " configuration properties).");

                                // Ordered listener was removed if timeout expired.
                                cctx.io().removeOrderedHandler(d.topic());

                                // Must create copy to be able to work with IO manager thread local caches.
                                d = new GridDhtPartitionDemandMessage(d, fut.remaining.get(node.id()).get2());

                                // Create new topic.
                                d.topic(topic(++cntr));

                                // Create new ordered listener.
                                cctx.io().addOrderedHandler(d.topic(),
                                    new CI2<UUID, GridDhtPartitionSupplyMessage>() {
                                        @Override public void apply(UUID nodeId,
                                            GridDhtPartitionSupplyMessage msg) {
                                            addMessage(new SupplyMessage(nodeId, msg));
                                        }
                                    });

                                // Resend message with larger timeout.
                                retry = true;

                                break; // While.
                            }
                            else
                                continue; // While.
                        }

                        if (s.senderId() == null)
                            return; // Stopping now.

                        // Check that message was received from expected node.
                        if (!s.senderId().equals(node.id())) {
                            U.warn(log, "Received supply message from unexpected node [expectedId=" + node.id() +
                                ", rcvdId=" + s.senderId() + ", msg=" + s + ']');

                            continue; // While.
                        }

                        if (log.isDebugEnabled())
                            log.debug("Received supply message: " + s);

                        GridDhtPartitionSupplyMessage supply = s.supply();

                        // Check whether there were class loading errors on unmarshal
                        if (supply.classError() != null) {
                            if (log.isDebugEnabled())
                                log.debug("Class got undeployed during preloading: " + supply.classError());

                            retry = true;

                            // Quit preloading.
                            break;
                        }

                        // Preload.
                        for (Map.Entry<Integer, CacheEntryInfoCollection> e : supply.infos().entrySet()) {
                            int p = e.getKey();

                            if (cctx.affinity().partitionLocalNode(p, topVer)) {
                                GridDhtLocalPartition part = top.localPartition(p, topVer, true);

                                assert part != null;

                                if (part.state() == MOVING) {
                                    boolean reserved = part.reserve();

                                    assert reserved : "Failed to reserve partition [gridName=" +
                                        cctx.gridName() + ", cacheName=" + cctx.namex() + ", part=" + part + ']';

                                    part.lock();

                                    try {
                                        Collection<Integer> invalidParts = new GridLeanSet<>();

                                        // Loop through all received entries and try to preload them.
                                        for (GridCacheEntryInfo entry : e.getValue().infos()) {
                                            if (!invalidParts.contains(p)) {
                                                if (!part.preloadingPermitted(entry.key(), entry.version())) {
                                                    if (log.isDebugEnabled())
                                                        log.debug("Preloading is not permitted for entry due to " +
                                                            "evictions [key=" + entry.key() +
                                                            ", ver=" + entry.version() + ']');

                                                    continue;
                                                }

                                                if (!preloadEntry(node, p, entry, topVer)) {
                                                    invalidParts.add(p);

                                                    if (log.isDebugEnabled())
                                                        log.debug("Got entries for invalid partition during " +
                                                            "preloading (will skip) [p=" + p + ", entry=" + entry + ']');
                                                }
                                            }
                                        }

                                        boolean last = supply.last().contains(p);

                                        // If message was last for this partition,
                                        // then we take ownership.
                                        if (last) {
                                            fut.partitionDone(node.id(), p);

                                            top.own(part);

                                            if (log.isDebugEnabled())
                                                log.debug("Finished rebalancing partition: " + part);

                                            if (cctx.events().isRecordable(EVT_CACHE_REBALANCE_PART_LOADED))
                                                preloadEvent(p, EVT_CACHE_REBALANCE_PART_LOADED,
                                                    exchFut.discoveryEvent());
                                        }
                                    }
                                    finally {
                                        part.unlock();
                                        part.release();
                                    }
                                }
                                else {
                                    fut.partitionDone(node.id(), p);

                                    if (log.isDebugEnabled())
                                        log.debug("Skipping rebalancing partition (state is not MOVING): " + part);
                                }
                            }
                            else {
                                fut.partitionDone(node.id(), p);

                                if (log.isDebugEnabled())
                                    log.debug("Skipping rebalancing partition (it does not belong on current node): " + p);
                            }
                        }

                        // Only request partitions based on latest topology version.
                        for (Integer miss : s.supply().missed()) {
                            if (cctx.affinity().partitionLocalNode(miss, topVer))
                                fut.partitionMissed(node.id(), miss);
                        }

                        for (Integer miss : s.supply().missed())
                            fut.partitionDone(node.id(), miss);

                        if (fut.remaining.get(node.id()) == null)
                            break; // While.

                        if (s.supply().ack()) {
                            retry = true;

                            break;
                        }
                    }
                }
                while (retry && !fut.isDone() && !topologyChanged(fut));
            }
            finally {
                cctx.io().removeOrderedHandler(d.topic());
            }
        }

        /**
         * @param node Node.
         * @param d D.
         * @throws IgniteCheckedException If failed.
         */
        public void run(ClusterNode node, GridDhtPartitionDemandMessage d) throws IgniteCheckedException {
            demandLock.readLock().lock();

            try {
                GridDhtPartitionsExchangeFuture exchFut = fut.exchFut;

                AffinityTopologyVersion topVer = fut.topVer;

                try {
                    demandFromNode(node, topVer, d, exchFut);
                }
                catch (InterruptedException e) {
                    throw new IgniteCheckedException(e);
                }
            }
            finally {
                demandLock.readLock().unlock();
            }
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(DemandWorker.class, this, "assignQ", assignQ, "msgQ", msgQ, "super", super.toString());
        }
    }
}

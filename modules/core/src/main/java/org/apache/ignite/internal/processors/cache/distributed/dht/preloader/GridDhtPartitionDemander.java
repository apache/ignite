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
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.IgniteNodeAttributes;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.managers.eventstorage.GridLocalEventListener;
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
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.GridLeanSet;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.CI2;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.thread.IgniteThread;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentHashMap8;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.events.EventType.EVT_CACHE_REBALANCE_OBJECT_LOADED;
import static org.apache.ignite.events.EventType.EVT_CACHE_REBALANCE_PART_LOADED;
import static org.apache.ignite.events.EventType.EVT_CACHE_REBALANCE_STOPPED;
import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
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
    private volatile SyncFuture syncFut;

    /** Last timeout object. */
    private AtomicReference<GridTimeoutObject> lastTimeoutObj = new AtomicReference<>();

    /** Last exchange future. */
    private volatile GridDhtPartitionsExchangeFuture lastExchangeFut;

    /** Demand lock. */
    private final ReadWriteLock demandLock;

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

        syncFut = new SyncFuture(null, cctx, log);

        if (!enabled)
            // Calling onDone() immediately since preloading is disabled.
            syncFut.onDone();
    }

    /**
     *
     */
    void start() {
    }

    /**
     *
     */
    void stop() {
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
     * Sets preload predicate for demand pool.
     *
     * @param preloadPred Preload predicate.
     */
    void preloadPredicate(IgnitePredicate<GridCacheEntryInfo> preloadPred) {
        this.preloadPred = preloadPred;
    }

    /**
     * Force preload.
     */
    void forcePreload() {
        GridTimeoutObject obj = lastTimeoutObj.getAndSet(null);

        if (obj != null)
            cctx.time().removeTimeoutObject(obj);

        final GridDhtPartitionsExchangeFuture exchFut = lastExchangeFut;

        if (exchFut != null) {
            if (log.isDebugEnabled())
                log.debug("Forcing rebalance event for future: " + exchFut);

            exchFut.listen(new CI1<IgniteInternalFuture<AffinityTopologyVersion>>() {
                @Override public void apply(IgniteInternalFuture<AffinityTopologyVersion> t) {
                    cctx.shared().exchange().forcePreloadExchange(exchFut);
                }
            });
        }
        else if (log.isDebugEnabled())
            log.debug("Ignoring force rebalance request (no topology event happened yet).");
    }

    /**
     * @param topVer Topology version.
     * @return {@code True} if topology changed.
     */
    private boolean topologyChanged(AffinityTopologyVersion topVer) {
        return !cctx.affinity().affinityTopologyVersion().equals(topVer);
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
     * @param assigns Assignments.
     * @param force {@code True} if dummy reassign.
     * @throws IgniteCheckedException
     */

    void addAssignments(final GridDhtPreloaderAssignments assigns, boolean force) throws IgniteCheckedException {
        if (log.isDebugEnabled())
            log.debug("Adding partition assignments: " + assigns);

        long delay = cctx.config().getRebalanceDelay();

        if (delay == 0 || force) {
            assert assigns != null;

            final AffinityTopologyVersion topVer = assigns.topologyVersion();

            SyncFuture fut = syncFut;

            if (fut.isInited()) {
                if (!fut.isDone())
                    fut.onCancel();

                fut = new SyncFuture(assigns, cctx, log);

                syncFut = fut;
            }
            else
                fut.init(assigns);

            if (assigns.isEmpty()) {
                fut.onDone();

                return;
            }

            if (topologyChanged(topVer)) {
                fut.onCancel();

                return;
            }

            final SyncFuture cSF = fut;

            IgniteThread thread = new IgniteThread(cctx.gridName(), "demand-thread-" + cctx.cache().name(), new Runnable() {
                @Override public void run() {
                    if (!CU.isMarshallerCache(cctx.name())) {
                        if (log.isDebugEnabled())
                            log.debug("Waiting for marshaller cache preload [cacheName=" + cctx.name() + ']');

                        try {
                            IgniteInternalFuture fut;
                            do {
                                fut = cctx.kernalContext().cache().marshallerCache().preloader().syncFuture();
                            }
                            while (!((SyncFuture)fut).isInited() || ((SyncFuture)fut).topologyVersion().topologyVersion() < cSF.topologyVersion().topologyVersion());

                            if (((SyncFuture)fut).topologyVersion().topologyVersion() > cSF.topologyVersion().topologyVersion()) {
                                cSF.onCancel();

                                return;
                            }

                            if (!topologyChanged(topVer))
                                fut.get();
                            else {
                                cSF.onCancel();

                                return;
                            }
                        }
                        catch (IgniteInterruptedCheckedException ignored) {
                            if (log.isDebugEnabled()) {
                                log.debug("Failed to wait for marshaller cache preload future (grid is stopping): " +
                                    "[cacheName=" + cctx.name() + ']');
                                cSF.onCancel();

                                return;
                            }
                        }
                        catch (IgniteCheckedException e) {
                            cSF.onCancel();

                            throw new Error("Ordered preload future should never fail: " + e.getMessage(), e);
                        }
                    }

                    int rebalanceOrder = cctx.config().getRebalanceOrder();

                    if (rebalanceOrder > 0) {
                        IgniteInternalFuture<?> fut = cctx.kernalContext().cache().orderedPreloadFuture(rebalanceOrder);

                        try {
                            if (fut != null) {
                                if (log.isDebugEnabled())
                                    log.debug("Waiting for dependant caches rebalance [cacheName=" + cctx.name() +
                                        ", rebalanceOrder=" + rebalanceOrder + ']');

                                if (!topologyChanged(topVer))
                                    fut.get();
                                else {
                                    cSF.onCancel();

                                    return;
                                }
                            }
                        }
                        catch (IgniteInterruptedCheckedException ignored) {
                            if (log.isDebugEnabled()) {
                                log.debug("Failed to wait for ordered rebalance future (grid is stopping): " +
                                    "[cacheName=" + cctx.name() + ", rebalanceOrder=" + rebalanceOrder + ']');
                                cSF.onCancel();

                                return;
                            }
                        }
                        catch (IgniteCheckedException e) {
                            cSF.onCancel();

                            throw new Error("Ordered rebalance future should never fail: " + e.getMessage(), e);
                        }
                    }

                    requestPartitions(cSF);
                }
            });

            fut.setDemandThread(thread);

            thread.start();
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
                            cctx.shared().exchange().forcePreloadExchange(exchFut);
                        }
                    });
                }
            };

            lastTimeoutObj.set(obj);

            cctx.time().addTimeoutObject(obj);
        }
    }

    /**
     * @param fut Future.
     */
    private void requestPartitions(SyncFuture fut) {
        final GridDhtPreloaderAssignments assigns = fut.assigns;

        AffinityTopologyVersion topVer = fut.topologyVersion();

        for (Map.Entry<ClusterNode, GridDhtPartitionDemandMessage> e : assigns.entrySet()) {
            if (topologyChanged(topVer) || Thread.interrupted()) {
                fut.onCancel();

                return;
            }

            final ClusterNode node = e.getKey();

            GridDhtPartitionDemandMessage d = e.getValue();

            d.timeout(cctx.config().getRebalanceTimeout());
            d.workerId(0);//old api support.

            final CacheConfiguration cfg = cctx.config();

            final long start = U.currentTimeMillis();

            fut.logStart(node.id(), start);

            U.log(log, "Starting rebalancing [cache=" + cctx.name() + ", mode=" + cfg.getRebalanceMode() +
                ", from node=" + node.id() + ", partitions count=" + d.partitions().size() + ", topology=" + d.topologyVersion() + "]");

            //Check remote node rebalancing API version.
            if (new Integer(1).equals(node.attribute(IgniteNodeAttributes.REBALANCING_VERSION))) {
                GridConcurrentHashSet<Integer> remainings = new GridConcurrentHashSet<>();

                remainings.addAll(d.partitions());

                fut.append(node.id(), remainings);

                int lsnrCnt = Math.max(1, cctx.gridConfig().getRebalanceThreadPoolSize() / 2);

                List<Set<Integer>> sParts = new ArrayList<>(lsnrCnt);

                for (int cnt = 0; cnt < lsnrCnt; cnt++)
                    sParts.add(new HashSet<Integer>());

                Iterator<Integer> it = d.partitions().iterator();

                int cnt = 0;

                while (it.hasNext())
                    sParts.get(cnt++ % lsnrCnt).add(it.next());

                for (cnt = 0; cnt < lsnrCnt; cnt++) {

                    if (!sParts.get(cnt).isEmpty()) {

                        // Create copy.
                        GridDhtPartitionDemandMessage initD = new GridDhtPartitionDemandMessage(d, sParts.get(cnt));

                        initD.topic(GridCachePartitionExchangeManager.demanderTopic(cnt));

                        try {
                            if (!topologyChanged(topVer))
                                cctx.io().sendOrderedMessage(node, GridCachePartitionExchangeManager.supplierTopic(cnt), initD, cctx.ioPolicy(), d.timeout());
                            else
                                fut.onCancel();
                        }
                        catch (IgniteCheckedException ex) {
                            fut.onCancel();

                            U.error(log, "Failed to send partition demand message to node", ex);
                        }

                        if (log.isDebugEnabled())
                            log.debug("Requested rebalancing [from node=" + node.id() + ", listener index=" + cnt + ", partitions count=" + sParts.get(cnt).size() + " (" + partitionsList(sParts.get(cnt)) + ")]");
                    }
                }
            }
            else {
                DemandWorker dw = new DemandWorker(dmIdx.incrementAndGet(), fut);

                fut.append(node.id(), d.partitions());

                dw.run(node, d);
            }
        }
    }

    /**
     * @param c Partitions.
     * @return String representation of partitions list.
     */
    private String partitionsList(Collection<Integer> c) {
        LinkedList<Integer> s = new LinkedList<>(c);

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
        final GridDhtPartitionSupplyMessageV2 supply) {
        AffinityTopologyVersion topVer = supply.topologyVersion();

        final SyncFuture fut = syncFut;

        if (!fut.topologyVersion().equals(topVer))
            return;

        if (topologyChanged(topVer)) {
            fut.onCancel();

            return;
        }

        ClusterNode node = cctx.node(id);

        assert node != null;

        if (log.isDebugEnabled())
            log.debug("Received supply message: " + supply);

        // Check whether there were class loading errors on unmarshal
        if (supply.classError() != null) {
            if (log.isDebugEnabled())
                log.debug("Class got undeployed during preloading: " + supply.classError());

            fut.onCancel(id);

            return;
        }

        final GridDhtPartitionTopology top = cctx.dht().topology();

        try {
            // Preload.
            for (Map.Entry<Integer, CacheEntryInfoCollection> e : supply.infos().entrySet()) {
                if (topologyChanged(topVer)) {
                    fut.onCancel();

                    return;
                }

                int p = e.getKey();

                if (cctx.affinity().localNode(p, topVer)) {
                    GridDhtLocalPartition part = top.localPartition(p, topVer, true);

                    assert part != null;

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

                            boolean last = supply.last().contains(p);

                            // If message was last for this partition,
                            // then we take ownership.
                            if (last) {
                                top.own(part);

                                fut.onPartitionDone(id, p);

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
                        fut.onPartitionDone(id, p);

                        if (log.isDebugEnabled())
                            log.debug("Skipping rebalancing partition (state is not MOVING): " + part);
                    }
                }
                else {
                    fut.onPartitionDone(id, p);

                    if (log.isDebugEnabled())
                        log.debug("Skipping rebalancing partition (it does not belong on current node): " + p);
                }
            }

            // Only request partitions based on latest topology version.
            for (Integer miss : supply.missed())
                if (cctx.affinity().localNode(miss, topVer))
                    fut.onMissedPartition(id, miss);

            for (Integer miss : supply.missed())
                fut.onPartitionDone(id, miss);

            if (!fut.isDone()) {
                GridDhtPartitionDemandMessage d = fut.getDemandMessage(node);

                if (d != null) {
                    // Create copy.
                    GridDhtPartitionDemandMessage nextD =
                        new GridDhtPartitionDemandMessage(d, Collections.<Integer>emptySet());

                    nextD.topic(GridCachePartitionExchangeManager.demanderTopic(idx));

                    if (!topologyChanged(topVer)) {
                        // Send demand message.
                        cctx.io().sendOrderedMessage(node, GridCachePartitionExchangeManager.supplierTopic(idx),
                            nextD, cctx.ioPolicy(), cctx.config().getRebalanceTimeout());
                    }
                    else
                        fut.onCancel();
                }
            }
        }
        catch (ClusterTopologyCheckedException e) {
            if (log.isDebugEnabled())
                log.debug("Node left during rebalancing [node=" + node.id() +
                    ", msg=" + e.getMessage() + ']');
            fut.onCancel();
        }
        catch (IgniteCheckedException ex) {
            U.error(log, "Failed to receive partitions from node (rebalancing will not " +
                "fully finish) [node=" + node.id() + ", msg=" + supply + ']', ex);

            fut.onCancel(node.id());
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
                        cached.context().cache().removeIfObsolete(cached.key());

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
                        cctx.isDrEnabled() ? DR_PRELOAD : DR_NONE
                    )) {
                        cctx.evicts().touch(cached, topVer); // Start tracking.

                        if (cctx.events().isRecordable(EVT_CACHE_REBALANCE_OBJECT_LOADED) && !cached.isInternal())
                            cctx.events().addEvent(cached.partition(), cached.key(), cctx.localNodeId(),
                                (IgniteUuid)null, null, EVT_CACHE_REBALANCE_OBJECT_LOADED, entry.value(), true, null,
                                false, null, null, null);
                    }
                    else if (log.isDebugEnabled())
                        log.debug("Rebalancing entry is already in cache (will ignore) [key=" + cached.key() +
                            ", part=" + p + ']');
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
     * Sets last exchange future.
     *
     * @param lastFut Last future to set.
     */
    void updateLastExchangeFuture(GridDhtPartitionsExchangeFuture lastFut) {
        lastExchangeFut = lastFut;
    }

    /**
     *
     */
    public static class SyncFuture extends GridFutureAdapter<Boolean> {
        /** */
        private static final long serialVersionUID = 1L;

        /** */
        private final GridCacheContext<?, ?> cctx;

        /** */
        private final IgniteLogger log;

        /** Remaining. */
        private ConcurrentHashMap8<UUID, Collection<Integer>> remaining = new ConcurrentHashMap8<>();

        /** Missed. */
        private ConcurrentHashMap8<UUID, Collection<Integer>> missed = new ConcurrentHashMap8<>();

        /** Started. */
        private ConcurrentHashMap8<UUID, Long> started = new ConcurrentHashMap8<>();

        private volatile IgniteThread thread;

        /** Lock. */
        private Lock lock = new ReentrantLock();

        /** Listener. */
        private volatile GridLocalEventListener lsnr;

        /** Assignments. */
        private volatile GridDhtPreloaderAssignments assigns;

        /** Completed. */
        private volatile boolean completed = true;

        /**
         * @param assigns Assigns.
         */
        SyncFuture(GridDhtPreloaderAssignments assigns, GridCacheContext<?, ?> cctx, IgniteLogger log) {
            this.assigns = assigns;
            this.cctx = cctx;
            this.log = log;
        }

        /**
         * @return Topology version.
         */
        public AffinityTopologyVersion topologyVersion() {
            return assigns != null ? assigns.topologyVersion() : null;
        }

        /**
         * @param assigns Assigns.
         */
        void init(GridDhtPreloaderAssignments assigns) {
            final SyncFuture fut = this;

            lsnr = new GridLocalEventListener() {
                @Override public void onEvent(Event evt) {
                    fut.onCancel();
                }
            };

            cctx.events().addListener(lsnr, EVT_NODE_FAILED);

            this.assigns = assigns;
        }

        /**
         * @param thread
         */
        void setDemandThread(IgniteThread thread) {
            this.thread = thread;
        }

        /**
         * @return Initialised or not.
         */
        boolean isInited() {
            return assigns != null;
        }

        /**
         * @param nodeId Node id.
         * @param parts Parts.
         */
        void append(UUID nodeId, Collection<Integer> parts) {
            remaining.put(nodeId, parts);

            missed.put(nodeId, new GridConcurrentHashSet<Integer>());
        }

        /**
         * @param nodeId Node id.
         * @param time Time.
         */
        void logStart(UUID nodeId, long time) {
            started.put(nodeId, time);
        }

        /**
         * @param node Node.
         */
        GridDhtPartitionDemandMessage getDemandMessage(ClusterNode node) {
            if (isDone())
                return null;

            return assigns.get(node);
        }

        /**
         *
         */
        void onCancel() {
            lock.lock();
            try {
                if (isDone())
                    return;

                remaining.clear();

                completed = false;

                U.log(log, (!completed ? "Cancelled" : "Completed") + " rebalancing from all nodes [cache=" + cctx.name()
                    + ", topology=" + topologyVersion() +
                    ", time=" +
                    (started.isEmpty() ? 0 : (U.currentTimeMillis() - Collections.min(started.values()))) + " ms]");

                checkIsDone();
            }
            finally {
                lock.unlock();
            }
        }

        /**
         * @param nodeId Node id.
         */
        void onCancel(UUID nodeId) {
            lock.lock();
            try {
                if (isDone())
                    return;

                remaining.remove(nodeId);

                completed = false;

                U.log(log, ("Cancelled rebalancing [cache=" + cctx.name() +
                    ", from node=" + nodeId + ", topology=" + topologyVersion() +
                    ", time=" + (U.currentTimeMillis() - started.get(nodeId)) + " ms]"));

                checkIsDone();
            }
            finally {
                lock.unlock();
            }

        }

        /**
         * @return Is completed.
         */
        boolean isCompleted() {
            return completed;
        }

        /**
         * @param nodeId Node id.
         * @param p P.
         */
        void onMissedPartition(UUID nodeId, int p) {
            lock.lock();
            try {
                if (isDone())
                    return;

                if (missed.get(nodeId) == null)
                    missed.put(nodeId, new GridConcurrentHashSet<Integer>());

                missed.get(nodeId).add(p);
            }
            finally {
                lock.unlock();
            }
        }

        /**
         * @param nodeId Node id.
         * @param p P.
         */
        void onPartitionDone(UUID nodeId, int p) {
            lock.lock();
            try {
                if (isDone())
                    return;

                if (cctx.events().isRecordable(EVT_CACHE_REBALANCE_PART_LOADED))
                    preloadEvent(p, EVT_CACHE_REBALANCE_PART_LOADED,
                        assigns.exchangeFuture().discoveryEvent());

                Collection<Integer> parts = remaining.get(nodeId);

                if (parts != null) {
                    parts.remove(p);

                    if (parts.isEmpty()) {
                        remaining.remove(nodeId);

                        U.log(log, ("Completed rebalancing [cache=" + cctx.name() +
                            ", from node=" + nodeId + ", topology=" + topologyVersion() +
                            ", time=" + (U.currentTimeMillis() - started.get(nodeId)) + " ms]"));
                    }
                }

                checkIsDone();
            }
            finally {
                lock.unlock();
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
            if (remaining.isEmpty()) {
                if (log.isDebugEnabled())
                    log.debug("Completed sync future.");

                Collection<Integer> m = new HashSet<>();

                for (Map.Entry<UUID, Collection<Integer>> e : missed.entrySet()) {
                    if (e.getValue() != null && !e.getValue().isEmpty())
                        m.addAll(e.getValue());
                }

                if (!m.isEmpty()) {
                    if (log.isDebugEnabled())
                        log.debug("Reassigning partitions that were missed: " + m);

                    cctx.shared().exchange().forceDummyExchange(true, assigns.exchangeFuture());
                }

                cctx.shared().exchange().scheduleResendPartitions();

                if (cctx.events().isRecordable(EVT_CACHE_REBALANCE_STOPPED))
                    preloadEvent(EVT_CACHE_REBALANCE_STOPPED, assigns.exchangeFuture().discoveryEvent());

                if (lsnr != null)
                    cctx.events().removeListener(lsnr);

                if (thread != null)
                    thread.interrupt();

                onDone(completed);
            }
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

    /** DemandWorker index. */
    @Deprecated//Backward compatibility. To be removed in future.
    private final AtomicInteger dmIdx = new AtomicInteger();

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

        private volatile SyncFuture fut;

        /**
         * @param id Worker ID.
         */
        private DemandWorker(int id, SyncFuture fut) {
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

        /**
         * @param node Node to demand from.
         * @param topVer Topology version.
         * @param d Demand message.
         * @param exchFut Exchange future.
         * @return Missed partitions.
         * @throws InterruptedException If interrupted.
         * @throws ClusterTopologyCheckedException If node left.
         * @throws IgniteCheckedException If failed to send message.
         */
        private Set<Integer> demandFromNode(
            ClusterNode node,
            final AffinityTopologyVersion topVer,
            GridDhtPartitionDemandMessage d,
            GridDhtPartitionsExchangeFuture exchFut
        ) throws InterruptedException, IgniteCheckedException {
            GridDhtPartitionTopology top = cctx.dht().topology();

            cntr++;

            d.topic(topic(cntr));
            d.workerId(id);

            Set<Integer> missed = new HashSet<>();

            // Get the same collection that will be sent in the message.
            Collection<Integer> remaining = d.partitions();

            if (topologyChanged(topVer))
                return missed;

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
                    d = new GridDhtPartitionDemandMessage(d, remaining);

                    long timeout = cctx.config().getRebalanceTimeout();

                    d.timeout(timeout);

                    if (log.isDebugEnabled())
                        log.debug("Sending demand message [node=" + node.id() + ", demand=" + d + ']');

                    // Send demand message.
                    cctx.io().send(node, d, cctx.ioPolicy());

                    // While.
                    // =====
                    while (!topologyChanged(topVer)) {
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
                                d = new GridDhtPartitionDemandMessage(d, remaining);

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

                            if (cctx.affinity().localNode(p, topVer)) {
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
                                            remaining.remove(p);
                                            fut.onPartitionDone(node.id(), p);

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
                                    remaining.remove(p);
                                    fut.onPartitionDone(node.id(), p);

                                    if (log.isDebugEnabled())
                                        log.debug("Skipping rebalancing partition (state is not MOVING): " + part);
                                }
                            }
                            else {
                                remaining.remove(p);
                                fut.onPartitionDone(node.id(), p);

                                if (log.isDebugEnabled())
                                    log.debug("Skipping rebalancing partition (it does not belong on current node): " + p);
                            }
                        }

                        remaining.removeAll(s.supply().missed());

                        // Only request partitions based on latest topology version.
                        for (Integer miss : s.supply().missed()) {
                            if (cctx.affinity().localNode(miss, topVer))
                                missed.add(miss);

                            fut.onMissedPartition(node.id(), miss);
                        }

                        if (remaining.isEmpty())
                            break; // While.

                        if (s.supply().ack()) {
                            retry = true;

                            break;
                        }
                    }
                }
                while (retry && !topologyChanged(topVer));

                return missed;
            }
            finally {
                cctx.io().removeOrderedHandler(d.topic());
            }
        }

        /**
         * @param node Node.
         * @param d D.
         */
        public void run(ClusterNode node, GridDhtPartitionDemandMessage d) {
            demandLock.readLock().lock();

            try {
                GridDhtPartitionsExchangeFuture exchFut = fut.assigns.exchangeFuture();

                AffinityTopologyVersion topVer = fut.assigns.topologyVersion();

                Collection<Integer> missed = new HashSet<>();

                if (topologyChanged(topVer)) {
                    fut.onCancel();

                    return;
                }

                try {
                    Set<Integer> set = demandFromNode(node, topVer, d, exchFut);

                    if (!set.isEmpty()) {
                        if (log.isDebugEnabled())
                            log.debug("Missed partitions from node [nodeId=" + node.id() + ", missed=" +
                                set + ']');

                        missed.addAll(set);
                    }
                }
                catch (ClusterTopologyCheckedException e) {
                    if (log.isDebugEnabled())
                        log.debug("Node left during rebalancing (will retry) [node=" + node.id() +
                            ", msg=" + e.getMessage() + ']');

                    fut.onCancel();
                }
                catch (IgniteCheckedException e) {
                    U.error(log, "Failed to receive partitions from node (rebalancing will not " +
                        "fully finish) [node=" + node.id() + ", msg=" + d + ']', e);

                    fut.onCancel(node.id());
                }
                catch (InterruptedException e) {
                    fut.onCancel();
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

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

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.events.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.cluster.*;
import org.apache.ignite.internal.events.*;
import org.apache.ignite.internal.managers.eventstorage.*;
import org.apache.ignite.internal.processors.affinity.*;
import org.apache.ignite.internal.processors.cache.distributed.dht.*;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.*;
import org.apache.ignite.internal.processors.timeout.*;
import org.apache.ignite.internal.util.*;
import org.apache.ignite.internal.util.future.*;
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.internal.util.worker.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.thread.*;
import org.jetbrains.annotations.*;
import org.jsr166.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.*;

import static java.util.concurrent.TimeUnit.*;
import static org.apache.ignite.IgniteSystemProperties.*;
import static org.apache.ignite.events.EventType.*;
import static org.apache.ignite.internal.events.DiscoveryCustomEvent.EVT_DISCOVERY_CUSTOM_EVT;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.*;
import static org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPreloader.*;

/**
 * Partition exchange manager.
 */
public class GridCachePartitionExchangeManager<K, V> extends GridCacheSharedManagerAdapter<K, V> {
    /** Exchange history size. */
    private static final int EXCHANGE_HISTORY_SIZE = 1000;

    /** Cleanup history size. */
    public static final int EXCH_FUT_CLEANUP_HISTORY_SIZE = 10;

    /** Atomic reference for pending timeout object. */
    private AtomicReference<ResendTimeoutObject> pendingResend = new AtomicReference<>();

    /** Partition resend timeout after eviction. */
    private final long partResendTimeout = getLong(IGNITE_PRELOAD_RESEND_TIMEOUT, DFLT_PRELOAD_RESEND_TIMEOUT);

    /** Latch which completes after local exchange future is created. */
    private GridFutureAdapter<?> locExchFut;

    /** */
    private final ReadWriteLock busyLock = new ReentrantReadWriteLock();

    /** Last partition refresh. */
    private final AtomicLong lastRefresh = new AtomicLong(-1);

    /** Pending futures. */
    private final Queue<GridDhtPartitionsExchangeFuture> pendingExchangeFuts = new ConcurrentLinkedQueue<>();

    /** */
    @GridToStringInclude
    private ExchangeWorker exchWorker;

    /** */
    @GridToStringExclude
    private final ConcurrentMap<Integer, GridClientPartitionTopology> clientTops = new ConcurrentHashMap8<>();

    /** */
    private volatile GridDhtPartitionsExchangeFuture lastInitializedFut;

    /** */
    private final ConcurrentMap<AffinityTopologyVersion, AffinityReadyFuture> readyFuts = new ConcurrentHashMap8<>();

    /** */
    private final AtomicReference<AffinityTopologyVersion> readyTopVer =
        new AtomicReference<>(AffinityTopologyVersion.NONE);


    /**
     * Partition map futures.
     * This set also contains already completed exchange futures to address race conditions when coordinator
     * leaves grid and new coordinator sends full partition message to a node which has not yet received
     * discovery event. In case if remote node will retry partition exchange, completed future will indicate
     * that full partition map should be sent to requesting node right away.
     */
    private ExchangeFutureSet exchFuts = new ExchangeFutureSet();

    /** Discovery listener. */
    private final GridLocalEventListener discoLsnr = new GridLocalEventListener() {
        @Override public void onEvent(Event evt) {
            if (!enterBusy())
                return;

            try {
                DiscoveryEvent e = (DiscoveryEvent)evt;

                ClusterNode loc = cctx.localNode();

                assert e.type() == EVT_NODE_JOINED || e.type() == EVT_NODE_LEFT || e.type() == EVT_NODE_FAILED ||
                    e.type() == EVT_DISCOVERY_CUSTOM_EVT;

                final ClusterNode n = e.eventNode();

                GridDhtPartitionExchangeId exchId = null;
                GridDhtPartitionsExchangeFuture exchFut = null;

                if (e.type() != EVT_DISCOVERY_CUSTOM_EVT) {
                    assert !loc.id().equals(n.id());

                    if (e.type() == EVT_NODE_LEFT || e.type() == EVT_NODE_FAILED) {
                        assert cctx.discovery().node(n.id()) == null;

                        for (GridDhtPartitionsExchangeFuture f : exchFuts.values())
                            f.onNodeLeft(n.id());
                    }

                    assert
                        e.type() != EVT_NODE_JOINED || n.order() > loc.order() :
                        "Node joined with smaller-than-local " +
                            "order [newOrder=" + n.order() + ", locOrder=" + loc.order() + ']';

                    exchId = exchangeId(n.id(),
                        affinityTopologyVersion(e),
                        e.type());

                    exchFut = exchangeFuture(exchId, e, null);
                }
                else {
                    DiscoveryCustomEvent customEvt = (DiscoveryCustomEvent)e;

                    if (customEvt.data() instanceof DynamicCacheChangeBatch) {
                        DynamicCacheChangeBatch batch = (DynamicCacheChangeBatch)customEvt.data();

                        Collection<DynamicCacheChangeRequest> valid = new ArrayList<>(batch.requests().size());

                        // Validate requests to check if event should trigger partition exchange.
                        for (DynamicCacheChangeRequest req : batch.requests()) {
                            if (cctx.cache().dynamicCacheRegistered(req))
                                valid.add(req);
                            else
                                cctx.cache().completeStartFuture(req);
                        }

                        if (!F.isEmpty(valid)) {
                            exchId = exchangeId(n.id(),
                                affinityTopologyVersion(e),
                                e.type());

                            exchFut = exchangeFuture(exchId, e, valid);
                        }
                    }
                }

                if (exchId != null) {
                    // Start exchange process.
                    pendingExchangeFuts.add(exchFut);

                    // Event callback - without this callback future will never complete.
                    exchFut.onEvent(exchId, e);

                    if (log.isDebugEnabled())
                        log.debug("Discovery event (will start exchange): " + exchId);

                    locExchFut.listen(new CI1<IgniteInternalFuture<?>>() {
                        @Override public void apply(IgniteInternalFuture<?> t) {
                            if (!enterBusy())
                                return;

                            try {
                                // Unwind in the order of discovery events.
                                for (GridDhtPartitionsExchangeFuture f = pendingExchangeFuts.poll(); f != null;
                                    f = pendingExchangeFuts.poll())
                                    addFuture(f);
                            }
                            finally {
                                leaveBusy();
                            }
                        }
                    });
                }
            }
            finally {
                leaveBusy();
            }
        }
    };

    /** {@inheritDoc} */
    @Override protected void start0() throws IgniteCheckedException {
        super.start0();

        locExchFut = new GridFutureAdapter<>();

        exchWorker = new ExchangeWorker();

        cctx.gridEvents().addLocalEventListener(discoLsnr, EVT_NODE_JOINED, EVT_NODE_LEFT, EVT_NODE_FAILED,
            EVT_DISCOVERY_CUSTOM_EVT);

        cctx.io().addHandler(0, GridDhtPartitionsSingleMessage.class,
            new MessageHandler<GridDhtPartitionsSingleMessage>() {
                @Override public void onMessage(ClusterNode node, GridDhtPartitionsSingleMessage msg) {
                    processSinglePartitionUpdate(node, msg);
                }
            });

        cctx.io().addHandler(0, GridDhtPartitionsFullMessage.class,
            new MessageHandler<GridDhtPartitionsFullMessage>() {
                @Override public void onMessage(ClusterNode node, GridDhtPartitionsFullMessage msg) {
                    processFullPartitionUpdate(node, msg);
                }
            });

        cctx.io().addHandler(0, GridDhtPartitionsSingleRequest.class,
            new MessageHandler<GridDhtPartitionsSingleRequest>() {
                @Override public void onMessage(ClusterNode node, GridDhtPartitionsSingleRequest msg) {
                    processSinglePartitionRequest(node, msg);
                }
            });
    }

    /** {@inheritDoc} */
    @Override protected void onKernalStart0() throws IgniteCheckedException {
        super.onKernalStart0();

        ClusterNode loc = cctx.localNode();

        long startTime = loc.metrics().getStartTime();

        assert startTime > 0;

        // Generate dummy discovery event for local node joining.
        DiscoveryEvent discoEvt = cctx.discovery().localJoinEvent();

        final AffinityTopologyVersion startTopVer = affinityTopologyVersion(discoEvt);

        GridDhtPartitionExchangeId exchId = exchangeId(loc.id(), startTopVer, EVT_NODE_JOINED);

        assert discoEvt != null;

        assert discoEvt.topologyVersion() == startTopVer.topologyVersion();

        GridDhtPartitionsExchangeFuture fut = exchangeFuture(exchId, discoEvt, null);

        new IgniteThread(cctx.gridName(), "exchange-worker", exchWorker).start();

        onDiscoveryEvent(cctx.localNodeId(), fut);

        // Allow discovery events to get processed.
        locExchFut.onDone();

        if (log.isDebugEnabled())
            log.debug("Beginning to wait on local exchange future: " + fut);

        try {
            boolean first = true;

            while (true) {
                try {
                    fut.get(cctx.preloadExchangeTimeout());

                    break;
                }
                catch (IgniteFutureTimeoutCheckedException ignored) {
                    if (first) {
                        U.warn(log, "Failed to wait for initial partition map exchange. " +
                            "Possible reasons are: " + U.nl() +
                            "  ^-- Transactions in deadlock." + U.nl() +
                            "  ^-- Long running transactions (ignore if this is the case)." + U.nl() +
                            "  ^-- Unreleased explicit locks.");

                        first = false;
                    }
                    else
                        U.warn(log, "Still waiting for initial partition map exchange [fut=" + fut + ']');
                }
            }

            for (GridCacheContext cacheCtx : cctx.cacheContexts())
                cacheCtx.preloader().onInitialExchangeComplete(null);
        }
        catch (IgniteFutureTimeoutCheckedException e) {
            IgniteCheckedException err = new IgniteCheckedException("Timed out waiting for exchange future: " + fut, e);

            for (GridCacheContext cacheCtx : cctx.cacheContexts())
                cacheCtx.preloader().onInitialExchangeComplete(err);

            throw err;
        }

        if (log.isDebugEnabled())
            log.debug("Finished waiting on local exchange: " + fut.exchangeId());
    }

    /** {@inheritDoc} */
    @Override protected void onKernalStop0(boolean cancel) {
        // Finish all exchange futures.
        for (GridDhtPartitionsExchangeFuture f : exchFuts.values())
            f.onDone(new IgniteInterruptedCheckedException("Grid is stopping: " + cctx.gridName()));

        for (AffinityReadyFuture f : readyFuts.values())
            f.onDone(new IgniteInterruptedCheckedException("Grid is stopping: " + cctx.gridName()));

        U.cancel(exchWorker);

        if (log.isDebugEnabled())
            log.debug("Before joining on exchange worker: " + exchWorker);

        U.join(exchWorker, log);

        ResendTimeoutObject resendTimeoutObj = pendingResend.getAndSet(null);

        if (resendTimeoutObj != null)
            cctx.time().removeTimeoutObject(resendTimeoutObj);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("LockAcquiredButNotSafelyReleased")
    @Override protected void stop0(boolean cancel) {
        super.stop0(cancel);

        // Do not allow any activity in exchange manager after stop.
        busyLock.writeLock().lock();

        exchFuts = null;
    }

    /**
     * @param cacheId Cache ID.
     * @param exchFut Exchange future.
     * @return Topology.
     */
    public GridDhtPartitionTopology clientTopology(int cacheId, GridDhtPartitionsExchangeFuture exchFut) {
        GridClientPartitionTopology top = clientTops.get(cacheId);

        if (top != null)
            return top;

        GridClientPartitionTopology old = clientTops.putIfAbsent(cacheId,
            top = new GridClientPartitionTopology(cctx, cacheId, exchFut));

        return old != null ? old : top;
    }

    /**
     * @return Collection of client topologies.
     */
    public Collection<GridClientPartitionTopology> clientTopologies() {
        return clientTops.values();
    }

    /**
     * @param cacheId Cache ID.
     * @return Client partition topology.
     */
    public GridClientPartitionTopology clearClientTopology(int cacheId) {
        return clientTops.remove(cacheId);
    }

    /**
     * Gets topology version of last partition exchange, it is possible that last partition exchange
     * is not completed yet.
     *
     * @return Topology version.
     */
    public AffinityTopologyVersion topologyVersion() {
        GridDhtPartitionsExchangeFuture lastInitializedFut0 = lastInitializedFut;

        return lastInitializedFut0 != null
            ? lastInitializedFut0.exchangeId().topologyVersion() : AffinityTopologyVersion.NONE;
    }

    /**
     * @return Topology version of latest completed partition exchange.
     */
    public AffinityTopologyVersion readyAffinityVersion() {
        return readyTopVer.get();
    }

    /**
     * @return Last completed topology future.
     */
    public GridDhtTopologyFuture lastTopologyFuture() {
        return lastInitializedFut;
    }

    /**
     * @param ver Topology version.
     * @return Future or {@code null} is future is already completed.
     */
    @Nullable IgniteInternalFuture<?> affinityReadyFuture(AffinityTopologyVersion ver) {
        GridDhtPartitionsExchangeFuture lastInitializedFut0 = lastInitializedFut;

        if (lastInitializedFut0 != null && lastInitializedFut0.topologyVersion().compareTo(ver) >= 0) {
            if (log.isDebugEnabled())
                log.debug("Return lastInitializedFut for topology ready future " +
                    "[ver=" + ver + ", fut=" + lastInitializedFut0 + ']');

            return lastInitializedFut0;
        }

        AffinityTopologyVersion topVer = readyTopVer.get();

        if (topVer.compareTo(ver) >= 0) {
            if (log.isDebugEnabled())
                log.debug("Return finished future for topology ready future [ver=" + ver + ", topVer=" + topVer + ']');

            return null;
        }

        GridFutureAdapter<AffinityTopologyVersion> fut = F.addIfAbsent(readyFuts, ver,
            new AffinityReadyFuture(ver));

        if (log.isDebugEnabled())
            log.debug("Created topology ready future [ver=" + ver + ", fut=" + fut + ']');

        topVer = readyTopVer.get();

        if (topVer.compareTo(ver) >= 0) {
            if (log.isDebugEnabled())
                log.debug("Completing created topology ready future " +
                    "[ver=" + topVer + ", topVer=" + topVer + ", fut=" + fut + ']');

            fut.onDone(topVer);
        }

        return fut;
    }

    /**
     * @return {@code true} if entered to busy state.
     */
    private boolean enterBusy() {
        if (busyLock.readLock().tryLock())
            return true;

        if (log.isDebugEnabled())
            log.debug("Failed to enter to busy state (exchange manager is stopping): " + cctx.localNodeId());

        return false;
    }

    /**
     *
     */
    private void leaveBusy() {
        busyLock.readLock().unlock();
    }

    /**
     * @return Exchange futures.
     */
    @SuppressWarnings( {"unchecked", "RedundantCast"})
    public List<IgniteInternalFuture<?>> exchangeFutures() {
        return (List<IgniteInternalFuture<?>>)(List)exchFuts.values();
    }

    /**
     * @return {@code True} if pending future queue is empty.
     */
    public boolean hasPendingExchange() {
        return !exchWorker.futQ.isEmpty();
    }

    /**
     * @param nodeId New node ID.
     * @param fut Exchange future.
     */
    void onDiscoveryEvent(UUID nodeId, GridDhtPartitionsExchangeFuture fut) {
        if (!enterBusy())
            return;

        try {
            addFuture(fut);
        }
        finally {
            leaveBusy();
        }
    }

    /**
     * @param evt Discovery event.
     * @return Affinity topology version.
     */
    private AffinityTopologyVersion affinityTopologyVersion(DiscoveryEvent evt) {
        if (evt.type() == DiscoveryCustomEvent.EVT_DISCOVERY_CUSTOM_EVT)
            return ((DiscoveryCustomEvent)evt).affinityTopologyVersion();

        return new AffinityTopologyVersion(evt.topologyVersion());
    }

    /**
     * @return {@code True} if topology has changed.
     */
    public boolean topologyChanged() {
        return exchWorker.topologyChanged();
    }

    /**
     * @param exchFut Exchange future.
     * @param reassign Dummy reassign flag.
     */
    public void forceDummyExchange(boolean reassign,
        GridDhtPartitionsExchangeFuture exchFut) {
        exchWorker.addFuture(
            new GridDhtPartitionsExchangeFuture(cctx, reassign, exchFut.discoveryEvent(), exchFut.exchangeId()));
    }

    /**
     * Forces preload exchange.
     *
     * @param exchFut Exchange future.
     */
    public void forcePreloadExchange(GridDhtPartitionsExchangeFuture exchFut) {
        exchWorker.addFuture(
            new GridDhtPartitionsExchangeFuture(cctx, exchFut.discoveryEvent(), exchFut.exchangeId()));
    }

    /**
     * Schedules next full partitions update.
     */
    public void scheduleResendPartitions() {
        ResendTimeoutObject timeout = pendingResend.get();

        if (timeout == null || timeout.started()) {
            ResendTimeoutObject update = new ResendTimeoutObject();

            if (pendingResend.compareAndSet(timeout, update))
                cctx.time().addTimeoutObject(update);
        }
    }

    /**
     * Partition refresh callback.
     */
    void refreshPartitions() {
        ClusterNode oldest = CU.oldest(cctx);

        if (log.isDebugEnabled())
            log.debug("Refreshing partitions [oldest=" + oldest.id() + ", loc=" + cctx.localNodeId() + ']');

        Collection<ClusterNode> rmts = null;

        try {
            // If this is the oldest node.
            if (oldest.id().equals(cctx.localNodeId())) {
                rmts = CU.remoteNodes(cctx);

                if (log.isDebugEnabled())
                    log.debug("Refreshing partitions from oldest node: " + cctx.localNodeId());

                sendAllPartitions(rmts);
            }
            else {
                if (log.isDebugEnabled())
                    log.debug("Refreshing local partitions from non-oldest node: " +
                        cctx.localNodeId());

                sendLocalPartitions(oldest, null);
            }
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Failed to refresh partition map [oldest=" + oldest.id() + ", rmts=" + U.nodeIds(rmts) +
                ", loc=" + cctx.localNodeId() + ']', e);
        }
    }

    /**
     * Refresh partitions.
     *
     * @param timeout Timeout.
     */
    private void refreshPartitions(long timeout) {
        long last = lastRefresh.get();

        long now = U.currentTimeMillis();

        if (last != -1 && now - last >= timeout && lastRefresh.compareAndSet(last, now)) {
            if (log.isDebugEnabled())
                log.debug("Refreshing partitions [last=" + last + ", now=" + now + ", delta=" + (now - last) +
                    ", timeout=" + timeout + ", lastRefresh=" + lastRefresh + ']');

            refreshPartitions();
        }
        else if (log.isDebugEnabled())
            log.debug("Partitions were not refreshed [last=" + last + ", now=" + now + ", delta=" + (now - last) +
                ", timeout=" + timeout + ", lastRefresh=" + lastRefresh + ']');
    }

    /**
     * @param nodes Nodes.
     * @return {@code True} if message was sent, {@code false} if node left grid.
     * @throws IgniteCheckedException If failed.
     */
    private boolean sendAllPartitions(Collection<? extends ClusterNode> nodes)
        throws IgniteCheckedException {
        GridDhtPartitionsFullMessage m = new GridDhtPartitionsFullMessage(null, null, AffinityTopologyVersion.NONE);

        for (GridCacheContext cacheCtx : cctx.cacheContexts()) {
            if (!cacheCtx.isLocal() && cacheCtx.started())
                m.addFullPartitionsMap(cacheCtx.cacheId(), cacheCtx.topology().partitionMap(true));
        }

        // It is important that client topologies be added after contexts.
        for (GridClientPartitionTopology top : cctx.exchange().clientTopologies())
            m.addFullPartitionsMap(top.cacheId(), top.partitionMap(true));

        if (log.isDebugEnabled())
            log.debug("Sending all partitions [nodeIds=" + U.nodeIds(nodes) + ", msg=" + m + ']');

        cctx.io().safeSend(nodes, m, SYSTEM_POOL, null);

        return true;
    }

    /**
     * @param node Node.
     * @param id ID.
     * @return {@code True} if message was sent, {@code false} if node left grid.
     * @throws IgniteCheckedException If failed.
     */
    private boolean sendLocalPartitions(ClusterNode node, @Nullable GridDhtPartitionExchangeId id)
        throws IgniteCheckedException {
        GridDhtPartitionsSingleMessage m = new GridDhtPartitionsSingleMessage(id, cctx.versions().last());

        for (GridCacheContext cacheCtx : cctx.cacheContexts()) {
            if (!cacheCtx.isLocal()) {
                GridDhtPartitionMap locMap = cacheCtx.topology().localPartitionMap();

                m.addLocalPartitionMap(cacheCtx.cacheId(), locMap);
            }
        }

        for (GridClientPartitionTopology top : clientTops.values()) {
            GridDhtPartitionMap locMap = top.localPartitionMap();

            m.addLocalPartitionMap(top.cacheId(), locMap);
        }

        if (log.isDebugEnabled())
            log.debug("Sending local partitions [nodeId=" + node.id() + ", msg=" + m + ']');

        try {
            cctx.io().send(node, m, SYSTEM_POOL);

            return true;
        }
        catch (ClusterTopologyCheckedException ignore) {
            if (log.isDebugEnabled())
                log.debug("Failed to send partition update to node because it left grid (will ignore) [node=" +
                    node.id() + ", msg=" + m + ']');

            return false;
        }
    }

    /**
     * @param nodeId Cause node ID.
     * @param topVer Topology version.
     * @param evt Event type.
     * @return Activity future ID.
     */
    private GridDhtPartitionExchangeId exchangeId(UUID nodeId, AffinityTopologyVersion topVer, int evt) {
        return new GridDhtPartitionExchangeId(nodeId, evt, topVer);
    }

    /**
     * @param exchId Exchange ID.
     * @param discoEvt Discovery event.
     * @return Exchange future.
     */
    GridDhtPartitionsExchangeFuture exchangeFuture(GridDhtPartitionExchangeId exchId,
        @Nullable DiscoveryEvent discoEvt, @Nullable Collection<DynamicCacheChangeRequest> reqs) {
        GridDhtPartitionsExchangeFuture fut;

        GridDhtPartitionsExchangeFuture old = exchFuts.addx(
            fut = new GridDhtPartitionsExchangeFuture(cctx, busyLock, exchId, reqs));

        if (old != null)
            fut = old;

        if (discoEvt != null)
            fut.onEvent(exchId, discoEvt);

        return fut;
    }

    /**
     * @param exchFut Exchange.
     * @param err Error.
     */
    public void onExchangeDone(GridDhtPartitionsExchangeFuture exchFut, @Nullable Throwable err) {
        if (err == null) {
            AffinityTopologyVersion topVer = exchFut.topologyVersion();

            if (log.isDebugEnabled())
                log.debug("Exchange done [topVer=" + topVer + ", fut=" + exchFut + ']');

            while (true) {
                AffinityTopologyVersion readyVer = readyTopVer.get();

                if (readyVer.compareTo(topVer) >= 0)
                    break;

                if (readyTopVer.compareAndSet(readyVer, topVer))
                    break;
            }

            for (Map.Entry<AffinityTopologyVersion, AffinityReadyFuture> entry : readyFuts.entrySet()) {
                if (entry.getKey().compareTo(topVer) <= 0) {
                    if (log.isDebugEnabled())
                        log.debug("Completing created topology ready future " +
                            "[ver=" + topVer + ", fut=" + entry.getValue() + ']');

                    entry.getValue().onDone(topVer);
                }
            }
        }
        else if (log.isDebugEnabled())
            log.debug("Exchange done with error [fut=" + exchFut + ", err=" + err + ']');

        ExchangeFutureSet exchFuts0 = exchFuts;

        if (exchFuts0 != null) {
            int skipped = 0;

            for (GridDhtPartitionsExchangeFuture fut : exchFuts0.values()) {
                skipped++;

                if (skipped == EXCH_FUT_CLEANUP_HISTORY_SIZE) {
                    for (GridCacheContext cacheCtx : cctx.cacheContexts()) {
                        if (err == null) {
                            if (!cacheCtx.isLocal())
                                cacheCtx.affinity().cleanUpCache(fut.topologyVersion());
                        }
                    }
                }
                if (skipped > 10)
                    fut.cleanUp();
            }
        }
    }

    /**
     * @param fut Future.
     * @return {@code True} if added.
     */
    private boolean addFuture(GridDhtPartitionsExchangeFuture fut) {
        if (fut.onAdded()) {
            exchWorker.addFuture(fut);

            return true;
        }

        return false;
    }

    /**
     * @param node Node.
     * @param msg Message.
     */
    private void processFullPartitionUpdate(ClusterNode node, GridDhtPartitionsFullMessage msg) {
        if (!enterBusy())
            return;

        try {
            if (msg.exchangeId() == null) {
                if (log.isDebugEnabled())
                    log.debug("Received full partition update [node=" + node.id() + ", msg=" + msg + ']');

                boolean updated = false;

                for (Map.Entry<Integer, GridDhtPartitionFullMap> entry : msg.partitions().entrySet()) {
                    Integer cacheId = entry.getKey();

                    GridCacheContext<K, V> cacheCtx = cctx.cacheContext(cacheId);

                    if (cacheCtx != null && !cacheCtx.started())
                        continue; // Can safely ignore background exchange.

                    GridDhtPartitionTopology top = null;

                    if (cacheCtx == null)
                        top = clientTops.get(cacheId);
                    else if (!cacheCtx.isLocal())
                        top = cacheCtx.topology();

                    if (top != null)
                        updated |= top.update(null, entry.getValue()) != null;
                }

                if (updated)
                    refreshPartitions();
            }
            else
                exchangeFuture(msg.exchangeId(), null, null).onReceive(node.id(), msg);
        }
        finally {
            leaveBusy();
        }
    }

    /**
     * @param node Node ID.
     * @param msg Message.
     */
    private void processSinglePartitionUpdate(ClusterNode node, GridDhtPartitionsSingleMessage msg) {
        if (!enterBusy())
            return;

        try {
            if (msg.exchangeId() == null) {
                if (log.isDebugEnabled())
                    log.debug("Received local partition update [nodeId=" + node.id() + ", parts=" +
                        msg + ']');

                boolean updated = false;

                for (Map.Entry<Integer, GridDhtPartitionMap> entry : msg.partitions().entrySet()) {
                    Integer cacheId = entry.getKey();

                    GridCacheContext<K, V> cacheCtx = cctx.cacheContext(cacheId);

                    GridDhtPartitionTopology top = null;

                    if (cacheCtx == null)
                        top = clientTops.get(cacheId);
                    else if (!cacheCtx.isLocal())
                        top = cacheCtx.topology();

                    if (top != null)
                        updated |= top.update(null, entry.getValue()) != null;
                }

                if (updated)
                    scheduleResendPartitions();
            }
            else
                exchangeFuture(msg.exchangeId(), null, null).onReceive(node.id(), msg);
        }
        finally {
            leaveBusy();
        }
    }

    /**
     * @param node Node ID.
     * @param msg Message.
     */
    private void processSinglePartitionRequest(ClusterNode node, GridDhtPartitionsSingleRequest msg) {
        if (!enterBusy())
            return;

        try {
            try {
                sendLocalPartitions(node, msg.exchangeId());
            }
            catch (IgniteCheckedException e) {
                U.error(log, "Failed to send local partition map to node [nodeId=" + node.id() + ", exchId=" +
                    msg.exchangeId() + ']', e);
            }
        }
        finally {
            leaveBusy();
        }
    }

    /**
     * @param deque Deque to poll from.
     * @param time Time to wait.
     * @param w Worker.
     * @return Polled item.
     * @throws InterruptedException If interrupted.
     */
    @Nullable private <T> T poll(BlockingQueue<T> deque, long time, GridWorker w) throws InterruptedException {
        assert w != null;

        // There is currently a case where {@code interrupted}
        // flag on a thread gets flipped during stop which causes the pool to hang.  This check
        // will always make sure that interrupted flag gets reset before going into wait conditions.
        // The true fix should actually make sure that interrupted flag does not get reset or that
        // interrupted exception gets propagated. Until we find a real fix, this method should
        // always work to make sure that there is no hanging during stop.
        if (w.isCancelled())
            Thread.currentThread().interrupt();

        return deque.poll(time, MILLISECONDS);
    }

    /**
     * Exchange future thread. All exchanges happen only by one thread and next
     * exchange will not start until previous one completes.
     */
    private class ExchangeWorker extends GridWorker {
        /** Future queue. */
        private final LinkedBlockingDeque<GridDhtPartitionsExchangeFuture> futQ =
            new LinkedBlockingDeque<>();

        /** Busy flag used as performance optimization to stop current preloading. */
        private volatile boolean busy;

        /**
         *
         */
        private ExchangeWorker() {
            super(cctx.gridName(), "partition-exchanger", GridCachePartitionExchangeManager.this.log);
        }

        /**
         * @param exchFut Exchange future.
         */
        void addFuture(GridDhtPartitionsExchangeFuture exchFut) {
            assert exchFut != null;

            if (!exchFut.dummy() || (futQ.isEmpty() && !busy))
                futQ.offer(exchFut);

            if (log.isDebugEnabled())
                log.debug("Added exchange future to exchange worker: " + exchFut);
        }

        /** {@inheritDoc} */
        @Override protected void body() throws InterruptedException, IgniteInterruptedCheckedException {
            long timeout = cctx.gridConfig().getNetworkTimeout();

            boolean startEvtFired = false;

            while (!isCancelled()) {
                GridDhtPartitionsExchangeFuture exchFut = null;

                try {
                    boolean preloadFinished = true;

                    for (GridCacheContext cacheCtx : cctx.cacheContexts()) {
                        preloadFinished &= cacheCtx.preloader().syncFuture().isDone();

                        if (!preloadFinished)
                            break;
                    }

                    // If not first preloading and no more topology events present,
                    // then we periodically refresh partition map.
                    if (futQ.isEmpty() && preloadFinished) {
                        refreshPartitions(timeout);

                        timeout = cctx.gridConfig().getNetworkTimeout();
                    }

                    // After workers line up and before preloading starts we initialize all futures.
                    if (log.isDebugEnabled())
                        log.debug("Before waiting for exchange futures [futs" +
                            F.view(exchFuts.values(), F.unfinishedFutures()) + ", worker=" + this + ']');

                    // Take next exchange future.
                    exchFut = poll(futQ, timeout, this);

                    if (exchFut == null)
                        continue; // Main while loop.

                    busy = true;

                    Map<Integer, GridDhtPreloaderAssignments<K, V>> assignsMap = new HashMap<>();

                    boolean dummyReassign = exchFut.dummyReassign();
                    boolean forcePreload = exchFut.forcePreload();

                    try {
                        if (isCancelled())
                            break;

                        if (!exchFut.dummy() && !exchFut.forcePreload()) {
                            lastInitializedFut = exchFut;

                            exchFut.init();

                            exchFut.get();

                            if (log.isDebugEnabled())
                                log.debug("After waiting for exchange future [exchFut=" + exchFut + ", worker=" +
                                    this + ']');

                            if (exchFut.exchangeId().nodeId().equals(cctx.localNodeId()))
                                lastRefresh.compareAndSet(-1, U.currentTimeMillis());

                            boolean changed = false;

                            // Just pick first worker to do this, so we don't
                            // invoke topology callback more than once for the
                            // same event.
                            for (GridCacheContext cacheCtx : cctx.cacheContexts()) {
                                if (cacheCtx.isLocal())
                                    continue;

                                changed |= cacheCtx.topology().afterExchange(exchFut);

                                // Preload event notification.
                                if (cacheCtx.events().isRecordable(EVT_CACHE_REBALANCE_STARTED)) {
                                    if (!cacheCtx.isReplicated() || !startEvtFired) {
                                        DiscoveryEvent discoEvt = exchFut.discoveryEvent();

                                        cacheCtx.events().addPreloadEvent(-1, EVT_CACHE_REBALANCE_STARTED,
                                            discoEvt.eventNode(), discoEvt.type(), discoEvt.timestamp());
                                    }
                                }
                            }

                            startEvtFired = true;

                            if (changed && futQ.isEmpty())
                                refreshPartitions();
                        }
                        else {
                            if (log.isDebugEnabled())
                                log.debug("Got dummy exchange (will reassign)");

                            if (!dummyReassign) {
                                timeout = 0; // Force refresh.

                                continue;
                            }
                        }

                        for (GridCacheContext cacheCtx : cctx.cacheContexts()) {
                            long delay = cacheCtx.config().getRebalanceDelay();

                            GridDhtPreloaderAssignments<K, V> assigns = null;

                            // Don't delay for dummy reassigns to avoid infinite recursion.
                            if (delay == 0 || forcePreload)
                                assigns = cacheCtx.preloader().assign(exchFut);

                            assignsMap.put(cacheCtx.cacheId(), assigns);
                        }
                    }
                    finally {
                        // Must flip busy flag before assignments are given to demand workers.
                        busy = false;
                    }

                    if (assignsMap != null) {
                        for (Map.Entry<Integer, GridDhtPreloaderAssignments<K, V>> e : assignsMap.entrySet()) {
                            int cacheId = e.getKey();

                            GridCacheContext<K, V> cacheCtx = cctx.cacheContext(cacheId);

                            cacheCtx.preloader().addAssignments(e.getValue(), forcePreload);
                        }
                    }
                }
                catch (IgniteInterruptedCheckedException e) {
                    throw e;
                }
                catch (IgniteCheckedException e) {
                    U.error(log, "Failed to wait for completion of partition map exchange " +
                        "(preloading will not start): " + exchFut, e);
                }
            }
        }

        /**
         * @return {@code True} if another exchange future has been queued up.
         */
        boolean topologyChanged() {
            return !futQ.isEmpty() || busy;
        }
    }

    /**
     * Partition resend timeout object.
     */
    private class ResendTimeoutObject implements GridTimeoutObject {
        /** Timeout ID. */
        private final IgniteUuid timeoutId = IgniteUuid.randomUuid();

        /** Timeout start time. */
        private final long createTime = U.currentTimeMillis();

        /** Started flag. */
        private AtomicBoolean started = new AtomicBoolean();

        /** {@inheritDoc} */
        @Override public IgniteUuid timeoutId() {
            return timeoutId;
        }

        /** {@inheritDoc} */
        @Override public long endTime() {
            return createTime + partResendTimeout;
        }

        /** {@inheritDoc} */
        @Override public void onTimeout() {
            if (!busyLock.readLock().tryLock())
                return;

            try {
                if (started.compareAndSet(false, true))
                    refreshPartitions();
            }
            finally {
                busyLock.readLock().unlock();

                cctx.time().removeTimeoutObject(this);

                pendingResend.compareAndSet(this, null);
            }
        }

        /**
         * @return {@code True} if timeout object started to run.
         */
        public boolean started() {
            return started.get();
        }
    }

    /**
     *
     */
    private static class ExchangeFutureSet extends GridListSet<GridDhtPartitionsExchangeFuture> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * Creates ordered, not strict list set.
         */
        private ExchangeFutureSet() {
            super(new Comparator<GridDhtPartitionsExchangeFuture>() {
                @Override public int compare(
                    GridDhtPartitionsExchangeFuture f1,
                    GridDhtPartitionsExchangeFuture f2
                ) {
                    AffinityTopologyVersion t1 = f1.exchangeId().topologyVersion();
                    AffinityTopologyVersion t2 = f2.exchangeId().topologyVersion();

                    assert t1.topologyVersion() > 0;
                    assert t2.topologyVersion() > 0;

                    // Reverse order.
                    return t2.compareTo(t1);
                }
            }, /*not strict*/false);
        }

        /**
         * @param fut Future to add.
         * @return {@code True} if added.
         */
        @Override public synchronized GridDhtPartitionsExchangeFuture addx(
            GridDhtPartitionsExchangeFuture fut) {
            GridDhtPartitionsExchangeFuture cur = super.addx(fut);

            while (size() > EXCHANGE_HISTORY_SIZE)
                removeLast();

            // Return the value in the set.
            return cur == null ? fut : cur;
        }

        /** {@inheritDoc} */
        @Nullable @Override public synchronized GridDhtPartitionsExchangeFuture removex(
            GridDhtPartitionsExchangeFuture val
        ) {
            return super.removex(val);
        }

        /**
         * @return Values.
         */
        @Override public synchronized List<GridDhtPartitionsExchangeFuture> values() {
            return super.values();
        }

        /** {@inheritDoc} */
        @Override public synchronized String toString() {
            return S.toString(ExchangeFutureSet.class, this, super.toString());
        }
    }

    /**
     *
     */
    private abstract class MessageHandler<M> implements IgniteBiInClosure<UUID, M> {
        /** */
        private static final long serialVersionUID = 0L;

        /** {@inheritDoc} */
        @Override public void apply(UUID nodeId, M msg) {
            ClusterNode node = cctx.node(nodeId);

            if (node == null) {
                if (log.isDebugEnabled())
                    log.debug("Received message from failed node [node=" + nodeId + ", msg=" + msg + ']');

                return;
            }

            if (log.isDebugEnabled())
                log.debug("Received message from node [node=" + nodeId + ", msg=" + msg + ']');

            onMessage(node , msg);
        }

        /**
         * @param node Node.
         * @param msg Message.
         */
        protected abstract void onMessage(ClusterNode node, M msg);
    }

    /**
     * Affinity ready future.
     */
    private class AffinityReadyFuture extends GridFutureAdapter<AffinityTopologyVersion> {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private AffinityTopologyVersion topVer;

        /**
         * @param topVer Topology version.
         */
        private AffinityReadyFuture(AffinityTopologyVersion topVer) {
            this.topVer = topVer;
        }

        /** {@inheritDoc} */
        @Override public boolean onDone(AffinityTopologyVersion res, @Nullable Throwable err) {
            assert res != null || err != null;

            boolean done = super.onDone(res, err);

            if (done)
                readyFuts.remove(topVer, this);

            return done;
        }
    }
}

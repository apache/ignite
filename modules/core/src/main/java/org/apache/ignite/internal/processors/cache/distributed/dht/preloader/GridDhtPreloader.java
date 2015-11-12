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
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.managers.eventstorage.GridLocalEventListener;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.GridCacheEntryInfo;
import org.apache.ignite.internal.processors.cache.GridCacheEntryRemovedException;
import org.apache.ignite.internal.processors.cache.GridCachePreloaderAdapter;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtAffinityAssignmentRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtAffinityAssignmentResponse;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtAssignmentFetchFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtInvalidPartitionException;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionTopology;
import org.apache.ignite.internal.util.GridAtomicLong;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.lang.GridPlainRunnable;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.GPC;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentHashMap8;
import org.jsr166.ConcurrentLinkedDeque8;

import static org.apache.ignite.events.EventType.EVT_CACHE_REBALANCE_PART_DATA_LOST;
import static org.apache.ignite.events.EventType.EVT_CACHE_REBALANCE_PART_UNLOADED;
import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_JOINED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.AFFINITY_POOL;
import static org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionState.MOVING;
import static org.apache.ignite.internal.util.GridConcurrentFactory.newMap;

/**
 * DHT cache preloader.
 */
public class GridDhtPreloader extends GridCachePreloaderAdapter {
    /**
     * Rebalancing was refactored at version 1.5.0, but backward compatibility to previous implementation was saved.
     * Node automatically chose communication protocol depends on remote node's version.
     * Backward compatibility may be removed at Ignite 2.x.
     */
    public static final IgniteProductVersion REBALANCING_VER_2_SINCE = IgniteProductVersion.fromString("1.5.0");

    /** Default preload resend timeout. */
    public static final long DFLT_PRELOAD_RESEND_TIMEOUT = 1500;

    /** */
    private GridDhtPartitionTopology top;

    /** Topology version. */
    private final GridAtomicLong topVer = new GridAtomicLong();

    /** Force key futures. */
    private final ConcurrentMap<IgniteUuid, GridDhtForceKeysFuture<?, ?>> forceKeyFuts = newMap();

    /** Partition suppliers. */
    private GridDhtPartitionSupplier supplier;

    /** Partition demanders. */
    private GridDhtPartitionDemander demander;

    /** Start future. */
    private GridFutureAdapter<Object> startFut;

    /** Busy lock to prevent activities from accessing exchanger while it's stopping. */
    private final ReadWriteLock busyLock = new ReentrantReadWriteLock();

    /** Demand lock. */
    private final ReadWriteLock demandLock = new ReentrantReadWriteLock();

    /** Pending affinity assignment futures. */
    private ConcurrentMap<AffinityTopologyVersion, GridDhtAssignmentFetchFuture> pendingAssignmentFetchFuts =
        new ConcurrentHashMap8<>();

    /** */
    private final ConcurrentLinkedDeque8<GridDhtLocalPartition> partsToEvict = new ConcurrentLinkedDeque8<>();

    /** */
    private final AtomicInteger partsEvictOwning = new AtomicInteger();

    /** Discovery listener. */
    private final GridLocalEventListener discoLsnr = new GridLocalEventListener() {
        @Override public void onEvent(Event evt) {
            if (!enterBusy())
                return;

            DiscoveryEvent e = (DiscoveryEvent)evt;

            try {
                ClusterNode loc = cctx.localNode();

                assert e.type() == EVT_NODE_JOINED || e.type() == EVT_NODE_LEFT || e.type() == EVT_NODE_FAILED;

                final ClusterNode n = e.eventNode();

                assert !loc.id().equals(n.id());

                for (GridDhtForceKeysFuture<?, ?> f : forceKeyFuts.values())
                    f.onDiscoveryEvent(e);

                assert e.type() != EVT_NODE_JOINED || n.order() > loc.order() : "Node joined with smaller-than-local " +
                    "order [newOrder=" + n.order() + ", locOrder=" + loc.order() + ']';

                boolean set = topVer.setIfGreater(e.topologyVersion());

                assert set : "Have you configured TcpDiscoverySpi for your in-memory data grid? [newVer=" +
                    e.topologyVersion() + ", curVer=" + topVer.get() + ", evt=" + e + ']';

                if (e.type() == EVT_NODE_LEFT || e.type() == EVT_NODE_FAILED) {
                    for (GridDhtAssignmentFetchFuture fut : pendingAssignmentFetchFuts.values())
                        fut.onNodeLeft(e.eventNode().id());
                }
            }
            finally {
                leaveBusy();
            }
        }
    };

    /**
     * @param cctx Cache context.
     */
    public GridDhtPreloader(GridCacheContext<?, ?> cctx) {
        super(cctx);

        top = cctx.dht().topology();

        startFut = new GridFutureAdapter<>();
    }

    /** {@inheritDoc} */
    @Override public void start() {
        if (log.isDebugEnabled())
            log.debug("Starting DHT rebalancer...");

        cctx.io().addHandler(cctx.cacheId(), GridDhtForceKeysRequest.class,
            new MessageHandler<GridDhtForceKeysRequest>() {
                @Override public void onMessage(ClusterNode node, GridDhtForceKeysRequest msg) {
                    processForceKeysRequest(node, msg);
                }
            });

        cctx.io().addHandler(cctx.cacheId(), GridDhtForceKeysResponse.class,
            new MessageHandler<GridDhtForceKeysResponse>() {
                @Override public void onMessage(ClusterNode node, GridDhtForceKeysResponse msg) {
                    processForceKeyResponse(node, msg);
                }
            });

        cctx.io().addHandler(cctx.cacheId(), GridDhtAffinityAssignmentRequest.class,
            new MessageHandler<GridDhtAffinityAssignmentRequest>() {
                @Override protected void onMessage(ClusterNode node, GridDhtAffinityAssignmentRequest msg) {
                    processAffinityAssignmentRequest(node, msg);
                }
            });

        cctx.io().addHandler(cctx.cacheId(), GridDhtAffinityAssignmentResponse.class,
            new MessageHandler<GridDhtAffinityAssignmentResponse>() {
                @Override protected void onMessage(ClusterNode node, GridDhtAffinityAssignmentResponse msg) {
                    processAffinityAssignmentResponse(node, msg);
                }
            });

        supplier = new GridDhtPartitionSupplier(cctx);
        demander = new GridDhtPartitionDemander(cctx, demandLock);

        supplier.start();
        demander.start();

        cctx.events().addListener(discoLsnr, EVT_NODE_JOINED, EVT_NODE_LEFT, EVT_NODE_FAILED);
    }

    /** {@inheritDoc} */
    @Override public void onKernalStart() throws IgniteCheckedException {
        if (log.isDebugEnabled())
            log.debug("DHT rebalancer onKernalStart callback.");

        ClusterNode loc = cctx.localNode();

        assert loc.metrics().getStartTime() > 0;

        final long startTopVer = loc.order();

        topVer.setIfGreater(startTopVer);
    }

    /** {@inheritDoc} */
    @Override public void preloadPredicate(IgnitePredicate<GridCacheEntryInfo> preloadPred) {
        super.preloadPredicate(preloadPred);

        assert supplier != null && demander != null : "preloadPredicate may be called only after start()";

        supplier.preloadPredicate(preloadPred);
        demander.preloadPredicate(preloadPred);
    }

    /** {@inheritDoc} */
    @SuppressWarnings( {"LockAcquiredButNotSafelyReleased"})
    @Override public void onKernalStop() {
        if (log.isDebugEnabled())
            log.debug("DHT rebalancer onKernalStop callback.");

        cctx.events().removeListener(discoLsnr);

        // Acquire write busy lock.
        busyLock.writeLock().lock();

        if (supplier != null)
            supplier.stop();

        if (demander != null)
            demander.stop();

        top = null;
    }

    /** {@inheritDoc} */
    @Override public void onInitialExchangeComplete(@Nullable Throwable err) {
        if (err == null)
            startFut.onDone();
        else
            startFut.onDone(err);
    }

    /** {@inheritDoc} */
    @Override public void updateLastExchangeFuture(GridDhtPartitionsExchangeFuture lastFut) {
        demander.updateLastExchangeFuture(lastFut);
    }

    /** {@inheritDoc} */
    @Override public void onTopologyChanged(AffinityTopologyVersion topVer) {
        supplier.onTopologyChanged(topVer);
    }

    /** {@inheritDoc} */
    @Override public GridDhtPreloaderAssignments assign(GridDhtPartitionsExchangeFuture exchFut) {
        // No assignments for disabled preloader.
        GridDhtPartitionTopology top = cctx.dht().topology();

        if (!cctx.rebalanceEnabled())
            return new GridDhtPreloaderAssignments(exchFut, top.topologyVersion());

        int partCnt = cctx.affinity().partitions();

        assert exchFut.forcePreload() || exchFut.dummyReassign() ||
            exchFut.exchangeId().topologyVersion().equals(top.topologyVersion()) :
            "Topology version mismatch [exchId=" + exchFut.exchangeId() +
                ", topVer=" + top.topologyVersion() + ']';

        GridDhtPreloaderAssignments assigns = new GridDhtPreloaderAssignments(exchFut, top.topologyVersion());

        AffinityTopologyVersion topVer = assigns.topologyVersion();

        for (int p = 0; p < partCnt; p++) {
            if (cctx.shared().exchange().hasPendingExchange()) {
                if (log.isDebugEnabled())
                    log.debug("Skipping assignments creation, exchange worker has pending assignments: " +
                        exchFut.exchangeId());

                break;
            }

            // If partition belongs to local node.
            if (cctx.affinity().localNode(p, topVer)) {
                GridDhtLocalPartition part = top.localPartition(p, topVer, true);

                assert part != null;
                assert part.id() == p;

                if (part.state() != MOVING) {
                    if (log.isDebugEnabled())
                        log.debug("Skipping partition assignment (state is not MOVING): " + part);

                    continue; // For.
                }

                Collection<ClusterNode> picked = pickedOwners(p, topVer);

                if (picked.isEmpty()) {
                    top.own(part);

                    if (cctx.events().isRecordable(EVT_CACHE_REBALANCE_PART_DATA_LOST)) {
                        DiscoveryEvent discoEvt = exchFut.discoveryEvent();

                        cctx.events().addPreloadEvent(p,
                            EVT_CACHE_REBALANCE_PART_DATA_LOST, discoEvt.eventNode(),
                            discoEvt.type(), discoEvt.timestamp());
                    }

                    if (log.isDebugEnabled())
                        log.debug("Owning partition as there are no other owners: " + part);
                }
                else {
                    ClusterNode n = F.rand(picked);

                    GridDhtPartitionDemandMessage msg = assigns.get(n);

                    if (msg == null) {
                        assigns.put(n, msg = new GridDhtPartitionDemandMessage(
                            top.updateSequence(),
                            exchFut.exchangeId().topologyVersion(),
                            cctx.cacheId()));
                    }

                    msg.addPartition(p);
                }
            }
        }

        return assigns;
    }

    /** {@inheritDoc} */
    @Override public void onReconnected() {
        startFut = new GridFutureAdapter<>();

        long topVer0 = cctx.kernalContext().discovery().topologyVersion();

        assert topVer0 > 0 : topVer0;

        topVer.set(topVer0);
    }

    /**
     * @param p Partition.
     * @param topVer Topology version.
     * @return Picked owners.
     */
    private Collection<ClusterNode> pickedOwners(int p, AffinityTopologyVersion topVer) {
        Collection<ClusterNode> affNodes = cctx.affinity().nodes(p, topVer);

        int affCnt = affNodes.size();

        Collection<ClusterNode> rmts = remoteOwners(p, topVer);

        int rmtCnt = rmts.size();

        if (rmtCnt <= affCnt)
            return rmts;

        List<ClusterNode> sorted = new ArrayList<>(rmts);

        // Sort in descending order, so nodes with higher order will be first.
        Collections.sort(sorted, CU.nodeComparator(false));

        // Pick newest nodes.
        return sorted.subList(0, affCnt);
    }

    /**
     * @param p Partition.
     * @param topVer Topology version.
     * @return Nodes owning this partition.
     */
    private Collection<ClusterNode> remoteOwners(int p, AffinityTopologyVersion topVer) {
        return F.view(cctx.dht().topology().owners(p, topVer), F.remoteNodes(cctx.nodeId()));
    }

    /** {@inheritDoc} */
    public void handleSupplyMessage(int idx, UUID id, final GridDhtPartitionSupplyMessageV2 s) {
        if (!enterBusy())
            return;

        try {
            demandLock.readLock().lock();
            try {
                demander.handleSupplyMessage(idx, id, s);
            }
            finally {
                demandLock.readLock().unlock();
            }
        }
        finally {
            leaveBusy();
        }
    }

    /** {@inheritDoc} */
    public void handleDemandMessage(int idx, UUID id, GridDhtPartitionDemandMessage d) {
        if (!enterBusy())
            return;

        try {
            supplier.handleDemandMessage(idx, id, d);
        }
        finally {
            leaveBusy();
        }
    }

    /** {@inheritDoc} */
    @Override public Callable<Boolean> addAssignments(GridDhtPreloaderAssignments assignments,
        boolean forcePreload, Collection<String> caches, int cnt) {
        return demander.addAssignments(assignments, forcePreload, caches, cnt);
    }

    /**
     * @return Start future.
     */
    @Override public IgniteInternalFuture<Object> startFuture() {
        return startFut;
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> syncFuture() {
        return cctx.kernalContext().clientNode() ? startFut : demander.syncFuture();
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Boolean> rebalanceFuture() {
        return cctx.kernalContext().clientNode() ? new GridFinishedFuture<>(true) : demander.rebalanceFuture();
    }

    /**
     * @param topVer Requested topology version.
     * @param fut Future to add.
     */
    public void addDhtAssignmentFetchFuture(AffinityTopologyVersion topVer, GridDhtAssignmentFetchFuture fut) {
        GridDhtAssignmentFetchFuture old = pendingAssignmentFetchFuts.putIfAbsent(topVer, fut);

        assert old == null : "More than one thread is trying to fetch partition assignments: " + topVer;
    }

    /**
     * @param topVer Requested topology version.
     * @param fut Future to remove.
     */
    public void removeDhtAssignmentFetchFuture(AffinityTopologyVersion topVer, GridDhtAssignmentFetchFuture fut) {
        boolean rmv = pendingAssignmentFetchFuts.remove(topVer, fut);

        assert rmv : "Failed to remove assignment fetch future: " + topVer;
    }

    /**
     * @return {@code true} if entered to busy state.
     */
    private boolean enterBusy() {
        if (busyLock.readLock().tryLock())
            return true;

        if (log.isDebugEnabled())
            log.debug("Failed to enter busy state on node (exchanger is stopping): " + cctx.nodeId());

        return false;
    }

    /**
     *
     */
    private void leaveBusy() {
        busyLock.readLock().unlock();
    }

    /**
     * @param node Node originated request.
     * @param msg Force keys message.
     */
    private void processForceKeysRequest(final ClusterNode node, final GridDhtForceKeysRequest msg) {
        IgniteInternalFuture<?> fut = cctx.mvcc().finishKeys(msg.keys(), msg.cacheId(), msg.topologyVersion());

        if (fut.isDone())
            processForceKeysRequest0(node, msg);
        else
            fut.listen(new CI1<IgniteInternalFuture<?>>() {
                @Override public void apply(IgniteInternalFuture<?> t) {
                    processForceKeysRequest0(node, msg);
                }
            });
    }

    /**
     * @param node Node originated request.
     * @param msg Force keys message.
     */
    private void processForceKeysRequest0(ClusterNode node, GridDhtForceKeysRequest msg) {
        if (!enterBusy())
            return;

        try {
            ClusterNode loc = cctx.localNode();

            GridDhtForceKeysResponse res = new GridDhtForceKeysResponse(
                cctx.cacheId(),
                msg.futureId(),
                msg.miniId(),
                cctx.deploymentEnabled());

            for (KeyCacheObject k : msg.keys()) {
                int p = cctx.affinity().partition(k);

                GridDhtLocalPartition locPart = top.localPartition(p, AffinityTopologyVersion.NONE, false);

                // If this node is no longer an owner.
                if (locPart == null && !top.owners(p).contains(loc)) {
                    res.addMissed(k);

                    continue;
                }

                GridCacheEntryEx entry = null;

                if (cctx.isSwapOrOffheapEnabled()) {
                    while (true) {
                        try {
                            entry = cctx.dht().entryEx(k);

                            entry.unswap();

                            break;
                        }
                        catch (GridCacheEntryRemovedException ignore) {
                            if (log.isDebugEnabled())
                                log.debug("Got removed entry: " + k);
                        }
                        catch (GridDhtInvalidPartitionException ignore) {
                            if (log.isDebugEnabled())
                                log.debug("Local node is no longer an owner: " + p);

                            res.addMissed(k);

                            break;
                        }
                    }
                }
                else
                    entry = cctx.dht().peekEx(k);

                // If entry is null, then local partition may have left
                // after the message was received. In that case, we are
                // confident that primary node knows of any changes to the key.
                if (entry != null) {
                    GridCacheEntryInfo info = entry.info();

                    if (info != null && !info.isNew())
                        res.addInfo(info);

                    if (cctx.isSwapOrOffheapEnabled())
                        cctx.evicts().touch(entry, msg.topologyVersion());
                }
                else if (log.isDebugEnabled())
                    log.debug("Key is not present in DHT cache: " + k);
            }

            if (log.isDebugEnabled())
                log.debug("Sending force key response [node=" + node.id() + ", res=" + res + ']');

            cctx.io().send(node, res, cctx.ioPolicy());
        }
        catch (ClusterTopologyCheckedException ignore) {
            if (log.isDebugEnabled())
                log.debug("Received force key request form failed node (will ignore) [nodeId=" + node.id() +
                    ", req=" + msg + ']');
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Failed to reply to force key request [nodeId=" + node.id() + ", req=" + msg + ']', e);
        }
        finally {
            leaveBusy();
        }
    }

    /**
     * @param node Node.
     * @param msg Message.
     */
    private void processForceKeyResponse(ClusterNode node, GridDhtForceKeysResponse msg) {
        if (!enterBusy())
            return;

        try {
            GridDhtForceKeysFuture<?, ?> f = forceKeyFuts.get(msg.futureId());

            if (f != null)
                f.onResult(node.id(), msg);
            else if (log.isDebugEnabled())
                log.debug("Receive force key response for unknown future (is it duplicate?) [nodeId=" + node.id() +
                    ", res=" + msg + ']');
        }
        finally {
            leaveBusy();
        }
    }

    /**
     * @param node Node.
     * @param req Request.
     */
    private void processAffinityAssignmentRequest(final ClusterNode node,
        final GridDhtAffinityAssignmentRequest req) {
        final AffinityTopologyVersion topVer = req.topologyVersion();

        if (log.isDebugEnabled())
            log.debug("Processing affinity assignment request [node=" + node + ", req=" + req + ']');

        cctx.affinity().affinityReadyFuture(req.topologyVersion()).listen(new CI1<IgniteInternalFuture<AffinityTopologyVersion>>() {
            @Override public void apply(IgniteInternalFuture<AffinityTopologyVersion> fut) {
                if (log.isDebugEnabled())
                    log.debug("Affinity is ready for topology version, will send response [topVer=" + topVer +
                        ", node=" + node + ']');

                List<List<ClusterNode>> assignment = cctx.affinity().assignments(topVer);

                try {
                    cctx.io().send(node,
                        new GridDhtAffinityAssignmentResponse(cctx.cacheId(), topVer, assignment), AFFINITY_POOL);
                }
                catch (IgniteCheckedException e) {
                    U.error(log, "Failed to send affinity assignment response to remote node [node=" + node + ']', e);
                }
            }
        });
    }

    /**
     * @param node Node.
     * @param res Response.
     */
    private void processAffinityAssignmentResponse(ClusterNode node, GridDhtAffinityAssignmentResponse res) {
        if (log.isDebugEnabled())
            log.debug("Processing affinity assignment response [node=" + node + ", res=" + res + ']');

        for (GridDhtAssignmentFetchFuture fut : pendingAssignmentFetchFuts.values())
            fut.onResponse(node, res);
    }

    /**
     * Resends partitions on partition evict within configured timeout.
     *
     * @param part Evicted partition.
     * @param updateSeq Update sequence.
     */
    public void onPartitionEvicted(GridDhtLocalPartition part, boolean updateSeq) {
        if (!enterBusy())
            return;

        try {
            top.onEvicted(part, updateSeq);

            if (cctx.events().isRecordable(EVT_CACHE_REBALANCE_PART_UNLOADED))
                cctx.events().addUnloadEvent(part.id());

            if (updateSeq)
                cctx.shared().exchange().scheduleResendPartitions();
        }
        finally {
            leaveBusy();
        }
    }

    /**
     * @param keys Keys to request.
     * @return Future for request.
     */
    @SuppressWarnings( {"unchecked", "RedundantCast"})
    @Override public GridDhtFuture<Object> request(Collection<KeyCacheObject> keys, AffinityTopologyVersion topVer) {
        final GridDhtForceKeysFuture<?, ?> fut = new GridDhtForceKeysFuture<>(cctx, topVer, keys, this);

        IgniteInternalFuture<?> topReadyFut = cctx.affinity().affinityReadyFuturex(topVer);

        if (startFut.isDone() && topReadyFut == null)
            fut.init();
        else {
            if (topReadyFut == null)
                startFut.listen(new CI1<IgniteInternalFuture<?>>() {
                    @Override public void apply(IgniteInternalFuture<?> syncFut) {
                        cctx.kernalContext().closure().runLocalSafe(
                            new GridPlainRunnable() {
                                @Override public void run() {
                                    fut.init();
                                }
                            });
                    }
                });
            else {
                GridCompoundFuture<Object, Object> compound = new GridCompoundFuture<>();

                compound.add((IgniteInternalFuture<Object>)startFut);
                compound.add((IgniteInternalFuture<Object>)topReadyFut);

                compound.markInitialized();

                compound.listen(new CI1<IgniteInternalFuture<?>>() {
                    @Override public void apply(IgniteInternalFuture<?> syncFut) {
                        fut.init();
                    }
                });
            }
        }

        return (GridDhtFuture)fut;
    }

    /** {@inheritDoc} */
    @Override public void forcePreload() {
        demander.forcePreload();
    }

    /** {@inheritDoc} */
    @Override public void unwindUndeploys() {
        demandLock.writeLock().lock();

        try {
            cctx.deploy().unwind(cctx);
        }
        finally {
            demandLock.writeLock().unlock();
        }
    }

    /**
     * Adds future to future map.
     *
     * @param fut Future to add.
     */
    void addFuture(GridDhtForceKeysFuture<?, ?> fut) {
        forceKeyFuts.put(fut.futureId(), fut);
    }

    /**
     * Removes future from future map.
     *
     * @param fut Future to remove.
     */
    void remoteFuture(GridDhtForceKeysFuture<?, ?> fut) {
        forceKeyFuts.remove(fut.futureId(), fut);
    }

    /** {@inheritDoc} */
    @Override public void evictPartitionAsync(GridDhtLocalPartition part) {
        partsToEvict.add(part);

        if (partsEvictOwning.get() == 0 && partsEvictOwning.compareAndSet(0, 1)) {
            cctx.closures().callLocalSafe(new GPC<Boolean>() {
                @Override public Boolean call() {
                    boolean locked = true;

                    while (locked || !partsToEvict.isEmptyx()) {
                        if (!locked && !partsEvictOwning.compareAndSet(0, 1))
                            return false;

                        try {
                            GridDhtLocalPartition part = partsToEvict.poll();

                            if (part != null)
                                part.tryEvict();
                        }
                        finally {
                            if (!partsToEvict.isEmptyx())
                                locked = true;
                            else {
                                boolean res = partsEvictOwning.compareAndSet(1, 0);

                                assert res;

                                locked = false;
                            }
                        }
                    }

                    return true;
                }
            }, /*system pool*/ true);
        }
    }

    /** {@inheritDoc} */
    @Override public void dumpDebugInfo() {
        if (!forceKeyFuts.isEmpty()) {
            U.warn(log, "Pending force key futures [cache=" + cctx.name() +"]:");

            for (GridDhtForceKeysFuture fut : forceKeyFuts.values())
                U.warn(log, ">>> " + fut);
        }

        if (!pendingAssignmentFetchFuts.isEmpty()) {
            U.warn(log, "Pending assignment fetch futures [cache=" + cctx.name() +"]:");

            for (GridDhtAssignmentFetchFuture fut : pendingAssignmentFetchFuts.values())
                U.warn(log, ">>> " + fut);
        }

        supplier.dumpDebugInfo();
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
}

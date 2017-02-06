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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.CacheEvent;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.IgniteFutureTimeoutCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.events.DiscoveryCustomEvent;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryTopologySnapshot;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.affinity.GridAffinityAssignmentCache;
import org.apache.ignite.internal.processors.cache.CacheAffinityChangeMessage;
import org.apache.ignite.internal.processors.cache.DynamicCacheChangeRequest;
import org.apache.ignite.internal.processors.cache.DynamicCacheDescriptor;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheMvccCandidate;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridClientPartitionTopology;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionTopology;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTopologyFuture;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxKey;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.timeout.GridTimeoutObjectAdapter;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgniteRunnable;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentHashMap8;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_THREAD_DUMP_ON_EXCHANGE_TIMEOUT;
import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_JOINED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.internal.events.DiscoveryCustomEvent.EVT_DISCOVERY_CUSTOM_EVT;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.SYSTEM_POOL;

/**
 * Future for exchanging partition maps.
 */
public class GridDhtPartitionsExchangeFuture extends GridFutureAdapter<AffinityTopologyVersion>
    implements Comparable<GridDhtPartitionsExchangeFuture>, GridDhtTopologyFuture {
    /** */
    public static final int DUMP_PENDING_OBJECTS_THRESHOLD =
        IgniteSystemProperties.getInteger(IgniteSystemProperties.IGNITE_DUMP_PENDING_OBJECTS_THRESHOLD, 10);

    /** */
    private static final long serialVersionUID = 0L;

    /** Dummy flag. */
    private final boolean dummy;

    /** Force preload flag. */
    private final boolean forcePreload;

    /** Dummy reassign flag. */
    private final boolean reassign;

    /** Discovery event. */
    private volatile DiscoveryEvent discoEvt;

    /** */
    @GridToStringExclude
    private final Set<UUID> remaining = new HashSet<>();

    /** */
    @GridToStringExclude
    private int pendingSingleUpdates;

    /** */
    @GridToStringExclude
    private List<ClusterNode> srvNodes;

    /** */
    private ClusterNode crd;

    /** ExchangeFuture id. */
    private final GridDhtPartitionExchangeId exchId;

    /** Cache context. */
    private final GridCacheSharedContext<?, ?> cctx;

    /** Busy lock to prevent activities from accessing exchanger while it's stopping. */
    private ReadWriteLock busyLock;

    /** */
    private AtomicBoolean added = new AtomicBoolean(false);

    /** Event latch. */
    @GridToStringExclude
    private CountDownLatch evtLatch = new CountDownLatch(1);

    /** */
    private GridFutureAdapter<Boolean> initFut;

    /** */
    @GridToStringExclude
    private final List<IgniteRunnable> discoEvts = new ArrayList<>();

    /** */
    private boolean init;

    /** Topology snapshot. */
    private AtomicReference<GridDiscoveryTopologySnapshot> topSnapshot = new AtomicReference<>();

    /** Last committed cache version before next topology version use. */
    private AtomicReference<GridCacheVersion> lastVer = new AtomicReference<>();

    /**
     * Messages received on non-coordinator are stored in case if this node
     * becomes coordinator.
     */
    private final Map<ClusterNode, GridDhtPartitionsSingleMessage> singleMsgs = new ConcurrentHashMap8<>();

    /** Messages received from new coordinator. */
    private final Map<ClusterNode, GridDhtPartitionsFullMessage> fullMsgs = new ConcurrentHashMap8<>();

    /** */
    @SuppressWarnings({"FieldCanBeLocal", "UnusedDeclaration"})
    @GridToStringInclude
    private volatile IgniteInternalFuture<?> partReleaseFut;

    /** */
    private final Object mux = new Object();

    /** Logger. */
    private IgniteLogger log;

    /** Dynamic cache change requests. */
    private Collection<DynamicCacheChangeRequest> reqs;

    /** */
    private CacheAffinityChangeMessage affChangeMsg;

    /** Cache validation results. */
    private volatile Map<Integer, Boolean> cacheValidRes;

    /** Skip preload flag. */
    private boolean skipPreload;

    /** */
    private boolean clientOnlyExchange;

    /** Init timestamp. Used to track the amount of time spent to complete the future. */
    private long initTs;

    /** */
    private boolean centralizedAff;

    /** Forced Rebalance future. */
    private GridFutureAdapter<Boolean> forcedRebFut;

    /**
     * Dummy future created to trigger reassignments if partition
     * topology changed while preloading.
     *
     * @param cctx Cache context.
     * @param reassign Dummy reassign flag.
     * @param discoEvt Discovery event.
     * @param exchId Exchange id.
     */
    public GridDhtPartitionsExchangeFuture(
        GridCacheSharedContext cctx,
        boolean reassign,
        DiscoveryEvent discoEvt,
        GridDhtPartitionExchangeId exchId
    ) {
        dummy = true;
        forcePreload = false;

        this.exchId = exchId;
        this.reassign = reassign;
        this.discoEvt = discoEvt;
        this.cctx = cctx;

        onDone(exchId.topologyVersion());
    }

    /**
     * Force preload future created to trigger reassignments if partition
     * topology changed while preloading.
     *
     * @param cctx Cache context.
     * @param discoEvt Discovery event.
     * @param exchId Exchange id.
     * @param forcedRebFut Forced Rebalance future.
     */
    public GridDhtPartitionsExchangeFuture(GridCacheSharedContext cctx, DiscoveryEvent discoEvt,
        GridDhtPartitionExchangeId exchId, GridFutureAdapter<Boolean> forcedRebFut) {
        dummy = false;
        forcePreload = true;

        this.exchId = exchId;
        this.discoEvt = discoEvt;
        this.cctx = cctx;
        this.forcedRebFut = forcedRebFut;

        reassign = true;

        onDone(exchId.topologyVersion());
    }

    /**
     * @param cctx Cache context.
     * @param busyLock Busy lock.
     * @param exchId Exchange ID.
     * @param reqs Cache change requests.
     * @param affChangeMsg Affinity change message.
     */
    public GridDhtPartitionsExchangeFuture(
        GridCacheSharedContext cctx,
        ReadWriteLock busyLock,
        GridDhtPartitionExchangeId exchId,
        Collection<DynamicCacheChangeRequest> reqs,
        CacheAffinityChangeMessage affChangeMsg
    ) {
        assert busyLock != null;
        assert exchId != null;

        dummy = false;
        forcePreload = false;
        reassign = false;

        this.cctx = cctx;
        this.busyLock = busyLock;
        this.exchId = exchId;
        this.reqs = reqs;
        this.affChangeMsg = affChangeMsg;

        log = cctx.logger(getClass());

        initFut = new GridFutureAdapter<>();

        if (log.isDebugEnabled())
            log.debug("Creating exchange future [localNode=" + cctx.localNodeId() + ", fut=" + this + ']');
    }

    /**
     * @param reqs Cache change requests.
     */
    public void cacheChangeRequests(Collection<DynamicCacheChangeRequest> reqs) {
        this.reqs = reqs;
    }

    /**
     * @param affChangeMsg Affinity change message.
     */
    public void affinityChangeMessage(CacheAffinityChangeMessage affChangeMsg) {
        this.affChangeMsg = affChangeMsg;
    }

    /** {@inheritDoc} */
    @Override public AffinityTopologyVersion topologyVersion() {
        return exchId.topologyVersion();
    }

    /**
     * @return Skip preload flag.
     */
    public boolean skipPreload() {
        return skipPreload;
    }

    /**
     * @return Dummy flag.
     */
    public boolean dummy() {
        return dummy;
    }

    /**
     * @return Force preload flag.
     */
    public boolean forcePreload() {
        return forcePreload;
    }

    /**
     * @return Dummy reassign flag.
     */
    public boolean reassign() {
        return reassign;
    }

    /**
     * @return {@code True} if dummy reassign.
     */
    public boolean dummyReassign() {
        return (dummy() || forcePreload()) && reassign();
    }

    /**
     * @param cacheId Cache ID to check.
     * @param topVer Topology version.
     * @return {@code True} if cache was added during this exchange.
     */
    public boolean isCacheAdded(int cacheId, AffinityTopologyVersion topVer) {
        if (cacheStarted(cacheId))
            return true;

        GridCacheContext<?, ?> cacheCtx = cctx.cacheContext(cacheId);

        return cacheCtx != null && F.eq(cacheCtx.startTopologyVersion(), topVer);
    }

    /**
     * @param cacheId Cache ID.
     * @return {@code True} if non-client cache was added during this exchange.
     */
    public boolean cacheStarted(int cacheId) {
        if (!F.isEmpty(reqs)) {
            for (DynamicCacheChangeRequest req : reqs) {
                if (req.start() && !req.clientStartOnly()) {
                    if (CU.cacheId(req.cacheName()) == cacheId)
                        return true;
                }
            }
        }

        return false;
    }

    /**
     * @return {@code True}
     */
    public boolean onAdded() {
        return added.compareAndSet(false, true);
    }

    /**
     * Event callback.
     *
     * @param exchId Exchange ID.
     * @param discoEvt Discovery event.
     */
    public void onEvent(GridDhtPartitionExchangeId exchId, DiscoveryEvent discoEvt) {
        assert exchId.equals(this.exchId);

        this.discoEvt = discoEvt;

        evtLatch.countDown();
    }

    /**
     * @return Discovery event.
     */
    public DiscoveryEvent discoveryEvent() {
        return discoEvt;
    }

    /**
     * @return Exchange ID.
     */
    public GridDhtPartitionExchangeId exchangeId() {
        return exchId;
    }

    /**
     * @return Forced Rebalance future.
     */
    @Nullable public GridFutureAdapter<Boolean> forcedRebalanceFuture() {
        return forcedRebFut;
    }

    /**
     * @return {@code true} if entered to busy state.
     */
    private boolean enterBusy() {
        if (busyLock.readLock().tryLock())
            return true;

        if (log.isDebugEnabled())
            log.debug("Failed to enter busy state (exchanger is stopping): " + this);

        return false;
    }

    /**
     *
     */
    private void leaveBusy() {
        busyLock.readLock().unlock();
    }

    /**
     * Starts activity.
     *
     * @throws IgniteInterruptedCheckedException If interrupted.
     */
    public void init() throws IgniteInterruptedCheckedException {
        if (isDone())
            return;

        initTs = U.currentTimeMillis();

        U.await(evtLatch);

        assert discoEvt != null : this;
        assert exchId.nodeId().equals(discoEvt.eventNode().id()) : this;
        assert !dummy && !forcePreload : this;

        try {
            srvNodes = new ArrayList<>(cctx.discovery().serverNodes(topologyVersion()));

            remaining.addAll(F.nodeIds(F.view(srvNodes, F.remoteNodes(cctx.localNodeId()))));

            crd = srvNodes.isEmpty() ? null : srvNodes.get(0);

            boolean crdNode = crd != null && crd.isLocal();

            skipPreload = cctx.kernalContext().clientNode();

            ExchangeType exchange;

            Collection<DynamicCacheDescriptor> receivedCaches;

            if (discoEvt.type() == EVT_DISCOVERY_CUSTOM_EVT) {
                if (!F.isEmpty(reqs))
                    exchange = onCacheChangeRequest(crdNode);
                else {
                    assert affChangeMsg != null : this;

                    exchange = onAffinityChangeRequest(crdNode);
                }
            }
            else {
                if (discoEvt.type() == EVT_NODE_JOINED) {
                    receivedCaches = cctx.cache().startReceivedCaches(topologyVersion());

                    if (!discoEvt.eventNode().isLocal())
                        cctx.affinity().initStartedCaches(crdNode, this, receivedCaches);
                }

                if (CU.clientNode(discoEvt.eventNode()))
                    exchange = onClientNodeEvent(crdNode);
                else
                    exchange = onServerNodeEvent(crdNode);
            }

            updateTopologies(crdNode);

            switch (exchange) {
                case ALL: {
                    distributedExchange();

                    break;
                }

                case CLIENT: {
                    initTopologies();

                    clientOnlyExchange();

                    break;
                }

                case NONE: {
                    initTopologies();

                    onDone(topologyVersion());

                    break;
                }

                default:
                    assert false;
            }
        }
        catch (IgniteInterruptedCheckedException e) {
            onDone(e);

            throw e;
        }
        catch (Throwable e) {
            U.error(log, "Failed to reinitialize local partitions (preloading will be stopped): " + exchId, e);

            onDone(e);

            if (e instanceof Error)
                throw (Error)e;
        }
    }

    /**
      * @throws IgniteCheckedException If failed.
     */
    private void initTopologies() throws IgniteCheckedException {
        if (crd != null) {
            for (GridCacheContext cacheCtx : cctx.cacheContexts()) {
                if (cacheCtx.isLocal())
                    continue;

                cacheCtx.topology().beforeExchange(this, !centralizedAff);
            }
        }
    }

    /**
     * @param crd Coordinator flag.
     * @throws IgniteCheckedException If failed.
     */
    private void updateTopologies(boolean crd) throws IgniteCheckedException {
        for (GridCacheContext cacheCtx : cctx.cacheContexts()) {
            if (cacheCtx.isLocal())
                continue;

            GridClientPartitionTopology clientTop = cctx.exchange().clearClientTopology(cacheCtx.cacheId());

            long updSeq = clientTop == null ? -1 : clientTop.lastUpdateSequence();

            GridDhtPartitionTopology top = cacheCtx.topology();

            if (crd) {
                boolean updateTop = !cacheCtx.isLocal() &&
                    exchId.topologyVersion().equals(cacheCtx.startTopologyVersion());

                if (updateTop && clientTop != null)
                    cacheCtx.topology().update(exchId, clientTop.partitionMap(true), clientTop.updateCounters(false));
            }

            top.updateTopologyVersion(exchId, this, updSeq, stopping(cacheCtx.cacheId()));
        }

        for (GridClientPartitionTopology top : cctx.exchange().clientTopologies())
            top.updateTopologyVersion(exchId, this, -1, stopping(top.cacheId()));
    }

    /**
     * @param crd Coordinator flag.
     * @throws IgniteCheckedException If failed.
     * @return Exchange type.
     */
    private ExchangeType onCacheChangeRequest(boolean crd) throws IgniteCheckedException {
        assert !F.isEmpty(reqs) : this;

        boolean clientOnly = cctx.affinity().onCacheChangeRequest(this, crd, reqs);

        if (clientOnly) {
            boolean clientCacheStarted = false;

            for (DynamicCacheChangeRequest req : reqs) {
                if (req.start() && req.clientStartOnly() && req.initiatingNodeId().equals(cctx.localNodeId())) {
                    clientCacheStarted = true;

                    break;
                }
            }

            if (clientCacheStarted)
                return ExchangeType.CLIENT;
            else
                return ExchangeType.NONE;
        }
        else
            return cctx.kernalContext().clientNode() ? ExchangeType.CLIENT : ExchangeType.ALL;
    }

    /**
     * @param crd Coordinator flag.
     * @throws IgniteCheckedException If failed.
     * @return Exchange type.
     */
    private ExchangeType onAffinityChangeRequest(boolean crd) throws IgniteCheckedException {
        assert affChangeMsg != null : this;

        cctx.affinity().onChangeAffinityMessage(this, crd, affChangeMsg);

        if (cctx.kernalContext().clientNode())
            return ExchangeType.CLIENT;

        return ExchangeType.ALL;
    }

    /**
     * @param crd Coordinator flag.
     * @throws IgniteCheckedException If failed.
     * @return Exchange type.
     */
    private ExchangeType onClientNodeEvent(boolean crd) throws IgniteCheckedException {
        assert CU.clientNode(discoEvt.eventNode()) : this;

        if (discoEvt.type() == EVT_NODE_LEFT || discoEvt.type() == EVT_NODE_FAILED) {
            onLeft();

            assert !discoEvt.eventNode().isLocal() : discoEvt;
        }
        else
            assert discoEvt.type() == EVT_NODE_JOINED : discoEvt;

        cctx.affinity().onClientEvent(this, crd);

        if (discoEvt.eventNode().isLocal())
            return ExchangeType.CLIENT;
        else
            return ExchangeType.NONE;
    }

    /**
     * @param crd Coordinator flag.
     * @throws IgniteCheckedException If failed.
     * @return Exchange type.
     */
    private ExchangeType onServerNodeEvent(boolean crd) throws IgniteCheckedException {
        assert !CU.clientNode(discoEvt.eventNode()) : this;

        if (discoEvt.type() == EVT_NODE_LEFT || discoEvt.type() == EVT_NODE_FAILED) {
            onLeft();

            warnNoAffinityNodes();

            centralizedAff = cctx.affinity().onServerLeft(this);
        }
        else {
            assert discoEvt.type() == EVT_NODE_JOINED : discoEvt;

            cctx.affinity().onServerJoin(this, crd);
        }

        if (cctx.kernalContext().clientNode())
            return ExchangeType.CLIENT;
        else
            return ExchangeType.ALL;
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    private void clientOnlyExchange() throws IgniteCheckedException {
        clientOnlyExchange = true;

        if (crd != null) {
            if (crd.isLocal()) {
                for (GridCacheContext cacheCtx : cctx.cacheContexts()) {
                    boolean updateTop = !cacheCtx.isLocal() &&
                        exchId.topologyVersion().equals(cacheCtx.startTopologyVersion());

                    if (updateTop) {
                        for (GridClientPartitionTopology top : cctx.exchange().clientTopologies()) {
                            if (top.cacheId() == cacheCtx.cacheId()) {
                                cacheCtx.topology().update(exchId,
                                    top.partitionMap(true),
                                    top.updateCounters(false));

                                break;
                            }
                        }
                    }
                }
            }
            else {
                if (!centralizedAff)
                    sendLocalPartitions(crd);

                initDone();

                return;
            }
        }
        else {
            if (centralizedAff) { // Last server node failed.
                for (GridCacheContext cacheCtx : cctx.cacheContexts()) {
                    GridAffinityAssignmentCache aff = cacheCtx.affinity().affinityCache();

                    aff.initialize(topologyVersion(), aff.idealAssignment());
                }
            }
        }

        onDone(topologyVersion());
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    private void distributedExchange() throws IgniteCheckedException {
        assert crd != null;

        assert !cctx.kernalContext().clientNode();

        for (GridCacheContext cacheCtx : cctx.cacheContexts()) {
            if (cacheCtx.isLocal())
                continue;

            cacheCtx.preloader().onTopologyChanged(this);
        }

        waitPartitionRelease();

        boolean topChanged = discoEvt.type() != EVT_DISCOVERY_CUSTOM_EVT || affChangeMsg != null;

        for (GridCacheContext cacheCtx : cctx.cacheContexts()) {
            if (cacheCtx.isLocal() || stopping(cacheCtx.cacheId()))
                continue;

            if (topChanged) {
                cacheCtx.continuousQueries().beforeExchange(exchId.topologyVersion());

                // Partition release future is done so we can flush the write-behind store.
                cacheCtx.store().forceFlush();
            }

            cacheCtx.topology().beforeExchange(this, !centralizedAff);
        }

        if (crd.isLocal()) {
            if (remaining.isEmpty())
                onAllReceived();
        }
        else
            sendPartitions(crd);

        initDone();
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    private void waitPartitionRelease() throws IgniteCheckedException {
        IgniteInternalFuture<?> partReleaseFut = cctx.partitionReleaseFuture(topologyVersion());

        // Assign to class variable so it will be included into toString() method.
        this.partReleaseFut = partReleaseFut;

        if (exchId.isLeft())
            cctx.mvcc().removeExplicitNodeLocks(exchId.nodeId(), exchId.topologyVersion());

        if (log.isDebugEnabled())
            log.debug("Before waiting for partition release future: " + this);

        int dumpedObjects = 0;

        while (true) {
            try {
                partReleaseFut.get(2 * cctx.gridConfig().getNetworkTimeout(), TimeUnit.MILLISECONDS);

                break;
            }
            catch (IgniteFutureTimeoutCheckedException ignored) {
                // Print pending transactions and locks that might have led to hang.
                if (dumpedObjects < DUMP_PENDING_OBJECTS_THRESHOLD) {
                    dumpPendingObjects();

                    dumpedObjects++;
                }
            }
        }

        if (log.isDebugEnabled())
            log.debug("After waiting for partition release future: " + this);

        IgniteInternalFuture<?> locksFut = cctx.mvcc().finishLocks(exchId.topologyVersion());

        dumpedObjects = 0;

        while (true) {
            try {
                locksFut.get(2 * cctx.gridConfig().getNetworkTimeout(), TimeUnit.MILLISECONDS);

                break;
            }
            catch (IgniteFutureTimeoutCheckedException ignored) {
                if (dumpedObjects < DUMP_PENDING_OBJECTS_THRESHOLD) {
                    U.warn(log, "Failed to wait for locks release future. Dumping pending objects that might be the " +
                        "cause [topVer=" + topologyVersion() + ", nodeId=" + cctx.localNodeId() + "]: ");

                    U.warn(log, "Locked keys:");

                    for (IgniteTxKey key : cctx.mvcc().lockedKeys())
                        U.warn(log, "Locked key: " + key);

                    for (IgniteTxKey key : cctx.mvcc().nearLockedKeys())
                        U.warn(log, "Locked near key: " + key);

                    Map<IgniteTxKey, Collection<GridCacheMvccCandidate>> locks =
                        cctx.mvcc().unfinishedLocks(exchId.topologyVersion());

                    for (Map.Entry<IgniteTxKey, Collection<GridCacheMvccCandidate>> e : locks.entrySet())
                        U.warn(log, "Awaited locked entry [key=" + e.getKey() + ", mvcc=" + e.getValue() + ']');

                    dumpedObjects++;

                    if (IgniteSystemProperties.getBoolean(IGNITE_THREAD_DUMP_ON_EXCHANGE_TIMEOUT, false))
                        U.dumpThreads(log);
                }
            }
        }
    }

    /**
     *
     */
    private void onLeft() {
        for (GridCacheContext cacheCtx : cctx.cacheContexts()) {
            if (cacheCtx.isLocal())
                continue;

            cacheCtx.preloader().unwindUndeploys();
        }

        cctx.mvcc().removeExplicitNodeLocks(exchId.nodeId(), exchId.topologyVersion());
    }

    /**
     *
     */
    private void warnNoAffinityNodes() {
        List<String> cachesWithoutNodes = null;

        for (String name : cctx.cache().cacheNames()) {
            if (cctx.discovery().cacheAffinityNodes(name, topologyVersion()).isEmpty()) {
                if (cachesWithoutNodes == null)
                    cachesWithoutNodes = new ArrayList<>();

                cachesWithoutNodes.add(name);

                // Fire event even if there is no client cache started.
                if (cctx.gridEvents().isRecordable(EventType.EVT_CACHE_NODES_LEFT)) {
                    Event evt = new CacheEvent(
                        name,
                        cctx.localNode(),
                        cctx.localNode(),
                        "All server nodes have left the cluster.",
                        EventType.EVT_CACHE_NODES_LEFT,
                        0,
                        false,
                        null,
                        null,
                        null,
                        null,
                        false,
                        null,
                        false,
                        null,
                        null,
                        null
                    );

                    cctx.gridEvents().record(evt);
                }
            }
        }

        if (cachesWithoutNodes != null) {
            StringBuilder sb =
                new StringBuilder("All server nodes for the following caches have left the cluster: ");

            for (int i = 0; i < cachesWithoutNodes.size(); i++) {
                String cache = cachesWithoutNodes.get(i);

                sb.append('\'').append(cache).append('\'');

                if (i != cachesWithoutNodes.size() - 1)
                    sb.append(", ");
            }

            U.quietAndWarn(log, sb.toString());

            U.quietAndWarn(log, "Must have server nodes for caches to operate.");
        }
    }

    /**
     *
     */
    private void dumpPendingObjects() {
        U.warn(log, "Failed to wait for partition release future [topVer=" + topologyVersion() +
            ", node=" + cctx.localNodeId() + "]. Dumping pending objects that might be the cause: ");

        try {
            cctx.exchange().dumpDebugInfo(topologyVersion());
        }
        catch (Exception e) {
            U.error(log, "Failed to dump debug information: " + e, e);
        }

        if (IgniteSystemProperties.getBoolean(IGNITE_THREAD_DUMP_ON_EXCHANGE_TIMEOUT, false))
            U.dumpThreads(log);
    }

    /**
     * @param cacheId Cache ID to check.
     * @return {@code True} if cache is stopping by this exchange.
     */
    public boolean stopping(int cacheId) {
        boolean stopping = false;

        if (!F.isEmpty(reqs)) {
            for (DynamicCacheChangeRequest req : reqs) {
                if (cacheId == CU.cacheId(req.cacheName())) {
                    stopping = req.stop();

                    break;
                }
            }
        }

        return stopping;
    }

    /**
     * @param node Node.
     * @throws IgniteCheckedException If failed.
     */
    private void sendLocalPartitions(ClusterNode node)
        throws IgniteCheckedException {
        GridDhtPartitionsSingleMessage m = cctx.exchange().createPartitionsSingleMessage(node,
            exchangeId(),
            clientOnlyExchange,
            true);

        if (log.isDebugEnabled())
            log.debug("Sending local partitions [nodeId=" + node.id() + ", exchId=" + exchId + ", msg=" + m + ']');

        try {
            cctx.io().send(node, m, SYSTEM_POOL);
        }
        catch (ClusterTopologyCheckedException ignored) {
            if (log.isDebugEnabled())
                log.debug("Node left during partition exchange [nodeId=" + node.id() + ", exchId=" + exchId + ']');
        }
    }

    /**
     * @param nodes Target nodes.
     * @param compress {@code True} if it is possible to use compression for message.
     * @return Message.
     */
    private GridDhtPartitionsFullMessage createPartitionsMessage(Collection<ClusterNode> nodes, boolean compress) {
        GridCacheVersion last = lastVer.get();

        return cctx.exchange().createPartitionsFullMessage(nodes,
            exchangeId(),
            last != null ? last : cctx.versions().last(),
            compress);
    }

    /**
     * @param nodes Nodes.
     * @throws IgniteCheckedException If failed.
     */
    private void sendAllPartitions(Collection<ClusterNode> nodes) throws IgniteCheckedException {
        GridDhtPartitionsFullMessage m = createPartitionsMessage(nodes, true);

        assert !nodes.contains(cctx.localNode());

        if (log.isDebugEnabled())
            log.debug("Sending full partition map [nodeIds=" + F.viewReadOnly(nodes, F.node2id()) +
                ", exchId=" + exchId + ", msg=" + m + ']');

        cctx.io().safeSend(nodes, m, SYSTEM_POOL, null);
    }

    /**
     * @param oldestNode Oldest node.
     */
    private void sendPartitions(ClusterNode oldestNode) {
        try {
            sendLocalPartitions(oldestNode);
        }
        catch (ClusterTopologyCheckedException ignore) {
            if (log.isDebugEnabled())
                log.debug("Oldest node left during partition exchange [nodeId=" + oldestNode.id() +
                    ", exchId=" + exchId + ']');
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Failed to send local partitions to oldest node (will retry after timeout) [oldestNodeId=" +
                oldestNode.id() + ", exchId=" + exchId + ']', e);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean onDone(@Nullable AffinityTopologyVersion res, @Nullable Throwable err) {
        boolean realExchange = !dummy && !forcePreload;

        if (err == null && realExchange) {
            for (GridCacheContext cacheCtx : cctx.cacheContexts()) {
                if (cacheCtx.isLocal())
                    continue;

                try {
                    if (centralizedAff)
                        cacheCtx.topology().initPartitions(this);
                }
                catch (IgniteInterruptedCheckedException e) {
                    U.error(log, "Failed to initialize partitions.", e);
                }

                GridCacheContext drCacheCtx = cacheCtx.isNear() ? cacheCtx.near().dht().context() : cacheCtx;

                if (drCacheCtx.isDrEnabled()) {
                    try {
                        drCacheCtx.dr().onExchange(topologyVersion(), exchId.isLeft());
                    }
                    catch (IgniteCheckedException e) {
                        U.error(log, "Failed to notify DR: " + e, e);
                    }
                }
            }

            Map<Integer, Boolean> m = null;

            for (GridCacheContext cacheCtx : cctx.cacheContexts()) {
                if (cacheCtx.config().getTopologyValidator() != null && !CU.isSystemCache(cacheCtx.name())) {
                    if (m == null)
                        m = new HashMap<>();

                    m.put(cacheCtx.cacheId(), cacheCtx.config().getTopologyValidator().validate(discoEvt.topologyNodes()));
                }
            }

            cacheValidRes = m != null ? m : Collections.<Integer, Boolean>emptyMap();
        }

        cctx.exchange().onExchangeDone(this, err);

        cctx.cache().onExchangeDone(exchId.topologyVersion(), reqs, err);

        if (super.onDone(res, err) && realExchange) {
            if (log.isDebugEnabled())
                log.debug("Completed partition exchange [localNode=" + cctx.localNodeId() + ", exchange= " + this +
                    "duration=" + duration() + ", durationFromInit=" + (U.currentTimeMillis() - initTs) + ']');

            initFut.onDone(err == null);

            if (exchId.isLeft()) {
                for (GridCacheContext cacheCtx : cctx.cacheContexts())
                    cacheCtx.config().getAffinity().removeNode(exchId.nodeId());
            }

            reqs = null;

            if (discoEvt instanceof DiscoveryCustomEvent)
                ((DiscoveryCustomEvent)discoEvt).customMessage(null);

            return true;
        }

        return dummy;
    }

    /** {@inheritDoc} */
    @Override public Throwable validateCache(GridCacheContext cctx) {
        Throwable err = error();

        if (err != null)
            return err;

        if (cctx.config().getTopologyValidator() != null) {
            Boolean res = cacheValidRes.get(cctx.cacheId());

            if (res != null && !res) {
                return new IgniteCheckedException("Failed to perform cache operation " +
                    "(cache topology is not valid): " + cctx.name());
            }
        }

        return null;
    }

    /**
     * Cleans up resources to avoid excessive memory usage.
     */
    public void cleanUp() {
        topSnapshot.set(null);
        singleMsgs.clear();
        fullMsgs.clear();
        crd = null;
        partReleaseFut = null;
    }

    /**
     * @param ver Version.
     */
    private void updateLastVersion(GridCacheVersion ver) {
        assert ver != null;

        while (true) {
            GridCacheVersion old = lastVer.get();

            if (old == null || Long.compare(old.order(), ver.order()) < 0) {
                if (lastVer.compareAndSet(old, ver))
                    break;
            }
            else
                break;
        }
    }

    /**
     * @param node Sender node.
     * @param msg Single partition info.
     */
    public void onReceive(final ClusterNode node, final GridDhtPartitionsSingleMessage msg) {
        assert msg != null;
        assert msg.exchangeId().equals(exchId) : msg;
        assert msg.lastVersion() != null : msg;

        if (!msg.client())
            updateLastVersion(msg.lastVersion());

        if (isDone()) {
            if (log.isDebugEnabled())
                log.debug("Received message for finished future (will reply only to sender) [msg=" + msg +
                    ", fut=" + this + ']');

            if (!centralizedAff)
                sendAllPartitions(node.id(), cctx.gridConfig().getNetworkSendRetryCount());
        }
        else {
            initFut.listen(new CI1<IgniteInternalFuture<Boolean>>() {
                @Override public void apply(IgniteInternalFuture<Boolean> f) {
                    try {
                        if (!f.get())
                            return;
                    }
                    catch (IgniteCheckedException e) {
                        U.error(log, "Failed to initialize exchange future: " + this, e);

                        return;
                    }

                    processMessage(node, msg);
                }
            });
        }
    }

    /**
     * @param node Sender node.
     * @param msg Message.
     */
    private void processMessage(ClusterNode node, GridDhtPartitionsSingleMessage msg) {
        boolean allReceived = false;
        boolean updateSingleMap = false;

        synchronized (mux) {
            assert crd != null;

            if (crd.isLocal()) {
                if (remaining.remove(node.id())) {
                    updateSingleMap = true;

                    pendingSingleUpdates++;

                    allReceived = remaining.isEmpty();
                }
            }
            else
                singleMsgs.put(node, msg);
        }

        if (updateSingleMap) {
            try {
                updatePartitionSingleMap(msg);
            }
            finally {
                synchronized (mux) {
                    assert pendingSingleUpdates > 0;

                    pendingSingleUpdates--;

                    if (pendingSingleUpdates == 0)
                        mux.notifyAll();
                }
            }
        }

        if (allReceived) {
            awaitSingleMapUpdates();

            onAllReceived();
        }
    }

    /**
     *
     */
    private void awaitSingleMapUpdates() {
        synchronized (mux) {
            try {
                while (pendingSingleUpdates > 0)
                    U.wait(mux);
            }
            catch (IgniteInterruptedCheckedException e) {
                U.warn(log, "Failed to wait for partition map updates, thread was interrupted: " + e);
            }
        }
    }

    /**
     * @param fut Affinity future.
     */
    private void onAffinityInitialized(IgniteInternalFuture<Map<Integer, Map<Integer, List<UUID>>>> fut) {
        try {
            assert fut.isDone();

            Map<Integer, Map<Integer, List<UUID>>> assignmentChange = fut.get();

            GridDhtPartitionsFullMessage m = createPartitionsMessage(null, false);

            CacheAffinityChangeMessage msg = new CacheAffinityChangeMessage(exchId, m, assignmentChange);

            if (log.isDebugEnabled())
                log.debug("Centralized affinity exchange, send affinity change message: " + msg);

            cctx.discovery().sendCustomEvent(msg);
        }
        catch (IgniteCheckedException e) {
            onDone(e);
        }
    }

    /**
     */
    private void onAllReceived() {
        try {
            assert crd.isLocal();

            if (!crd.equals(cctx.discovery().serverNodes(topologyVersion()).get(0))) {
                for (GridCacheContext cacheCtx : cctx.cacheContexts()) {
                    if (!cacheCtx.isLocal())
                        cacheCtx.topology().beforeExchange(GridDhtPartitionsExchangeFuture.this, !centralizedAff);
                }
            }

            for (GridCacheContext cacheCtx : cctx.cacheContexts()) {
                if (!cacheCtx.isLocal())
                    cacheCtx.topology().checkEvictions();
            }

            updateLastVersion(cctx.versions().last());

            cctx.versions().onExchange(lastVer.get().order());

            if (centralizedAff) {
                IgniteInternalFuture<Map<Integer, Map<Integer, List<UUID>>>> fut = cctx.affinity().initAffinityOnNodeLeft(this);

                if (!fut.isDone()) {
                    fut.listen(new IgniteInClosure<IgniteInternalFuture<Map<Integer, Map<Integer, List<UUID>>>>>() {
                        @Override public void apply(IgniteInternalFuture<Map<Integer, Map<Integer, List<UUID>>>> fut) {
                            onAffinityInitialized(fut);
                        }
                    });
                }
                else
                    onAffinityInitialized(fut);
            }
            else {
                List<ClusterNode> nodes;

                synchronized (mux) {
                    srvNodes.remove(cctx.localNode());

                    nodes = new ArrayList<>(srvNodes);
                }

                if (!nodes.isEmpty())
                    sendAllPartitions(nodes);

                onDone(exchangeId().topologyVersion());
            }
        }
        catch (IgniteCheckedException e) {
            onDone(e);
        }
    }

    /**
     * @param nodeId Node ID.
     * @param retryCnt Number of retries.
     */
    private void sendAllPartitions(final UUID nodeId, final int retryCnt) {
        ClusterNode n = cctx.node(nodeId);

        try {
            if (n != null)
                sendAllPartitions(F.asList(n));
        }
        catch (IgniteCheckedException e) {
            if (e instanceof ClusterTopologyCheckedException || !cctx.discovery().alive(n)) {
                log.debug("Failed to send full partition map to node, node left grid " +
                    "[rmtNode=" + nodeId + ", exchangeId=" + exchId + ']');

                return;
            }

            if (retryCnt > 0) {
                long timeout = cctx.gridConfig().getNetworkSendRetryDelay();

                LT.error(log, e, "Failed to send full partition map to node (will retry after timeout) " +
                    "[node=" + nodeId + ", exchangeId=" + exchId + ", timeout=" + timeout + ']');

                cctx.time().addTimeoutObject(new GridTimeoutObjectAdapter(timeout) {
                    @Override public void onTimeout() {
                        sendAllPartitions(nodeId, retryCnt - 1);
                    }
                });
            }
            else
                U.error(log, "Failed to send full partition map [node=" + n + ", exchangeId=" + exchId + ']', e);
        }
    }

    /**
     * @param node Sender node.
     * @param msg Full partition info.
     */
    public void onReceive(final ClusterNode node, final GridDhtPartitionsFullMessage msg) {
        assert msg != null;

        final UUID nodeId = node.id();

        if (isDone()) {
            if (log.isDebugEnabled())
                log.debug("Received message for finished future [msg=" + msg + ", fut=" + this + ']');

            return;
        }

        if (log.isDebugEnabled())
            log.debug("Received full partition map from node [nodeId=" + nodeId + ", msg=" + msg + ']');

        initFut.listen(new CI1<IgniteInternalFuture<Boolean>>() {
            @Override public void apply(IgniteInternalFuture<Boolean> f) {
                try {
                    if (!f.get())
                        return;
                }
                catch (IgniteCheckedException e) {
                    U.error(log, "Failed to initialize exchange future: " + this, e);

                    return;
                }

                processMessage(node, msg);
            }
        });
    }

    /**
     * @param node Sender node.
     * @param msg Message.
     */
    private void processMessage(ClusterNode node, GridDhtPartitionsFullMessage msg) {
        assert msg.exchangeId().equals(exchId) : msg;
        assert msg.lastVersion() != null : msg;

        synchronized (mux) {
            if (crd == null)
                return;

            if (!crd.equals(node)) {
                if (log.isDebugEnabled())
                    log.debug("Received full partition map from unexpected node [oldest=" + crd.id() +
                            ", nodeId=" + node.id() + ']');

                if (node.order() > crd.order())
                    fullMsgs.put(node, msg);

                return;
            }
        }

        updatePartitionFullMap(msg);

        onDone(exchId.topologyVersion());
    }

    /**
     * Updates partition map in all caches.
     *
     * @param msg Partitions full messages.
     */
    private void updatePartitionFullMap(GridDhtPartitionsFullMessage msg) {
        cctx.versions().onExchange(msg.lastVersion().order());

        for (Map.Entry<Integer, GridDhtPartitionFullMap> entry : msg.partitions().entrySet()) {
            Integer cacheId = entry.getKey();

            Map<Integer, Long> cntrMap = msg.partitionUpdateCounters(cacheId);

            GridCacheContext cacheCtx = cctx.cacheContext(cacheId);

            if (cacheCtx != null)
                cacheCtx.topology().update(exchId, entry.getValue(), cntrMap);
            else {
                ClusterNode oldest = cctx.discovery().oldestAliveCacheServerNode(AffinityTopologyVersion.NONE);

                if (oldest != null && oldest.isLocal())
                    cctx.exchange().clientTopology(cacheId, this).update(exchId, entry.getValue(), cntrMap);
            }
        }
    }

    /**
     * Updates partition map in all caches.
     *
     * @param msg Partitions single message.
     */
    private void updatePartitionSingleMap(GridDhtPartitionsSingleMessage msg) {
        for (Map.Entry<Integer, GridDhtPartitionMap2> entry : msg.partitions().entrySet()) {
            Integer cacheId = entry.getKey();
            GridCacheContext cacheCtx = cctx.cacheContext(cacheId);

            GridDhtPartitionTopology top = cacheCtx != null ? cacheCtx.topology() :
                cctx.exchange().clientTopology(cacheId, this);

            top.update(exchId, entry.getValue(), msg.partitionUpdateCounters(cacheId), false);
        }
    }

    /**
     * Affinity change message callback, processed from the same thread as {@link #onNodeLeft}.
     *
     * @param node Message sender node.
     * @param msg Message.
     */
    public void onAffinityChangeMessage(final ClusterNode node, final CacheAffinityChangeMessage msg) {
        assert exchId.equals(msg.exchangeId()) : msg;

        onDiscoveryEvent(new IgniteRunnable() {
            @Override public void run() {
                if (isDone() || !enterBusy())
                    return;

                try {
                    assert centralizedAff;

                    if (crd.equals(node)) {
                        cctx.affinity().onExchangeChangeAffinityMessage(GridDhtPartitionsExchangeFuture.this,
                            crd.isLocal(),
                            msg);

                        if (!crd.isLocal()) {
                            GridDhtPartitionsFullMessage partsMsg = msg.partitionsMessage();

                            assert partsMsg != null : msg;
                            assert partsMsg.lastVersion() != null : partsMsg;

                            updatePartitionFullMap(partsMsg);
                        }

                        onDone(topologyVersion());
                    }
                    else {
                        if (log.isDebugEnabled()) {
                            log.debug("Ignore affinity change message, coordinator changed [node=" + node.id() +
                                ", crd=" + crd.id() +
                                ", msg=" + msg +
                                ']');
                        }
                    }
                }
                finally {
                    leaveBusy();
                }
            }
        });
    }

    /**
     * @param c Closure.
     */
    private void onDiscoveryEvent(IgniteRunnable c) {
        synchronized (discoEvts) {
            if (!init) {
                discoEvts.add(c);

                return;
            }

            assert discoEvts.isEmpty() : discoEvts;
        }

        c.run();
    }

    /**
     *
     */
    private void initDone() {
        while (!isDone()) {
            List<IgniteRunnable> evts;

            synchronized (discoEvts) {
                if (discoEvts.isEmpty()) {
                    init = true;

                    break;
                }

                evts = new ArrayList<>(discoEvts);

                discoEvts.clear();
            }

            for (IgniteRunnable c : evts)
                c.run();
        }

        initFut.onDone(true);
    }

    /**
     * Node left callback, processed from the same thread as {@link #onAffinityChangeMessage}.
     *
     * @param node Left node.
     */
    public void onNodeLeft(final ClusterNode node) {
        if (isDone() || !enterBusy())
            return;

        cctx.mvcc().removeExplicitNodeLocks(node.id(), topologyVersion());

        try {
            onDiscoveryEvent(new IgniteRunnable() {
                @Override public void run() {
                    if (isDone() || !enterBusy())
                        return;

                    try {
                        boolean crdChanged = false;
                        boolean allReceived = false;
                        Set<UUID> reqFrom = null;

                        ClusterNode crd0;

                        synchronized (mux) {
                            if (!srvNodes.remove(node))
                                return;

                            boolean rmvd = remaining.remove(node.id());

                            if (node.equals(crd)) {
                                crdChanged = true;

                                crd = srvNodes.size() > 0 ? srvNodes.get(0) : null;
                            }

                            if (crd != null && crd.isLocal()) {
                                if (rmvd)
                                    allReceived = remaining.isEmpty();

                                if (crdChanged && !remaining.isEmpty())
                                    reqFrom = new HashSet<>(remaining);
                            }

                            crd0 = crd;
                        }

                        if (crd0 == null) {
                            assert cctx.kernalContext().clientNode() || cctx.localNode().isDaemon() : cctx.localNode();

                            List<ClusterNode> empty = Collections.emptyList();

                            for (GridCacheContext cacheCtx : cctx.cacheContexts()) {
                                List<List<ClusterNode>> affAssignment = new ArrayList<>(cacheCtx.affinity().partitions());

                                for (int i = 0; i < cacheCtx.affinity().partitions(); i++)
                                    affAssignment.add(empty);

                                cacheCtx.affinity().affinityCache().initialize(topologyVersion(), affAssignment);
                            }

                            onDone(topologyVersion());

                            return;
                        }

                        if (crd0.isLocal()) {
                            if (allReceived) {
                                awaitSingleMapUpdates();

                                onAllReceived();

                                return;
                            }

                            if (crdChanged && reqFrom != null) {
                                GridDhtPartitionsSingleRequest req = new GridDhtPartitionsSingleRequest(exchId);

                                for (UUID nodeId : reqFrom) {
                                    try {
                                        // It is possible that some nodes finished exchange with previous coordinator.
                                        cctx.io().send(nodeId, req, SYSTEM_POOL);
                                    }
                                    catch (ClusterTopologyCheckedException ignored) {
                                        if (log.isDebugEnabled())
                                            log.debug("Node left during partition exchange [nodeId=" + nodeId +
                                                ", exchId=" + exchId + ']');
                                    }
                                    catch (IgniteCheckedException e) {
                                        U.error(log, "Failed to request partitions from node: " + nodeId, e);
                                    }
                                }
                            }

                            for (Map.Entry<ClusterNode, GridDhtPartitionsSingleMessage> m : singleMsgs.entrySet())
                                processMessage(m.getKey(), m.getValue());
                        }
                        else {
                            if (crdChanged) {
                                sendPartitions(crd0);

                                for (Map.Entry<ClusterNode, GridDhtPartitionsFullMessage> m : fullMsgs.entrySet())
                                    processMessage(m.getKey(), m.getValue());
                            }
                        }
                    }
                    finally {
                        leaveBusy();
                    }
                }
            });
        }
        finally {
            leaveBusy();
        }
    }

    /** {@inheritDoc} */
    @Override public int compareTo(GridDhtPartitionsExchangeFuture fut) {
        return exchId.compareTo(fut.exchId);
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        GridDhtPartitionsExchangeFuture fut = (GridDhtPartitionsExchangeFuture)o;

        return exchId.equals(fut.exchId);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return exchId.hashCode();
    }

    /**
     *
     */
    enum ExchangeType {
        /** */
        CLIENT,
        /** */
        ALL,
        /** */
        NONE
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        Set<UUID> remaining;
        List<ClusterNode> srvNodes;

        synchronized (mux) {
            remaining = new HashSet<>(this.remaining);
            srvNodes = this.srvNodes != null ? new ArrayList<>(this.srvNodes) : null;
        }

        return S.toString(GridDhtPartitionsExchangeFuture.class, this,
            "evtLatch", evtLatch == null ? "null" : evtLatch.getCount(),
            "remaining", remaining,
            "srvNodes", srvNodes,
            "super", super.toString());
    }
}

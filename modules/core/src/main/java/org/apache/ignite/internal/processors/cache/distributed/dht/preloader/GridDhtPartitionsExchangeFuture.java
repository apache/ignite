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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.CacheEvent;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.IgniteClientDisconnectedCheckedException;
import org.apache.ignite.internal.IgniteDiagnosticAware;
import org.apache.ignite.internal.IgniteDiagnosticPrepareContext;
import org.apache.ignite.internal.IgniteFutureTimeoutCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.IgniteNeedReconnectException;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.events.DiscoveryCustomEvent;
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.pagemem.snapshot.StartFullSnapshotAckDiscoveryMessage;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.affinity.GridAffinityAssignmentCache;
import org.apache.ignite.internal.processors.cache.CacheAffinityChangeMessage;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.CachePartitionExchangeWorkerTask;
import org.apache.ignite.internal.processors.cache.ClusterState;
import org.apache.ignite.internal.processors.cache.DynamicCacheChangeBatch;
import org.apache.ignite.internal.processors.cache.DynamicCacheDescriptor;
import org.apache.ignite.internal.processors.cache.ExchangeActions;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheMvccCandidate;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTopologyFutureAdapter;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridClientPartitionTopology;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionTopology;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxKey;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.cluster.GridClusterStateProcessor;
import org.apache.ignite.internal.processors.timeout.GridTimeoutObjectAdapter;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgniteRunnable;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentHashMap8;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_LONG_OPERATIONS_DUMP_TIMEOUT_LIMIT;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_THREAD_DUMP_ON_EXCHANGE_TIMEOUT;
import static org.apache.ignite.IgniteSystemProperties.getBoolean;
import static org.apache.ignite.IgniteSystemProperties.getLong;
import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_JOINED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.internal.events.DiscoveryCustomEvent.EVT_DISCOVERY_CUSTOM_EVT;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.SYSTEM_POOL;

/**
 * Future for exchanging partition maps.
 */
@SuppressWarnings({"TypeMayBeWeakened", "unchecked"})
public class GridDhtPartitionsExchangeFuture extends GridDhtTopologyFutureAdapter
    implements Comparable<GridDhtPartitionsExchangeFuture>, CachePartitionExchangeWorkerTask, IgniteDiagnosticAware {
    /** Dummy flag. */
    private final boolean dummy;

    /** Force preload flag. */
    private final boolean forcePreload;

    /** Dummy reassign flag. */
    private final boolean reassign;

    /** */
    @GridToStringExclude
    private volatile DiscoCache discoCache;

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
    private final CountDownLatch evtLatch = new CountDownLatch(1);

    /** */
    private GridFutureAdapter<Boolean> initFut;

    /** */
    @GridToStringExclude
    private final List<IgniteRunnable> discoEvts = new ArrayList<>();

    /** */
    private boolean init;

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

    /** Logger. */
    private final IgniteLogger log;

    /** Cache change requests. */
    private ExchangeActions exchActions;

    /** */
    private CacheAffinityChangeMessage affChangeMsg;

    /** Skip preload flag. */
    private boolean skipPreload;

    /** */
    private boolean clientOnlyExchange;

    /** Init timestamp. Used to track the amount of time spent to complete the future. */
    private long initTs;

    /** */
    private boolean centralizedAff;

    /** Change global state exception. */
    private Exception changeGlobalStateE;

    /** Change global state exceptions. */
    private final Map<UUID, Exception> changeGlobalStateExceptions = new ConcurrentHashMap8<>();

    /** This exchange for change global state. */
    private boolean exchangeOnChangeGlobalState;

    /** */
    private final ConcurrentMap<UUID, GridDhtPartitionsAbstractMessage> msgs = new ConcurrentHashMap8<>();

    /** Forced Rebalance future. */
    private GridCompoundFuture<Boolean, Boolean> forcedRebFut;

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

        log = cctx.logger(getClass());

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
        GridDhtPartitionExchangeId exchId, GridCompoundFuture<Boolean, Boolean> forcedRebFut) {
        dummy = false;
        forcePreload = true;

        this.exchId = exchId;
        this.discoEvt = discoEvt;
        this.cctx = cctx;
        this.forcedRebFut = forcedRebFut;

        log = cctx.logger(getClass());

        reassign = true;

        onDone(exchId.topologyVersion());
    }

    /**
     * @param cctx Cache context.
     * @param busyLock Busy lock.
     * @param exchId Exchange ID.
     * @param exchActions Cache change requests.
     * @param affChangeMsg Affinity change message.
     */
    public GridDhtPartitionsExchangeFuture(
        GridCacheSharedContext cctx,
        ReadWriteLock busyLock,
        GridDhtPartitionExchangeId exchId,
        ExchangeActions exchActions,
        CacheAffinityChangeMessage affChangeMsg
    ) {
        assert busyLock != null;
        assert exchId != null;
        assert exchId.topologyVersion() != null;
        assert exchActions == null || !exchActions.empty();

        dummy = false;
        forcePreload = false;
        reassign = false;

        this.cctx = cctx;
        this.busyLock = busyLock;
        this.exchId = exchId;
        this.exchActions = exchActions;
        this.affChangeMsg = affChangeMsg;

        log = cctx.logger(getClass());

        initFut = new GridFutureAdapter<>();

        if (log.isDebugEnabled())
            log.debug("Creating exchange future [localNode=" + cctx.localNodeId() + ", fut=" + this + ']');
    }

    /**
     * @param exchActions Exchange actions.
     */
    public void exchangeActions(ExchangeActions exchActions) {
        assert exchActions == null || !exchActions.empty() : exchActions;
        assert evtLatch != null && evtLatch.getCount() == 1L : this;

        this.exchActions = exchActions;
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
     * @return Discovery cache.
     */
    public DiscoCache discoCache() {
        return discoCache;
    }

    /**
     * @param cacheId Cache ID.
     * @param rcvdFrom Node ID cache was received from.
     * @return {@code True} if cache was added during this exchange.
     */
    public boolean cacheAddedOnExchange(int cacheId, UUID rcvdFrom) {
        return dynamicCacheStarted(cacheId) || (exchId.isJoined() && exchId.nodeId().equals(rcvdFrom));
    }

    /**
     * @param grpId Cache group ID.
     * @param rcvdFrom Node ID cache group was received from.
     * @return {@code True} if cache group was added during this exchange.
     */
    public boolean cacheGroupAddedOnExchange(int grpId, UUID rcvdFrom) {
        return dynamicCacheGroupStarted(grpId) ||
            (exchId.isJoined() && exchId.nodeId().equals(rcvdFrom));
    }

    /**
     * @param cacheId Cache ID.
     * @return {@code True} if non-client cache was added during this exchange.
     */
    private boolean dynamicCacheStarted(int cacheId) {
        return exchActions != null && exchActions.cacheStarted(cacheId);
    }

    /**
     * @param grpId Cache group ID.
     * @return {@code True} if non-client cache group was added during this exchange.
     */
    public boolean dynamicCacheGroupStarted(int grpId) {
        return exchActions != null && exchActions.cacheGroupStarting(grpId);
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
     * @param discoCache Discovery data cache.
     */
    public void onEvent(GridDhtPartitionExchangeId exchId, DiscoveryEvent discoEvt, DiscoCache discoCache) {
        assert exchId.equals(this.exchId);

        this.discoEvt = discoEvt;
        this.discoCache = discoCache;

        evtLatch.countDown();
    }

    /**
     *
     */
    public ClusterState newClusterState() {
        return exchActions != null ? exchActions.newClusterState() : null;
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
    @Nullable public GridCompoundFuture<Boolean, Boolean> forcedRebalanceFuture() {
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
            discoCache.updateAlives(cctx.discovery());

            AffinityTopologyVersion topVer = topologyVersion();

            srvNodes = new ArrayList<>(discoCache.serverNodes());

            remaining.addAll(F.nodeIds(F.view(srvNodes, F.remoteNodes(cctx.localNodeId()))));

            crd = srvNodes.isEmpty() ? null : srvNodes.get(0);

            boolean crdNode = crd != null && crd.isLocal();

            skipPreload = cctx.kernalContext().clientNode();

            ExchangeType exchange;

            if (discoEvt.type() == EVT_DISCOVERY_CUSTOM_EVT) {
                DiscoveryCustomMessage msg = ((DiscoveryCustomEvent)discoEvt).customMessage();

                if (msg instanceof DynamicCacheChangeBatch){
                    assert exchActions != null && !exchActions.empty();

                    exchange = onCacheChangeRequest(crdNode);
                }
                else if (msg instanceof StartFullSnapshotAckDiscoveryMessage)
                    exchange = CU.clientNode(discoEvt.eventNode()) ?
                        onClientNodeEvent(crdNode) :
                        onServerNodeEvent(crdNode);
                else {
                    assert affChangeMsg != null : this;

                    exchange = onAffinityChangeRequest(crdNode);
                }
            }
            else {
                if (discoEvt.type() == EVT_NODE_JOINED) {
                    if (!discoEvt.eventNode().isLocal()) {
                        Collection<DynamicCacheDescriptor> receivedCaches = cctx.cache().startReceivedCaches(
                            discoEvt.eventNode().id(),
                            topVer);

                        cctx.affinity().initStartedCaches(crdNode, this, receivedCaches);
                    }
                    else
                        cctx.cache().startCachesOnLocalJoin(topVer);
                }

                exchange = CU.clientNode(discoEvt.eventNode()) ?
                    onClientNodeEvent(crdNode) :
                    onServerNodeEvent(crdNode);
            }

            updateTopologies(crdNode);

            if (exchActions != null && exchActions.hasStop())
                cctx.cache().context().database().beforeCachesStop();

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

                    onDone(topVer);

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
        catch (IgniteNeedReconnectException e) {
            onDone(e);
        }
        catch (Throwable e) {
            if (reconnectOnError(e))
                onDone(new IgniteNeedReconnectException(cctx.localNode(), e));
            else {
                U.error(log, "Failed to reinitialize local partitions (preloading will be stopped): " + exchId, e);

                onDone(e);
            }

            if (e instanceof Error)
                throw (Error)e;
        }
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    private void initTopologies() throws IgniteCheckedException {
        cctx.database().checkpointReadLock();

        try {
            if (crd != null) {
                for (CacheGroupContext grp : cctx.cache().cacheGroups()) {
                    if (grp.isLocal())
                        continue;

                    grp.topology().beforeExchange(this, !centralizedAff);
                }
            }
        }
        finally {
            cctx.database().checkpointReadUnlock();
        }
    }

    /**
     * @param crd Coordinator flag.
     * @throws IgniteCheckedException If failed.
     */
    private void updateTopologies(boolean crd) throws IgniteCheckedException {
        for (CacheGroupContext grp : cctx.cache().cacheGroups()) {
            if (grp.isLocal())
                continue;

            GridClientPartitionTopology clientTop = cctx.exchange().clearClientTopology(grp.groupId());

            long updSeq = clientTop == null ? -1 : clientTop.lastUpdateSequence();

            GridDhtPartitionTopology top = grp.topology();

            if (crd) {
                boolean updateTop = exchId.topologyVersion().equals(grp.localStartVersion());

                if (updateTop && clientTop != null) {
                    top.update(topologyVersion(),
                        clientTop.partitionMap(true),
                        clientTop.updateCounters(false));
                }
            }

            top.updateTopologyVersion(
                this,
                discoCache(),
                updSeq,
                cacheGroupStopping(grp.groupId()));
        }

        for (GridClientPartitionTopology top : cctx.exchange().clientTopologies())
            top.updateTopologyVersion(this, discoCache(), -1, cacheGroupStopping(top.groupId()));
    }

    /**
     * @param crd Coordinator flag.
     * @return Exchange type.
     * @throws IgniteCheckedException If failed.
     */
    private ExchangeType onCacheChangeRequest(boolean crd) throws IgniteCheckedException {
        assert exchActions != null && !exchActions.empty() : this;

        GridClusterStateProcessor stateProc = cctx.kernalContext().state();

        if (exchangeOnChangeGlobalState = stateProc.changeGlobalState(exchActions, topologyVersion())) {
            changeGlobalStateE = stateProc.onChangeGlobalState();

            if (crd && changeGlobalStateE != null)
                changeGlobalStateExceptions.put(cctx.localNodeId(), changeGlobalStateE);
        }

        assert !exchActions.clientOnlyExchange() : exchActions;

        cctx.affinity().onCacheChangeRequest(this, crd, exchActions);

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
            assert discoEvt.type() == EVT_NODE_JOINED || discoEvt.type() == EVT_DISCOVERY_CUSTOM_EVT : discoEvt;

        cctx.affinity().onClientEvent(this, crd);

        return discoEvt.eventNode().isLocal() ? ExchangeType.CLIENT : ExchangeType.NONE;
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
        else
            cctx.affinity().onServerJoin(this, crd);

        return cctx.kernalContext().clientNode() ? ExchangeType.CLIENT : ExchangeType.ALL;
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    private void clientOnlyExchange() throws IgniteCheckedException {
        clientOnlyExchange = true;

        if (crd != null) {
            if (crd.isLocal()) {
                for (CacheGroupContext grp : cctx.cache().cacheGroups()) {
                    boolean updateTop = !grp.isLocal() &&
                        exchId.topologyVersion().equals(grp.localStartVersion());

                    if (updateTop) {
                        for (GridClientPartitionTopology top : cctx.exchange().clientTopologies()) {
                            if (top.groupId() == grp.groupId()) {
                                GridDhtPartitionFullMap fullMap = top.partitionMap(true);

                                assert fullMap != null;

                                grp.topology().update(topologyVersion(),
                                    fullMap,
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
                for (CacheGroupContext grp : cctx.cache().cacheGroups()) {
                    GridAffinityAssignmentCache aff = grp.affinity();

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

        for (CacheGroupContext grp : cctx.cache().cacheGroups()) {
            if (grp.isLocal())
                continue;

            grp.preloader().onTopologyChanged(this);
        }

        waitPartitionRelease();

        boolean topChanged = discoEvt.type() != EVT_DISCOVERY_CUSTOM_EVT || affChangeMsg != null;

        for (GridCacheContext cacheCtx : cctx.cacheContexts()) {
            if (cacheCtx.isLocal() || cacheStopping(cacheCtx.cacheId()))
                continue;

            if (topChanged) {
                // Partition release future is done so we can flush the write-behind store.
                cacheCtx.store().forceFlush();
            }
        }

        for (CacheGroupContext grp : cctx.cache().cacheGroups()) {
            if (grp.isLocal() || cacheGroupStopping(grp.groupId()))
                continue;

            grp.topology().beforeExchange(this, !centralizedAff);
        }

        cctx.database().beforeExchange(this);

        // If a backup request, synchronously wait for backup start.
        if (discoEvt.type() == EVT_DISCOVERY_CUSTOM_EVT) {
            DiscoveryCustomMessage customMsg = ((DiscoveryCustomEvent)discoEvt).customMessage();

            if (customMsg instanceof StartFullSnapshotAckDiscoveryMessage) {
                StartFullSnapshotAckDiscoveryMessage backupMsg = (StartFullSnapshotAckDiscoveryMessage)customMsg;

                if (!cctx.localNode().isClient() && !cctx.localNode().isDaemon()) {
                    ClusterNode node = cctx.discovery().node(backupMsg.initiatorNodeId());

                    assert node != null;

                    IgniteInternalFuture fut = cctx.database().startLocalSnapshotCreation(backupMsg, node, backupMsg.message());

                    if (fut != null)
                        fut.get();
                }
            }
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
     * The main purpose of this method is to wait for all ongoing updates (transactional and atomic), initiated on
     * the previous topology version, to finish to prevent inconsistencies during rebalancing and to prevent two
     * different simultaneous owners of the same lock.
     * For the exact list of the objects being awaited for see
     * {@link GridCacheSharedContext#partitionReleaseFuture(AffinityTopologyVersion)} javadoc.
     *
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

        int dumpCnt = 0;

        long nextDumpTime = 0;

        long futTimeout = 2 * cctx.gridConfig().getNetworkTimeout();

        while (true) {
            try {
                partReleaseFut.get(futTimeout, TimeUnit.MILLISECONDS);

                break;
            }
            catch (IgniteFutureTimeoutCheckedException ignored) {
                // Print pending transactions and locks that might have led to hang.
                if (nextDumpTime <= U.currentTimeMillis()) {
                    dumpPendingObjects();

                    nextDumpTime = U.currentTimeMillis() + nextDumpTimeout(dumpCnt++, futTimeout);
                }
            }
        }

        if (log.isDebugEnabled())
            log.debug("After waiting for partition release future: " + this);

        IgniteInternalFuture<?> locksFut = cctx.mvcc().finishLocks(exchId.topologyVersion());

        nextDumpTime = 0;
        dumpCnt = 0;

        while (true) {
            try {
                locksFut.get(futTimeout, TimeUnit.MILLISECONDS);

                break;
            }
            catch (IgniteFutureTimeoutCheckedException ignored) {
                if (nextDumpTime <= U.currentTimeMillis()) {
                    U.warn(log, "Failed to wait for locks release future. " +
                        "Dumping pending objects that might be the cause: " + cctx.localNodeId());

                    U.warn(log, "Locked keys:");

                    for (IgniteTxKey key : cctx.mvcc().lockedKeys())
                        U.warn(log, "Locked key: " + key);

                    for (IgniteTxKey key : cctx.mvcc().nearLockedKeys())
                        U.warn(log, "Locked near key: " + key);

                    Map<IgniteTxKey, Collection<GridCacheMvccCandidate>> locks =
                        cctx.mvcc().unfinishedLocks(exchId.topologyVersion());

                    for (Map.Entry<IgniteTxKey, Collection<GridCacheMvccCandidate>> e : locks.entrySet())
                        U.warn(log, "Awaited locked entry [key=" + e.getKey() + ", mvcc=" + e.getValue() + ']');

                    nextDumpTime = U.currentTimeMillis() + nextDumpTimeout(dumpCnt++, futTimeout);

                    if (getBoolean(IGNITE_THREAD_DUMP_ON_EXCHANGE_TIMEOUT, false))
                        U.dumpThreads(log);
                }
            }
        }
    }

    /**
     *
     */
    private void onLeft() {
        for (CacheGroupContext grp : cctx.cache().cacheGroups()) {
            if (grp.isLocal())
                continue;

            grp.preloader().unwindUndeploys();
        }

        cctx.mvcc().removeExplicitNodeLocks(exchId.nodeId(), exchId.topologyVersion());
    }

    /**
     *
     */
    private void warnNoAffinityNodes() {
        List<String> cachesWithoutNodes = null;

        for (DynamicCacheDescriptor cacheDesc : cctx.cache().cacheDescriptors().values()) {
            if (discoCache.cacheGroupAffinityNodes(cacheDesc.groupId()).isEmpty()) {
                if (cachesWithoutNodes == null)
                    cachesWithoutNodes = new ArrayList<>();

                cachesWithoutNodes.add(cacheDesc.cacheName());

                // Fire event even if there is no client cache started.
                if (cctx.gridEvents().isRecordable(EventType.EVT_CACHE_NODES_LEFT)) {
                    Event evt = new CacheEvent(
                        cacheDesc.cacheName(),
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
        U.warn(cctx.kernalContext().cluster().diagnosticLog(),
            "Failed to wait for partition release future [topVer=" + topologyVersion() +
            ", node=" + cctx.localNodeId() + "]. Dumping pending objects that might be the cause: ");

        try {
            cctx.exchange().dumpDebugInfo(this);
        }
        catch (Exception e) {
            U.error(cctx.kernalContext().cluster().diagnosticLog(), "Failed to dump debug information: " + e, e);
        }
    }

    /**
     * @param grpId Cache group ID to check.
     * @return {@code True} if cache group us stopping by this exchange.
     */
    private boolean cacheGroupStopping(int grpId) {
        return exchActions != null && exchActions.cacheGroupStopping(grpId);
    }

    /**
     * @param cacheId Cache ID to check.
     * @return {@code True} if cache is stopping by this exchange.
     */
    private boolean cacheStopping(int cacheId) {
        return exchActions != null && exchActions.cacheStopped(cacheId);
    }

    /**
     * @param node Node.
     * @throws IgniteCheckedException If failed.
     */
    private void sendLocalPartitions(ClusterNode node) throws IgniteCheckedException {
        assert node != null;

        // Reset lost partition before send local partition to coordinator.
        if (exchActions != null) {
            Set<String> caches = exchActions.cachesToResetLostPartitions();

            if (!F.isEmpty(caches))
                resetLostPartitions(caches);
        }

        GridDhtPartitionsSingleMessage m = cctx.exchange().createPartitionsSingleMessage(
            node, exchangeId(), clientOnlyExchange, true);

        if (exchangeOnChangeGlobalState && changeGlobalStateE != null)
            m.setException(changeGlobalStateE);

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
     * @param compress {@code True} if it is possible to use compression for message.
     * @return Message.
     */
    private GridDhtPartitionsFullMessage createPartitionsMessage(Collection<ClusterNode> nodes, boolean compress) {
        GridCacheVersion last = lastVer.get();

        GridDhtPartitionsFullMessage m = cctx.exchange().createPartitionsFullMessage(
            nodes,
            exchangeId(),
            last != null ? last : cctx.versions().last(),
            compress);

        if (exchangeOnChangeGlobalState && !F.isEmpty(changeGlobalStateExceptions))
            m.setExceptionsMap(changeGlobalStateExceptions);

        return m;
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

        for (ClusterNode node : nodes) {
            try {
                cctx.io().send(node, m, SYSTEM_POOL);
            }
            catch (IgniteCheckedException e) {
                if (cctx.io().checkNodeLeft(node.id(), e, false)) {
                    if (log.isDebugEnabled())
                        log.debug("Failed to send partitions, node failed: " + node);
                }
                else
                    U.error(log, "Failed to send partitions [node=" + node + ']', e);
            }
        }
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

    /**
     * @return {@code True} if exchange triggered by server node join or fail.
     */
    private boolean serverNodeDiscoveryEvent() {
        assert discoEvt != null;

        return discoEvt.type() != EVT_DISCOVERY_CUSTOM_EVT && !CU.clientNode(discoEvt.eventNode());
    }

    /** {@inheritDoc} */
    @Override public boolean onDone(@Nullable AffinityTopologyVersion res, @Nullable Throwable err) {
        boolean realExchange = !dummy && !forcePreload;

        if (err == null &&
            realExchange &&
            !cctx.kernalContext().clientNode() &&
            (serverNodeDiscoveryEvent() || affChangeMsg != null)) {
            for (GridCacheContext cacheCtx : cctx.cacheContexts()) {
                if (!cacheCtx.affinityNode() || cacheCtx.isLocal())
                    continue;

                cacheCtx.continuousQueries().flushBackupQueue(exchId.topologyVersion());
            }
       }

        if (err == null && realExchange) {
            for (CacheGroupContext grp : cctx.cache().cacheGroups()) {
                if (grp.isLocal())
                    continue;

                try {
                    if (centralizedAff)
                        grp.topology().initPartitions(this);
                }
                catch (IgniteInterruptedCheckedException e) {
                    U.error(log, "Failed to initialize partitions.", e);
                }
            }

            for (GridCacheContext cacheCtx : cctx.cacheContexts()) {
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

            if (discoEvt.type() == EVT_NODE_LEFT ||
                discoEvt.type() == EVT_NODE_FAILED ||
                discoEvt.type() == EVT_NODE_JOINED)
                detectLostPartitions();

            Map<Integer, CacheValidation> m = U.newHashMap(cctx.cache().cacheGroups().size());

            for (CacheGroupContext grp : cctx.cache().cacheGroups())
                m.put(grp.groupId(), validateCacheGroup(grp, discoEvt.topologyNodes()));

            grpValidRes = m;
        }

        cctx.cache().onExchangeDone(exchId.topologyVersion(), exchActions, err);

        cctx.exchange().onExchangeDone(this, err);

        if (exchActions != null && err == null)
            exchActions.completeRequestFutures(cctx);

        if (exchangeOnChangeGlobalState && err == null)
            cctx.kernalContext().state().onExchangeDone();

        if (super.onDone(res, err) && realExchange) {
            if (log.isDebugEnabled())
                log.debug("Completed partition exchange [localNode=" + cctx.localNodeId() + ", exchange= " + this +
                    ", durationFromInit=" + (U.currentTimeMillis() - initTs) + ']');

            initFut.onDone(err == null);

            if (exchId.isLeft()) {
                for (CacheGroupContext grp : cctx.cache().cacheGroups())
                    grp.affinityFunction().removeNode(exchId.nodeId());
            }

            exchActions = null;

            if (discoEvt instanceof DiscoveryCustomEvent)
                ((DiscoveryCustomEvent)discoEvt).customMessage(null);

            cctx.exchange().lastFinishedFuture(this);

            return true;
        }

        return dummy;
    }

    /**
     * Cleans up resources to avoid excessive memory usage.
     */
    public void cleanUp() {
        singleMsgs.clear();
        fullMsgs.clear();
        changeGlobalStateExceptions.clear();
        crd = null;
        partReleaseFut = null;
        changeGlobalStateE = null;
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

        synchronized (this) {
            assert crd != null;

            if (crd.isLocal()) {
                if (remaining.remove(node.id())) {
                    updateSingleMap = true;

                    pendingSingleUpdates++;

                    if (exchangeOnChangeGlobalState && msg.getException() != null)
                        changeGlobalStateExceptions.put(node.id(), msg.getException());

                    allReceived = remaining.isEmpty();
                }
            }
            else
                singleMsgs.put(node, msg);
        }

        if (updateSingleMap) {
            try {
                updatePartitionSingleMap(node, msg);
            }
            finally {
                synchronized (this) {
                    assert pendingSingleUpdates > 0;

                    pendingSingleUpdates--;

                    if (pendingSingleUpdates == 0)
                        notifyAll();
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
    private synchronized void awaitSingleMapUpdates() {
        try {
            while (pendingSingleUpdates > 0)
                U.wait(this);
        }
        catch (IgniteInterruptedCheckedException e) {
            U.warn(log, "Failed to wait for partition map updates, thread was interrupted: " + e);
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
     * @param top Topology to assign.
     */
    private void assignPartitionStates(GridDhtPartitionTopology top) {
        Map<Integer, CounterWithNodes> maxCntrs = new HashMap<>();

        for (Map.Entry<UUID, GridDhtPartitionsAbstractMessage> e : msgs.entrySet()) {
            assert e.getValue().partitionUpdateCounters(top.groupId()) != null;

            for (Map.Entry<Integer, T2<Long, Long>> e0 : e.getValue().partitionUpdateCounters(top.groupId()).entrySet()) {
                int p = e0.getKey();

                UUID uuid = e.getKey();

                GridDhtPartitionState state = top.partitionState(uuid, p);

                if (state != GridDhtPartitionState.OWNING)
                    continue;

                Long cntr = e0.getValue().get1();

                if (cntr == null)
                    continue;

                CounterWithNodes maxCntr = maxCntrs.get(p);

                if (maxCntr == null || cntr > maxCntr.cnt)
                    maxCntrs.put(p, new CounterWithNodes(cntr, uuid));
                else if (cntr == maxCntr.cnt)
                    maxCntr.nodes.add(uuid);
            }
        }

        // Also must process counters from the local node.
        for (GridDhtLocalPartition part : top.currentLocalPartitions()) {
            GridDhtPartitionState state = top.partitionState(cctx.localNodeId(), part.id());

            if (state != GridDhtPartitionState.OWNING)
                continue;

            CounterWithNodes maxCntr = maxCntrs.get(part.id());

            if (maxCntr == null || part.initialUpdateCounter() > maxCntr.cnt)
                maxCntrs.put(part.id(), new CounterWithNodes(part.updateCounter(), cctx.localNodeId()));
            else if (part.initialUpdateCounter() == maxCntr.cnt)
                maxCntr.nodes.add(cctx.localNodeId());
        }

        int entryLeft = maxCntrs.size();

        for (Map.Entry<Integer, CounterWithNodes> e : maxCntrs.entrySet()) {
            int p = e.getKey();
            long maxCntr = e.getValue().cnt;

            entryLeft --;

            if (entryLeft != 0 && maxCntr == 0)
                continue;

            top.setOwners(p, e.getValue().nodes, entryLeft == 0);
        }
    }

    /**
     * Detect lost partitions.
     */
    private void detectLostPartitions() {
        synchronized (cctx.exchange().interruptLock()) {
            if (Thread.currentThread().isInterrupted())
                return;

            for (CacheGroupContext grp : cctx.cache().cacheGroups()) {
                if (!grp.isLocal())
                    grp.topology().detectLostPartitions(discoEvt);
            }
        }
    }

    /**
     * @param cacheNames Cache names.
     */
    private void resetLostPartitions(Collection<String> cacheNames) {
        synchronized (cctx.exchange().interruptLock()) {
            if (Thread.currentThread().isInterrupted())
                return;

            for (CacheGroupContext grp : cctx.cache().cacheGroups()) {
                if (grp.isLocal())
                    continue;

                for (String cacheName : cacheNames) {
                    if (grp.hasCache(cacheName)) {
                        grp.topology().resetLostPartitions();

                        break;
                    }
                }
            }
        }
    }

    /**
     *
     */
    private void onAllReceived() {
        try {
            assert crd.isLocal();

            if (!crd.equals(discoCache.serverNodes().get(0))) {
                for (CacheGroupContext grp : cctx.cache().cacheGroups()) {
                    if (!grp.isLocal())
                        grp.topology().beforeExchange(this, !centralizedAff);
                }
            }

            for (GridDhtPartitionsAbstractMessage msg : msgs.values()) {
                if (msg instanceof GridDhtPartitionsSingleMessage) {
                    GridDhtPartitionsSingleMessage msg0 = (GridDhtPartitionsSingleMessage)msg;

                    for (Map.Entry<Integer, GridDhtPartitionMap> entry : msg0.partitions().entrySet()) {
                        Integer grpId = entry.getKey();
                        CacheGroupContext grp = cctx.cache().cacheGroup(grpId);

                        GridDhtPartitionTopology top = grp != null ? grp.topology() :
                            cctx.exchange().clientTopology(grpId, this);

                        Map<Integer, T2<Long, Long>> cntrs = msg0.partitionUpdateCounters(grpId);

                        if (cntrs != null)
                            top.applyUpdateCounters(cntrs);
                    }
                }
            }

            if (discoEvt.type() == EVT_NODE_JOINED) {
                if (cctx.kernalContext().state().active())
                    assignPartitionsStates();
            }
            else if (discoEvt.type() == EVT_DISCOVERY_CUSTOM_EVT) {
                assert discoEvt instanceof DiscoveryCustomEvent;

                if (((DiscoveryCustomEvent)discoEvt).customMessage() instanceof DynamicCacheChangeBatch) {
                    if (exchActions != null) {
                        if (exchActions.newClusterState() == ClusterState.ACTIVE)
                            assignPartitionsStates();

                        Set<String> caches = exchActions.cachesToResetLostPartitions();

                        if (!F.isEmpty(caches))
                            resetLostPartitions(caches);
                    }
                }
            }
            else if (discoEvt.type() == EVT_NODE_LEFT || discoEvt.type() == EVT_NODE_FAILED)
                detectLostPartitions();

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

                synchronized (this) {
                    srvNodes.remove(cctx.localNode());

                    nodes = new ArrayList<>(srvNodes);
                }

                if (!nodes.isEmpty())
                    sendAllPartitions(nodes);

                if (exchangeOnChangeGlobalState && !F.isEmpty(changeGlobalStateExceptions))
                    cctx.kernalContext().state().onFullResponseMessage(changeGlobalStateExceptions);

                onDone(exchangeId().topologyVersion());
            }
        }
        catch (IgniteCheckedException e) {
            if (reconnectOnError(e))
                onDone(new IgniteNeedReconnectException(cctx.localNode(), e));
            else
                onDone(e);
        }
    }

    /**
     *
     */
    private void assignPartitionsStates() {
        if (cctx.database().persistenceEnabled()) {
            for (CacheGroupContext grp : cctx.cache().cacheGroups()) {
                if (grp.isLocal())
                    continue;

                assignPartitionStates(grp.topology());
            }
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
                if (log.isDebugEnabled())
                    log.debug("Failed to send full partition map to node, node left grid " +
                        "[rmtNode=" + nodeId + ", exchangeId=" + exchId + ']');

                return;
            }

            if (reconnectOnError(e)) {
                onDone(new IgniteNeedReconnectException(cctx.localNode(), e));

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

        synchronized (this) {
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

        if (exchangeOnChangeGlobalState && !F.isEmpty(msg.getExceptionsMap()))
            cctx.kernalContext().state().onFullResponseMessage(msg.getExceptionsMap());

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
            Integer grpId = entry.getKey();

            Map<Integer, T2<Long, Long>> cntrMap = msg.partitionUpdateCounters(grpId);

            CacheGroupContext grp = cctx.cache().cacheGroup(grpId);

            if (grp != null) {
                grp.topology().update(topologyVersion(),
                    entry.getValue(),
                    cntrMap);
            }
            else {
                ClusterNode oldest = cctx.discovery().oldestAliveCacheServerNode(AffinityTopologyVersion.NONE);

                if (oldest != null && oldest.isLocal()) {
                    cctx.exchange().clientTopology(grpId, this).update(topologyVersion(),
                        entry.getValue(),
                        cntrMap);
                }
            }
        }
    }

    /**
     * Updates partition map in all caches.
     *
     * @param msg Partitions single message.
     */
    private void updatePartitionSingleMap(ClusterNode node, GridDhtPartitionsSingleMessage msg) {
        msgs.put(node.id(), msg);

        for (Map.Entry<Integer, GridDhtPartitionMap> entry : msg.partitions().entrySet()) {
            Integer grpId = entry.getKey();
            CacheGroupContext grp = cctx.cache().cacheGroup(grpId);

            GridDhtPartitionTopology top = grp != null ? grp.topology() :
                cctx.exchange().clientTopology(grpId, this);

            top.update(exchId, entry.getValue());
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

                        discoCache.updateAlives(node);

                        synchronized (this) {
                            if (!srvNodes.remove(node))
                                return;

                            boolean rmvd = remaining.remove(node.id());

                            if (node.equals(crd)) {
                                crdChanged = true;

                                crd = !srvNodes.isEmpty() ? srvNodes.get(0) : null;
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

                            for (CacheGroupContext grp : cctx.cache().cacheGroups()) {
                                List<List<ClusterNode>> affAssignment = new ArrayList<>(grp.affinity().partitions());

                                for (int i = 0; i < grp.affinity().partitions(); i++)
                                    affAssignment.add(empty);

                                grp.affinity().initialize(topologyVersion(), affAssignment);
                            }

                            onDone(topologyVersion());

                            return;
                        }

                        if (crd0.isLocal()) {
                            if (exchangeOnChangeGlobalState && changeGlobalStateE !=null)
                                changeGlobalStateExceptions.put(crd0.id(), changeGlobalStateE);

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
                    catch (Exception e) {
                        if (reconnectOnError(e))
                            onDone(new IgniteNeedReconnectException(cctx.localNode(), e));
                        else
                            throw e;
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

    /**
     * @param e Exception.
     * @return {@code True} if local node should try reconnect in case of error.
     */
    public boolean reconnectOnError(Throwable e) {
        return X.hasCause(e, IOException.class, IgniteClientDisconnectedCheckedException.class) &&
            cctx.discovery().reconnectSupported();
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
    @Override public void addDiagnosticRequest(IgniteDiagnosticPrepareContext diagCtx) {
        if (!isDone()) {
            ClusterNode crd;
            Set<UUID> remaining;

            synchronized (this) {
                crd = this.crd;
                remaining = new HashSet<>(this.remaining);
            }

            if (crd != null) {
                if (!crd.isLocal()) {
                    diagCtx.exchangeInfo(crd.id(), topologyVersion(), "Exchange future waiting for coordinator " +
                        "response [crd=" + crd.id() + ", topVer=" + topologyVersion() + ']');
                }
                else if (!remaining.isEmpty()){
                    UUID nodeId = remaining.iterator().next();

                    diagCtx.exchangeInfo(crd.id(), topologyVersion(), "Exchange future on coordinator waiting for " +
                        "server response [node=" + nodeId + ", topVer=" + topologyVersion() + ']');
                }
            }
        }
    }

    /**
     * @return Short information string.
     */
    public String shortInfo() {
        return "GridDhtPartitionsExchangeFuture [topVer=" + topologyVersion() +
            ", evt=" + (discoEvt != null ? discoEvt.type() : -1) +
            ", evtNode=" + (discoEvt != null ? discoEvt.eventNode() : null) +
            ", done=" + isDone() + ']';
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        Set<UUID> remaining;

        synchronized (this) {
            remaining = new HashSet<>(this.remaining);
        }

        return S.toString(GridDhtPartitionsExchangeFuture.class, this,
            "evtLatch", evtLatch == null ? "null" : evtLatch.getCount(),
            "remaining", remaining,
            "super", super.toString());
    }

    /**
     *
     */
    private static class CounterWithNodes {
        /** */
        private final long cnt;

        /** */
        private final Set<UUID> nodes = new HashSet<>();

        /**
         * @param cnt Count.
         * @param firstNode Node ID.
         */
        private CounterWithNodes(long cnt, UUID firstNode) {
            this.cnt = cnt;

            nodes.add(firstNode);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(CounterWithNodes.class, this);
        }
    }

    /**
     * @param step Exponent coefficient.
     * @param timeout Base timeout.
     * @return Time to wait before next debug dump.
     */
    public static long nextDumpTimeout(int step, long timeout) {
        long limit = getLong(IGNITE_LONG_OPERATIONS_DUMP_TIMEOUT_LIMIT, 30 * 60_000);

        if (limit <= 0)
            limit = 30 * 60_000;

        assert step >= 0 : step;

        long dumpFactor = Math.round(Math.pow(2, step));

        long nextTimeout = timeout * dumpFactor;

        if (nextTimeout <= 0)
            return limit;

        return nextTimeout <= limit ? nextTimeout : limit;
    }
}

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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.internal.IgniteClientDisconnectedCheckedException;
import org.apache.ignite.internal.IgniteDiagnosticAware;
import org.apache.ignite.internal.IgniteDiagnosticPrepareContext;
import org.apache.ignite.internal.IgniteFutureTimeoutCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.IgniteNeedReconnectException;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.events.DiscoveryCustomEvent;
import org.apache.ignite.internal.managers.communication.GridIoPolicy;
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.pagemem.wal.record.ExchangeRecord;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.affinity.GridAffinityAssignmentCache;
import org.apache.ignite.internal.processors.cache.CacheAffinityChangeMessage;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.CacheGroupDescriptor;
import org.apache.ignite.internal.processors.cache.CachePartitionExchangeWorkerTask;
import org.apache.ignite.internal.processors.cache.DynamicCacheChangeBatch;
import org.apache.ignite.internal.processors.cache.DynamicCacheChangeFailureMessage;
import org.apache.ignite.internal.processors.cache.DynamicCacheDescriptor;
import org.apache.ignite.internal.processors.cache.ExchangeActions;
import org.apache.ignite.internal.processors.cache.ExchangeContext;
import org.apache.ignite.internal.processors.cache.ExchangeDiscoveryEvents;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheMvccCandidate;
import org.apache.ignite.internal.processors.cache.GridCacheProcessor;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.GridCacheUtils;
import org.apache.ignite.internal.processors.cache.LocalJoinCachesContext;
import org.apache.ignite.internal.processors.cache.StateChangeRequest;
import org.apache.ignite.internal.processors.cache.WalStateAbstractMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridClientPartitionTopology;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionTopology;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionsStateValidator;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTopologyFutureAdapter;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.latch.Latch;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotDiscoveryMessage;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxKey;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.cluster.BaselineTopology;
import org.apache.ignite.internal.processors.cluster.ChangeGlobalStateFinishMessage;
import org.apache.ignite.internal.processors.cluster.ChangeGlobalStateMessage;
import org.apache.ignite.internal.processors.cluster.DiscoveryDataClusterState;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.lang.IgniteRunnable;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_LONG_OPERATIONS_DUMP_TIMEOUT_LIMIT;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_PARTITION_RELEASE_FUTURE_DUMP_THRESHOLD;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_THREAD_DUMP_ON_EXCHANGE_TIMEOUT;
import static org.apache.ignite.IgniteSystemProperties.getBoolean;
import static org.apache.ignite.IgniteSystemProperties.getLong;
import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_JOINED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_DYNAMIC_CACHE_START_ROLLBACK_SUPPORTED;
import static org.apache.ignite.internal.events.DiscoveryCustomEvent.EVT_DISCOVERY_CUSTOM_EVT;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.SYSTEM_POOL;
import static org.apache.ignite.internal.processors.cache.ExchangeDiscoveryEvents.serverJoinEvent;
import static org.apache.ignite.internal.processors.cache.ExchangeDiscoveryEvents.serverLeftEvent;
import static org.apache.ignite.internal.processors.cache.distributed.dht.preloader.CachePartitionPartialCountersMap.PARTIAL_COUNTERS_MAP_SINCE;

/**
 * Future for exchanging partition maps.
 */
@SuppressWarnings({"TypeMayBeWeakened", "unchecked"})
public class GridDhtPartitionsExchangeFuture extends GridDhtTopologyFutureAdapter
    implements Comparable<GridDhtPartitionsExchangeFuture>, CachePartitionExchangeWorkerTask, IgniteDiagnosticAware {
    /** */
    public static final String EXCHANGE_LOG = "org.apache.ignite.internal.exchange.time";

    /** */
    private static final int RELEASE_FUTURE_DUMP_THRESHOLD =
        IgniteSystemProperties.getInteger(IGNITE_PARTITION_RELEASE_FUTURE_DUMP_THRESHOLD, 0);

    /** */
    private static final IgniteProductVersion FORCE_AFF_REASSIGNMENT_SINCE = IgniteProductVersion.fromString("2.4.3");

    /**
     * This may be useful when per-entry (not per-cache based) partition policy is in use.
     * See {@link IgniteSystemProperties#IGNITE_SKIP_PARTITION_SIZE_VALIDATION} for details.
     * Default value is {@code false}.
     */
    private static final boolean SKIP_PARTITION_SIZE_VALIDATION = Boolean.getBoolean(IgniteSystemProperties.IGNITE_SKIP_PARTITION_SIZE_VALIDATION);

    /** */
    private static final String DISTRIBUTED_LATCH_ID = "exchange";

    /** */
    @GridToStringExclude
    private final Object mux = new Object();

    /** */
    @GridToStringExclude
    private volatile DiscoCache firstEvtDiscoCache;

    /** Discovery event triggered this exchange. */
    private volatile DiscoveryEvent firstDiscoEvt;

    /** */
    @GridToStringExclude
    private final Set<UUID> remaining = new HashSet<>();

    /** Guarded by this */
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

    /**
     * Discovery event receive latch. There is a race between discovery event processing and single message
     * processing, so it is possible to create an exchange future before the actual discovery event is received.
     * This latch is notified when the discovery event arrives.
     */
    @GridToStringExclude
    private final CountDownLatch evtLatch = new CountDownLatch(1);

    /** Exchange future init method completes this future. */
    private GridFutureAdapter<Boolean> initFut;

    /** */
    @GridToStringExclude
    private final List<IgniteRunnable> discoEvts = new ArrayList<>();

    /** */
    private boolean init;

    /** Last committed cache version before next topology version use. */
    private AtomicReference<GridCacheVersion> lastVer = new AtomicReference<>();

    /**
     * Message received from node joining cluster (if this is 'node join' exchange),
     * needed if this exchange is merged with another one.
     */
    @GridToStringExclude
    private GridDhtPartitionsSingleMessage pendingJoinMsg;

    /**
     * Messages received on non-coordinator are stored in case if this node
     * becomes coordinator.
     */
    private final Map<UUID, GridDhtPartitionsSingleMessage> pendingSingleMsgs = new ConcurrentHashMap<>();

    /** Messages received from new coordinator. */
    private final Map<ClusterNode, GridDhtPartitionsFullMessage> fullMsgs = new ConcurrentHashMap<>();

    /** */
    @SuppressWarnings({"FieldCanBeLocal", "UnusedDeclaration"})
    @GridToStringInclude
    private volatile IgniteInternalFuture<?> partReleaseFut;

    /** Logger. */
    private final IgniteLogger log;

    /** Cache change requests. */
    private ExchangeActions exchActions;

    /** */
    private final IgniteLogger exchLog;

    /** */
    private CacheAffinityChangeMessage affChangeMsg;

    /** Init timestamp. Used to track the amount of time spent to complete the future. */
    private long initTs;

    /**
     * Centralized affinity assignment required. Activated for node left of failed. For this mode crd will send full
     * partitions maps to nodes using discovery (ring) instead of communication.
     */
    private boolean centralizedAff;

    /**
     * Enforce affinity reassignment based on actual partition distribution. This mode should be used when partitions
     * might be distributed not according to affinity assignment.
     */
    private boolean forceAffReassignment;

    /** Exception that was thrown during init phase on local node. */
    private Exception exchangeLocE;

    /** Exchange exceptions from all participating nodes. */
    private final Map<UUID, Exception> exchangeGlobalExceptions = new ConcurrentHashMap<>();

    /** Used to track the fact that {@link DynamicCacheChangeFailureMessage} was sent. */
    private volatile boolean cacheChangeFailureMsgSent;

    /** */
    private ConcurrentMap<UUID, GridDhtPartitionsSingleMessage> msgs = new ConcurrentHashMap<>();

    /** Single messages from merged 'node join' exchanges. */
    @GridToStringExclude
    private Map<UUID, GridDhtPartitionsSingleMessage> mergedJoinExchMsgs;

    /** Number of awaited messages for merged 'node join' exchanges. */
    @GridToStringExclude
    private int awaitMergedMsgs;

    /** */
    @GridToStringExclude
    private volatile IgniteDhtPartitionHistorySuppliersMap partHistSuppliers = new IgniteDhtPartitionHistorySuppliersMap();

    /** */
    private volatile Map<Integer, Map<Integer, Long>> partHistReserved;

    /** */
    @GridToStringExclude
    private final IgniteDhtPartitionsToReloadMap partsToReload = new IgniteDhtPartitionsToReloadMap();

    /** */
    private final AtomicBoolean done = new AtomicBoolean();

    /** */
    private ExchangeLocalState state;

    /** */
    @GridToStringExclude
    private ExchangeContext exchCtx;

    /** */
    @GridToStringExclude
    private FinishState finishState;

    /** Initialized when node becomes new coordinator. */
    @GridToStringExclude
    private InitNewCoordinatorFuture newCrdFut;

    /** */
    @GridToStringExclude
    private GridDhtPartitionsExchangeFuture mergedWith;

    /** Validator for partition states. */
    @GridToStringExclude
    private final GridDhtPartitionsStateValidator validator;

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

        this.cctx = cctx;
        this.busyLock = busyLock;
        this.exchId = exchId;
        this.exchActions = exchActions;
        this.affChangeMsg = affChangeMsg;
        this.validator = new GridDhtPartitionsStateValidator(cctx);

        log = cctx.logger(getClass());
        exchLog = cctx.logger(EXCHANGE_LOG);

        initFut = new GridFutureAdapter<Boolean>() {
            @Override public IgniteLogger logger() {
                return log;
            }
        };

        if (log.isDebugEnabled())
            log.debug("Creating exchange future [localNode=" + cctx.localNodeId() + ", fut=" + this + ']');
    }

    /**
     * @return Future mutex.
     */
    public Object mutex() {
        return mux;
    }

    /**
     * @return Shared cache context.
     */
    public GridCacheSharedContext sharedContext() {
        return cctx;
    }

    /** {@inheritDoc} */
    @Override public boolean skipForExchangeMerge() {
        return false;
    }

    /**
     * @return Exchange context.
     */
    public ExchangeContext context() {
        assert exchCtx != null : this;

        return exchCtx;
    }

    /**
     * Sets exchange actions associated with the exchange future (such as cache start or stop).
     * Exchange actions is created from discovery event, so the actions must be set before the event is processed,
     * thus the setter requires that {@code evtLatch} be armed.
     *
     * @param exchActions Exchange actions.
     */
    public void exchangeActions(ExchangeActions exchActions) {
        assert exchActions == null || !exchActions.empty() : exchActions;
        assert evtLatch != null && evtLatch.getCount() == 1L : this;

        this.exchActions = exchActions;
    }

    /**
     * Gets exchanges actions (such as cache start or stop) associated with the exchange future.
     * Exchange actions can be {@code null} (for example, if the exchange is created for topology
     * change event).
     *
     * @return Exchange actions.
     */
    @Nullable public ExchangeActions exchangeActions() {
        return exchActions;
    }

    /**
     * Sets affinity change message associated with the exchange. Affinity change message is required when
     * centralized affinity change is performed.
     *
     * @param affChangeMsg Affinity change message.
     */
    public void affinityChangeMessage(CacheAffinityChangeMessage affChangeMsg) {
        this.affChangeMsg = affChangeMsg;
    }

    /**
     * Gets the affinity topology version for which this exchange was created. If several exchanges
     * were merged, initial version is the version of the earliest merged exchange.
     *
     * @return Initial exchange version.
     */
    @Override public AffinityTopologyVersion initialVersion() {
        return exchId.topologyVersion();
    }

    /** {@inheritDoc} */
    @Override public AffinityTopologyVersion topologyVersion() {
        /*
        Should not be called before exchange is finished since result version can change in
        case of merged exchanges.
         */
        assert exchangeDone() : "Should not be called before exchange is finished";

        return isDone() ? result() : exchCtx.events().topologyVersion();
    }

    /**
     * Retreives the node which has WAL history since {@code cntrSince}.
     *
     * @param grpId Cache group ID.
     * @param partId Partition ID.
     * @param cntrSince Partition update counter since history supplying is requested.
     * @return ID of history supplier node or null if it doesn't exist.
     */
    @Nullable public UUID partitionHistorySupplier(int grpId, int partId, long cntrSince) {
        return partHistSuppliers.getSupplier(grpId, partId, cntrSince);
    }

    /**
     * @param cacheId Cache ID.
     * @param rcvdFrom Node ID cache was received from.
     * @return {@code True} if cache was added during this exchange.
     */
    public boolean cacheAddedOnExchange(int cacheId, UUID rcvdFrom) {
        return dynamicCacheStarted(cacheId) || exchCtx.events().nodeJoined(rcvdFrom);
    }

    /**
     * @param grpId Cache group ID.
     * @param rcvdFrom Node ID cache group was received from.
     * @return {@code True} if cache group was added during this exchange.
     */
    public boolean cacheGroupAddedOnExchange(int grpId, UUID rcvdFrom) {
        return dynamicCacheGroupStarted(grpId) || exchCtx.events().nodeJoined(rcvdFrom);
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

        this.exchId.discoveryEvent(discoEvt);
        this.firstDiscoEvt= discoEvt;
        this.firstEvtDiscoCache = discoCache;

        evtLatch.countDown();
    }

    /**
     * @return {@code True} if cluster state change exchange.
     */
    private boolean stateChangeExchange() {
        return exchActions != null && exchActions.stateChangeRequest() != null;
    }

    /**
     * @return {@code True} if this exchange was triggered by DynamicCacheChangeBatch message
     * in order to start cache(s).
     */
    private boolean dynamicCacheStartExchange() {
        return exchActions != null && !exchActions.cacheStartRequests().isEmpty()
            && exchActions.cacheStopRequests().isEmpty();
    }

    /**
     * @return {@code True} if activate cluster exchange.
     */
    public boolean activateCluster() {
        return exchActions != null && exchActions.activate();
    }

    /**
     * @return {@code True} if deactivate cluster exchange.
     */
    private boolean deactivateCluster() {
        return exchActions != null && exchActions.deactivate();
    }

    /** */
    public boolean changedBaseline() {
        return exchActions != null && exchActions.changedBaseline();
    }

    /**
     * @return {@code True} if there are caches to start.
     */
    public boolean hasCachesToStart() {
        return exchActions != null && !exchActions.cacheStartRequests().isEmpty();
    }

    /**
     * @return First event discovery event.
     *
     */
    public DiscoveryEvent firstEvent() {
        return firstDiscoEvt;
    }

    /**
     * @return Discovery cache for first event.
     */
    public DiscoCache firstEventCache() {
        return firstEvtDiscoCache;
    }

    /**
     * @return Events processed in this exchange.
     */
    public ExchangeDiscoveryEvents events() {
        return exchCtx.events();
    }

    /**
     * @return Exchange ID.
     */
    public GridDhtPartitionExchangeId exchangeId() {
        return exchId;
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
     * @param newCrd {@code True} if node become coordinator on this exchange.
     * @throws IgniteCheckedException If failed.
     */
    private void initCoordinatorCaches(boolean newCrd) throws IgniteCheckedException {
        if (newCrd) {
            IgniteInternalFuture<?> fut = cctx.affinity().initCoordinatorCaches(this, false);

            if (fut != null)
                fut.get();

            cctx.exchange().onCoordinatorInitialized();
        }
    }

    /**
     * Starts activity.
     *
     * @param newCrd {@code True} if node become coordinator on this exchange.
     * @throws IgniteInterruptedCheckedException If interrupted.
     */
    public void init(boolean newCrd) throws IgniteInterruptedCheckedException {
        if (isDone())
            return;

        assert !cctx.kernalContext().isDaemon();

        initTs = U.currentTimeMillis();

        U.await(evtLatch);

        assert firstDiscoEvt != null : this;
        assert exchId.nodeId().equals(firstDiscoEvt.eventNode().id()) : this;

        try {
            AffinityTopologyVersion topVer = initialVersion();

            srvNodes = new ArrayList<>(firstEvtDiscoCache.serverNodes());

            remaining.addAll(F.nodeIds(F.view(srvNodes, F.remoteNodes(cctx.localNodeId()))));

            crd = srvNodes.isEmpty() ? null : srvNodes.get(0);

            boolean crdNode = crd != null && crd.isLocal();

            exchCtx = new ExchangeContext(crdNode, this);

            assert state == null : state;

            if (crdNode)
                state = ExchangeLocalState.CRD;
            else
                state = cctx.kernalContext().clientNode() ? ExchangeLocalState.CLIENT : ExchangeLocalState.SRV;

            if (exchLog.isInfoEnabled()) {
                exchLog.info("Started exchange init [topVer=" + topVer +
                    ", crd=" + crdNode +
                    ", evt=" + IgniteUtils.gridEventName(firstDiscoEvt.type()) +
                    ", evtNode=" + firstDiscoEvt.eventNode().id() +
                    ", customEvt=" + (firstDiscoEvt.type() == EVT_DISCOVERY_CUSTOM_EVT ? ((DiscoveryCustomEvent)firstDiscoEvt).customMessage() : null) +
                    ", allowMerge=" + exchCtx.mergeExchanges() + ']');
            }

            ExchangeType exchange;

            if (firstDiscoEvt.type() == EVT_DISCOVERY_CUSTOM_EVT) {
                assert !exchCtx.mergeExchanges();

                DiscoveryCustomMessage msg = ((DiscoveryCustomEvent)firstDiscoEvt).customMessage();

                forceAffReassignment = DiscoveryCustomEvent.requiresCentralizedAffinityAssignment(msg)
                    && firstEventCache().minimumNodeVersion().compareToIgnoreTimestamp(FORCE_AFF_REASSIGNMENT_SINCE) >= 0;

                if (msg instanceof ChangeGlobalStateMessage) {
                    assert exchActions != null && !exchActions.empty();

                    exchange = onClusterStateChangeRequest(crdNode);
                }
                else if (msg instanceof DynamicCacheChangeBatch) {
                    assert exchActions != null && !exchActions.empty();

                    exchange = onCacheChangeRequest(crdNode);
                }
                else if (msg instanceof SnapshotDiscoveryMessage) {
                    exchange = onCustomMessageNoAffinityChange(crdNode);
                }
                else if (msg instanceof WalStateAbstractMessage)
                    exchange = onCustomMessageNoAffinityChange(crdNode);
                else {
                    assert affChangeMsg != null : this;

                    exchange = onAffinityChangeRequest(crdNode);
                }

                if (forceAffReassignment)
                    cctx.affinity().onCentralizedAffinityChange(this, crdNode);

                initCoordinatorCaches(newCrd);
            }
            else {
                if (firstDiscoEvt.type() == EVT_NODE_JOINED) {
                    if (!firstDiscoEvt.eventNode().isLocal()) {
                        Collection<DynamicCacheDescriptor> receivedCaches = cctx.cache().startReceivedCaches(
                            firstDiscoEvt.eventNode().id(),
                            topVer);

                        cctx.affinity().initStartedCaches(crdNode, this, receivedCaches);
                    }
                    else
                        initCachesOnLocalJoin();
                }

                initCoordinatorCaches(newCrd);

                if (exchCtx.mergeExchanges()) {
                    if (localJoinExchange()) {
                        if (cctx.kernalContext().clientNode()) {
                            onClientNodeEvent(crdNode);

                            exchange = ExchangeType.CLIENT;
                        }
                        else {
                            onServerNodeEvent(crdNode);

                            exchange = ExchangeType.ALL;
                        }
                    }
                    else {
                        if (CU.clientNode(firstDiscoEvt.eventNode()))
                            exchange = onClientNodeEvent(crdNode);
                        else
                            exchange = cctx.kernalContext().clientNode() ? ExchangeType.CLIENT : ExchangeType.ALL;
                    }

                    if (exchId.isLeft())
                        onLeft();
                }
                else {
                    exchange = CU.clientNode(firstDiscoEvt.eventNode()) ? onClientNodeEvent(crdNode) :
                        onServerNodeEvent(crdNode);
                }
            }

            updateTopologies(crdNode);

            switch (exchange) {
                case ALL: {
                    distributedExchange();

                    break;
                }

                case CLIENT: {
                    if (!exchCtx.mergeExchanges() && exchCtx.fetchAffinityOnJoin())
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

            if (cctx.localNode().isClient())
                tryToPerformLocalSnapshotOperation();

            if (exchLog.isInfoEnabled())
                exchLog.info("Finished exchange init [topVer=" + topVer + ", crd=" + crdNode + ']');
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
                U.error(log, "Failed to reinitialize local partitions (rebalancing will be stopped): " + exchId, e);

                onDone(e);
            }

            if (e instanceof Error)
                throw (Error)e;
        }
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    private void initCachesOnLocalJoin() throws IgniteCheckedException {
        if (isLocalNodeNotInBaseline()) {
            cctx.cache().cleanupCachesDirectories();

            cctx.database().cleanupCheckpointDirectory();

            if (cctx.wal() != null)
                cctx.wal().cleanupWalDirectories();
        }

        cctx.activate();

        LocalJoinCachesContext locJoinCtx = exchActions == null ? null : exchActions.localJoinContext();

        List<T2<DynamicCacheDescriptor, NearCacheConfiguration>> caches = locJoinCtx == null ? null :
            locJoinCtx.caches();

        if (!cctx.kernalContext().clientNode()) {
            List<DynamicCacheDescriptor> startDescs = new ArrayList<>();

            if (caches != null) {
                for (T2<DynamicCacheDescriptor, NearCacheConfiguration> c : caches) {
                    DynamicCacheDescriptor startDesc = c.get1();

                    if (CU.isPersistentCache(startDesc.cacheConfiguration(), cctx.gridConfig().getDataStorageConfiguration()))
                        startDescs.add(startDesc);
                }
            }

            cctx.database().readCheckpointAndRestoreMemory(startDescs);
        }

        cctx.cache().startCachesOnLocalJoin(locJoinCtx, initialVersion());

        ensureClientCachesStarted();
    }

    /**
     * Start client caches if absent.
     */
    private void ensureClientCachesStarted() {
        GridCacheProcessor cacheProcessor = cctx.cache();

        Set<String> cacheNames = new HashSet<>(cacheProcessor.cacheNames());

        List<CacheConfiguration> notStartedCacheConfigs = new ArrayList<>();

        for (CacheConfiguration cCfg : cctx.gridConfig().getCacheConfiguration()) {
            if (!cacheNames.contains(cCfg.getName()) && !GridCacheUtils.isCacheTemplateName(cCfg.getName()))
                notStartedCacheConfigs.add(cCfg);
        }

        if (!notStartedCacheConfigs.isEmpty())
            cacheProcessor.dynamicStartCaches(notStartedCacheConfigs, false, false, false);
    }

    /**
     * @return {@code true} if local node is not in baseline and {@code false} otherwise.
     */
    private boolean isLocalNodeNotInBaseline() {
        BaselineTopology topology = cctx.discovery().discoCache().state().baselineTopology();

        return topology!= null && !topology.consistentIds().contains(cctx.localNode().consistentId());
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

                    grp.topology().beforeExchange(this, !centralizedAff && !forceAffReassignment, false);
                }
            }
        }
        finally {
            cctx.database().checkpointReadUnlock();
        }
    }

    /**
     * Updates topology versions and discovery caches on all topologies.
     *
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
                    top.update(null,
                        clientTop.partitionMap(true),
                        clientTop.fullUpdateCounters(),
                        Collections.emptySet(),
                        null,
                        null);
                }
            }

            top.updateTopologyVersion(
                this,
                events().discoveryCache(),
                updSeq,
                cacheGroupStopping(grp.groupId()));
        }

        for (GridClientPartitionTopology top : cctx.exchange().clientTopologies())
            top.updateTopologyVersion(this, events().discoveryCache(), -1, cacheGroupStopping(top.groupId()));
    }

    /**
     * @param crd Coordinator flag.
     * @return Exchange type.
     */
    private ExchangeType onClusterStateChangeRequest(boolean crd) {
        assert exchActions != null && !exchActions.empty() : this;

        StateChangeRequest req = exchActions.stateChangeRequest();

        assert req != null : exchActions;

        DiscoveryDataClusterState state = cctx.kernalContext().state().clusterState();

        if (state.transitionError() != null)
            exchangeLocE = state.transitionError();

        if (req.activeChanged()) {
            if (req.activate()) {
                if (log.isInfoEnabled()) {
                    log.info("Start activation process [nodeId=" + cctx.localNodeId() +
                        ", client=" + cctx.kernalContext().clientNode() +
                        ", topVer=" + initialVersion() + "]");
                }

                try {
                    cctx.activate();

                    if (!cctx.kernalContext().clientNode()) {
                        List<DynamicCacheDescriptor> startDescs = new ArrayList<>();

                        for (ExchangeActions.CacheActionData startReq : exchActions.cacheStartRequests()) {
                            DynamicCacheDescriptor desc = startReq.descriptor();

                            if (CU.isPersistentCache(desc.cacheConfiguration(),
                                cctx.gridConfig().getDataStorageConfiguration()))
                                startDescs.add(desc);
                        }

                        cctx.database().readCheckpointAndRestoreMemory(startDescs);
                    }

                    cctx.affinity().onCacheChangeRequest(this, crd, exchActions);

                    if (log.isInfoEnabled()) {
                        log.info("Successfully activated caches [nodeId=" + cctx.localNodeId() +
                            ", client=" + cctx.kernalContext().clientNode() +
                            ", topVer=" + initialVersion() + "]");
                    }
                }
                catch (Exception e) {
                    U.error(log, "Failed to activate node components [nodeId=" + cctx.localNodeId() +
                        ", client=" + cctx.kernalContext().clientNode() +
                        ", topVer=" + initialVersion() + "]", e);

                    exchangeLocE = e;

                    if (crd) {
                        synchronized (mux) {
                            exchangeGlobalExceptions.put(cctx.localNodeId(), e);
                        }
                    }
                }
            }
            else {
                if (log.isInfoEnabled()) {
                    log.info("Start deactivation process [nodeId=" + cctx.localNodeId() +
                        ", client=" + cctx.kernalContext().clientNode() +
                        ", topVer=" + initialVersion() + "]");
                }

                try {
                    cctx.kernalContext().dataStructures().onDeActivate(cctx.kernalContext());

                    cctx.kernalContext().service().onDeActivate(cctx.kernalContext());

                    cctx.affinity().onCacheChangeRequest(this, crd, exchActions);

                    if (log.isInfoEnabled()) {
                        log.info("Successfully deactivated data structures, services and caches [" +
                            "nodeId=" + cctx.localNodeId() +
                            ", client=" + cctx.kernalContext().clientNode() +
                            ", topVer=" + initialVersion() + "]");
                    }
                }
                catch (Exception e) {
                    U.error(log, "Failed to deactivate node components [nodeId=" + cctx.localNodeId() +
                        ", client=" + cctx.kernalContext().clientNode() +
                        ", topVer=" + initialVersion() + "]", e);

                    exchangeLocE = e;
                }
            }
        }
        else if (req.activate()) {
            // TODO: BLT changes on inactive cluster can't be handled easily because persistent storage hasn't been initialized yet.
            try {
                if (!forceAffReassignment) {
                    // possible only if cluster contains nodes without forceAffReassignment mode
                    assert firstEventCache().minimumNodeVersion()
                        .compareToIgnoreTimestamp(FORCE_AFF_REASSIGNMENT_SINCE) < 0
                        : firstEventCache().minimumNodeVersion();

                    cctx.affinity().onBaselineTopologyChanged(this, crd);
                }

                if (CU.isPersistenceEnabled(cctx.kernalContext().config()) && !cctx.kernalContext().clientNode())
                    cctx.kernalContext().state().onBaselineTopologyChanged(req.baselineTopology(),
                        req.prevBaselineTopologyHistoryItem());
            }
            catch (Exception e) {
                U.error(log, "Failed to change baseline topology [nodeId=" + cctx.localNodeId() +
                    ", client=" + cctx.kernalContext().clientNode() +
                    ", topVer=" + initialVersion() + "]", e);

                exchangeLocE = e;
            }
        }

        return cctx.kernalContext().clientNode() ? ExchangeType.CLIENT : ExchangeType.ALL;
    }

    /**
     * @param crd Coordinator flag.
     * @return Exchange type.
     * @throws IgniteCheckedException If failed.
     */
    private ExchangeType onCacheChangeRequest(boolean crd) throws IgniteCheckedException {
        assert exchActions != null && !exchActions.empty() : this;

        assert !exchActions.clientOnlyExchange() : exchActions;

        try {
            cctx.affinity().onCacheChangeRequest(this, crd, exchActions);
        }
        catch (Exception e) {
            if (reconnectOnError(e) || !isRollbackSupported())
                // This exception will be handled by init() method.
                throw e;

            U.error(log, "Failed to initialize cache(s) (will try to rollback). " + exchId, e);

            exchangeLocE = new IgniteCheckedException(
                "Failed to initialize exchange locally [locNodeId=" + cctx.localNodeId() + "]", e);

            exchangeGlobalExceptions.put(cctx.localNodeId(), exchangeLocE);
        }

        return cctx.kernalContext().clientNode() ? ExchangeType.CLIENT : ExchangeType.ALL;
    }

    /**
     * @param crd Coordinator flag.
     * @return Exchange type.
     */
    private ExchangeType onCustomMessageNoAffinityChange(boolean crd) {
        if (!forceAffReassignment)
            cctx.affinity().onCustomMessageNoAffinityChange(this, crd, exchActions);

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
        assert CU.clientNode(firstDiscoEvt.eventNode()) : this;

        if (firstDiscoEvt.type() == EVT_NODE_LEFT || firstDiscoEvt.type() == EVT_NODE_FAILED) {
            onLeft();

            assert !firstDiscoEvt.eventNode().isLocal() : firstDiscoEvt;
        }
        else
            assert firstDiscoEvt.type() == EVT_NODE_JOINED || firstDiscoEvt.type() == EVT_DISCOVERY_CUSTOM_EVT : firstDiscoEvt;

        cctx.affinity().onClientEvent(this, crd);

        return firstDiscoEvt.eventNode().isLocal() ? ExchangeType.CLIENT : ExchangeType.NONE;
    }

    /**
     * @param crd Coordinator flag.
     * @throws IgniteCheckedException If failed.
     * @return Exchange type.
     */
    private ExchangeType onServerNodeEvent(boolean crd) throws IgniteCheckedException {
        assert !CU.clientNode(firstDiscoEvt.eventNode()) : this;

        if (firstDiscoEvt.type() == EVT_NODE_LEFT || firstDiscoEvt.type() == EVT_NODE_FAILED) {
            onLeft();

            exchCtx.events().warnNoAffinityNodes(cctx);

            centralizedAff = cctx.affinity().onCentralizedAffinityChange(this, crd);
        }
        else
            cctx.affinity().onServerJoin(this, crd);

        return cctx.kernalContext().clientNode() ? ExchangeType.CLIENT : ExchangeType.ALL;
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    private void clientOnlyExchange() throws IgniteCheckedException {
        if (crd != null) {
            assert !crd.isLocal() : crd;

            if (!centralizedAff)
                sendLocalPartitions(crd);

            initDone();

            return;
        }
        else {
            if (centralizedAff) { // Last server node failed.
                for (CacheGroupContext grp : cctx.cache().cacheGroups()) {
                    GridAffinityAssignmentCache aff = grp.affinity();

                    aff.initialize(initialVersion(), aff.idealAssignment());
                }
            }
            else
                onAllServersLeft();
        }

        onDone(initialVersion());
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

        cctx.database().releaseHistoryForPreloading();

        // To correctly rebalance when persistence is enabled, it is necessary to reserve history within exchange.
        partHistReserved = cctx.database().reserveHistoryForExchange();

        boolean distributed = true;

        // Do not perform distributed partition release in case of cluster activation or caches start.
        if (activateCluster() || hasCachesToStart())
            distributed = false;

        // On first phase we wait for finishing all local tx updates, atomic updates and lock releases on all nodes.
        waitPartitionRelease(distributed, true);

        // Second phase is needed to wait for finishing all tx updates from primary to backup nodes remaining after first phase.
        if (distributed)
            waitPartitionRelease(false, false);

        boolean topChanged = firstDiscoEvt.type() != EVT_DISCOVERY_CUSTOM_EVT || affChangeMsg != null;

        for (GridCacheContext cacheCtx : cctx.cacheContexts()) {
            if (cacheCtx.isLocal() || cacheStopping(cacheCtx.cacheId()))
                continue;

            if (topChanged) {
                // Partition release future is done so we can flush the write-behind store.
                cacheCtx.store().forceFlush();
            }
        }

        /* It is necessary to run database callback before all topology callbacks.
           In case of persistent store is enabled we first restore partitions presented on disk.
           We need to guarantee that there are no partition state changes logged to WAL before this callback
           to make sure that we correctly restored last actual states. */
        boolean restored = cctx.database().beforeExchange(this);

        // Pre-create missing partitions using current affinity.
        if (!exchCtx.mergeExchanges()) {
            for (CacheGroupContext grp : cctx.cache().cacheGroups()) {
                if (grp.isLocal() || cacheGroupStopping(grp.groupId()))
                    continue;

                // It is possible affinity is not initialized yet if node joins to cluster.
                if (grp.affinity().lastVersion().topologyVersion() > 0)
                    grp.topology().beforeExchange(this, !centralizedAff && !forceAffReassignment, false);
            }
        }

        // After all partitions have been restored and pre-created it's safe to make first checkpoint.
        if (restored)
            cctx.database().onStateRestored();

        changeWalModeIfNeeded();

        if (crd.isLocal()) {
            if (remaining.isEmpty())
                onAllReceived(null);
        }
        else
            sendPartitions(crd);

        initDone();
    }

    /**
     * Try to start local snapshot operation if it is needed by discovery event
     */
    private void tryToPerformLocalSnapshotOperation() {
        try {
            long start = U.currentTimeMillis();

            IgniteInternalFuture fut = cctx.snapshot().tryStartLocalSnapshotOperation(firstDiscoEvt, exchId.topologyVersion());

            if (fut != null) {
                fut.get();

                long end = U.currentTimeMillis();

                if (log.isInfoEnabled())
                    log.info("Snapshot initialization completed [topVer=" + exchangeId().topologyVersion() +
                        ", time=" + (end - start) + "ms]");
            }
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Error while starting snapshot operation", e);
        }
    }

    /**
     * Change WAL mode if needed.
     */
    private void changeWalModeIfNeeded() {
        WalStateAbstractMessage msg = firstWalMessage();

        if (msg != null)
            cctx.walState().onProposeExchange(msg.exchangeMessage());
    }

    /**
     * Get first message if and only if this is WAL message.
     *
     * @return WAL message or {@code null}.
     */
    @Nullable private WalStateAbstractMessage firstWalMessage() {
        if (firstDiscoEvt != null && firstDiscoEvt.type() == EVT_DISCOVERY_CUSTOM_EVT) {
            DiscoveryCustomMessage customMsg = ((DiscoveryCustomEvent)firstDiscoEvt).customMessage();

            if (customMsg instanceof WalStateAbstractMessage) {
                WalStateAbstractMessage msg0 = (WalStateAbstractMessage)customMsg;

                assert msg0.needExchange();

                return msg0;
            }
        }

        return null;
    }

    /**
     * The main purpose of this method is to wait for all ongoing updates (transactional and atomic), initiated on
     * the previous topology version, to finish to prevent inconsistencies during rebalancing and to prevent two
     * different simultaneous owners of the same lock.
     * For the exact list of the objects being awaited for see
     * {@link GridCacheSharedContext#partitionReleaseFuture(AffinityTopologyVersion)} javadoc.
     *
     * @param distributed If {@code true} then node should wait for partition release completion on all other nodes.
     * @param doRollback If {@code true} tries to rollback transactions which lock partitions. Avoids unnecessary calls
     *      of {@link org.apache.ignite.internal.processors.cache.transactions.IgniteTxManager#rollbackOnTopologyChange}
     *
     * @throws IgniteCheckedException If failed.
     */
    private void waitPartitionRelease(boolean distributed, boolean doRollback) throws IgniteCheckedException {
        Latch releaseLatch = null;

        // Wait for other nodes only on first phase.
        if (distributed)
            releaseLatch = cctx.exchange().latch().getOrCreate(DISTRIBUTED_LATCH_ID, initialVersion());

        IgniteInternalFuture<?> partReleaseFut = cctx.partitionReleaseFuture(initialVersion());

        // Assign to class variable so it will be included into toString() method.
        this.partReleaseFut = partReleaseFut;

        if (exchId.isLeft())
            cctx.mvcc().removeExplicitNodeLocks(exchId.nodeId(), exchId.topologyVersion());

        if (log.isDebugEnabled())
            log.debug("Before waiting for partition release future: " + this);

        int dumpCnt = 0;

        long nextDumpTime = 0;

        IgniteConfiguration cfg = cctx.gridConfig();

        long waitStart = U.currentTimeMillis();

        long waitTimeout = 2 * cfg.getNetworkTimeout();

        boolean txRolledBack = !doRollback;

        while (true) {
            // Read txTimeoutOnPME from configuration after every iteration.
            long curTimeout = cfg.getTransactionConfiguration().getTxTimeoutOnPartitionMapExchange();

            try {
                // This avoids unnessesary waiting for rollback.
                partReleaseFut.get(curTimeout > 0 && !txRolledBack ?
                        Math.min(curTimeout, waitTimeout) : waitTimeout, TimeUnit.MILLISECONDS);

                break;
            }
            catch (IgniteFutureTimeoutCheckedException ignored) {
                // Print pending transactions and locks that might have led to hang.
                if (nextDumpTime <= U.currentTimeMillis()) {
                    dumpPendingObjects(partReleaseFut, curTimeout <= 0 && !txRolledBack);

                    nextDumpTime = U.currentTimeMillis() + nextDumpTimeout(dumpCnt++, waitTimeout);
                }

                if (!txRolledBack && curTimeout > 0 && U.currentTimeMillis() - waitStart >= curTimeout) {
                    txRolledBack = true;

                    cctx.tm().rollbackOnTopologyChange(initialVersion());
                }
            }
            catch (IgniteCheckedException e) {
                U.warn(log,"Unable to await partitions release future", e);

                throw e;
            }
        }

        long waitEnd = U.currentTimeMillis();

        if (log.isInfoEnabled()) {
            long waitTime = (waitEnd - waitStart);

            String futInfo = RELEASE_FUTURE_DUMP_THRESHOLD > 0 && waitTime > RELEASE_FUTURE_DUMP_THRESHOLD ?
                partReleaseFut.toString() : "NA";

            if (log.isInfoEnabled())
                log.info("Finished waiting for partition release future [topVer=" + exchangeId().topologyVersion() +
                    ", waitTime=" + (waitEnd - waitStart) + "ms, futInfo=" + futInfo + "]");
        }

        IgniteInternalFuture<?> locksFut = cctx.mvcc().finishLocks(exchId.topologyVersion());

        nextDumpTime = 0;
        dumpCnt = 0;

        while (true) {
            try {
                locksFut.get(waitTimeout, TimeUnit.MILLISECONDS);

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

                    nextDumpTime = U.currentTimeMillis() + nextDumpTimeout(dumpCnt++, waitTimeout);

                    if (getBoolean(IGNITE_THREAD_DUMP_ON_EXCHANGE_TIMEOUT, false))
                        U.dumpThreads(log);
                }
            }
        }

        if (releaseLatch == null)
            return;

        releaseLatch.countDown();

        if (!localJoinExchange()) {
            try {
                while (true) {
                    try {
                        releaseLatch.await(waitTimeout, TimeUnit.MILLISECONDS);

                        if (log.isInfoEnabled())
                            log.info("Finished waiting for partitions release latch: " + releaseLatch);

                        break;
                    }
                    catch (IgniteFutureTimeoutCheckedException ignored) {
                        U.warn(log, "Unable to await partitions release latch within timeout: " + releaseLatch);

                        // Try to resend ack.
                        releaseLatch.countDown();
                    }
                }
            }
            catch (IgniteCheckedException e) {
                U.warn(log, "Stop waiting for partitions release latch: " + e.getMessage());
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
     * @param partReleaseFut Partition release future.
     * @param txTimeoutNotifyFlag If {@code true} print transaction rollback timeout on PME notification.
     */
    private void dumpPendingObjects(IgniteInternalFuture<?> partReleaseFut, boolean txTimeoutNotifyFlag) {
        U.warn(cctx.kernalContext().cluster().diagnosticLog(),
            "Failed to wait for partition release future [topVer=" + initialVersion() +
            ", node=" + cctx.localNodeId() + "]");

        if (txTimeoutNotifyFlag)
            U.warn(cctx.kernalContext().cluster().diagnosticLog(), "Consider changing TransactionConfiguration." +
                    "txTimeoutOnPartitionMapExchange to non default value to avoid this message.");

        U.warn(log, "Partition release future: " + partReleaseFut);

        U.warn(cctx.kernalContext().cluster().diagnosticLog(),
            "Dumping pending objects that might be the cause: ");

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
     * @return {@code True} if exchange for local node join.
     */
    public boolean localJoinExchange() {
        return firstDiscoEvt.type() == EVT_NODE_JOINED && firstDiscoEvt.eventNode().isLocal();
    }

    /**
     * @param node Target Node.
     * @throws IgniteCheckedException If failed.
     */
    private void sendLocalPartitions(ClusterNode node) throws IgniteCheckedException {
        assert node != null;

        GridDhtPartitionsSingleMessage msg;

        // Reset lost partitions before sending local partitions to coordinator.
        if (exchActions != null) {
            Set<String> caches = exchActions.cachesToResetLostPartitions();

            if (!F.isEmpty(caches))
                resetLostPartitions(caches);
        }

        if (cctx.kernalContext().clientNode() || (dynamicCacheStartExchange() && exchangeLocE != null)) {
            msg = new GridDhtPartitionsSingleMessage(exchangeId(),
                cctx.kernalContext().clientNode(),
                cctx.versions().last(),
                true);
        }
        else {
            msg = cctx.exchange().createPartitionsSingleMessage(exchangeId(),
                false,
                true,
                node.version().compareToIgnoreTimestamp(PARTIAL_COUNTERS_MAP_SINCE) >= 0,
                exchActions);

            Map<Integer, Map<Integer, Long>> partHistReserved0 = partHistReserved;

            if (partHistReserved0 != null)
                msg.partitionHistoryCounters(partHistReserved0);
        }

        if ((stateChangeExchange() || dynamicCacheStartExchange()) && exchangeLocE != null)
            msg.setError(exchangeLocE);
        else if (localJoinExchange())
            msg.cacheGroupsAffinityRequest(exchCtx.groupsAffinityRequestOnJoin());

        if (log.isDebugEnabled())
            log.debug("Sending local partitions [nodeId=" + node.id() + ", exchId=" + exchId + ", msg=" + msg + ']');

        try {
            cctx.io().send(node, msg, SYSTEM_POOL);
        }
        catch (ClusterTopologyCheckedException ignored) {
            if (log.isDebugEnabled())
                log.debug("Node left during partition exchange [nodeId=" + node.id() + ", exchId=" + exchId + ']');
        }
    }

    /**
     * @param compress Message compress flag.
     * @param newCntrMap {@code True} if possible to use {@link CachePartitionFullCountersMap}.
     * @return Message.
     */
    private GridDhtPartitionsFullMessage createPartitionsMessage(boolean compress,
        boolean newCntrMap) {
        GridCacheVersion last = lastVer.get();

        GridDhtPartitionsFullMessage m = cctx.exchange().createPartitionsFullMessage(
            compress,
            newCntrMap,
            exchangeId(),
            last != null ? last : cctx.versions().last(),
            partHistSuppliers,
            partsToReload);

        if (stateChangeExchange() && !F.isEmpty(exchangeGlobalExceptions))
            m.setErrorsMap(exchangeGlobalExceptions);

        return m;
    }

    /**
     * @param msg Message to send.
     * @param nodes Nodes.
     * @param mergedJoinExchMsgs Messages received from merged 'join node' exchanges.
     * @param joinedNodeAff Affinity if was requested by some nodes.
     */
    private void sendAllPartitions(
        GridDhtPartitionsFullMessage msg,
        Collection<ClusterNode> nodes,
        Map<UUID, GridDhtPartitionsSingleMessage> mergedJoinExchMsgs,
        Map<Integer, CacheGroupAffinityMessage> joinedNodeAff) {
        boolean singleNode = nodes.size() == 1;

        GridDhtPartitionsFullMessage joinedNodeMsg = null;

        assert !nodes.contains(cctx.localNode());

        if (log.isDebugEnabled()) {
            log.debug("Sending full partition map [nodeIds=" + F.viewReadOnly(nodes, F.node2id()) +
                ", exchId=" + exchId + ", msg=" + msg + ']');
        }

        for (ClusterNode node : nodes) {
            GridDhtPartitionsFullMessage sndMsg = msg;

            if (joinedNodeAff != null) {
                if (singleNode)
                    msg.joinedNodeAffinity(joinedNodeAff);
                else {
                    GridDhtPartitionsSingleMessage singleMsg = msgs.get(node.id());

                    if (singleMsg != null && singleMsg.cacheGroupsAffinityRequest() != null) {
                        if (joinedNodeMsg == null) {
                            joinedNodeMsg = msg.copy();

                            joinedNodeMsg.joinedNodeAffinity(joinedNodeAff);
                        }

                        sndMsg = joinedNodeMsg;
                    }
                }
            }

            try {
                GridDhtPartitionExchangeId sndExchId = exchangeId();

                if (mergedJoinExchMsgs != null) {
                    GridDhtPartitionsSingleMessage mergedMsg = mergedJoinExchMsgs.get(node.id());

                    if (mergedMsg != null)
                        sndExchId = mergedMsg.exchangeId();
                }

                if (sndExchId != null && !sndExchId.equals(exchangeId())) {
                    sndMsg = sndMsg.copy();

                    sndMsg.exchangeId(sndExchId);
                }

                cctx.io().send(node, sndMsg, SYSTEM_POOL);
            }
            catch (ClusterTopologyCheckedException e) {
                if (log.isDebugEnabled())
                    log.debug("Failed to send partitions, node failed: " + node);
            }
            catch (IgniteCheckedException e) {
                U.error(log, "Failed to send partitions [node=" + node + ']', e);
            }
        }
    }

    /**
     * @param oldestNode Oldest node. Target node to send message to.
     */
    private void sendPartitions(ClusterNode oldestNode) {
        try {
            sendLocalPartitions(oldestNode);
        }
        catch (ClusterTopologyCheckedException ignore) {
            if (log.isDebugEnabled())
                log.debug("Coordinator left during partition exchange [nodeId=" + oldestNode.id() +
                    ", exchId=" + exchId + ']');
        }
        catch (IgniteCheckedException e) {
            if (reconnectOnError(e))
                onDone(new IgniteNeedReconnectException(cctx.localNode(), e));
            else {
                U.error(log, "Failed to send local partitions to coordinator [crd=" + oldestNode.id() +
                    ", exchId=" + exchId + ']', e);
            }
        }
    }

    /**
     * @return {@code True} if exchange triggered by server node join or fail.
     */
    public boolean serverNodeDiscoveryEvent() {
        assert exchCtx != null;

        return exchCtx.events().hasServerJoin() || exchCtx.events().hasServerLeft();
    }

    /** {@inheritDoc} */
    @Override public boolean exchangeDone() {
        return done.get();
    }

    /**
     * Finish merged future to allow GridCachePartitionExchangeManager.ExchangeFutureSet cleanup.
     */
    public void finishMerged() {
        super.onDone(null, null);
    }

    /** {@inheritDoc} */
    @Override public boolean onDone(@Nullable AffinityTopologyVersion res, @Nullable Throwable err) {
        if (isDone() || !done.compareAndSet(false, true))
            return false;

        if (log.isInfoEnabled()) {
            log.info("Finish exchange future [startVer=" + initialVersion() +
                ", resVer=" + res +
                ", err=" + err + ']');
        }

        assert res != null || err != null;

        if (err == null &&
            !cctx.kernalContext().clientNode() &&
            (serverNodeDiscoveryEvent() || affChangeMsg != null)) {
            for (GridCacheContext cacheCtx : cctx.cacheContexts()) {
                if (!cacheCtx.affinityNode() || cacheCtx.isLocal())
                    continue;

                cacheCtx.continuousQueries().flushBackupQueue(res);
            }
        }

        if (err == null) {
            if (centralizedAff || forceAffReassignment) {
                assert !exchCtx.mergeExchanges();

                for (CacheGroupContext grp : cctx.cache().cacheGroups()) {
                    if (grp.isLocal())
                        continue;

                    boolean needRefresh = false;

                    try {
                        needRefresh = grp.topology().initPartitionsWhenAffinityReady(res, this);
                    }
                    catch (IgniteInterruptedCheckedException e) {
                        U.error(log, "Failed to initialize partitions.", e);
                    }

                    if (needRefresh)
                        cctx.exchange().refreshPartitions();
                }
            }

            for (GridCacheContext cacheCtx : cctx.cacheContexts()) {
                GridCacheContext drCacheCtx = cacheCtx.isNear() ? cacheCtx.near().dht().context() : cacheCtx;

                if (drCacheCtx.isDrEnabled()) {
                    try {
                        drCacheCtx.dr().onExchange(res, exchId.isLeft());
                    }
                    catch (IgniteCheckedException e) {
                        U.error(log, "Failed to notify DR: " + e, e);
                    }
                }
            }

            if (serverNodeDiscoveryEvent())
                detectLostPartitions(res);

            Map<Integer, CacheValidation> m = U.newHashMap(cctx.cache().cacheGroups().size());

            for (CacheGroupContext grp : cctx.cache().cacheGroups())
                m.put(grp.groupId(), validateCacheGroup(grp, events().lastEvent().topologyNodes()));

            grpValidRes = m;
        }

        if (!cctx.localNode().isClient())
            tryToPerformLocalSnapshotOperation();

        cctx.cache().onExchangeDone(initialVersion(), exchActions, err);

        cctx.exchange().onExchangeDone(res, initialVersion(), err);

        cctx.kernalContext().authentication().onActivate();

        if (exchActions != null && err == null)
            exchActions.completeRequestFutures(cctx, null);

        if (stateChangeExchange() && err == null)
            cctx.kernalContext().state().onStateChangeExchangeDone(exchActions.stateChangeRequest());

        Map<T2<Integer, Integer>, Long> localReserved = partHistSuppliers.getReservations(cctx.localNodeId());

        if (localReserved != null) {
            for (Map.Entry<T2<Integer, Integer>, Long> e : localReserved.entrySet()) {
                boolean success = cctx.database().reserveHistoryForPreloading(
                    e.getKey().get1(), e.getKey().get2(), e.getValue());

                if (!success) {
                    // TODO: how to handle?
                    err = new IgniteCheckedException("Could not reserve history");
                }
            }
        }

        cctx.database().releaseHistoryForExchange();

        cctx.database().rebuildIndexesIfNeeded(this);

        if (err == null) {
            for (CacheGroupContext grp : cctx.cache().cacheGroups()) {
                if (!grp.isLocal())
                    grp.topology().onExchangeDone(this, grp.affinity().readyAffinity(res), false);
            }

            cctx.walState().changeLocalStatesOnExchangeDone(res);
        }

        if (super.onDone(res, err)) {
            if (log.isDebugEnabled())
                log.debug("Completed partition exchange [localNode=" + cctx.localNodeId() + ", exchange= " + this +
                    ", durationFromInit=" + (U.currentTimeMillis() - initTs) + ']');
            else if(log.isInfoEnabled())
                log.info("Completed partition exchange [localNode=" + cctx.localNodeId() + ", exchange=" + shortInfo() +
                     ", topVer=" + topologyVersion() + ", durationFromInit=" + (U.currentTimeMillis() - initTs) + ']');

            initFut.onDone(err == null);

            if (exchCtx != null && exchCtx.events().hasServerLeft()) {
                ExchangeDiscoveryEvents evts = exchCtx.events();

                for (DiscoveryEvent evt : exchCtx.events().events()) {
                    if (serverLeftEvent(evt)) {
                        for (CacheGroupContext grp : cctx.cache().cacheGroups())
                            grp.affinityFunction().removeNode(evt.eventNode().id());
                    }
                }
            }

            exchActions = null;

            if (firstDiscoEvt instanceof DiscoveryCustomEvent)
                ((DiscoveryCustomEvent)firstDiscoEvt).customMessage(null);

            if (err == null) {
                cctx.exchange().lastFinishedFuture(this);

                if (exchCtx != null && (exchCtx.events().hasServerLeft() || exchCtx.events().hasServerJoin())) {
                    ExchangeDiscoveryEvents evts = exchCtx.events();

                    for (DiscoveryEvent evt : exchCtx.events().events()) {
                        if (serverLeftEvent(evt) || serverJoinEvent(evt))
                            logExchange(evt);
                    }
                }

            }

            return true;
        }

        return false;
    }

    /**
     * Log exchange event.
     *
     * @param evt Discovery event.
     */
    private void logExchange(DiscoveryEvent evt) {
        if (cctx.kernalContext().state().publicApiActiveState(false) && cctx.wal() != null) {
            if (cctx.wal().serializerVersion() > 1)
                try {
                    ExchangeRecord.Type type = null;

                    if (evt.type() == EVT_NODE_JOINED)
                        type = ExchangeRecord.Type.JOIN;
                    else if (evt.type() == EVT_NODE_LEFT || evt.type() == EVT_NODE_FAILED)
                        type = ExchangeRecord.Type.LEFT;

                    BaselineTopology blt = cctx.kernalContext().state().clusterState().baselineTopology();

                    if (type != null && blt != null) {
                        Short constId = blt.consistentIdMapping().get(evt.eventNode().consistentId());

                        if (constId != null)
                            cctx.wal().log(new ExchangeRecord(constId, type));
                    }
                }
                catch (IgniteCheckedException e) {
                    U.error(log, "Fail during log exchange record.", e);
                }
        }
    }

    /**
     * Cleans up resources to avoid excessive memory usage.
     */
    public void cleanUp() {
        pendingSingleMsgs.clear();
        fullMsgs.clear();
        msgs.clear();
        crd = null;
        partReleaseFut = null;
        exchActions = null;
        mergedJoinExchMsgs = null;
        pendingJoinMsg = null;
        exchCtx = null;
        newCrdFut = null;
        exchangeLocE = null;
        exchangeGlobalExceptions.clear();
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
     * Records that this exchange if merged with another 'node join' exchange.
     *
     * @param node Joined node.
     * @param msg Joined node message if already received.
     * @return {@code True} if need to wait for message from joined server node.
     */
    private boolean addMergedJoinExchange(ClusterNode node, @Nullable GridDhtPartitionsSingleMessage msg) {
        assert Thread.holdsLock(mux);
        assert node != null;
        assert state == ExchangeLocalState.CRD : state;

        if (msg == null && newCrdFut != null)
            msg = newCrdFut.joinExchangeMessage(node.id());

        UUID nodeId = node.id();

        boolean wait = false;

        if (CU.clientNode(node)) {
            if (msg != null)
                waitAndReplyToNode(nodeId, msg);
        }
        else {
            if (mergedJoinExchMsgs == null)
                mergedJoinExchMsgs = new LinkedHashMap<>();

            if (msg != null) {
                assert msg.exchangeId().topologyVersion().equals(new AffinityTopologyVersion(node.order()));

                if (log.isInfoEnabled()) {
                    log.info("Merge server join exchange, message received [curFut=" + initialVersion() +
                        ", node=" + nodeId + ']');
                }

                mergedJoinExchMsgs.put(nodeId, msg);
            }
            else {
                if (cctx.discovery().alive(nodeId)) {
                    if (log.isInfoEnabled()) {
                        log.info("Merge server join exchange, wait for message [curFut=" + initialVersion() +
                            ", node=" + nodeId + ']');
                    }

                    wait = true;

                    mergedJoinExchMsgs.put(nodeId, null);

                    awaitMergedMsgs++;
                }
                else {
                    if (log.isInfoEnabled()) {
                        log.info("Merge server join exchange, awaited node left [curFut=" + initialVersion() +
                            ", node=" + nodeId + ']');
                    }
                }
            }
        }

        return wait;
    }

    /**
     * Merges this exchange with given one.
     *
     * @param fut Current exchange to merge with.
     * @return {@code True} if need wait for message from joined server node.
     */
    public boolean mergeJoinExchange(GridDhtPartitionsExchangeFuture fut) {
        boolean wait;

        synchronized (mux) {
            assert (!isDone() && !initFut.isDone()) || cctx.kernalContext().isStopping() : this;
            assert (mergedWith == null && state == null) || cctx.kernalContext().isStopping()  : this;

            state = ExchangeLocalState.MERGED;

            mergedWith = fut;

            ClusterNode joinedNode = firstDiscoEvt.eventNode();

            wait = fut.addMergedJoinExchange(joinedNode, pendingJoinMsg);
        }

        return wait;
    }

    /**
     * @param fut Current future.
     * @return Pending join request if any.
     */
    @Nullable public GridDhtPartitionsSingleMessage mergeJoinExchangeOnDone(GridDhtPartitionsExchangeFuture fut) {
        synchronized (mux) {
            assert !isDone();
            assert !initFut.isDone();
            assert mergedWith == null;
            assert state == null;

            state = ExchangeLocalState.MERGED;

            mergedWith = fut;

            return pendingJoinMsg;
        }
    }

    /**
     * @param node Sender node.
     * @param msg Message.
     */
    private void processMergedMessage(final ClusterNode node, final GridDhtPartitionsSingleMessage msg) {
        if (msg.client()) {
            waitAndReplyToNode(node.id(), msg);

            return;
        }

        boolean done = false;

        FinishState finishState0 = null;

        synchronized (mux) {
            if (state == ExchangeLocalState.DONE) {
                assert finishState != null;

                finishState0 = finishState;
            }
            else {
                boolean process = mergedJoinExchMsgs != null &&
                    mergedJoinExchMsgs.containsKey(node.id()) &&
                    mergedJoinExchMsgs.get(node.id()) == null;

                if (log.isInfoEnabled()) {
                    log.info("Merge server join exchange, received message [curFut=" + initialVersion() +
                        ", node=" + node.id() +
                        ", msgVer=" + msg.exchangeId().topologyVersion() +
                        ", process=" + process +
                        ", awaited=" + awaitMergedMsgs + ']');
                }

                if (process) {
                    mergedJoinExchMsgs.put(node.id(), msg);

                    assert awaitMergedMsgs > 0 : awaitMergedMsgs;

                    awaitMergedMsgs--;

                    done = awaitMergedMsgs == 0;
                }
            }
        }

        if (finishState0 != null) {
            sendAllPartitionsToNode(finishState0, msg, node.id());

            return;
        }

        if (done)
            finishExchangeOnCoordinator(null);
    }

    /**
     * Method is called on coordinator in situation when initial ExchangeFuture created on client join event was preempted
     * from exchange history because of IGNITE_EXCHANGE_HISTORY_SIZE property.
     *
     * @param node Client node that should try to reconnect to the cluster.
     * @param msg Single message received from the client which didn't find original ExchangeFuture.
     */
    public void forceClientReconnect(ClusterNode node, GridDhtPartitionsSingleMessage msg) {
        Exception reconnectException = new IgniteNeedReconnectException(node, null);

        exchangeGlobalExceptions.put(node.id(), reconnectException);

        onDone(null, reconnectException);

        GridDhtPartitionsFullMessage fullMsg = createPartitionsMessage(true, false);

        fullMsg.setErrorsMap(exchangeGlobalExceptions);

        try {
            cctx.io().send(node, fullMsg, SYSTEM_POOL);

            if (log.isDebugEnabled())
                log.debug("Full message for reconnect client was sent to node: " + node + ", fullMsg: " + fullMsg);
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Failed to send reconnect client message [node=" + node + ']', e);
        }
    }

    /**
     * Processing of received single message. Actual processing in future may be delayed if init method was not
     * completed, see {@link #initDone()}
     *
     * @param node Sender node.
     * @param msg Single partition info.
     */
    public void onReceiveSingleMessage(final ClusterNode node, final GridDhtPartitionsSingleMessage msg) {
        assert !node.isDaemon() : node;
        assert msg != null;
        assert exchId.equals(msg.exchangeId()) : msg;
        assert !cctx.kernalContext().clientNode();

        if (msg.restoreState()) {
            InitNewCoordinatorFuture newCrdFut0;

            synchronized (mux) {
                assert newCrdFut != null;

                newCrdFut0 = newCrdFut;
            }

            newCrdFut0.onMessage(node, msg);

            return;
        }

        if (!msg.client()) {
            assert msg.lastVersion() != null : msg;

            updateLastVersion(msg.lastVersion());
        }

        GridDhtPartitionsExchangeFuture mergedWith0 = null;

        synchronized (mux) {
            if (state == ExchangeLocalState.MERGED) {
                assert mergedWith != null;

                mergedWith0 = mergedWith;
            }
            else {
                assert state != ExchangeLocalState.CLIENT;

                if (exchangeId().isJoined() && node.id().equals(exchId.nodeId()))
                    pendingJoinMsg = msg;
            }
        }

        if (mergedWith0 != null) {
            mergedWith0.processMergedMessage(node, msg);

            if (log.isDebugEnabled())
                log.debug("Merged message processed, message handling finished: " + msg);

            return;
        }

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

                processSingleMessage(node.id(), msg);
            }
        });
    }

    /**
     * @param nodeId Node ID.
     * @param msg Client's message.
     */
    public void waitAndReplyToNode(final UUID nodeId, final GridDhtPartitionsSingleMessage msg) {
        if (log.isDebugEnabled())
            log.debug("Single message will be handled on completion of exchange future: " + this);

        listen(new CI1<IgniteInternalFuture<AffinityTopologyVersion>>() {
            @Override public void apply(IgniteInternalFuture<AffinityTopologyVersion> fut) {
                if (cctx.kernalContext().isStopping())
                    return;

                // DynamicCacheChangeFailureMessage was sent.
                // Thus, there is no need to create and send GridDhtPartitionsFullMessage.
                if (cacheChangeFailureMsgSent)
                    return;

                FinishState finishState0;

                synchronized (mux) {
                    finishState0 = finishState;
                }

                if (finishState0 == null) {
                    assert firstDiscoEvt.type() == EVT_NODE_JOINED && CU.clientNode(firstDiscoEvt.eventNode()) : this;

                    ClusterNode node = cctx.node(nodeId);

                    if (node == null) {
                        if (log.isDebugEnabled()) {
                            log.debug("No node found for nodeId: " +
                                nodeId +
                                ", handling of single message will be stopped: " +
                                msg
                            );
                        }

                        return;
                    }

                    finishState0 = new FinishState(cctx.localNodeId(),
                        initialVersion(),
                        createPartitionsMessage(true, node.version().compareToIgnoreTimestamp(PARTIAL_COUNTERS_MAP_SINCE) >= 0));
                }

                sendAllPartitionsToNode(finishState0, msg, nodeId);
            }
        });
    }

    /**
     * Note this method performs heavy updatePartitionSingleMap operation, this operation is moved out from the
     * synchronized block. Only count of such updates {@link #pendingSingleUpdates} is managed under critical section.
     *
     * @param nodeId Sender node.
     * @param msg Partition single message.
     */
    private void processSingleMessage(UUID nodeId, GridDhtPartitionsSingleMessage msg) {
        if (msg.client()) {
            waitAndReplyToNode(nodeId, msg);

            return;
        }

        boolean allReceived = false; // Received all expected messages.
        boolean updateSingleMap = false;

        FinishState finishState0 = null;

        synchronized (mux) {
            assert crd != null;

            switch (state) {
                case DONE: {
                    if (log.isInfoEnabled()) {
                        log.info("Received single message, already done [ver=" + initialVersion() +
                            ", node=" + nodeId + ']');
                    }

                    assert finishState != null;

                    finishState0 = finishState;

                    break;
                }

                case CRD: {
                    assert crd.isLocal() : crd;

                    if (remaining.remove(nodeId)) {
                        updateSingleMap = true;

                        pendingSingleUpdates++;

                        if ((stateChangeExchange() || dynamicCacheStartExchange()) && msg.getError() != null)
                            exchangeGlobalExceptions.put(nodeId, msg.getError());

                        allReceived = remaining.isEmpty();

                        if (log.isInfoEnabled()) {
                            log.info("Coordinator received single message [ver=" + initialVersion() +
                                ", node=" + nodeId +
                                ", allReceived=" + allReceived + ']');
                        }
                    }
                    else if (log.isDebugEnabled())
                        log.debug("Coordinator received single message it didn't expect to receive: " + msg);

                    break;
                }

                case SRV:
                case BECOME_CRD: {
                    if (log.isInfoEnabled()) {
                        log.info("Non-coordinator received single message [ver=" + initialVersion() +
                            ", node=" + nodeId + ", state=" + state + ']');
                    }

                    pendingSingleMsgs.put(nodeId, msg);

                    break;
                }

                default:
                    assert false : state;
            }
        }

        if (finishState0 != null) {
            // DynamicCacheChangeFailureMessage was sent.
            // Thus, there is no need to create and send GridDhtPartitionsFullMessage.
            if (!cacheChangeFailureMsgSent)
                sendAllPartitionsToNode(finishState0, msg, nodeId);

            return;
        }

        if (updateSingleMap) {
            try {
                // Do not update partition map, in case cluster transitioning to inactive state.
                if (!deactivateCluster())
                    updatePartitionSingleMap(nodeId, msg);
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
            if (!awaitSingleMapUpdates())
                return;

            onAllReceived(null);
        }
    }

    /**
     * @return {@code False} if interrupted.
     */
    private boolean awaitSingleMapUpdates() {
        try {
            synchronized (mux) {
                while (pendingSingleUpdates > 0)
                    U.wait(mux);
            }

            return true;
        }
        catch (IgniteInterruptedCheckedException e) {
            U.warn(log, "Failed to wait for partition map updates, thread was interrupted: " + e);

            return false;
        }
    }

    /**
     * @param fut Affinity future.
     */
    private void onAffinityInitialized(IgniteInternalFuture<Map<Integer, Map<Integer, List<UUID>>>> fut) {
        try {
            assert fut.isDone();

            Map<Integer, Map<Integer, List<UUID>>> assignmentChange = fut.get();

            GridDhtPartitionsFullMessage m = createPartitionsMessage(false, false);

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
     * @param top Topology.
     */
    private void assignPartitionSizes(GridDhtPartitionTopology top) {
        Map<Integer, Long> partSizes = new HashMap<>();

        for (Map.Entry<UUID, GridDhtPartitionsSingleMessage> e : msgs.entrySet()) {
            GridDhtPartitionsSingleMessage singleMsg = e.getValue();

            GridDhtPartitionMap partMap = singleMsg.partitions().get(top.groupId());

            if (partMap == null)
                continue;

            for (Map.Entry<Integer, GridDhtPartitionState> e0 : partMap.entrySet()) {
                int p = e0.getKey();
                GridDhtPartitionState state = e0.getValue();

                if (state == GridDhtPartitionState.OWNING)
                    partSizes.put(p, singleMsg.partitionSizes(top.groupId()).get(p));
            }
        }

        for (GridDhtLocalPartition locPart : top.currentLocalPartitions()) {
            if (locPart.state() == GridDhtPartitionState.OWNING)
                partSizes.put(locPart.id(), locPart.fullSize());
        }

        top.globalPartSizes(partSizes);
    }

    /**
     * Collects and determines new owners of partitions for all nodes for given {@code top}.
     *
     * @param top Topology to assign.
     */
    private void assignPartitionStates(GridDhtPartitionTopology top) {
        Map<Integer, CounterWithNodes> maxCntrs = new HashMap<>();
        Map<Integer, Long> minCntrs = new HashMap<>();

        for (Map.Entry<UUID, GridDhtPartitionsSingleMessage> e : msgs.entrySet()) {
            CachePartitionPartialCountersMap nodeCntrs = e.getValue().partitionUpdateCounters(top.groupId(),
                top.partitions());

            assert nodeCntrs != null;

            for (int i = 0; i < nodeCntrs.size(); i++) {
                int p = nodeCntrs.partitionAt(i);

                UUID uuid = e.getKey();

                GridDhtPartitionState state = top.partitionState(uuid, p);

                if (state != GridDhtPartitionState.OWNING && state != GridDhtPartitionState.MOVING)
                    continue;

                long cntr = state == GridDhtPartitionState.MOVING ?
                    nodeCntrs.initialUpdateCounterAt(i) :
                    nodeCntrs.updateCounterAt(i);

                Long minCntr = minCntrs.get(p);

                if (minCntr == null || minCntr > cntr)
                    minCntrs.put(p, cntr);

                if (state != GridDhtPartitionState.OWNING)
                    continue;

                CounterWithNodes maxCntr = maxCntrs.get(p);

                if (maxCntr == null || cntr > maxCntr.cnt)
                    maxCntrs.put(p, new CounterWithNodes(cntr, e.getValue().partitionSizes(top.groupId()).get(p), uuid));
                else if (cntr == maxCntr.cnt)
                    maxCntr.nodes.add(uuid);
            }
        }

        // Also must process counters from the local node.
        for (GridDhtLocalPartition part : top.currentLocalPartitions()) {
            GridDhtPartitionState state = top.partitionState(cctx.localNodeId(), part.id());

            if (state != GridDhtPartitionState.OWNING && state != GridDhtPartitionState.MOVING)
                continue;

            final long cntr = state == GridDhtPartitionState.MOVING ? part.initialUpdateCounter() : part.updateCounter();

            Long minCntr = minCntrs.get(part.id());

            if (minCntr == null || minCntr > cntr)
                minCntrs.put(part.id(), cntr);

            if (state != GridDhtPartitionState.OWNING)
                continue;

            CounterWithNodes maxCntr = maxCntrs.get(part.id());

            if (maxCntr == null && cntr == 0) {
                CounterWithNodes cntrObj = new CounterWithNodes(0, 0L, cctx.localNodeId());

                for (UUID nodeId : msgs.keySet()) {
                    if (top.partitionState(nodeId, part.id()) == GridDhtPartitionState.OWNING)
                        cntrObj.nodes.add(nodeId);
                }

                maxCntrs.put(part.id(), cntrObj);
            }
            else if (maxCntr == null || cntr > maxCntr.cnt)
                maxCntrs.put(part.id(), new CounterWithNodes(cntr, part.fullSize(), cctx.localNodeId()));
            else if (cntr == maxCntr.cnt)
                maxCntr.nodes.add(cctx.localNodeId());
        }

        Map<Integer, Map<Integer, Long>> partHistReserved0 = partHistReserved;

        Map<Integer, Long> localReserved = partHistReserved0 != null ? partHistReserved0.get(top.groupId()) : null;

        Set<Integer> haveHistory = new HashSet<>();

        for (Map.Entry<Integer, Long> e : minCntrs.entrySet()) {
            int p = e.getKey();
            long minCntr = e.getValue();

            CounterWithNodes maxCntrObj = maxCntrs.get(p);

            long maxCntr = maxCntrObj != null ? maxCntrObj.cnt : 0;

            // If minimal counter is zero, do clean preloading.
            if (minCntr == 0 || minCntr == maxCntr)
                continue;

            if (localReserved != null) {
                Long localCntr = localReserved.get(p);

                if (localCntr != null && localCntr <= minCntr && maxCntrObj.nodes.contains(cctx.localNodeId())) {
                    partHistSuppliers.put(cctx.localNodeId(), top.groupId(), p, localCntr);

                    haveHistory.add(p);

                    continue;
                }
            }

            for (Map.Entry<UUID, GridDhtPartitionsSingleMessage> e0 : msgs.entrySet()) {
                Long histCntr = e0.getValue().partitionHistoryCounters(top.groupId()).get(p);

                if (histCntr != null && histCntr <= minCntr && maxCntrObj.nodes.contains(e0.getKey())) {
                    partHistSuppliers.put(e0.getKey(), top.groupId(), p, histCntr);

                    haveHistory.add(p);

                    break;
                }
            }
        }

        Map<Integer, Set<UUID>> ownersByUpdCounters = new HashMap<>(maxCntrs.size());
        for (Map.Entry<Integer, CounterWithNodes> e : maxCntrs.entrySet())
            ownersByUpdCounters.put(e.getKey(), e.getValue().nodes);

        Map<Integer, Long> partSizes = new HashMap<>(maxCntrs.size());
        for (Map.Entry<Integer, CounterWithNodes> e : maxCntrs.entrySet())
            partSizes.put(e.getKey(), e.getValue().size);

        top.globalPartSizes(partSizes);

        Map<UUID, Set<Integer>> partitionsToRebalance = top.resetOwners(ownersByUpdCounters, haveHistory);

        for (Map.Entry<UUID, Set<Integer>> e : partitionsToRebalance.entrySet()) {
            UUID nodeId = e.getKey();
            Set<Integer> parts = e.getValue();

            for (int part : parts)
                partsToReload.put(nodeId, top.groupId(), part);
        }
    }

    /**
     * Detect lost partitions.
     *
     * @param resTopVer Result topology version.
     */
    private void detectLostPartitions(AffinityTopologyVersion resTopVer) {
        boolean detected = false;

        synchronized (cctx.exchange().interruptLock()) {
            if (Thread.currentThread().isInterrupted())
                return;

            for (CacheGroupContext grp : cctx.cache().cacheGroups()) {
                if (!grp.isLocal()) {
                    boolean detectedOnGrp = grp.topology().detectLostPartitions(resTopVer, events().lastEvent());

                    detected |= detectedOnGrp;
                }
            }
        }

        if (detected)
            cctx.exchange().scheduleResendPartitions();
    }

    /**
     * @param cacheNames Cache names.
     */
    private void resetLostPartitions(Collection<String> cacheNames) {
        assert !exchCtx.mergeExchanges();

        synchronized (cctx.exchange().interruptLock()) {
            if (Thread.currentThread().isInterrupted())
                return;

            for (CacheGroupContext grp : cctx.cache().cacheGroups()) {
                if (grp.isLocal())
                    continue;

                for (String cacheName : cacheNames) {
                    if (grp.hasCache(cacheName)) {
                        grp.topology().resetLostPartitions(initialVersion());

                        break;
                    }
                }
            }
        }
    }

    /**
     * Creates an IgniteCheckedException that is used as root cause of the exchange initialization failure.
     * This method aggregates all the exceptions provided from all participating nodes.
     *
     * @param globalExceptions collection exceptions from all participating nodes.
     * @return exception that represents a cause of the exchange initialization failure.
     */
    private IgniteCheckedException createExchangeException(Map<UUID, Exception> globalExceptions) {
        IgniteCheckedException ex = new IgniteCheckedException("Failed to complete exchange process.");

        for (Map.Entry<UUID, Exception> entry : globalExceptions.entrySet())
            if (ex != entry.getValue())
                ex.addSuppressed(entry.getValue());

        return ex;
    }

    /**
     * @return {@code true} if the given {@code discoEvt} supports the rollback procedure.
     */
    private boolean isRollbackSupported() {
        if (!firstEvtDiscoCache.checkAttribute(ATTR_DYNAMIC_CACHE_START_ROLLBACK_SUPPORTED, Boolean.TRUE))
            return false;

        // Currently the rollback process is supported for dynamically started caches only.
        return firstDiscoEvt.type() == EVT_DISCOVERY_CUSTOM_EVT && dynamicCacheStartExchange();
    }

    /**
     * Sends {@link DynamicCacheChangeFailureMessage} to all participated nodes
     * that represents a cause of exchange failure.
     */
    private void sendExchangeFailureMessage() {
        assert crd != null && crd.isLocal();

        try {
            IgniteCheckedException err = createExchangeException(exchangeGlobalExceptions);

            List<String> cacheNames = new ArrayList<>(exchActions.cacheStartRequests().size());

            for (ExchangeActions.CacheActionData actionData : exchActions.cacheStartRequests())
                cacheNames.add(actionData.request().cacheName());

            DynamicCacheChangeFailureMessage msg = new DynamicCacheChangeFailureMessage(
                cctx.localNode(), exchId, err, cacheNames);

            if (log.isDebugEnabled())
                log.debug("Dynamic cache change failed (send message to all participating nodes): " + msg);

            cacheChangeFailureMsgSent = true;

            cctx.discovery().sendCustomEvent(msg);

            return;
        }
        catch (IgniteCheckedException  e) {
            if (reconnectOnError(e))
                onDone(new IgniteNeedReconnectException(cctx.localNode(), e));
            else
                onDone(e);
        }
    }

    /**
     * @param sndResNodes Additional nodes to send finish message to.
     */
    private void onAllReceived(@Nullable Collection<ClusterNode> sndResNodes) {
        try {
            assert crd.isLocal();

            assert partHistSuppliers.isEmpty() : partHistSuppliers;

            if (!exchCtx.mergeExchanges() && !crd.equals(events().discoveryCache().serverNodes().get(0))) {
                for (CacheGroupContext grp : cctx.cache().cacheGroups()) {
                    if (grp.isLocal())
                        continue;

                    // It is possible affinity is not initialized.
                    // For example, dynamic cache start failed.
                    if (grp.affinity().lastVersion().topologyVersion() > 0)
                        grp.topology().beforeExchange(this, !centralizedAff && !forceAffReassignment, false);
                    else
                        assert exchangeLocE != null :
                            "Affinity is not calculated for the cache group [groupName=" + grp.name() + "]";
                }
            }

            if (exchCtx.mergeExchanges()) {
                if (log.isInfoEnabled())
                    log.info("Coordinator received all messages, try merge [ver=" + initialVersion() + ']');

                boolean finish = cctx.exchange().mergeExchangesOnCoordinator(this);

                if (!finish)
                    return;
            }

            finishExchangeOnCoordinator(sndResNodes);
        }
        catch (IgniteCheckedException e) {
            if (reconnectOnError(e))
                onDone(new IgniteNeedReconnectException(cctx.localNode(), e));
            else
                onDone(e);
        }
    }

    /**
     * @param sndResNodes Additional nodes to send finish message to.
     */
    private void finishExchangeOnCoordinator(@Nullable Collection<ClusterNode> sndResNodes) {
        try {
            if (!F.isEmpty(exchangeGlobalExceptions) && dynamicCacheStartExchange() && isRollbackSupported()) {
                sendExchangeFailureMessage();

                return;
            }

            AffinityTopologyVersion resTopVer = exchCtx.events().topologyVersion();

            if (log.isInfoEnabled()) {
                log.info("finishExchangeOnCoordinator [topVer=" + initialVersion() +
                    ", resVer=" + resTopVer + ']');
            }

            Map<Integer, CacheGroupAffinityMessage> idealAffDiff = null;

            if (exchCtx.mergeExchanges()) {
                synchronized (mux) {
                    if (mergedJoinExchMsgs != null) {
                        for (Map.Entry<UUID, GridDhtPartitionsSingleMessage> e : mergedJoinExchMsgs.entrySet()) {
                            msgs.put(e.getKey(), e.getValue());

                            updatePartitionSingleMap(e.getKey(), e.getValue());
                        }
                    }
                }

                assert exchCtx.events().hasServerJoin() || exchCtx.events().hasServerLeft();

                exchCtx.events().processEvents(this);

                if (exchCtx.events().hasServerLeft())
                    idealAffDiff = cctx.affinity().onServerLeftWithExchangeMergeProtocol(this);
                else
                    cctx.affinity().onServerJoinWithExchangeMergeProtocol(this, true);

                for (CacheGroupDescriptor desc : cctx.affinity().cacheGroups().values()) {
                    if (desc.config().getCacheMode() == CacheMode.LOCAL)
                        continue;

                    CacheGroupContext grp = cctx.cache().cacheGroup(desc.groupId());

                    GridDhtPartitionTopology top = grp != null ? grp.topology() :
                        cctx.exchange().clientTopology(desc.groupId(), events().discoveryCache());

                    top.beforeExchange(this, true, true);
                }
            }

            Map<Integer, CacheGroupAffinityMessage> joinedNodeAff = null;

            for (Map.Entry<UUID, GridDhtPartitionsSingleMessage> e : msgs.entrySet()) {
                GridDhtPartitionsSingleMessage msg = e.getValue();

                // Apply update counters after all single messages are received.
                for (Map.Entry<Integer, GridDhtPartitionMap> entry : msg.partitions().entrySet()) {
                    Integer grpId = entry.getKey();

                    CacheGroupContext grp = cctx.cache().cacheGroup(grpId);

                    GridDhtPartitionTopology top = grp != null ? grp.topology() :
                        cctx.exchange().clientTopology(grpId, events().discoveryCache());

                    CachePartitionPartialCountersMap cntrs = msg.partitionUpdateCounters(grpId,
                        top.partitions());

                    if (cntrs != null)
                        top.collectUpdateCounters(cntrs);
                }

                Collection<Integer> affReq = msg.cacheGroupsAffinityRequest();

                if (affReq != null) {
                    joinedNodeAff = CacheGroupAffinityMessage.createAffinityMessages(cctx,
                        resTopVer,
                        affReq,
                        joinedNodeAff);
                }
            }

            // Don't validate partitions state in case of caches start.
            boolean skipValidation = hasCachesToStart();

            if (!skipValidation)
                validatePartitionsState();

            if (firstDiscoEvt.type() == EVT_DISCOVERY_CUSTOM_EVT) {
                assert firstDiscoEvt instanceof DiscoveryCustomEvent;

                if (activateCluster() || changedBaseline())
                    assignPartitionsStates();

                DiscoveryCustomMessage discoveryCustomMessage = ((DiscoveryCustomEvent) firstDiscoEvt).customMessage();

                if (discoveryCustomMessage instanceof DynamicCacheChangeBatch) {
                    if (exchActions != null) {
                        assignPartitionsStates();

                        Set<String> caches = exchActions.cachesToResetLostPartitions();

                        if (!F.isEmpty(caches))
                            resetLostPartitions(caches);
                    }
                }
                else if (discoveryCustomMessage instanceof SnapshotDiscoveryMessage
                        && ((SnapshotDiscoveryMessage)discoveryCustomMessage).needAssignPartitions())
                    assignPartitionsStates();
            }
            else {
                if (exchCtx.events().hasServerJoin())
                    assignPartitionsStates();

                if (exchCtx.events().hasServerLeft())
                    detectLostPartitions(resTopVer);
            }

            // Recalculate new affinity based on partitions availability.
            if (!exchCtx.mergeExchanges() && forceAffReassignment)
                idealAffDiff = cctx.affinity().onCustomEventWithEnforcedAffinityReassignment(this);

            for (CacheGroupContext grpCtx : cctx.cache().cacheGroups()) {
                if (!grpCtx.isLocal())
                    grpCtx.topology().applyUpdateCounters();
            }

            updateLastVersion(cctx.versions().last());

            cctx.versions().onExchange(lastVer.get().order());

            IgniteProductVersion minVer = exchCtx.events().discoveryCache().minimumNodeVersion();

            GridDhtPartitionsFullMessage msg = createPartitionsMessage(true,
                minVer.compareToIgnoreTimestamp(PARTIAL_COUNTERS_MAP_SINCE) >= 0);

            if (exchCtx.mergeExchanges()) {
                assert !centralizedAff;

                msg.resultTopologyVersion(resTopVer);

                if (exchCtx.events().hasServerLeft())
                    msg.idealAffinityDiff(idealAffDiff);
            }
            else if (forceAffReassignment)
                msg.idealAffinityDiff(idealAffDiff);

            msg.prepareMarshal(cctx);

            synchronized (mux) {
                finishState = new FinishState(crd.id(), resTopVer, msg);

                state = ExchangeLocalState.DONE;
            }

            if (centralizedAff) {
                assert !exchCtx.mergeExchanges();

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
                Set<ClusterNode> nodes;

                Map<UUID, GridDhtPartitionsSingleMessage> mergedJoinExchMsgs0;

                synchronized (mux) {
                    srvNodes.remove(cctx.localNode());

                    nodes = U.newHashSet(srvNodes.size());

                    nodes.addAll(srvNodes);

                    mergedJoinExchMsgs0 = mergedJoinExchMsgs;

                    if (mergedJoinExchMsgs != null) {
                        for (Map.Entry<UUID, GridDhtPartitionsSingleMessage> e : mergedJoinExchMsgs.entrySet()) {
                            if (e.getValue() != null) {
                                ClusterNode node = cctx.discovery().node(e.getKey());

                                if (node != null)
                                    nodes.add(node);
                            }
                        }
                    }

                    if (!F.isEmpty(sndResNodes))
                        nodes.addAll(sndResNodes);
                }

                if (!nodes.isEmpty())
                    sendAllPartitions(msg, nodes, mergedJoinExchMsgs0, joinedNodeAff);

                if (!stateChangeExchange())
                    onDone(exchCtx.events().topologyVersion(), null);

                for (Map.Entry<UUID, GridDhtPartitionsSingleMessage> e : pendingSingleMsgs.entrySet()) {
                    if (log.isInfoEnabled()) {
                        log.info("Process pending message on coordinator [node=" + e.getKey() +
                            ", ver=" + initialVersion() +
                            ", resVer=" + resTopVer + ']');
                    }

                    processSingleMessage(e.getKey(), e.getValue());
                }
            }

            if (stateChangeExchange()) {
                IgniteCheckedException err = null;

                StateChangeRequest req = exchActions.stateChangeRequest();

                assert req != null : exchActions;

                boolean stateChangeErr = false;

                if (!F.isEmpty(exchangeGlobalExceptions)) {
                    stateChangeErr = true;

                    err = new IgniteCheckedException("Cluster state change failed.");

                    cctx.kernalContext().state().onStateChangeError(exchangeGlobalExceptions, req);
                }
                else {
                    boolean hasMoving = !partsToReload.isEmpty();

                    Set<Integer> waitGrps = cctx.affinity().waitGroups();

                    if (!hasMoving) {
                        for (CacheGroupContext grpCtx : cctx.cache().cacheGroups()) {
                            if (waitGrps.contains(grpCtx.groupId()) && grpCtx.topology().hasMovingPartitions()) {
                                hasMoving = true;

                                break;
                            }

                        }
                    }

                    cctx.kernalContext().state().onExchangeFinishedOnCoordinator(this, hasMoving);
                }

                boolean active = !stateChangeErr && req.activate();

                ChangeGlobalStateFinishMessage stateFinishMsg = new ChangeGlobalStateFinishMessage(
                    req.requestId(),
                    active,
                    !stateChangeErr);

                cctx.discovery().sendCustomEvent(stateFinishMsg);

                if (!centralizedAff)
                    onDone(exchCtx.events().topologyVersion(), err);
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
     * Validates that partition update counters and cache sizes for all caches are consistent.
     */
    private void validatePartitionsState() {
        for (Map.Entry<Integer, CacheGroupDescriptor> e : cctx.affinity().cacheGroups().entrySet()) {
            CacheGroupDescriptor grpDesc = e.getValue();
            if (grpDesc.config().getCacheMode() == CacheMode.LOCAL)
                continue;

            int grpId = e.getKey();

            CacheGroupContext grpCtx = cctx.cache().cacheGroup(grpId);

            GridDhtPartitionTopology top = grpCtx != null ?
                    grpCtx.topology() :
                    cctx.exchange().clientTopology(grpId, events().discoveryCache());

            // Do not validate read or write through caches or caches with disabled rebalance
            // or ExpiryPolicy is set or validation is disabled.
            if (grpCtx == null
                    || grpCtx.config().isReadThrough()
                    || grpCtx.config().isWriteThrough()
                    || grpCtx.config().getCacheStoreFactory() != null
                    || grpCtx.config().getRebalanceDelay() == -1
                    || grpCtx.config().getRebalanceMode() == CacheRebalanceMode.NONE
                    || grpCtx.config().getExpiryPolicyFactory() == null
                    || SKIP_PARTITION_SIZE_VALIDATION)
                continue;

            try {
                validator.validatePartitionCountersAndSizes(this, top, msgs);
            }
            catch (IgniteCheckedException ex) {
                log.warning("Partition states validation has failed for group: " + grpDesc.cacheOrGroupName() + ". " + ex.getMessage());
                // TODO: Handle such errors https://issues.apache.org/jira/browse/IGNITE-7833
            }
        }
    }

    /**
     *
     */
    private void assignPartitionsStates() {
        for (Map.Entry<Integer, CacheGroupDescriptor> e : cctx.affinity().cacheGroups().entrySet()) {
            CacheGroupDescriptor grpDesc = e.getValue();
            if (grpDesc.config().getCacheMode() == CacheMode.LOCAL)
                continue;

            CacheGroupContext grpCtx = cctx.cache().cacheGroup(e.getKey());

            GridDhtPartitionTopology top = grpCtx != null ?
                grpCtx.topology() :
                cctx.exchange().clientTopology(e.getKey(), events().discoveryCache());

            if (!CU.isPersistentCache(grpDesc.config(), cctx.gridConfig().getDataStorageConfiguration()))
                assignPartitionSizes(top);
            else
                assignPartitionStates(top);
        }
    }

    /**
     * @param finishState State.
     * @param msg Request.
     * @param nodeId Node ID.
     */
    private void sendAllPartitionsToNode(FinishState finishState, GridDhtPartitionsSingleMessage msg, UUID nodeId) {
        ClusterNode node = cctx.node(nodeId);

        if (node != null) {
            GridDhtPartitionsFullMessage fullMsg = finishState.msg.copy();

            Collection<Integer> affReq = msg.cacheGroupsAffinityRequest();

            if (affReq != null) {
                Map<Integer, CacheGroupAffinityMessage> aff = CacheGroupAffinityMessage.createAffinityMessages(
                    cctx,
                    finishState.resTopVer,
                    affReq,
                    null);

                fullMsg.joinedNodeAffinity(aff);
            }

            if (!fullMsg.exchangeId().equals(msg.exchangeId())) {
                fullMsg = fullMsg.copy();

                fullMsg.exchangeId(msg.exchangeId());
            }

            try {
                cctx.io().send(node, fullMsg, SYSTEM_POOL);

                if (log.isDebugEnabled()) {
                    log.debug("Full message was sent to node: " +
                        node +
                        ", fullMsg: " + fullMsg
                    );
                }
            }
            catch (ClusterTopologyCheckedException e) {
                if (log.isDebugEnabled())
                    log.debug("Failed to send partitions, node failed: " + node);
            }
            catch (IgniteCheckedException e) {
                U.error(log, "Failed to send partitions [node=" + node + ']', e);
            }
        }
        else if (log.isDebugEnabled())
            log.debug("Failed to send partitions, node failed: " + nodeId);

    }

    /**
     * @param node Sender node.
     * @param msg Full partition info.
     */
    public void onReceiveFullMessage(final ClusterNode node, final GridDhtPartitionsFullMessage msg) {
        assert msg != null;
        assert msg.exchangeId() != null : msg;
        assert !node.isDaemon() : node;

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

                processFullMessage(true, node, msg);
            }
        });
    }

    /**
     * @param node Sender node.
     * @param msg Message with full partition info.
     */
    public void onReceivePartitionRequest(final ClusterNode node, final GridDhtPartitionsSingleRequest msg) {
        assert !cctx.kernalContext().clientNode() || msg.restoreState();
        assert !node.isDaemon() && !CU.clientNode(node) : node;

        initFut.listen(new CI1<IgniteInternalFuture<Boolean>>() {
            @Override public void apply(IgniteInternalFuture<Boolean> fut) {
                processSinglePartitionRequest(node, msg);
            }
        });
    }

    /**
     * @param node Sender node.
     * @param msg Message.
     */
    private void processSinglePartitionRequest(ClusterNode node, GridDhtPartitionsSingleRequest msg) {
        FinishState finishState0 = null;

        synchronized (mux) {
            if (crd == null) {
                if (log.isInfoEnabled())
                    log.info("Ignore partitions request, no coordinator [node=" + node.id() + ']');

                return;
            }

            switch (state) {
                case DONE: {
                    assert finishState != null;

                    if (node.id().equals(finishState.crdId)) {
                        if (log.isInfoEnabled())
                            log.info("Ignore partitions request, finished exchange with this coordinator: " + msg);

                        return;
                    }

                    finishState0 = finishState;

                    break;
                }

                case CRD:
                case BECOME_CRD: {
                    if (log.isInfoEnabled())
                        log.info("Ignore partitions request, node is coordinator: " + msg);

                    return;
                }

                case CLIENT:
                case SRV: {
                    if (!cctx.discovery().alive(node)) {
                        if (log.isInfoEnabled())
                            log.info("Ignore partitions request, node is not alive [node=" + node.id() + ']');

                        return;
                    }

                    if (msg.restoreState()) {
                        if (!node.equals(crd)) {
                            if (node.order() > crd.order()) {
                                if (log.isInfoEnabled()) {
                                    log.info("Received partitions request, change coordinator [oldCrd=" + crd.id() +
                                        ", newCrd=" + node.id() + ']');
                                }

                                crd = node; // Do not allow to process FullMessage from old coordinator.
                            }
                            else {
                                if (log.isInfoEnabled()) {
                                    log.info("Ignore restore state request, coordinator changed [oldCrd=" + crd.id() +
                                        ", newCrd=" + node.id() + ']');
                                }

                                return;
                            }
                        }
                    }

                    break;
                }

                default:
                    assert false : state;
            }
        }

        if (msg.restoreState()) {
            try {
                assert msg.restoreExchangeId() != null : msg;

                GridDhtPartitionsSingleMessage res;

                if (dynamicCacheStartExchange() && exchangeLocE != null) {
                    res = new GridDhtPartitionsSingleMessage(msg.restoreExchangeId(),
                        cctx.kernalContext().clientNode(),
                        cctx.versions().last(),
                        true);

                    res.setError(exchangeLocE);
                }
                else {
                    res = cctx.exchange().createPartitionsSingleMessage(
                        msg.restoreExchangeId(),
                        cctx.kernalContext().clientNode(),
                        true,
                        node.version().compareToIgnoreTimestamp(PARTIAL_COUNTERS_MAP_SINCE) >= 0,
                        exchActions);

                    if (localJoinExchange() && finishState0 == null)
                        res.cacheGroupsAffinityRequest(exchCtx.groupsAffinityRequestOnJoin());
                }

                res.restoreState(true);

                if (log.isInfoEnabled()) {
                    log.info("Send restore state response [node=" + node.id() +
                        ", exchVer=" + msg.restoreExchangeId().topologyVersion() +
                        ", hasState=" + (finishState0 != null) +
                        ", affReq=" + !F.isEmpty(res.cacheGroupsAffinityRequest()) + ']');
                }

                res.finishMessage(finishState0 != null ? finishState0.msg : null);

                cctx.io().send(node, res, SYSTEM_POOL);
            }
            catch (ClusterTopologyCheckedException ignored) {
                if (log.isDebugEnabled())
                    log.debug("Node left during partition exchange [nodeId=" + node.id() + ", exchId=" + exchId + ']');
            }
            catch (IgniteCheckedException e) {
                U.error(log, "Failed to send partitions message [node=" + node + ", msg=" + msg + ']', e);
            }

            return;
        }

        try {
            sendLocalPartitions(node);
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Failed to send message to coordinator: " + e);
        }
    }

    /**
     * @param checkCrd If {@code true} checks that local node is exchange coordinator.
     * @param node Sender node.
     * @param msg Message.
     */
    private void processFullMessage(boolean checkCrd, ClusterNode node, GridDhtPartitionsFullMessage msg) {
        try {
            assert exchId.equals(msg.exchangeId()) : msg;
            assert msg.lastVersion() != null : msg;

            if (checkCrd) {
                assert node != null;

                synchronized (mux) {
                    if (crd == null) {
                        if (log.isInfoEnabled())
                            log.info("Ignore full message, all server nodes left: " + msg);

                        return;
                    }

                    switch (state) {
                        case CRD:
                        case BECOME_CRD: {
                            if (log.isInfoEnabled())
                                log.info("Ignore full message, node is coordinator: " + msg);

                            return;
                        }

                        case DONE: {
                            if (log.isInfoEnabled())
                                log.info("Ignore full message, future is done: " + msg);

                            return;
                        }

                        case SRV:
                        case CLIENT: {
                            if (!crd.equals(node)) {
                                if (log.isInfoEnabled()) {
                                    log.info("Received full message from non-coordinator [node=" + node.id() +
                                        ", nodeOrder=" + node.order() +
                                        ", crd=" + crd.id() +
                                        ", crdOrder=" + crd.order() + ']');
                                }

                                if (node.order() > crd.order())
                                    fullMsgs.put(node, msg);

                                return;
                            }
                            else {
                                if (!F.isEmpty(msg.getErrorsMap())) {
                                    Exception e = msg.getErrorsMap().get(cctx.localNodeId());

                                    if (e instanceof IgniteNeedReconnectException) {
                                        onDone(e);

                                        return;
                                    }
                                }

                                AffinityTopologyVersion resVer = msg.resultTopologyVersion() != null ? msg.resultTopologyVersion() : initialVersion();

                                if (log.isInfoEnabled()) {
                                    log.info("Received full message, will finish exchange [node=" + node.id() +
                                        ", resVer=" + resVer + ']');
                                }

                                finishState = new FinishState(crd.id(), resVer, msg);

                                state = ExchangeLocalState.DONE;

                                break;
                            }
                        }
                    }
                }
            }
            else
                assert node == null : node;

            AffinityTopologyVersion resTopVer = initialVersion();

            if (exchCtx.mergeExchanges()) {
                if (msg.resultTopologyVersion() != null && !initialVersion().equals(msg.resultTopologyVersion())) {
                    if (log.isInfoEnabled()) {
                        log.info("Received full message, need merge [curFut=" + initialVersion() +
                            ", resVer=" + msg.resultTopologyVersion() + ']');
                    }

                    resTopVer = msg.resultTopologyVersion();

                    if (cctx.exchange().mergeExchanges(this, msg)) {
                        assert cctx.kernalContext().isStopping();

                        return; // Node is stopping, no need to further process exchange.
                    }

                    assert resTopVer.equals(exchCtx.events().topologyVersion()) :  "Unexpected result version [" +
                        "msgVer=" + resTopVer +
                        ", locVer=" + exchCtx.events().topologyVersion() + ']';
                }

                exchCtx.events().processEvents(this);

                if (localJoinExchange())
                    cctx.affinity().onLocalJoin(this, msg, resTopVer);
                else {
                    if (exchCtx.events().hasServerLeft())
                        cctx.affinity().applyAffinityFromFullMessage(this, msg);
                    else
                        cctx.affinity().onServerJoinWithExchangeMergeProtocol(this, false);

                    for (CacheGroupContext grp : cctx.cache().cacheGroups()) {
                        if (grp.isLocal() || cacheGroupStopping(grp.groupId()))
                            continue;

                        grp.topology().beforeExchange(this, true, false);
                    }
                }
            }
            else if (localJoinExchange() && !exchCtx.fetchAffinityOnJoin())
                cctx.affinity().onLocalJoin(this, msg, resTopVer);
            else if (forceAffReassignment)
                cctx.affinity().applyAffinityFromFullMessage(this, msg);

            if (dynamicCacheStartExchange() && !F.isEmpty(exchangeGlobalExceptions)) {
                assert cctx.localNode().isClient();

                // TODO: https://issues.apache.org/jira/browse/IGNITE-8796
                // The current exchange has been successfully completed on all server nodes,
                // but has failed on that client node for some reason.
                // It looks like that we need to rollback dynamically started caches on the client node,
                // complete DynamicCacheStartFutures (if they are registered) with the cause of that failure
                // and complete current exchange without errors.

                onDone(exchangeLocE);

                return;
            }

            updatePartitionFullMap(resTopVer, msg);

            IgniteCheckedException err = null;

            if (stateChangeExchange() && !F.isEmpty(msg.getErrorsMap())) {
                err = new IgniteCheckedException("Cluster state change failed");

                cctx.kernalContext().state().onStateChangeError(msg.getErrorsMap(), exchActions.stateChangeRequest());
            }

            onDone(resTopVer, err);
        }
        catch (IgniteCheckedException e) {
            onDone(e);
        }
    }

    /**
     * Updates partition map in all caches.
     *
     * @param resTopVer Result topology version.
     * @param msg Partitions full messages.
     */
    private void updatePartitionFullMap(AffinityTopologyVersion resTopVer, GridDhtPartitionsFullMessage msg) {
        cctx.versions().onExchange(msg.lastVersion().order());

        assert partHistSuppliers.isEmpty();

        partHistSuppliers.putAll(msg.partitionHistorySuppliers());

        for (Map.Entry<Integer, GridDhtPartitionFullMap> entry : msg.partitions().entrySet()) {
            Integer grpId = entry.getKey();

            CacheGroupContext grp = cctx.cache().cacheGroup(grpId);

            if (grp != null) {
                CachePartitionFullCountersMap cntrMap = msg.partitionUpdateCounters(grpId,
                    grp.topology().partitions());

                grp.topology().update(resTopVer,
                    entry.getValue(),
                    cntrMap,
                    msg.partsToReload(cctx.localNodeId(), grpId),
                    msg.partitionSizes(grpId),
                    null);
            }
            else {
                ClusterNode oldest = cctx.discovery().oldestAliveServerNode(AffinityTopologyVersion.NONE);

                if (oldest != null && oldest.isLocal()) {
                    GridDhtPartitionTopology top = cctx.exchange().clientTopology(grpId, events().discoveryCache());

                    CachePartitionFullCountersMap cntrMap = msg.partitionUpdateCounters(grpId,
                        top.partitions());

                    top.update(resTopVer,
                        entry.getValue(),
                        cntrMap,
                        Collections.emptySet(),
                        null,
                        null);
                }
            }
        }
    }

    /**
     * Updates partition map in all caches.
     *
     * @param nodeId Node message received from.
     * @param msg Partitions single message.
     */
    private void updatePartitionSingleMap(UUID nodeId, GridDhtPartitionsSingleMessage msg) {
        msgs.put(nodeId, msg);

        for (Map.Entry<Integer, GridDhtPartitionMap> entry : msg.partitions().entrySet()) {
            Integer grpId = entry.getKey();
            CacheGroupContext grp = cctx.cache().cacheGroup(grpId);

            GridDhtPartitionTopology top = grp != null ? grp.topology() :
                cctx.exchange().clientTopology(grpId, events().discoveryCache());

            top.update(exchId, entry.getValue(), false);
        }
    }

    /**
     * Cache change failure message callback, processed from the discovery thread.
     *
     * @param node Message sender node.
     * @param msg Failure message.
     */
    public void onDynamicCacheChangeFail(final ClusterNode node, final DynamicCacheChangeFailureMessage msg) {
        assert exchId.equals(msg.exchangeId()) : msg;
        assert firstDiscoEvt.type() == EVT_DISCOVERY_CUSTOM_EVT && dynamicCacheStartExchange();

        final ExchangeActions actions = exchangeActions();

        onDiscoveryEvent(new IgniteRunnable() {
            @Override public void run() {
                // The rollbackExchange() method has to wait for checkpoint.
                // That operation is time consumed, and therefore it should be executed outside the discovery thread.
                cctx.kernalContext().getSystemExecutorService().submit(new Runnable() {
                    @Override public void run() {
                        if (isDone() || !enterBusy())
                            return;

                        try {
                            assert msg.error() != null: msg;

                            // Try to revert all the changes that were done during initialization phase
                            cctx.affinity().forceCloseCaches(GridDhtPartitionsExchangeFuture.this,
                                crd.isLocal(), msg.exchangeActions());

                            synchronized (mux) {
                                finishState = new FinishState(crd.id(), initialVersion(), null);

                                state = ExchangeLocalState.DONE;
                            }

                            if (actions != null)
                                actions.completeRequestFutures(cctx, msg.error());

                            onDone(exchId.topologyVersion());
                        }
                        catch (Throwable e) {
                            onDone(e);
                        }
                        finally {
                            leaveBusy();
                        }
                    }
                });
            }
        });
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
                        AffinityTopologyVersion resTopVer = initialVersion();

                        cctx.affinity().onExchangeChangeAffinityMessage(GridDhtPartitionsExchangeFuture.this,
                            crd.isLocal(),
                            msg);

                        IgniteCheckedException err = !F.isEmpty(msg.partitionsMessage().getErrorsMap()) ?
                            new IgniteCheckedException("Cluster state change failed.") : null;

                        if (!crd.isLocal()) {
                            GridDhtPartitionsFullMessage partsMsg = msg.partitionsMessage();

                            assert partsMsg != null : msg;
                            assert partsMsg.lastVersion() != null : partsMsg;

                            updatePartitionFullMap(resTopVer, partsMsg);

                            if (exchActions != null && exchActions.stateChangeRequest() != null && err != null)
                                cctx.kernalContext().state().onStateChangeError(msg.partitionsMessage().getErrorsMap(), exchActions.stateChangeRequest());
                        }

                        onDone(resTopVer, err);
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
     * Moves exchange future to state 'init done' using {@link #initFut}.
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
     *
     */
    private void onAllServersLeft() {
        assert cctx.kernalContext().clientNode() : cctx.localNode();

        List<ClusterNode> empty = Collections.emptyList();

        for (CacheGroupContext grp : cctx.cache().cacheGroups()) {
            List<List<ClusterNode>> affAssignment = new ArrayList<>(grp.affinity().partitions());

            for (int i = 0; i < grp.affinity().partitions(); i++)
                affAssignment.add(empty);

            grp.affinity().idealAssignment(affAssignment);

            grp.affinity().initialize(initialVersion(), affAssignment);
        }
    }

    /**
     * Node left callback, processed from the same thread as {@link #onAffinityChangeMessage}.
     *
     * @param node Left node.
     */
    public void onNodeLeft(final ClusterNode node) {
        if (isDone() || !enterBusy())
            return;

        cctx.mvcc().removeExplicitNodeLocks(node.id(), initialVersion());

        try {
            onDiscoveryEvent(new IgniteRunnable() {
                @Override public void run() {
                    if (isDone() || !enterBusy())
                        return;

                    try {
                        boolean crdChanged = false;
                        boolean allReceived = false;

                        ClusterNode crd0;

                        events().discoveryCache().updateAlives(node);

                        InitNewCoordinatorFuture newCrdFut0;

                        synchronized (mux) {
                            newCrdFut0 = newCrdFut;
                        }

                        if (newCrdFut0 != null)
                            newCrdFut0.onNodeLeft(node.id());

                        synchronized (mux) {
                            if (!srvNodes.remove(node))
                                return;

                            boolean rmvd = remaining.remove(node.id());

                            if (!rmvd) {
                                if (mergedJoinExchMsgs != null && mergedJoinExchMsgs.containsKey(node.id())) {
                                    if (mergedJoinExchMsgs.get(node.id()) == null) {
                                        mergedJoinExchMsgs.remove(node.id());

                                        rmvd = true;
                                    }
                                }
                            }

                            if (node.equals(crd)) {
                                crdChanged = true;

                                crd = !srvNodes.isEmpty() ? srvNodes.get(0) : null;
                            }

                            switch (state) {
                                case DONE:
                                    return;

                                case CRD:
                                    allReceived = rmvd && (remaining.isEmpty() && F.isEmpty(mergedJoinExchMsgs));

                                    break;

                                case SRV:
                                    assert crd != null;

                                    if (crdChanged && crd.isLocal()) {
                                        state = ExchangeLocalState.BECOME_CRD;

                                        newCrdFut = new InitNewCoordinatorFuture(cctx);
                                    }

                                    break;
                            }

                            crd0 = crd;

                            if (crd0 == null) {
                                finishState = new FinishState(null, initialVersion(), null);
                            }
                        }

                        if (crd0 == null) {
                            onAllServersLeft();

                            onDone(initialVersion());

                            return;
                        }

                        if (crd0.isLocal()) {
                            if (stateChangeExchange() && exchangeLocE != null)
                                exchangeGlobalExceptions.put(crd0.id(), exchangeLocE);

                            if (crdChanged) {
                                if (log.isInfoEnabled()) {
                                    log.info("Coordinator failed, node is new coordinator [ver=" + initialVersion() +
                                        ", prev=" + node.id() + ']');
                                }

                                assert newCrdFut != null;

                                cctx.kernalContext().closure().callLocal(new Callable<Void>() {
                                    @Override public Void call() throws Exception {
                                        newCrdFut.init(GridDhtPartitionsExchangeFuture.this);

                                        newCrdFut.listen(new CI1<IgniteInternalFuture>() {
                                            @Override public void apply(IgniteInternalFuture fut) {
                                                if (isDone())
                                                    return;

                                                Lock lock = cctx.io().readLock();

                                                if (lock == null)
                                                    return;

                                                try {
                                                    onBecomeCoordinator((InitNewCoordinatorFuture) fut);
                                                }
                                                finally {
                                                    lock.unlock();
                                                }
                                            }
                                        });

                                        return null;
                                    }
                                }, GridIoPolicy.SYSTEM_POOL);

                                return;
                            }

                            if (allReceived) {
                                cctx.kernalContext().getSystemExecutorService().submit(new Runnable() {
                                    @Override public void run() {
                                        awaitSingleMapUpdates();

                                        onAllReceived(null);
                                    }
                                });
                            }
                        }
                        else {
                            if (crdChanged) {
                                for (Map.Entry<ClusterNode, GridDhtPartitionsFullMessage> m : fullMsgs.entrySet()) {
                                    if (crd0.equals(m.getKey())) {
                                        if (log.isInfoEnabled()) {
                                            log.info("Coordinator changed, process pending full message [" +
                                                "ver=" + initialVersion() +
                                                ", crd=" + node.id() +
                                                ", pendingMsgNode=" + m.getKey() + ']');
                                        }

                                        processFullMessage(true, m.getKey(), m.getValue());

                                        if (isDone())
                                            return;
                                    }
                                }

                                if (log.isInfoEnabled()) {
                                    log.info("Coordinator changed, send partitions to new coordinator [" +
                                        "ver=" + initialVersion() +
                                        ", crd=" + node.id() +
                                        ", newCrd=" + crd0.id() + ']');
                                }

                                final ClusterNode newCrd = crd0;

                                cctx.kernalContext().getSystemExecutorService().submit(new Runnable() {
                                    @Override public void run() {
                                        sendPartitions(newCrd);
                                    }
                                });
                            }
                        }
                    }
                    catch (IgniteCheckedException e) {
                        if (reconnectOnError(e))
                            onDone(new IgniteNeedReconnectException(cctx.localNode(), e));
                        else
                            U.error(log, "Failed to process node left event: " + e, e);
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
     * @param newCrdFut Coordinator initialization future.
     */
    private void onBecomeCoordinator(InitNewCoordinatorFuture newCrdFut) {
        boolean allRcvd = false;

        cctx.exchange().onCoordinatorInitialized();

        if (newCrdFut.restoreState()) {
            GridDhtPartitionsFullMessage fullMsg = newCrdFut.fullMessage();

            assert msgs.isEmpty() : msgs;

            if (fullMsg != null) {
                if (log.isInfoEnabled()) {
                    log.info("New coordinator restored state [ver=" + initialVersion() +
                        ", resVer=" + fullMsg.resultTopologyVersion() + ']');
                }

                synchronized (mux) {
                    state = ExchangeLocalState.DONE;

                    finishState = new FinishState(crd.id(), fullMsg.resultTopologyVersion(), fullMsg);
                }

                fullMsg.exchangeId(exchId);

                processFullMessage(false, null, fullMsg);

                Map<ClusterNode, GridDhtPartitionsSingleMessage> msgs = newCrdFut.messages();

                if (!F.isEmpty(msgs)) {
                    Map<Integer, CacheGroupAffinityMessage> joinedNodeAff = null;

                    for (Map.Entry<ClusterNode, GridDhtPartitionsSingleMessage> e : msgs.entrySet()) {
                        this.msgs.put(e.getKey().id(), e.getValue());

                        GridDhtPartitionsSingleMessage msg = e.getValue();

                        Collection<Integer> affReq = msg.cacheGroupsAffinityRequest();

                        if (!F.isEmpty(affReq)) {
                            joinedNodeAff = CacheGroupAffinityMessage.createAffinityMessages(cctx,
                                fullMsg.resultTopologyVersion(),
                                affReq,
                                joinedNodeAff);
                        }
                    }

                    Map<UUID, GridDhtPartitionsSingleMessage> mergedJoins = newCrdFut.mergedJoinExchangeMessages();

                    if (log.isInfoEnabled()) {
                        log.info("New coordinator sends full message [ver=" + initialVersion() +
                            ", resVer=" + fullMsg.resultTopologyVersion() +
                            ", nodes=" + F.nodeIds(msgs.keySet()) +
                            ", mergedJoins=" + (mergedJoins != null ? mergedJoins.keySet() : null) + ']');
                    }

                    sendAllPartitions(fullMsg, msgs.keySet(), mergedJoins, joinedNodeAff);
                }

                return;
            }
            else {
                if (log.isInfoEnabled())
                    log.info("New coordinator restore state finished [ver=" + initialVersion() + ']');

                for (Map.Entry<ClusterNode, GridDhtPartitionsSingleMessage> e : newCrdFut.messages().entrySet()) {
                    GridDhtPartitionsSingleMessage msg = e.getValue();

                    if (!msg.client()) {
                        msgs.put(e.getKey().id(), e.getValue());

                        if (dynamicCacheStartExchange() && msg.getError() != null)
                            exchangeGlobalExceptions.put(e.getKey().id(), msg.getError());

                        updatePartitionSingleMap(e.getKey().id(), msg);
                    }
                }
            }

            allRcvd = true;

            synchronized (mux) {
                remaining.clear(); // Do not process messages.

                assert crd != null && crd.isLocal();

                state = ExchangeLocalState.CRD;

                assert mergedJoinExchMsgs == null;
            }
        }
        else {
            Set<UUID> remaining0 = null;

            synchronized (mux) {
                assert crd != null && crd.isLocal();

                state = ExchangeLocalState.CRD;

                assert mergedJoinExchMsgs == null;

                if (log.isInfoEnabled()) {
                    log.info("New coordinator initialization finished [ver=" + initialVersion() +
                        ", remaining=" + remaining + ']');
                }

                if (!remaining.isEmpty())
                    remaining0 = new HashSet<>(remaining);
            }

            if (remaining0 != null) {
                // It is possible that some nodes finished exchange with previous coordinator.
                GridDhtPartitionsSingleRequest req = new GridDhtPartitionsSingleRequest(exchId);

                for (UUID nodeId : remaining0) {
                    try {
                        if (!pendingSingleMsgs.containsKey(nodeId)) {
                            if (log.isInfoEnabled()) {
                                log.info("New coordinator sends request [ver=" + initialVersion() +
                                    ", node=" + nodeId + ']');
                            }

                            cctx.io().send(nodeId, req, SYSTEM_POOL);
                        }
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

                for (Map.Entry<UUID, GridDhtPartitionsSingleMessage> m : pendingSingleMsgs.entrySet()) {
                    if (log.isInfoEnabled()) {
                        log.info("New coordinator process pending message [ver=" + initialVersion() +
                            ", node=" + m.getKey() + ']');
                    }

                    processSingleMessage(m.getKey(), m.getValue());
                }
            }
        }

        if (allRcvd) {
            awaitSingleMapUpdates();

            onAllReceived(newCrdFut.messages().keySet());
        }
    }

    /**
     * @param e Exception.
     * @return {@code True} if local node should try reconnect in case of error.
     */
    public boolean reconnectOnError(Throwable e) {
        return (e instanceof IgniteNeedReconnectException
            || X.hasCause(e, IOException.class, IgniteClientDisconnectedCheckedException.class))
            && cctx.discovery().reconnectSupported();
    }

    /** {@inheritDoc} */
    @Override public int compareTo(GridDhtPartitionsExchangeFuture fut) {
        return exchId.compareTo(fut.exchId);
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || o.getClass() != getClass())
            return false;

        GridDhtPartitionsExchangeFuture fut = (GridDhtPartitionsExchangeFuture)o;

        return exchId.equals(fut.exchId);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return exchId.hashCode();
    }

    /** {@inheritDoc} */
    @Override public void addDiagnosticRequest(IgniteDiagnosticPrepareContext diagCtx) {
        if (!isDone()) {
            ClusterNode crd;
            Set<UUID> remaining;

            synchronized (mux) {
                crd = this.crd;
                remaining = new HashSet<>(this.remaining);
            }

            if (crd != null) {
                if (!crd.isLocal()) {
                    diagCtx.exchangeInfo(crd.id(), initialVersion(), "Exchange future waiting for coordinator " +
                        "response [crd=" + crd.id() + ", topVer=" + initialVersion() + ']');
                }
                else if (!remaining.isEmpty()){
                    UUID nodeId = remaining.iterator().next();

                    diagCtx.exchangeInfo(nodeId, initialVersion(), "Exchange future on coordinator waiting for " +
                        "server response [node=" + nodeId + ", topVer=" + initialVersion() + ']');
                }
            }
        }
    }

    /**
     * @return Short information string.
     */
    public String shortInfo() {
        return "GridDhtPartitionsExchangeFuture [topVer=" + initialVersion() +
            ", evt=" + (firstDiscoEvt != null ? IgniteUtils.gridEventName(firstDiscoEvt.type()) : -1) +
            ", evtNode=" + (firstDiscoEvt != null ? firstDiscoEvt.eventNode() : null) +
            ", done=" + isDone() + ']';
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        Set<UUID> remaining;

        synchronized (mux) {
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
        private final long size;

        /** */
        private final Set<UUID> nodes = new HashSet<>();

        /**
         * @param cnt Count.
         * @param firstNode Node ID.
         */
        private CounterWithNodes(long cnt, @Nullable Long size, UUID firstNode) {
            this.cnt = cnt;
            this.size = size != null ? size : 0;

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

    /**
     *
     */
    private static class FinishState {
        /** */
        private final UUID crdId;

        /** */
        private final AffinityTopologyVersion resTopVer;

        /** */
        private final GridDhtPartitionsFullMessage msg;

        /**
         * @param crdId Coordinator node.
         * @param resTopVer Result version.
         * @param msg Result message.
         */
        FinishState(UUID crdId, AffinityTopologyVersion resTopVer, GridDhtPartitionsFullMessage msg) {
            this.crdId = crdId;
            this.resTopVer = resTopVer;
            this.msg = msg;
        }
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

    /**
     *
     */
    private enum ExchangeLocalState {
        /** Local node is coordinator. */
        CRD,

        /** Local node is non-coordinator server. */
        SRV,

        /** Local node is client node. */
        CLIENT,

        /**
         * Previous coordinator failed before exchange finished and
         * local performs initialization to become new coordinator.
         */
        BECOME_CRD,

        /** Exchange finished. */
        DONE,

        /** This exchange was merged with another one. */
        MERGED
    }
}

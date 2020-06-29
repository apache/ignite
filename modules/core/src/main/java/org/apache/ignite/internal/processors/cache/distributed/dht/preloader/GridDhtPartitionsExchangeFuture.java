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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
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
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.cache.expiry.EternalExpiryPolicy;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.failure.FailureType;
import org.apache.ignite.internal.GridKernalContext;
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
import org.apache.ignite.internal.processors.cache.DynamicCacheChangeRequest;
import org.apache.ignite.internal.processors.cache.DynamicCacheDescriptor;
import org.apache.ignite.internal.processors.cache.ExchangeActions;
import org.apache.ignite.internal.processors.cache.ExchangeContext;
import org.apache.ignite.internal.processors.cache.ExchangeDiscoveryEvents;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheMvccCandidate;
import org.apache.ignite.internal.processors.cache.GridCacheProcessor;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.GridCacheUtils;
import org.apache.ignite.internal.processors.cache.StateChangeRequest;
import org.apache.ignite.internal.processors.cache.WalStateAbstractMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTopologyFutureAdapter;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.latch.Latch;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridClientPartitionTopology;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionTopology;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionsStateValidator;
import org.apache.ignite.internal.processors.cache.persistence.DatabaseLifecycleListener;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotDiscoveryMessage;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxEntry;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxKey;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.cluster.BaselineTopology;
import org.apache.ignite.internal.processors.cluster.ChangeGlobalStateFinishMessage;
import org.apache.ignite.internal.processors.cluster.ChangeGlobalStateMessage;
import org.apache.ignite.internal.processors.cluster.DiscoveryDataClusterState;
import org.apache.ignite.internal.processors.cluster.IgniteChangeGlobalStateSupport;
import org.apache.ignite.internal.processors.metric.GridMetricManager;
import org.apache.ignite.internal.processors.service.GridServiceProcessor;
import org.apache.ignite.internal.processors.tracing.NoopSpan;
import org.apache.ignite.internal.processors.tracing.Span;
import org.apache.ignite.internal.processors.tracing.SpanTags;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.TimeBag;
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
import org.apache.ignite.lang.IgnitePredicate;
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
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager.isSnapshotOperation;
import static org.apache.ignite.internal.util.IgniteUtils.doInParallel;
import static org.apache.ignite.internal.util.IgniteUtils.doInParallelUninterruptibly;

/**
 * Future for exchanging partition maps.
 */
@SuppressWarnings({"TypeMayBeWeakened", "unchecked"})
public class GridDhtPartitionsExchangeFuture extends GridDhtTopologyFutureAdapter
    implements Comparable<GridDhtPartitionsExchangeFuture>, CachePartitionExchangeWorkerTask, IgniteDiagnosticAware {
    /** */
    public static final String EXCHANGE_LOG = "org.apache.ignite.internal.exchange.time";

    /** Partition state failed message. */
    public static final String PARTITION_STATE_FAILED_MSG = "Partition states validation has failed for group: %s, msg: %s";

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
    private static final String EXCHANGE_LATCH_ID = "exchange";

    /** */
    private static final String EXCHANGE_FREE_LATCH_ID = "exchange-free";

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
    private volatile ClusterNode crd;

    /** ExchangeFuture id. */
    private final GridDhtPartitionExchangeId exchId;

    /** Cache context. */
    private final GridCacheSharedContext<?, ?> cctx;

    /**
     * Busy lock to prevent activities from accessing exchanger while it's stopping. Stopping uses write lock, so every
     * {@link #enterBusy()} will be failed as false. But regular operation uses read lock acquired multiple times.
     */
    private ReadWriteLock busyLock;

    /** */
    private AtomicBoolean added = new AtomicBoolean(false);

    /** Exchange type. */
    private volatile ExchangeType exchangeType;

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

    /** Reserved max available history for calculation of history supplier on coordinator. */
    private volatile Map<Integer /** Group. */, Map<Integer /** Partition */, Long /** Counter. */>> partHistReserved;

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

    /** Register caches future. Initialized on exchange init. Must be waited on exchange end. */
    private IgniteInternalFuture<?> registerCachesFuture;

    /** Latest (by update sequences) full message with exchangeId == null, need to be processed right after future is done. */
    @GridToStringExclude
    private GridDhtPartitionsFullMessage delayedLatestMsg;

    /** Future for wait all exchange listeners comepleted. */
    @GridToStringExclude
    private final GridFutureAdapter<?> afterLsnrCompleteFut = new GridFutureAdapter<>();

    /** Time bag to measure and store exchange stages times. */
    @GridToStringExclude
    private final TimeBag timeBag;

    /** Start time of exchange. */
    private long startTime = System.currentTimeMillis();

    /** Init time of exchange in milliseconds. */
    private volatile long initTime;

    /** Discovery lag / Clocks discrepancy, calculated on coordinator when all single messages are received. */
    private T2<Long, UUID> discoveryLag;

    /** Partitions scheduled for clearing before rebalancing for this topology version. */
    private Map<Integer, Set<Integer>> clearingPartitions;

    /** Specified only in case of 'cluster is fully rebalanced' state achieved. */
    private volatile RebalancedInfo rebalancedInfo;

    /** Some of owned by affinity partitions were changed state to moving on this exchange. */
    private volatile boolean affinityReassign;

    /** Tracing span. */
    private Span span = NoopSpan.INSTANCE;

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
        if (exchActions != null && exchActions.deactivate())
            this.clusterIsActive = false;

        log = cctx.logger(getClass());
        exchLog = cctx.logger(EXCHANGE_LOG);

        timeBag = new TimeBag(log.isInfoEnabled());

        initFut = new GridFutureAdapter<Boolean>() {
            @Override public IgniteLogger logger() {
                return log;
            }
        };

        if (log.isDebugEnabled())
            log.debug("Creating exchange future [localNode=" + cctx.localNodeId() + ", fut=" + this + ']');
    }

    /**
     * Set span.
     *
     * @param span Span.
     */
    public void span(Span span) {
        this.span = span;
    }

    /**
     * Gets span instance.
     *
     * @return Span.
     */
    public Span span() {
        return span;
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

        if (isDone())
            return result();

        final ExchangeContext exchCtx0;

        synchronized (mux) {
            if (state == ExchangeLocalState.MERGED) {
                assert mergedWith != null;

                exchCtx0 = mergedWith.exchCtx;
            }
            else
                exchCtx0 = exchCtx;
        }

        return exchCtx0.events().topologyVersion();
    }

    /**
     * @return Exchange type or <code>null</code> if not determined yet.
     */
    public @Nullable ExchangeType exchangeType() {
        return exchangeType;
    }

    /**
     * Retreives the node which has WAL history since {@code cntrSince}.
     *
     * @param grpId Cache group ID.
     * @param partId Partition ID.
     * @param cntrSince Partition update counter since history supplying is requested.
     * @return List of IDs of history supplier nodes or empty list if these doesn't exist.
     */
    @Nullable public List<UUID> partitionHistorySupplier(int grpId, int partId, long cntrSince) {
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
     * @param blocked {@code True} if take into account only cache operations blocked PME.
     * @return Gets execution duration for current partition map exchange in milliseconds. {@code 0} If there is no
     * running PME or {@code blocked} was set to {@code true} and current PME don't block cache operations.
     */
    public long currentPMEDuration(boolean blocked) {
        return (isDone() || initTime == 0 || (blocked && !changedAffinity())) ?
            0 : System.currentTimeMillis() - initTime;
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
        this.firstDiscoEvt = discoEvt;
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
     * @param cacheOrGroupName Group or cache name for reset lost partitions.
     * @return {@code True} if reset lost partition exchange.
     */
    public boolean resetLostPartitionFor(String cacheOrGroupName) {
        return exchActions != null && exchActions.cachesToResetLostPartitions().contains(cacheOrGroupName);
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

    /** {@inheritDoc} */
    @Override public boolean changedAffinity() {
        DiscoveryEvent firstDiscoEvt0 = firstDiscoEvt;

        assert firstDiscoEvt0 != null;

        return firstDiscoEvt0.type() == DiscoveryCustomEvent.EVT_DISCOVERY_CUSTOM_EVT
            || !firstDiscoEvt0.eventNode().isClient()
            || firstDiscoEvt0.eventNode().isLocal()
            || ((firstDiscoEvt.type() == EVT_NODE_JOINED) &&
            cctx.cache().hasCachesReceivedFromJoin(firstDiscoEvt.eventNode()));
    }

    /**
     * @return {@code True} if there are caches to start.
     */
    public boolean hasCachesToStart() {
        return exchActions != null && !exchActions.cacheStartRequests().isEmpty();
    }

    /**
     * @return First event discovery event.
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
     * @return {@code true} if entered to busy state. {@code false} for stop node.
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

            if (fut != null) {
                fut.get();

                cctx.exchange().exchangerUpdateHeartbeat();
            }

            cctx.exchange().onCoordinatorInitialized();

            cctx.exchange().exchangerUpdateHeartbeat();
        }
    }

    /**
     * @return Object to collect exchange timings.
     */
    public TimeBag timeBag() {
        return timeBag;
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

        cctx.exchange().exchangerBlockingSectionBegin();

        try {
            U.await(evtLatch);
        }
        finally {
            cctx.exchange().exchangerBlockingSectionEnd();
        }

        assert firstDiscoEvt != null : this;
        assert exchId.nodeId().equals(firstDiscoEvt.eventNode().id()) : this;

        try {
            AffinityTopologyVersion topVer = initialVersion();

            srvNodes = new ArrayList<>(firstEvtDiscoCache.serverNodes());

            remaining.addAll(F.nodeIds(F.view(srvNodes, F.remoteNodes(cctx.localNodeId()))));

            crd = srvNodes.isEmpty() ? null : srvNodes.get(0);

            boolean crdNode = crd != null && crd.isLocal();

            exchCtx = new ExchangeContext(cctx, crdNode, this);

            cctx.exchange().exchangerBlockingSectionBegin();

            assert state == null : state;

            if (crdNode)
                state = ExchangeLocalState.CRD;
            else
                state = cctx.kernalContext().clientNode() ? ExchangeLocalState.CLIENT : ExchangeLocalState.SRV;

            initTime = System.currentTimeMillis();

            if (exchLog.isInfoEnabled()) {
                exchLog.info("Started exchange init [topVer=" + topVer +
                    ", crd=" + crdNode +
                    ", evt=" + IgniteUtils.gridEventName(firstDiscoEvt.type()) +
                    ", evtNode=" + firstDiscoEvt.eventNode().id() +
                    ", customEvt=" + (firstDiscoEvt.type() == EVT_DISCOVERY_CUSTOM_EVT ? ((DiscoveryCustomEvent)firstDiscoEvt).customMessage() : null) +
                    ", allowMerge=" + exchCtx.mergeExchanges() +
                    ", exchangeFreeSwitch=" + exchCtx.exchangeFreeSwitch() + ']');
            }

            span.addLog(() -> "Exchange parameters initialization");

            timeBag.finishGlobalStage("Exchange parameters initialization");

            ExchangeType exchange;

            if (exchCtx.exchangeFreeSwitch()) {
                if (isSnapshotOperation(firstDiscoEvt)) {
                    // Keep if the cluster was rebalanced.
                    if (wasRebalanced())
                        markRebalanced();

                    if (!forceAffReassignment)
                        cctx.affinity().onCustomMessageNoAffinityChange(this, exchActions);

                    exchange = cctx.kernalContext().clientNode() ? ExchangeType.NONE : ExchangeType.ALL;
                }
                else
                    exchange = onExchangeFreeSwitchNodeLeft();

                initCoordinatorCaches(newCrd);
            }
            else if (firstDiscoEvt.type() == EVT_DISCOVERY_CUSTOM_EVT) {
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
                else if (msg instanceof SnapshotDiscoveryMessage)
                    exchange = onCustomMessageNoAffinityChange();
                else if (msg instanceof WalStateAbstractMessage)
                    exchange = onCustomMessageNoAffinityChange();
                else {
                    assert affChangeMsg != null : this;

                    exchange = onAffinityChangeRequest();
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

                        registerCachesFuture = cctx.affinity().initStartedCaches(crdNode, this, receivedCaches);
                    }
                    else
                        registerCachesFuture = initCachesOnLocalJoin();
                }

                initCoordinatorCaches(newCrd);

                if (exchCtx.mergeExchanges()) {
                    if (localJoinExchange()) {
                        if (cctx.kernalContext().clientNode()) {
                            onClientNodeEvent();

                            exchange = ExchangeType.CLIENT;
                        }
                        else {
                            onServerNodeEvent(crdNode);

                            exchange = ExchangeType.ALL;
                        }
                    }
                    else {
                        if (firstDiscoEvt.eventNode().isClient())
                            exchange = onClientNodeEvent();
                        else
                            exchange = cctx.kernalContext().clientNode() ? ExchangeType.CLIENT : ExchangeType.ALL;
                    }

                    if (exchId.isLeft())
                        onLeft();
                }
                else {
                    exchange = firstDiscoEvt.eventNode().isClient() ? onClientNodeEvent() :
                        onServerNodeEvent(crdNode);
                }
            }

            cctx.cache().registrateProxyRestart(resolveCacheRequests(exchActions), afterLsnrCompleteFut);

            exchangeType = exchange;

            for (PartitionsExchangeAware comp : cctx.exchange().exchangeAwareComponents())
                comp.onInitBeforeTopologyLock(this);

            updateTopologies(crdNode);

            timeBag.finishGlobalStage("Determine exchange type");

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

                    synchronized (mux) {
                        state = ExchangeLocalState.DONE;
                    }

                    onDone(topVer);

                    break;
                }

                default:
                    assert false;
            }

            if (cctx.localNode().isClient()) {
                cctx.exchange().exchangerBlockingSectionBegin();

                try {
                    tryToPerformLocalSnapshotOperation();
                }
                finally {
                    cctx.exchange().exchangerBlockingSectionEnd();
                }
            }

            for (PartitionsExchangeAware comp : cctx.exchange().exchangeAwareComponents())
                comp.onInitAfterTopologyLock(this);

            // For pme-free exchanges onInitAfterTopologyLock must be
            // invoked prior to onDoneBeforeTopologyUnlock.
            if (exchange == ExchangeType.ALL && context().exchangeFreeSwitch()) {
                cctx.exchange().exchangerBlockingSectionBegin();

                try {
                    onDone(initialVersion());
                }
                finally {
                    cctx.exchange().exchangerBlockingSectionEnd();
                }
            }

            if (exchLog.isInfoEnabled())
                exchLog.info("Finished exchange init [topVer=" + topVer + ", crd=" + crdNode + ']');
        }
        catch (IgniteInterruptedCheckedException e) {
            assert cctx.kernalContext().isStopping() || cctx.kernalContext().clientDisconnected();

            if (cctx.kernalContext().clientDisconnected())
                onDone(new IgniteCheckedException("Client disconnected"));
            else
                onDone(new IgniteCheckedException("Node stopped"));

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
    private IgniteInternalFuture<?> initCachesOnLocalJoin() throws IgniteCheckedException {
        if (!cctx.kernalContext().clientNode() && !isLocalNodeInBaseline()) {
            cctx.exchange().exchangerBlockingSectionBegin();

            try {
                List<DatabaseLifecycleListener> listeners = cctx.kernalContext().internalSubscriptionProcessor()
                    .getDatabaseListeners();

                for (DatabaseLifecycleListener lsnr : listeners)
                    lsnr.onBaselineChange();
            }
            finally {
                cctx.exchange().exchangerBlockingSectionEnd();
            }

            timeBag.finishGlobalStage("Baseline change callback");
        }

        cctx.exchange().exchangerBlockingSectionBegin();

        try {
            cctx.activate();
        }
        finally {
            cctx.exchange().exchangerBlockingSectionEnd();
        }

        timeBag.finishGlobalStage("Components activation");

        IgniteInternalFuture<?> cachesRegistrationFut = cctx.cache().startCachesOnLocalJoin(initialVersion(),
            exchActions == null ? null : exchActions.localJoinContext());

        if (!cctx.kernalContext().clientNode())
            cctx.cache().shutdownNotFinishedRecoveryCaches();

        ensureClientCachesStarted();

        return cachesRegistrationFut;
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
     * @return {@code true} if local node is in baseline and {@code false} otherwise.
     */
    private boolean isLocalNodeInBaseline() {
        BaselineTopology topology = cctx.discovery().discoCache().state().baselineTopology();

        return topology != null && topology.consistentIds().contains(cctx.localNode().consistentId());
    }

    /**
     * @return {@code True} if event node is in baseline and failed and {@code false} otherwise.
     */
    public boolean isBaselineNodeFailed() {
        BaselineTopology top = firstEvtDiscoCache.state().baselineTopology();

        return (firstDiscoEvt.type() == EVT_NODE_LEFT || firstDiscoEvt.type() == EVT_NODE_FAILED) &&
            !firstDiscoEvt.eventNode().isClient() &&
            top != null &&
            top.consistentIds().contains(firstDiscoEvt.eventNode().consistentId());
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

                    cctx.exchange().exchangerUpdateHeartbeat();
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
                    cctx.exchange().exchangerBlockingSectionBegin();

                    try {
                        top.update(null,
                            clientTop.partitionMap(true),
                            clientTop.fullUpdateCounters(),
                            Collections.emptySet(),
                            null,
                            null,
                            null,
                            clientTop.lostPartitions());
                    }
                    finally {
                        cctx.exchange().exchangerBlockingSectionEnd();
                    }
                }
            }

            cctx.exchange().exchangerBlockingSectionBegin();

            try {
                top.updateTopologyVersion(
                    this,
                    events().discoveryCache(),
                    updSeq,
                    cacheGroupStopping(grp.groupId()));
            }
            finally {
                cctx.exchange().exchangerBlockingSectionEnd();
            }
        }

        cctx.exchange().exchangerBlockingSectionBegin();

        try {
            for (GridClientPartitionTopology top : cctx.exchange().clientTopologies()) {
                top.updateTopologyVersion(this,
                    events().discoveryCache(),
                    -1,
                    cacheGroupStopping(top.groupId()));
            }
        }
        finally {
            cctx.exchange().exchangerBlockingSectionEnd();
        }
    }

    /**
     * @param crd Coordinator flag.
     * @return Exchange type.
     */
    private ExchangeType onClusterStateChangeRequest(boolean crd) {
        assert exchActions != null && !exchActions.empty() : this;

        StateChangeRequest req = exchActions.stateChangeRequest();

        assert req != null : exchActions;

        GridKernalContext kctx = cctx.kernalContext();

        DiscoveryDataClusterState state = kctx.state().clusterState();

        if (state.transitionError() != null)
            exchangeLocE = state.transitionError();

        if (req.activeChanged()) {
            if (req.state().active()) {
                if (log.isInfoEnabled()) {
                    log.info("Start activation process [nodeId=" + cctx.localNodeId() +
                        ", client=" + kctx.clientNode() +
                        ", topVer=" + initialVersion() + "]. New state: " + req.state());
                }

                try {
                    cctx.exchange().exchangerBlockingSectionBegin();

                    try {
                        cctx.activate();
                    }
                    finally {
                        cctx.exchange().exchangerBlockingSectionEnd();
                    }

                    assert registerCachesFuture == null : "No caches registration should be scheduled before new caches have started.";

                    cctx.exchange().exchangerBlockingSectionBegin();

                    try {
                        registerCachesFuture = cctx.affinity().onCacheChangeRequest(this, crd, exchActions);

                        if (!kctx.clientNode())
                            cctx.cache().shutdownNotFinishedRecoveryCaches();
                    }
                    finally {
                        cctx.exchange().exchangerBlockingSectionEnd();
                    }

                    if (log.isInfoEnabled()) {
                        log.info("Successfully activated caches [nodeId=" + cctx.localNodeId() +
                            ", client=" + kctx.clientNode() +
                            ", topVer=" + initialVersion() + ", newState=" + req.state() + "]");
                    }
                }
                catch (Exception e) {
                    U.error(log, "Failed to activate node components [nodeId=" + cctx.localNodeId() +
                        ", client=" + kctx.clientNode() +
                        ", topVer=" + initialVersion() + ", newState=" + req.state() + "]", e);

                    exchangeLocE = e;

                    if (crd) {
                        cctx.exchange().exchangerBlockingSectionBegin();

                        try {
                            synchronized (mux) {
                                exchangeGlobalExceptions.put(cctx.localNodeId(), e);
                            }
                        }
                        finally {
                            cctx.exchange().exchangerBlockingSectionEnd();
                        }
                    }
                }
            }
            else {
                if (log.isInfoEnabled()) {
                    log.info("Start deactivation process [nodeId=" + cctx.localNodeId() +
                        ", client=" + kctx.clientNode() +
                        ", topVer=" + initialVersion() + "]");
                }

                cctx.exchange().exchangerBlockingSectionBegin();

                try {
                    kctx.dataStructures().onDeActivate(kctx);

                    if (cctx.kernalContext().service() instanceof GridServiceProcessor)
                        ((GridServiceProcessor)kctx.service()).onDeActivate(cctx.kernalContext());

                    assert registerCachesFuture == null : "No caches registration should be scheduled before new caches have started.";

                    registerCachesFuture = cctx.affinity().onCacheChangeRequest(this, crd, exchActions);

                    kctx.encryption().onDeActivate(kctx);

                    ((IgniteChangeGlobalStateSupport)kctx.distributedMetastorage()).onDeActivate(kctx);

                    if (log.isInfoEnabled()) {
                        log.info("Successfully deactivated data structures, services and caches [" +
                            "nodeId=" + cctx.localNodeId() +
                            ", client=" + kctx.clientNode() +
                            ", topVer=" + initialVersion() + "]");
                    }
                }
                catch (Exception e) {
                    U.error(log, "Failed to deactivate node components [nodeId=" + cctx.localNodeId() +
                        ", client=" + kctx.clientNode() +
                        ", topVer=" + initialVersion() + "]", e);

                    exchangeLocE = e;
                }
                finally {
                    cctx.exchange().exchangerBlockingSectionEnd();
                }
            }
        }
        else if (req.state().active()) {
            cctx.exchange().exchangerBlockingSectionBegin();

            // TODO: BLT changes on inactive cluster can't be handled easily because persistent storage hasn't been initialized yet.
            try {
                if (!forceAffReassignment) {
                    // possible only if cluster contains nodes without forceAffReassignment mode
                    assert firstEventCache().minimumNodeVersion()
                        .compareToIgnoreTimestamp(FORCE_AFF_REASSIGNMENT_SINCE) < 0
                        : firstEventCache().minimumNodeVersion();

                    cctx.affinity().onBaselineTopologyChanged(this, crd);
                }

                if (CU.isPersistenceEnabled(kctx.config()) && !kctx.clientNode())
                    kctx.state().onBaselineTopologyChanged(req.baselineTopology(),
                        req.prevBaselineTopologyHistoryItem());
            }
            catch (Exception e) {
                U.error(log, "Failed to change baseline topology [nodeId=" + cctx.localNodeId() +
                    ", client=" + kctx.clientNode() +
                    ", topVer=" + initialVersion() + "]", e);

                exchangeLocE = e;
            }
            finally {
                cctx.exchange().exchangerBlockingSectionEnd();
            }
        }

        return kctx.clientNode() ? ExchangeType.CLIENT : ExchangeType.ALL;
    }

    /**
     * @param crd Coordinator flag.
     * @return Exchange type.
     * @throws IgniteCheckedException If failed.
     */
    private ExchangeType onCacheChangeRequest(boolean crd) throws IgniteCheckedException {
        assert exchActions != null && !exchActions.empty() : this;

        assert !exchActions.clientOnlyExchange() : exchActions;

        cctx.exchange().exchangerBlockingSectionBegin();

        try {
            assert registerCachesFuture == null : "No caches registration should be scheduled before new caches have started.";

            registerCachesFuture = cctx.affinity().onCacheChangeRequest(this, crd, exchActions);
        }
        catch (Exception e) {
            if (reconnectOnError(e) || !isRollbackSupported())
                // This exception will be handled by init() method.
                throw e;

            U.error(log, "Failed to initialize cache(s) (will try to rollback) [exchId=" + exchId +
                ", caches=" + exchActions.cacheGroupsToStart() + ']', e);

            exchangeLocE = new IgniteCheckedException(
                "Failed to initialize exchange locally [locNodeId=" + cctx.localNodeId() + "]", e);

            exchangeGlobalExceptions.put(cctx.localNodeId(), exchangeLocE);
        }
        finally {
            cctx.exchange().exchangerBlockingSectionEnd();
        }

        return cctx.kernalContext().clientNode() ? ExchangeType.CLIENT : ExchangeType.ALL;
    }

    /**
     * @return Exchange type.
     */
    private ExchangeType onCustomMessageNoAffinityChange() {
        if (!forceAffReassignment)
            cctx.affinity().onCustomMessageNoAffinityChange(this, exchActions);

        return cctx.kernalContext().clientNode() ? ExchangeType.CLIENT : ExchangeType.ALL;
    }

    /**
     * @return Exchange type.
     */
    private ExchangeType onAffinityChangeRequest() {
        assert affChangeMsg != null : this;

        cctx.affinity().onChangeAffinityMessage(this, affChangeMsg);

        if (cctx.kernalContext().clientNode())
            return ExchangeType.CLIENT;

        return ExchangeType.ALL;
    }

    /**
     * @return Exchange type.
     * @throws IgniteCheckedException If failed.
     */
    private ExchangeType onClientNodeEvent() throws IgniteCheckedException {
        assert firstDiscoEvt.eventNode().isClient() : this;

        if (firstDiscoEvt.type() == EVT_NODE_LEFT || firstDiscoEvt.type() == EVT_NODE_FAILED) {
            onLeft();

            assert !firstDiscoEvt.eventNode().isLocal() : firstDiscoEvt;
        }
        else
            assert firstDiscoEvt.type() == EVT_NODE_JOINED || firstDiscoEvt.type() == EVT_DISCOVERY_CUSTOM_EVT : firstDiscoEvt;

        cctx.affinity().onClientEvent(this);

        if (firstDiscoEvt.eventNode().isLocal())
            return ExchangeType.CLIENT;
        else {
            if (wasRebalanced())
                keepRebalanced();

            return ExchangeType.NONE;
        }
    }

    /**
     * @param crd Coordinator flag.
     * @return Exchange type.
     * @throws IgniteCheckedException If failed.
     */
    private ExchangeType onServerNodeEvent(boolean crd) throws IgniteCheckedException {
        assert !firstDiscoEvt.eventNode().isClient() : this;

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
     * @return Exchange type.
     */
    private ExchangeType onExchangeFreeSwitchNodeLeft() {
        assert !firstDiscoEvt.eventNode().isClient() : this;

        assert firstDiscoEvt.type() == EVT_NODE_LEFT || firstDiscoEvt.type() == EVT_NODE_FAILED;

        assert exchCtx.exchangeFreeSwitch();

        keepRebalanced(); // Still rebalanced.

        onLeft();

        exchCtx.events().warnNoAffinityNodes(cctx);

        cctx.affinity().onExchangeFreeSwitch(this);

        return cctx.kernalContext().clientNode() ? ExchangeType.NONE : ExchangeType.ALL;
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    private void clientOnlyExchange() throws IgniteCheckedException {
        if (crd != null) {
            assert !crd.isLocal() : crd;
            assert !exchCtx.exchangeFreeSwitch() : this;

            cctx.exchange().exchangerBlockingSectionBegin();

            try {
                if (!centralizedAff)
                    sendLocalPartitions(crd);

                initDone();
            }
            finally {
                cctx.exchange().exchangerBlockingSectionEnd();
            }
        }
        else {
            if (centralizedAff) { // Last server node failed.
                for (CacheGroupContext grp : cctx.cache().cacheGroups()) {
                    GridAffinityAssignmentCache aff = grp.affinity();

                    aff.initialize(initialVersion(), aff.idealAssignmentRaw());

                    cctx.exchange().exchangerUpdateHeartbeat();
                }
            }
            else
                onAllServersLeft();

            cctx.exchange().exchangerBlockingSectionBegin();

            try {
                onDone(initialVersion());
            }
            finally {
                cctx.exchange().exchangerBlockingSectionEnd();
            }
        }
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

            cctx.exchange().exchangerBlockingSectionBegin();

            try {
                grp.preloader().onTopologyChanged(this);
            }
            finally {
                cctx.exchange().exchangerBlockingSectionEnd();
            }
        }

        timeBag.finishGlobalStage("Preloading notification");

        // Skipping wait on local join is available when all cluster nodes have the same protocol.
        boolean skipWaitOnLocalJoin = localJoinExchange()
            && cctx.exchange().latch().canSkipJoiningNodes(initialVersion());

        if (context().exchangeFreeSwitch() && isBaselineNodeFailed()) {
            // Currently MVCC does not support operations on partially switched cluster.
            if (cctx.kernalContext().coordinators().mvccEnabled())
                waitPartitionRelease(EXCHANGE_FREE_LATCH_ID, true, false, null);
            else {
                boolean partitionedRecoveryRequired = rebalancedInfo.primaryNodes.contains(firstDiscoEvt.eventNode());

                IgnitePredicate<IgniteInternalTx> replicatedOnly = tx -> {
                    Collection<IgniteTxEntry> entries = tx.writeEntries();
                    for (IgniteTxEntry entry : entries)
                        if (cctx.cacheContext(entry.cacheId()).isReplicated())
                            return true;

                    // Checks only affected nodes contain replicated-free txs with failed primaries.
                    assert partitionedRecoveryRequired;

                    return false;
                };

                // Assuming that replicated transactions are absent, non-affected nodes will wait only this short sync.
                waitPartitionRelease(EXCHANGE_FREE_LATCH_ID + "-replicated", true, false, replicatedOnly);

                String partitionedLatchId = EXCHANGE_FREE_LATCH_ID + "-partitioned";

                if (partitionedRecoveryRequired)
                    // This node contain backup partitions for failed partitioned caches primaries. Waiting for recovery.
                    waitPartitionRelease(partitionedLatchId, true, false, null);
                else {
                    // This node contain no backup partitions for failed partitioned caches primaries. Recovery is not needed.
                    Latch releaseLatch = cctx.exchange().latch().getOrCreate(partitionedLatchId, initialVersion());

                    releaseLatch.countDown(); // Await-free confirmation.
                }
            }
        }
        else if (!skipWaitOnLocalJoin) { // Skip partition release if node has locally joined (it doesn't have any updates to be finished).
            boolean distributed = true;

            // Do not perform distributed partition release in case of cluster activation.
            if (activateCluster())
                distributed = false;

            // On first phase we wait for finishing all local tx updates, atomic updates and lock releases on all nodes.
            waitPartitionRelease(EXCHANGE_LATCH_ID, distributed, true, null);

            // Second phase is needed to wait for finishing all tx updates from primary to backup nodes remaining after first phase.
            if (distributed)
                waitPartitionRelease(EXCHANGE_LATCH_ID, false, false, null);
        }
        else {
            if (log.isInfoEnabled())
                log.info("Skipped waiting for partitions release future (local node is joining) " +
                    "[topVer=" + initialVersion() + "]");
        }

        boolean topChanged = firstDiscoEvt.type() != EVT_DISCOVERY_CUSTOM_EVT || affChangeMsg != null;

        for (GridCacheContext cacheCtx : cctx.cacheContexts()) {
            if (cacheCtx.isLocal() || cacheStopping(cacheCtx.cacheId()))
                continue;

            if (topChanged) {
                // Partition release future is done so we can flush the write-behind store.
                cctx.exchange().exchangerBlockingSectionBegin();

                try {
                    cacheCtx.store().forceFlush();
                }
                finally {
                    cctx.exchange().exchangerBlockingSectionEnd();
                }
            }
        }

        cctx.exchange().exchangerBlockingSectionBegin();

        try {
            /* It is necessary to run database callback before all topology callbacks.
               In case of persistent store is enabled we first restore partitions presented on disk.
               We need to guarantee that there are no partition state changes logged to WAL before this callback
               to make sure that we correctly restored last actual states. */

            cctx.database().beforeExchange(this);
        }
        finally {
            cctx.exchange().exchangerBlockingSectionEnd();
        }

        // Pre-create missing partitions using current affinity.
        if (!exchCtx.mergeExchanges() && !exchCtx.exchangeFreeSwitch()) {
            for (CacheGroupContext grp : cctx.cache().cacheGroups()) {
                if (grp.isLocal() || cacheGroupStopping(grp.groupId()))
                    continue;

                // It is possible affinity is not initialized yet if node joins to cluster.
                if (grp.affinity().lastVersion().topologyVersion() > 0) {
                    cctx.exchange().exchangerBlockingSectionBegin();

                    try {
                        grp.topology().beforeExchange(this, !centralizedAff && !forceAffReassignment, false);
                    }
                    finally {
                        cctx.exchange().exchangerBlockingSectionEnd();
                    }
                }
            }
        }

        // After all partitions have been restored and pre-created it's safe to make first checkpoint.
        if (localJoinExchange() || activateCluster()) {
            cctx.exchange().exchangerBlockingSectionBegin();

            try {
                cctx.database().onStateRestored(initialVersion());
            }
            finally {
                cctx.exchange().exchangerBlockingSectionEnd();
            }
        }

        timeBag.finishGlobalStage("After states restored callback");

        cctx.exchange().exchangerBlockingSectionBegin();

        try {
            cctx.database().releaseHistoryForPreloading();

            // To correctly rebalance when persistence is enabled, it is necessary to reserve history within exchange.
            partHistReserved = cctx.database().reserveHistoryForExchange();
        }
        finally {
            cctx.exchange().exchangerBlockingSectionEnd();
        }

        clearingPartitions = new HashMap();

        timeBag.finishGlobalStage("WAL history reservation");

        changeWalModeIfNeeded();

        if (events().hasServerLeft())
            finalizePartitionCounters();

        cctx.exchange().exchangerBlockingSectionBegin();

        try {
            if (context().exchangeFreeSwitch()) {
                // Update local maps, see CachePartitionLossWithRestartsTest.
                doInParallel(
                    U.availableThreadCount(cctx.kernalContext(), GridIoPolicy.SYSTEM_POOL, 2),
                    cctx.kernalContext().getSystemExecutorService(),
                    cctx.affinity().cacheGroups().values(),
                    desc -> {
                        if (desc.config().getCacheMode() == CacheMode.LOCAL)
                            return null;

                        CacheGroupContext grp = cctx.cache().cacheGroup(desc.groupId());

                        GridDhtPartitionTopology top = grp != null ? grp.topology() :
                            cctx.exchange().clientTopology(desc.groupId(), events().discoveryCache());

                        top.beforeExchange(this, true, false); // Not expecting new moving partitions.

                        return null;
                    });
            }
            else {
                if (crd.isLocal()) {
                    if (remaining.isEmpty()) {
                        initFut.onDone(true);

                        onAllReceived(null);
                    }
                }
                else
                    sendPartitions(crd);

                initDone();
            }
        }
        finally {
            cctx.exchange().exchangerBlockingSectionEnd();
        }
    }

    /**
     * Try to start local snapshot operation if it is needed by discovery event
     */
    private void tryToPerformLocalSnapshotOperation() {
        try {
            long start = System.nanoTime();

            IgniteInternalFuture fut = cctx.snapshot().tryStartLocalSnapshotOperation(firstDiscoEvt, exchId.topologyVersion());

            if (fut != null) {
                fut.get();

                long end = System.nanoTime();

                if (log.isInfoEnabled())
                    log.info("Snapshot initialization completed [topVer=" + exchangeId().topologyVersion() +
                        ", time=" + U.nanosToMillis(end - start) + "ms]");
            }
        }
        catch (IgniteException | IgniteCheckedException e) {
            U.error(log, "Error while starting snapshot operation", e);
        }
    }

    /**
     * Change WAL mode if needed.
     */
    private void changeWalModeIfNeeded() {
        WalStateAbstractMessage msg = firstWalMessage();

        if (msg != null) {
            cctx.exchange().exchangerBlockingSectionBegin();

            try {
                cctx.walState().onProposeExchange(msg.exchangeMessage());
            }
            finally {
                cctx.exchange().exchangerBlockingSectionEnd();
            }
        }
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
     * Also, this method can be used to wait for tx recovery only in case of PME-free switch.
     *
     * @param latchId Distributed latch Id.
     * @param distributed If {@code true} then node should wait for partition release completion on all other nodes.
     * @param doRollback If {@code true} tries to rollback transactions which lock partitions. Avoids unnecessary calls
     *      of {@link org.apache.ignite.internal.processors.cache.transactions.IgniteTxManager#rollbackOnTopologyChange}
     * @param filter Recovery filter.
     *
     * @throws IgniteCheckedException If failed.
     */
    private void waitPartitionRelease(
        String latchId,
        boolean distributed,
        boolean doRollback,
        IgnitePredicate<IgniteInternalTx> filter) throws IgniteCheckedException {
        assert context().exchangeFreeSwitch() || filter == null;

        Latch releaseLatch = null;

        IgniteInternalFuture<?> partReleaseFut;

        cctx.exchange().exchangerBlockingSectionBegin();

        try {
            // Wait for other nodes only on first phase.
            if (distributed)
                releaseLatch = cctx.exchange().latch().getOrCreate(latchId, initialVersion());

            partReleaseFut = context().exchangeFreeSwitch() && isBaselineNodeFailed() ?
                cctx.partitionRecoveryFuture(initialVersion(), firstDiscoEvt.eventNode(), filter) :
                cctx.partitionReleaseFuture(initialVersion());

            // Assign to class variable so it will be included into toString() method.
            this.partReleaseFut = partReleaseFut;
        }
        finally {
            cctx.exchange().exchangerBlockingSectionEnd();
        }

        if (log.isTraceEnabled())
            log.trace("Before waiting for partition release future: " + this);

        int dumpCnt = 0;

        long nextDumpTime = 0;

        IgniteConfiguration cfg = cctx.gridConfig();

        long waitStartNanos = System.nanoTime();

        long waitTimeout = 2 * cfg.getNetworkTimeout();

        boolean txRolledBack = !doRollback;

        while (true) {
            // Read txTimeoutOnPME from configuration after every iteration.
            long curTimeout = cfg.getTransactionConfiguration().getTxTimeoutOnPartitionMapExchange();

            cctx.exchange().exchangerBlockingSectionBegin();

            try {
                // This avoids unnecessary waiting for rollback.
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

                long passedMillis = U.millisSinceNanos(waitStartNanos);

                if (!txRolledBack && curTimeout > 0 && passedMillis >= curTimeout) {
                    txRolledBack = true;

                    cctx.tm().rollbackOnTopologyChange(initialVersion());
                }
            }
            catch (IgniteCheckedException e) {
                U.warn(log, "Unable to await partitions release future", e);

                throw e;
            }
            finally {
                cctx.exchange().exchangerBlockingSectionEnd();
            }
        }

        long waitEndNanos = System.nanoTime();

        if (log.isInfoEnabled()) {
            long waitTime = U.nanosToMillis(waitEndNanos - waitStartNanos);

            String futInfo = RELEASE_FUTURE_DUMP_THRESHOLD > 0 && waitTime > RELEASE_FUTURE_DUMP_THRESHOLD ?
                partReleaseFut.toString() : "NA";

            String mode = distributed ? "DISTRIBUTED" : "LOCAL";

            if (log.isInfoEnabled())
                log.info("Finished waiting for partition release future [topVer=" + exchangeId().topologyVersion() +
                    ", waitTime=" + waitTime + "ms, futInfo=" + futInfo + ", mode=" + mode + "]");
        }

        if (!context().exchangeFreeSwitch()) {
            IgniteInternalFuture<?> locksFut = cctx.mvcc().finishLocks(exchId.topologyVersion());

            nextDumpTime = 0;
            dumpCnt = 0;

            while (true) {
                cctx.exchange().exchangerBlockingSectionBegin();

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
                finally {
                    cctx.exchange().exchangerBlockingSectionEnd();
                }
            }
        }

        timeBag.finishGlobalStage("Wait partitions release [latch=" + latchId + "]");

        if (releaseLatch == null) {
            assert !distributed : "Partitions release latch must be initialized in distributed mode.";

            return;
        }

        releaseLatch.countDown();

        // For compatibility with old version where joining nodes are not waiting for latch.
        if (localJoinExchange() && !cctx.exchange().latch().canSkipJoiningNodes(initialVersion()))
            return;

        try {
            String troubleshootingHint;

            if (crd.isLocal())
                troubleshootingHint = "Some nodes have not sent acknowledgement for latch completion. "
                    + "It's possible due to unfinishined atomic updates, transactions "
                    + "or not released explicit locks on that nodes. "
                    + "Please check logs for errors on nodes with ids reported in latch `pendingAcks` collection";
            else
                troubleshootingHint = "For more details please check coordinator node logs [crdNode=" + crd.toString() + "]";

            while (true) {
                try {
                    cctx.exchange().exchangerBlockingSectionBegin();

                    try {
                        releaseLatch.await(waitTimeout, TimeUnit.MILLISECONDS);
                    }
                    finally {
                        cctx.exchange().exchangerBlockingSectionEnd();
                    }

                    if (log.isInfoEnabled())
                        log.info("Finished waiting for partitions release latch: " + releaseLatch);

                    break;
                }
                catch (IgniteFutureTimeoutCheckedException ignored) {
                    U.warn(log, "Unable to await partitions release latch within timeout. "
                        + troubleshootingHint + " [latch=" + releaseLatch + "]");

                    // Try to resend ack.
                    releaseLatch.countDown();
                }
            }
        }
        catch (IgniteCheckedException e) {
            U.warn(log, "Stop waiting for partitions release latch: " + e.getMessage());
        }

        timeBag.finishGlobalStage("Wait partitions release latch [latch=" + latchId + "]");
    }

    /**
     *
     */
    private void onLeft() {
        for (CacheGroupContext grp : cctx.cache().cacheGroups()) {
            if (grp.isLocal())
                continue;

            grp.preloader().pause();

            try {
                grp.unwindUndeploys();
            }
            finally {
                grp.preloader().resume();
            }

            cctx.exchange().exchangerUpdateHeartbeat();
        }
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

        msg.exchangeStartTime(startTime);

        if (log.isTraceEnabled())
            log.trace("Sending local partitions [nodeId=" + node.id() + ", exchId=" + exchId + ", msg=" + msg + ']');

        while (true) {
            try {
                cctx.io().send(node, msg, SYSTEM_POOL);
            }
            catch (ClusterTopologyCheckedException ignored) {
                if (log.isDebugEnabled()) {
                    log.debug(
                        "Failed to send local partitions on exchange [nodeId=" + node.id() + ", exchId=" + exchId + ']'
                    );
                }

                if (cctx.discovery().alive(node.id())) {
                    U.sleep(cctx.gridConfig().getNetworkSendRetryDelay());

                    continue;
                }
            }

            return;
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
     * @param fullMsg Message to send.
     * @param nodes Target Nodes.
     * @param mergedJoinExchMsgs Messages received from merged 'join node' exchanges.
     * @param affinityForJoinedNodes Affinity if was requested by some nodes.
     */
    private void sendAllPartitions(
        GridDhtPartitionsFullMessage fullMsg,
        Collection<ClusterNode> nodes,
        Map<UUID, GridDhtPartitionsSingleMessage> mergedJoinExchMsgs,
        Map<Integer, CacheGroupAffinityMessage> affinityForJoinedNodes
    ) {
        assert !nodes.contains(cctx.localNode());

        if (log.isTraceEnabled()) {
            log.trace("Sending full partition map [nodeIds=" + F.viewReadOnly(nodes, F.node2id()) +
                ", exchId=" + exchId + ", msg=" + fullMsg + ']');
        }

        // Find any single message with affinity request. This request exists only for newly joined nodes.
        Optional<GridDhtPartitionsSingleMessage> singleMsgWithAffinityReq = nodes.stream()
            .flatMap(node -> Optional.ofNullable(msgs.get(node.id()))
                .filter(singleMsg -> singleMsg.cacheGroupsAffinityRequest() != null)
                .map(Stream::of)
                .orElse(Stream.empty()))
            .findAny();

        // Prepare full message for newly joined nodes with affinity request.
        final GridDhtPartitionsFullMessage fullMsgWithAffinity = singleMsgWithAffinityReq
            .filter(singleMessage -> affinityForJoinedNodes != null)
            .map(singleMessage -> fullMsg.copy().joinedNodeAffinity(affinityForJoinedNodes))
            .orElse(null);

        // Prepare and send full messages for given nodes.
        nodes.stream()
            .map(node -> {
                // No joined nodes, just send a regular full message.
                if (fullMsgWithAffinity == null)
                    return new T2<>(node, fullMsg);

                return new T2<>(
                    node,
                    // If single message contains affinity request, use special full message for such single messages.
                    Optional.ofNullable(msgs.get(node.id()))
                        .filter(singleMsg -> singleMsg.cacheGroupsAffinityRequest() != null)
                        .map(singleMsg -> fullMsgWithAffinity)
                        .orElse(fullMsg)
                );
            })
            .map(nodeAndMsg -> {
                ClusterNode node = nodeAndMsg.get1();
                GridDhtPartitionsFullMessage fullMsgToSend = nodeAndMsg.get2();

                // If exchange has merged, use merged version of exchange id.
                GridDhtPartitionExchangeId sndExchId = mergedJoinExchMsgs != null
                    ? Optional.ofNullable(mergedJoinExchMsgs.get(node.id()))
                    .map(GridDhtPartitionsAbstractMessage::exchangeId)
                    .orElse(exchangeId())
                    : exchangeId();

                if (sndExchId != null && !sndExchId.equals(exchangeId())) {
                    GridDhtPartitionsFullMessage fullMsgWithUpdatedExchangeId = fullMsgToSend.copy();

                    fullMsgWithUpdatedExchangeId.exchangeId(sndExchId);

                    return new T2<>(node, fullMsgWithUpdatedExchangeId);
                }

                return new T2<>(node, fullMsgToSend);
            })
            .forEach(nodeAndMsg -> {
                ClusterNode node = nodeAndMsg.get1();
                GridDhtPartitionsFullMessage fullMsgToSend = nodeAndMsg.get2();

                try {
                    cctx.io().send(node, fullMsgToSend, SYSTEM_POOL);
                }
                catch (ClusterTopologyCheckedException e) {
                    if (log.isDebugEnabled())
                        log.debug("Failed to send partitions, node failed: " + node);
                }
                catch (IgniteCheckedException e) {
                    U.error(log, "Failed to send partitions [node=" + node + ']', e);
                }
            });
    }

    /**
     * @param oldestNode Oldest node. Target node to send message to.
     */
    private void sendPartitions(ClusterNode oldestNode) {
        assert !exchCtx.exchangeFreeSwitch() : this;

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
    public void finishMerged(AffinityTopologyVersion resVer, GridDhtPartitionsExchangeFuture exchFut) {
        synchronized (mux) {
            if (state == null) {
                state = ExchangeLocalState.MERGED;

                mergedWith = exchFut;
            }
        }

        done.set(true);

        super.onDone(resVer, null);
    }

    /**
     * @return {@code True} if future was merged.
     */
    public boolean isMerged() {
        synchronized (mux) {
            return state == ExchangeLocalState.MERGED;
        }
    }

    /**
     * Make a log message that contains given exchange timings.
     *
     * @param header Header of log message.
     * @param timings Exchange stages timings.
     * @return Log message with exchange timings and exchange version.
     */
    private String exchangeTimingsLogMessage(String header, List<String> timings) {
        StringBuilder timingsToLog = new StringBuilder();

        timingsToLog.append(header).append(" [");
        timingsToLog.append("startVer=").append(initialVersion());
        timingsToLog.append(", resVer=").append(topologyVersion());

        for (String stageTiming : timings)
            timingsToLog.append(", ").append(stageTiming);

        timingsToLog.append(']');

        return timingsToLog.toString();
    }

    /** {@inheritDoc} */
    @Override public boolean onDone(@Nullable AffinityTopologyVersion res, @Nullable Throwable err) {
        assert res != null || err != null : "TopVer=" + res + ", err=" + err;

        if (isDone() || !done.compareAndSet(false, true))
            return false;

        if (log.isInfoEnabled()) {
            log.info("Finish exchange future [startVer=" + initialVersion() +
                ", resVer=" + res +
                ", err=" + err +
                ", rebalanced=" + rebalanced() +
                ", wasRebalanced=" + wasRebalanced() + ']');
        }

        assert res != null || err != null;

        if (res != null) {
            span.addTag(SpanTags.tag(SpanTags.RESULT, SpanTags.TOPOLOGY_VERSION, SpanTags.MAJOR),
                () -> String.valueOf(res.topologyVersion()));
            span.addTag(SpanTags.tag(SpanTags.RESULT, SpanTags.TOPOLOGY_VERSION, SpanTags.MINOR),
                () -> String.valueOf(res.minorTopologyVersion()));
        }

        if (err != null) {
            Throwable errf = err;

            span.addTag(SpanTags.ERROR, errf::toString);
        }

        try {
            waitUntilNewCachesAreRegistered();

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

                    Collection<CacheGroupContext> grpToRefresh = U.newHashSet(cctx.cache().cacheGroups().size());

                    for (CacheGroupContext grp : cctx.cache().cacheGroups()) {
                        if (grp.isLocal())
                            continue;

                        try {
                            if (grp.topology().initPartitionsWhenAffinityReady(res, this))
                                grpToRefresh.add(grp);
                        }
                        catch (IgniteInterruptedCheckedException e) {
                            U.error(log, "Failed to initialize partitions.", e);
                        }

                    }

                    if (!grpToRefresh.isEmpty()) {
                        if (log.isDebugEnabled())
                            log.debug("Refresh partitions due to partitions initialized when affinity ready [" +
                                grpToRefresh.stream().map(CacheGroupContext::name).collect(Collectors.toList()) + ']');

                        cctx.exchange().refreshPartitions(grpToRefresh);
                    }
                }

                for (GridCacheContext cacheCtx : cctx.cacheContexts()) {
                    GridCacheContext drCacheCtx = cacheCtx.isNear() ? cacheCtx.near().dht().context() : cacheCtx;

                    if (drCacheCtx.isDrEnabled()) {
                        try {
                            drCacheCtx.dr().onExchange(res, exchId.isLeft(), activateCluster());
                        }
                        catch (IgniteCheckedException e) {
                            U.error(log, "Failed to notify DR: " + e, e);
                        }
                    }
                }

                if (exchCtx.events().hasServerLeft() || activateCluster())
                    detectLostPartitions(res);

                Map<Integer, CacheGroupValidation> m = U.newHashMap(cctx.cache().cacheGroups().size());

                for (CacheGroupContext grp : cctx.cache().cacheGroups()) {
                    CacheGroupValidation valRes = validateCacheGroup(grp, events().lastEvent().topologyNodes());

                    if (!valRes.isValid() || valRes.hasLostPartitions())
                        m.put(grp.groupId(), valRes);
                }

                grpValidRes = m;
            }

            if (!cctx.localNode().isClient())
                tryToPerformLocalSnapshotOperation();

            if (err == null)
                cctx.coordinators().onExchangeDone(events().discoveryCache());

            for (PartitionsExchangeAware comp : cctx.exchange().exchangeAwareComponents())
                comp.onDoneBeforeTopologyUnlock(this);

            // Create and destroy caches and cache proxies.
            cctx.cache().onExchangeDone(initialVersion(), exchActions, err);

            cctx.kernalContext().authentication().onActivate();

            Map<T2<Integer, Integer>, Long> localReserved = partHistSuppliers.getReservations(cctx.localNodeId());

            if (localReserved != null) {
                boolean success = cctx.database().reserveHistoryForPreloading(localReserved);

                // TODO: how to handle?
                if (!success)
                    err = new IgniteCheckedException("Could not reserve history");
            }

            cctx.database().releaseHistoryForExchange();

            if (err == null) {
                cctx.database().rebuildIndexesIfNeeded(this);

                for (CacheGroupContext grp : cctx.cache().cacheGroups()) {
                    if (!grp.isLocal())
                        grp.topology().onExchangeDone(this, grp.affinity().readyAffinity(res), false);
                }

                if (changedAffinity())
                    cctx.walState().changeLocalStatesOnExchangeDone(res, this);
            }
        }
        catch (Throwable t) {
            // In any case, this exchange future has to be completed. The original error should be preserved if exists.
            if (err != null)
                t.addSuppressed(err);

            err = t;
        }

        final Throwable err0 = err;

        // Should execute this listener first, before any external listeners.
        // Listeners use stack as data structure.
        listen(f -> {
            // Update last finished future in the first.
            cctx.exchange().lastFinishedFuture(this);

            // Complete any affReady futures and update last exchange done version.
            cctx.exchange().onExchangeDone(res, initialVersion(), err0);

            cctx.cache().completeProxyRestart(resolveCacheRequests(exchActions), initialVersion(), res);

            if (exchActions != null && err0 == null)
                exchActions.completeRequestFutures(cctx, null);

            if (stateChangeExchange() && err0 == null)
                cctx.kernalContext().state().onStateChangeExchangeDone(exchActions.stateChangeRequest());
        });

        if (super.onDone(res, err)) {
            afterLsnrCompleteFut.onDone();

            span.addLog(() -> "Completed partition exchange");

            span.end();

            if (err == null) {
                updateDurationHistogram(System.currentTimeMillis() - initTime);

                cctx.exchange().clusterRebalancedMetric().value(rebalanced());
            }

            if (log.isInfoEnabled()) {
                log.info("Completed partition exchange [localNode=" + cctx.localNodeId() +
                    ", exchange=" + (log.isDebugEnabled() ? this : shortInfo()) + ", topVer=" + topologyVersion() + "]");

                if (err == null) {
                    timeBag.finishGlobalStage("Exchange done");

                    // Collect all stages timings.
                    List<String> timings = timeBag.stagesTimings();

                    if (discoveryLag != null && discoveryLag.get1() != 0)
                        timings.add("Discovery lag=" + discoveryLag.get1() +
                            " ms, Latest started node id=" + discoveryLag.get2());

                    log.info(exchangeTimingsLogMessage("Exchange timings", timings));

                    List<String> localTimings = timeBag.longestLocalStagesTimings(3);

                    log.info(exchangeTimingsLogMessage("Exchange longest local stages", localTimings));
                }
            }

            initFut.onDone(err == null);

            cctx.exchange().latch().dropClientLatches(initialVersion());

            if (exchCtx != null && exchCtx.events().hasServerLeft()) {
                ExchangeDiscoveryEvents evts = exchCtx.events();

                for (DiscoveryEvent evt : evts.events()) {
                    if (serverLeftEvent(evt)) {
                        for (CacheGroupContext grp : cctx.cache().cacheGroups())
                            grp.affinityFunction().removeNode(evt.eventNode().id());
                    }
                }
            }

            for (PartitionsExchangeAware comp : cctx.exchange().exchangeAwareComponents())
                comp.onDoneAfterTopologyUnlock(this);

            if (firstDiscoEvt instanceof DiscoveryCustomEvent)
                ((DiscoveryCustomEvent)firstDiscoEvt).customMessage(null);

            if (err == null) {
                if (exchCtx != null && (exchCtx.events().hasServerLeft() || exchCtx.events().hasServerJoin())) {
                    ExchangeDiscoveryEvents evts = exchCtx.events();

                    for (DiscoveryEvent evt : evts.events()) {
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
     * Updates the {@link GridMetricManager#PME_OPS_BLOCKED_DURATION_HISTOGRAM} and {@link
     * GridMetricManager#PME_DURATION_HISTOGRAM} metrics if needed.
     *
     * @param duration The total duration of the current PME.
     */
    private void updateDurationHistogram(long duration) {
        cctx.exchange().durationHistogram().value(duration);

        if (changedAffinity())
            cctx.exchange().blockingDurationHistogram().value(duration);
    }

    /**
     * Calculates discovery lag (Maximal difference between exchange start times across all nodes).
     *
     * @param declared Single messages that were expected to be received during exchange.
     * @param merged Single messages from nodes that were merged during exchange.
     * @return Pair with discovery lag and node id which started exchange later than others.
     */
    private T2<Long, UUID> calculateDiscoveryLag(
        Map<UUID, GridDhtPartitionsSingleMessage> declared,
        Map<UUID, GridDhtPartitionsSingleMessage> merged
    ) {
        Map<UUID, GridDhtPartitionsSingleMessage> msgs = new HashMap<>(declared);

        msgs.putAll(merged);

        long minStartTime = startTime;
        long maxStartTime = startTime;
        UUID latestStartedNode = cctx.localNodeId();

        for (Map.Entry<UUID, GridDhtPartitionsSingleMessage> msg : msgs.entrySet()) {
            UUID nodeId = msg.getKey();
            long exchangeTime = msg.getValue().exchangeStartTime();

            if (exchangeTime != 0) {
                minStartTime = Math.min(minStartTime, exchangeTime);
                maxStartTime = Math.max(maxStartTime, exchangeTime);
            }

            if (maxStartTime == exchangeTime)
                latestStartedNode = nodeId;
        }

        return new T2<>(maxStartTime - minStartTime, latestStartedNode);
    }

    /**
     * @param exchangeActions Exchange actions.
     * @return Map of cache names and start descriptors.
     */
    private Map<String, DynamicCacheChangeRequest> resolveCacheRequests(ExchangeActions exchangeActions) {
        if (exchangeActions == null)
            return Collections.emptyMap();

        return exchangeActions.cacheStartRequests()
            .stream()
            .map(ExchangeActions.CacheActionData::request)
            .collect(Collectors.toMap(DynamicCacheChangeRequest::cacheName, r -> r));
    }

    /**
     * Method waits for new caches registration and cache configuration persisting to disk.
     */
    private void waitUntilNewCachesAreRegistered() {
        try {
            IgniteInternalFuture<?> registerCachesFut = registerCachesFuture;

            if (registerCachesFut != null && !registerCachesFut.isDone()) {
                final int timeout = Math.max(1000,
                    (int)(cctx.kernalContext().config().getFailureDetectionTimeout() / 2));

                for (; ; ) {
                    cctx.exchange().exchangerBlockingSectionBegin();

                    try {
                        registerCachesFut.get(timeout, TimeUnit.SECONDS);

                        break;
                    }
                    catch (IgniteFutureTimeoutCheckedException te) {
                        List<String> cacheNames = exchActions.cacheStartRequests().stream()
                            .map(req -> req.descriptor().cacheName())
                            .collect(Collectors.toList());

                        U.warn(log, "Failed to wait for caches configuration registration and saving within timeout. " +
                            "Probably disk is too busy or slow." +
                            "[caches=" + cacheNames + "]");
                    }
                    finally {
                        cctx.exchange().exchangerBlockingSectionEnd();
                    }
                }
            }
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Failed to wait for caches registration and saving", e);
        }
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
        if (finishState != null)
            finishState.cleanUp();
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

        if (node.isClient()) {
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
     * Merges this exchange with given one. Invoked under synchronization on {@code mux} of the {@code fut}.
     * All futures being merged are merged under a single synchronized section.
     *
     * @param fut Current exchange to merge with.
     * @return {@code True} if need wait for message from joined server node.
     */
    public boolean mergeJoinExchange(GridDhtPartitionsExchangeFuture fut) {
        boolean wait;

        synchronized (mux) {
            assert (!isDone() && !initFut.isDone()) || cctx.kernalContext().isStopping() : this;
            assert (mergedWith == null && state == null) || cctx.kernalContext().isStopping() : this;

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
     * Method is called on coordinator in situation when initial ExchangeFuture created on client join event was
     * preempted from exchange history because of IGNITE_EXCHANGE_HISTORY_SIZE property.
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

        fullMsg.rebalanced(rebalanced());

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
     * Tries to fast reply with {@link GridDhtPartitionsFullMessage} on received single message in case of exchange
     * future has already completed.
     *
     * @param node Cluster node which sent single message.
     * @param msg Single message.
     * @return {@code true} if fast reply succeed.
     */
    public boolean fastReplyOnSingleMessage(final ClusterNode node, final GridDhtPartitionsSingleMessage msg) {
        GridDhtPartitionsExchangeFuture futToFastReply = this;

        ExchangeLocalState currState;

        synchronized (mux) {
            currState = state;

            if (currState == ExchangeLocalState.MERGED)
                futToFastReply = mergedWith;
        }

        if (currState == ExchangeLocalState.DONE)
            futToFastReply.processSingleMessage(node.id(), msg);
        else if (currState == ExchangeLocalState.MERGED)
            futToFastReply.processMergedMessage(node, msg);

        return currState == ExchangeLocalState.MERGED || currState == ExchangeLocalState.DONE;
    }

    /**
     * @param nodeId Node ID.
     * @param msg Client's message.
     */
    public void waitAndReplyToNode(final UUID nodeId, final GridDhtPartitionsSingleMessage msg) {
        if (log.isDebugEnabled())
            log.debug("Single message will be handled on completion of exchange future: " + this);

        listen(failureHandlerWrapper(new CI1<IgniteInternalFuture<AffinityTopologyVersion>>() {
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
                    assert (firstDiscoEvt.type() == EVT_NODE_JOINED && firstDiscoEvt.eventNode().isClient()) :
                        GridDhtPartitionsExchangeFuture.this;

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

                    GridDhtPartitionsFullMessage msg =
                        createPartitionsMessage(true, node.version().compareToIgnoreTimestamp(PARTIAL_COUNTERS_MAP_SINCE) >= 0);

                    msg.rebalanced(rebalanced());

                    finishState0 = new FinishState(cctx.localNodeId(), initialVersion(), msg);
                }

                sendAllPartitionsToNode(finishState0, msg, nodeId);
            }
        }));
    }

    /**
     * @param clsr Closure to wrap with failure handler.
     * @return Wrapped closure.
     */
    private <T extends IgniteInternalFuture<?>> IgniteInClosure<T> failureHandlerWrapper(IgniteInClosure<T> clsr) {
        try {
            return (CI1<T>)clsr::apply;
        }
        catch (Error e) {
            cctx.kernalContext().failure().process(new FailureContext(FailureType.CRITICAL_ERROR, e));

            throw e;
        }
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
                                (allReceived ? "" : ", remainingNodes=" + remaining.size()) +
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
     * @param resetOwners True if need to reset partition state considering of counter, false otherwise.
     */
    private void assignPartitionStates(GridDhtPartitionTopology top, boolean resetOwners) {
        Map<Integer, CounterWithNodes> maxCntrs = new HashMap<>();
        Map<Integer, TreeSet<Long>> varCntrs = new HashMap<>();

        for (Map.Entry<UUID, GridDhtPartitionsSingleMessage> e : msgs.entrySet()) {
            CachePartitionPartialCountersMap nodeCntrs = e.getValue().partitionUpdateCounters(top.groupId(),
                top.partitions());

            assert nodeCntrs != null;

            for (int i = 0; i < nodeCntrs.size(); i++) {
                int p = nodeCntrs.partitionAt(i);

                UUID remoteNodeId = e.getKey();

                GridDhtPartitionState state = top.partitionState(remoteNodeId, p);

                if (state != GridDhtPartitionState.OWNING && state != GridDhtPartitionState.MOVING)
                    continue;

                long cntr = state == GridDhtPartitionState.MOVING ?
                    nodeCntrs.initialUpdateCounterAt(i) :
                    nodeCntrs.updateCounterAt(i);

                varCntrs.computeIfAbsent(p, key -> new TreeSet<>()).add(cntr);

                if (state != GridDhtPartitionState.OWNING)
                    continue;

                CounterWithNodes maxCntr = maxCntrs.get(p);

                if (maxCntr == null || cntr > maxCntr.cnt)
                    maxCntrs.put(p, new CounterWithNodes(cntr, e.getValue().partitionSizes(top.groupId()).get(p), remoteNodeId));
                else if (cntr == maxCntr.cnt)
                    maxCntr.nodes.add(remoteNodeId);
            }
        }

        // Also must process counters from the local node.
        for (GridDhtLocalPartition part : top.currentLocalPartitions()) {
            GridDhtPartitionState state = top.partitionState(cctx.localNodeId(), part.id());

            if (state != GridDhtPartitionState.OWNING && state != GridDhtPartitionState.MOVING)
                continue;

            final long cntr = state == GridDhtPartitionState.MOVING ?
                part.initialUpdateCounter() :
                part.updateCounter();

            varCntrs.computeIfAbsent(part.id(), key -> new TreeSet<>()).add(cntr);

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

        Set<Integer> haveHistory = new HashSet<>();

        assignHistoricalSuppliers(top, maxCntrs, varCntrs, haveHistory);

        if (resetOwners)
            resetOwnersByCounter(top, maxCntrs, haveHistory);
    }

    /**
     * Determine new owners for partitions.
     * If anyone of OWNING partitions have a counter less than maximum this partition changes state to MOVING forcibly.
     *
     * @param top Topology.
     * @param maxCntrs Max counter partiton map.
     * @param haveHistory Set of partitions witch have historical supplier.
     */
    private void resetOwnersByCounter(GridDhtPartitionTopology top,
        Map<Integer, CounterWithNodes> maxCntrs, Set<Integer> haveHistory) {
        Map<Integer, Set<UUID>> ownersByUpdCounters = U.newHashMap(maxCntrs.size());
        Map<Integer, Long> partSizes = U.newHashMap(maxCntrs.size());

        for (Map.Entry<Integer, CounterWithNodes> e : maxCntrs.entrySet()) {
            ownersByUpdCounters.put(e.getKey(), e.getValue().nodes);

            partSizes.put(e.getKey(), e.getValue().size);
        }

        top.globalPartSizes(partSizes);

        Map<UUID, Set<Integer>> partitionsToRebalance = top.resetOwners(
            ownersByUpdCounters, haveHistory, this);

        for (Map.Entry<UUID, Set<Integer>> e : partitionsToRebalance.entrySet()) {
            UUID nodeId = e.getKey();
            Set<Integer> parts = e.getValue();

            for (int part : parts)
                partsToReload.put(nodeId, top.groupId(), part);
        }
    }

    /**
     * Find and assign suppliers for history rebalance.
     *
     * @param top Topology.
     * @param maxCntrs Max counter partiton map.
     * @param varCntrs Various counters for each partition.
     * @param haveHistory Set of partitions witch have historical supplier.
     */
    private void assignHistoricalSuppliers(
        GridDhtPartitionTopology top,
        Map<Integer, CounterWithNodes> maxCntrs,
        Map<Integer, TreeSet<Long>> varCntrs,
        Set<Integer> haveHistory
    ) {
        Map<Integer, Map<Integer, Long>> partHistReserved0 = partHistReserved;

        Map<Integer, Long> localReserved = partHistReserved0 != null ? partHistReserved0.get(top.groupId()) : null;

        for (Map.Entry<Integer, TreeSet<Long>> e : varCntrs.entrySet()) {
            int p = e.getKey();

            CounterWithNodes maxCntrObj = maxCntrs.get(p);

            long maxCntr = maxCntrObj != null ? maxCntrObj.cnt : 0;

            NavigableSet<Long> nonMaxCntrs = e.getValue().headSet(maxCntr, false);

            // If minimal counter equals maximum then historical supplier does not necessary.
            if (nonMaxCntrs.isEmpty())
                continue;

            T2<UUID, Long> deepestReserved = new T2<>(null, Long.MAX_VALUE);

            if (localReserved != null) {
                Long localHistCntr = localReserved.get(p);

                if (localHistCntr != null && maxCntrObj.nodes.contains(cctx.localNodeId())) {
                    Long ceilingMinReserved = nonMaxCntrs.ceiling(localHistCntr);

                    if (ceilingMinReserved != null) {
                        partHistSuppliers.put(cctx.localNodeId(), top.groupId(), p, ceilingMinReserved);

                        haveHistory.add(p);
                    }

                    if (deepestReserved.get2() > localHistCntr)
                        deepestReserved.set(cctx.localNodeId(), localHistCntr);
                }
            }

            for (Map.Entry<UUID, GridDhtPartitionsSingleMessage> e0 : msgs.entrySet()) {
                Long histCntr = e0.getValue().partitionHistoryCounters(top.groupId()).get(p);

                if (histCntr != null && maxCntrObj.nodes.contains(e0.getKey())) {
                    Long ceilingMinReserved = nonMaxCntrs.ceiling(histCntr);

                    if (ceilingMinReserved != null) {
                        partHistSuppliers.put(e0.getKey(), top.groupId(), p, ceilingMinReserved);

                        haveHistory.add(p);
                    }

                    if (deepestReserved.get2() > histCntr)
                        deepestReserved.set(e0.getKey(), histCntr);
                }
            }
        }
    }

    /**
     * Detect lost partitions in case of node left or failed. For topology coordinator is called when all {@link
     * GridDhtPartitionsSingleMessage} were received. For other nodes is called when exchange future is completed by
     * {@link GridDhtPartitionsFullMessage}.
     *
     * @param resTopVer Result topology version.
     */
    private void detectLostPartitions(AffinityTopologyVersion resTopVer) {
        try {
            // Reserve at least 2 threads for system operations.
            doInParallelUninterruptibly(
                U.availableThreadCount(cctx.kernalContext(), GridIoPolicy.SYSTEM_POOL, 2),
                cctx.kernalContext().getSystemExecutorService(),
                cctx.affinity().cacheGroups().values(),
                desc -> {
                    if (desc.config().getCacheMode() == CacheMode.LOCAL)
                        return null;

                    CacheGroupContext grp = cctx.cache().cacheGroup(desc.groupId());

                    GridDhtPartitionTopology top = grp != null ? grp.topology() :
                        cctx.exchange().clientTopology(desc.groupId(), events().discoveryCache());

                    top.detectLostPartitions(resTopVer, this);

                    return null;
                });
        } catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }

        timeBag.finishGlobalStage("Detect lost partitions");
    }

    /**
     * @param cacheNames Cache names.
     */
    private void resetLostPartitions(Collection<String> cacheNames) {
        assert !exchCtx.mergeExchanges();

        try {
            doInParallelUninterruptibly(
                U.availableThreadCount(cctx.kernalContext(), GridIoPolicy.SYSTEM_POOL, 2),
                cctx.kernalContext().getSystemExecutorService(),
                cctx.affinity().caches().values(),
                desc -> {
                    if (desc.cacheConfiguration().getCacheMode() == CacheMode.LOCAL)
                        return null;

                    if (cacheNames.contains(desc.cacheName())) {
                        CacheGroupContext grp = cctx.cache().cacheGroup(desc.groupId());

                        GridDhtPartitionTopology top = grp != null ? grp.topology() :
                            cctx.exchange().clientTopology(desc.groupId(), events().discoveryCache());

                        top.resetLostPartitions(initialVersion());
                    }

                    return null;
                });
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /**
     * Creates an IgniteCheckedException that is used as root cause of the exchange initialization failure. This method
     * aggregates all the exceptions provided from all participating nodes.
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
     * Sends {@link DynamicCacheChangeFailureMessage} to all participated nodes that represents a cause of exchange
     * failure.
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
        catch (IgniteCheckedException e) {
            if (reconnectOnError(e))
                onDone(new IgniteNeedReconnectException(cctx.localNode(), e));
            else
                onDone(e);
        }
    }

    /**
     * Called only for coordinator node when all {@link GridDhtPartitionsSingleMessage}s were received
     *
     * @param sndResNodes Additional nodes to send finish message to.
     */
    private void onAllReceived(@Nullable Collection<ClusterNode> sndResNodes) {
        try {
            initFut.get();

            span.addLog(() -> "Waiting for all single messages");

            timeBag.finishGlobalStage("Waiting for all single messages");

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

                AffinityTopologyVersion threshold = newCrdFut != null ? newCrdFut.resultTopologyVersion() : null;

                if (threshold != null) {
                    assert newCrdFut.fullMessage() == null :
                        "There is full message in new coordinator future, but exchange was not finished using it: "
                        + newCrdFut.fullMessage();
                }

                boolean finish = cctx.exchange().mergeExchangesOnCoordinator(this, threshold);

                timeBag.finishGlobalStage("Exchanges merge");

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
        if (isDone() || !enterBusy())
            return;

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

            // Reserve at least 2 threads for system operations.
            int parallelismLvl = U.availableThreadCount(cctx.kernalContext(), GridIoPolicy.SYSTEM_POOL, 2);

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

                doInParallel(
                    parallelismLvl,
                    cctx.kernalContext().getSystemExecutorService(),
                    cctx.affinity().cacheGroups().values(),
                    desc -> {
                        if (desc.config().getCacheMode() == CacheMode.LOCAL)
                            return null;

                        CacheGroupContext grp = cctx.cache().cacheGroup(desc.groupId());

                        GridDhtPartitionTopology top = grp != null ? grp.topology() :
                            cctx.exchange().clientTopology(desc.groupId(), events().discoveryCache());

                        top.beforeExchange(this, true, true);

                        return null;
                    });
            }

            span.addLog(() -> "Affinity recalculation (crd)");

            timeBag.finishGlobalStage("Affinity recalculation (crd)");

            Map<Integer, CacheGroupAffinityMessage> joinedNodeAff = new ConcurrentHashMap<>(cctx.cache().cacheGroups().size());

            doInParallel(
                parallelismLvl,
                cctx.kernalContext().getSystemExecutorService(),
                msgs.values(),
                msg -> {
                    processSingleMessageOnCrdFinish(msg, joinedNodeAff);

                    return null;
                }
            );

            timeBag.finishGlobalStage("Collect update counters and create affinity messages");

            validatePartitionsState();

            if (firstDiscoEvt.type() == EVT_DISCOVERY_CUSTOM_EVT) {
                assert firstDiscoEvt instanceof DiscoveryCustomEvent;

                if (activateCluster() || changedBaseline())
                    assignPartitionsStates(true);

                DiscoveryCustomMessage discoveryCustomMessage = ((DiscoveryCustomEvent) firstDiscoEvt).customMessage();

                if (discoveryCustomMessage instanceof DynamicCacheChangeBatch) {
                    if (exchActions != null) {
                        Set<String> caches = exchActions.cachesToResetLostPartitions();

                        if (!F.isEmpty(caches))
                            resetLostPartitions(caches);

                        assignPartitionsStates(true);
                    }
                }
                else if (discoveryCustomMessage instanceof SnapshotDiscoveryMessage
                    && ((SnapshotDiscoveryMessage)discoveryCustomMessage).needAssignPartitions()) {
                    markAffinityReassign();

                    assignPartitionsStates(true);
                }
            }
            else if (exchCtx.events().hasServerJoin())
                assignPartitionsStates(true);
            else if (exchCtx.events().hasServerLeft())
                assignPartitionsStates(false);

            // Recalculate new affinity based on partitions availability.
            if (!exchCtx.mergeExchanges() && forceAffReassignment) {
                idealAffDiff = cctx.affinity().onCustomEventWithEnforcedAffinityReassignment(this);

                timeBag.finishGlobalStage("Ideal affinity diff calculation (enforced)");
            }

            for (CacheGroupContext grpCtx : cctx.cache().cacheGroups()) {
                if (!grpCtx.isLocal())
                    grpCtx.topology().applyUpdateCounters();
            }

            timeBag.finishGlobalStage("Apply update counters");

            updateLastVersion(cctx.versions().last());

            cctx.versions().onExchange(lastVer.get().order());

            IgniteProductVersion minVer = exchCtx.events().discoveryCache().minimumNodeVersion();

            GridDhtPartitionsFullMessage msg = createPartitionsMessage(true,
                minVer.compareToIgnoreTimestamp(PARTIAL_COUNTERS_MAP_SINCE) >= 0);

            if (!cctx.affinity().rebalanceRequired() && !deactivateCluster())
                msg.rebalanced(true);

            if (exchCtx.mergeExchanges()) {
                assert !centralizedAff;

                msg.resultTopologyVersion(resTopVer);

                if (exchCtx.events().hasServerLeft())
                    msg.idealAffinityDiff(idealAffDiff);
            }
            else if (forceAffReassignment)
                msg.idealAffinityDiff(idealAffDiff);

            msg.prepareMarshal(cctx);

            timeBag.finishGlobalStage("Full message preparing");

            synchronized (mux) {
                finishState = new FinishState(crd.id(), resTopVer, msg);

                state = ExchangeLocalState.DONE;
            }

            if (centralizedAff) {
                assert !exchCtx.mergeExchanges();

                IgniteInternalFuture<Map<Integer, Map<Integer, List<UUID>>>> fut = cctx.affinity().initAffinityOnNodeLeft(this);

                if (!fut.isDone())
                    fut.listen(this::onAffinityInitialized);
                else
                    onAffinityInitialized(fut);
            }
            else {
                Set<ClusterNode> nodes;

                Map<UUID, GridDhtPartitionsSingleMessage> mergedJoinExchMsgs0;

                synchronized (mux) {
                    srvNodes.remove(cctx.localNode());

                    nodes = new LinkedHashSet<>(srvNodes);

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
                    else
                        mergedJoinExchMsgs0 = Collections.emptyMap();

                    if (!F.isEmpty(sndResNodes))
                        nodes.addAll(sndResNodes);
                }

                if (msg.rebalanced())
                    markRebalanced();

                if (!nodes.isEmpty())
                    sendAllPartitions(msg, nodes, mergedJoinExchMsgs0, joinedNodeAff);

                timeBag.finishGlobalStage("Full message sending");

                discoveryLag = calculateDiscoveryLag(
                    msgs,
                    mergedJoinExchMsgs0
                );

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
                StateChangeRequest req = exchActions.stateChangeRequest();

                assert req != null : exchActions;

                boolean stateChangeErr = false;

                if (!F.isEmpty(exchangeGlobalExceptions)) {
                    stateChangeErr = true;

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

                if (!cctx.kernalContext().state().clusterState().localBaselineAutoAdjustment()) {
                    ClusterState state = stateChangeErr ? ClusterState.INACTIVE : req.state();

                    ChangeGlobalStateFinishMessage stateFinishMsg = new ChangeGlobalStateFinishMessage(
                        req.requestId(),
                        state,
                        !stateChangeErr
                    );

                    cctx.discovery().sendCustomEvent(stateFinishMsg);
                }

                timeBag.finishGlobalStage("State finish message sending");

                if (!centralizedAff)
                    onDone(exchCtx.events().topologyVersion(), null);
            }

            // Try switch late affinity right now if an exchange has been completed normally.
            if (!centralizedAff && isDone() && error() == null && !cctx.kernalContext().isStopping())
                cctx.exchange().checkRebalanceState();
        }
        catch (IgniteCheckedException e) {
            if (reconnectOnError(e))
                onDone(new IgniteNeedReconnectException(cctx.localNode(), e));
            else
                onDone(e);
        }
        finally {
            leaveBusy();
        }
    }

    /**
     * @param msg Single message to process.
     * @param messageAccumulator Message to store message which need to be sent after.
     */
    private void processSingleMessageOnCrdFinish(
        GridDhtPartitionsSingleMessage msg,
        Map<Integer, CacheGroupAffinityMessage> messageAccumulator
    ) {
        for (Map.Entry<Integer, GridDhtPartitionMap> e : msg.partitions().entrySet()) {
            Integer grpId = e.getKey();

            CacheGroupContext grp = cctx.cache().cacheGroup(grpId);

            GridDhtPartitionTopology top = grp != null
                ? grp.topology()
                : cctx.exchange().clientTopology(grpId, events().discoveryCache());

            CachePartitionPartialCountersMap cntrs = msg.partitionUpdateCounters(grpId, top.partitions());

            if (cntrs != null)
                top.collectUpdateCounters(cntrs);
        }

        Collection<Integer> affReq = msg.cacheGroupsAffinityRequest();

        if (affReq != null)
            CacheGroupAffinityMessage.createAffinityMessages(
                cctx,
                exchCtx.events().topologyVersion(),
                affReq,
                messageAccumulator
            );
    }

    /**
     * Collects non local cache group descriptors.
     *
     * @return Collection of non local cache group descriptors.
     */
    private List<CacheGroupDescriptor> nonLocalCacheGroupDescriptors() {
        return cctx.affinity().cacheGroups().values().stream()
            .filter(grpDesc -> grpDesc.config().getCacheMode() != CacheMode.LOCAL)
            .collect(Collectors.toList());
    }

    /**
     * Collects non local cache groups.
     *
     * @return Collection of non local cache groups.
     */
    private List<CacheGroupContext> nonLocalCacheGroups() {
        return cctx.cache().cacheGroups().stream()
            .filter(grp -> !grp.isLocal() && !cacheGroupStopping(grp.groupId()))
            .collect(Collectors.toList());
    }

    /**
     * Validates that partition update counters and cache sizes for all caches are consistent.
     */
    private void validatePartitionsState() {
        try {
            U.doInParallel(
                cctx.kernalContext().getSystemExecutorService(),
                nonLocalCacheGroupDescriptors(),
                grpDesc -> {
                    CacheGroupContext grpCtx = cctx.cache().cacheGroup(grpDesc.groupId());

                    GridDhtPartitionTopology top = grpCtx != null
                        ? grpCtx.topology()
                        : cctx.exchange().clientTopology(grpDesc.groupId(), events().discoveryCache());

                    // Do not validate read or write through caches or caches with disabled rebalance
                    // or ExpiryPolicy is set or validation is disabled.
                    boolean customExpiryPlc = Optional.ofNullable(grpCtx)
                        .map(CacheGroupContext::caches)
                        .orElseGet(Collections::emptyList)
                        .stream()
                        .anyMatch(ctx -> ctx.expiry() != null && !(ctx.expiry() instanceof EternalExpiryPolicy));

                    if (grpCtx == null
                        || grpCtx.config().isReadThrough()
                        || grpCtx.config().isWriteThrough()
                        || grpCtx.config().getCacheStoreFactory() != null
                        || grpCtx.config().getRebalanceDelay() == -1
                        || grpCtx.config().getRebalanceMode() == CacheRebalanceMode.NONE
                        || customExpiryPlc
                        || SKIP_PARTITION_SIZE_VALIDATION)
                        return null;

                    try {
                        validator.validatePartitionCountersAndSizes(GridDhtPartitionsExchangeFuture.this, top, msgs);
                    }
                    catch (IgniteCheckedException ex) {
                        log.warning(String.format(PARTITION_STATE_FAILED_MSG, grpCtx.cacheOrGroupName(), ex.getMessage()));
                        // TODO: Handle such errors https://issues.apache.org/jira/browse/IGNITE-7833
                    }

                    return null;
                }
            );
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException("Failed to validate partitions state", e);
        }

        timeBag.finishGlobalStage("Validate partitions states");
    }

    /**
     * @param resetOwners True if reset partitions state needed, false otherwise.
     */
    private void assignPartitionsStates(boolean resetOwners) {
        try {
            U.doInParallel(
                cctx.kernalContext().getSystemExecutorService(),
                nonLocalCacheGroupDescriptors(),
                grpDesc -> {
                    CacheGroupContext grpCtx = cctx.cache().cacheGroup(grpDesc.groupId());

                    GridDhtPartitionTopology top = grpCtx != null
                        ? grpCtx.topology()
                        : cctx.exchange().clientTopology(grpDesc.groupId(), events().discoveryCache());

                    if (CU.isPersistentCache(grpDesc.config(), cctx.gridConfig().getDataStorageConfiguration()))
                        assignPartitionStates(top, resetOwners);
                    else if (resetOwners)
                        assignPartitionSizes(top);

                    return null;
                }
            );
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException("Failed to assign partition states", e);
        }

        timeBag.finishGlobalStage("Assign partitions states");
    }

    /**
     * Removes gaps in the local update counters. Gaps in update counters are possible on backup node when primary
     * failed to send update counter deltas to backup.
     */
    private void finalizePartitionCounters() {
        // Reserve at least 2 threads for system operations.
        int parallelismLvl = U.availableThreadCount(cctx.kernalContext(), GridIoPolicy.SYSTEM_POOL, 2);

        try {
            U.<CacheGroupContext, Void>doInParallelUninterruptibly(
                parallelismLvl,
                cctx.kernalContext().getSystemExecutorService(),
                nonLocalCacheGroups(),
                grp -> {
                    Set<Integer> parts;

                    if (exchCtx.exchangeFreeSwitch()) {
                        assert !isSnapshotOperation(firstDiscoEvt) : "Not allowed for taking snapshots: " + this;

                        // Previous topology to resolve failed primaries set.
                        AffinityTopologyVersion topVer = sharedContext().exchange().readyAffinityVersion();

                        // Failed node's primary partitions. Safe to use affinity since topology was fully rebalanced.
                        parts = grp.affinity().primaryPartitions(firstDiscoEvt.eventNode().id(), topVer);
                    }
                    else
                        parts = grp.topology().localPartitionMap().keySet();

                    grp.topology().finalizeUpdateCounters(parts);

                    return null;
                }
            );
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException("Failed to finalize partition counters", e);
        }

        timeBag.finishGlobalStage("Finalize update counters");
    }

    /**
     * @param finishState State.
     * @param msg Request.
     * @param nodeId Node ID.
     */
    private void sendAllPartitionsToNode(FinishState finishState, GridDhtPartitionsSingleMessage msg, UUID nodeId) {
        ClusterNode node = cctx.node(nodeId);

        if (node == null) {
            if (log.isDebugEnabled())
                log.debug("Failed to send partitions, node failed: " + nodeId);

            return;
        }

        GridDhtPartitionsFullMessage fullMsg = finishState.msg.copy();

        Collection<Integer> affReq = msg.cacheGroupsAffinityRequest();

        if (affReq != null) {
            Map<Integer, CacheGroupAffinityMessage> cachesAff = U.newHashMap(affReq.size());

            CacheGroupAffinityMessage.createAffinityMessages(
                cctx,
                finishState.resTopVer,
                affReq,
                cachesAff);

            fullMsg.joinedNodeAffinity(cachesAff);
        }

        if (!fullMsg.exchangeId().equals(msg.exchangeId())) {
            fullMsg = fullMsg.copy();

            fullMsg.exchangeId(msg.exchangeId());
        }

        try {
            cctx.io().send(node, fullMsg, SYSTEM_POOL);

            if (log.isTraceEnabled()) {
                log.trace("Full message was sent to node: " +
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
        assert !node.isDaemon() && !node.isClient() : node;

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

            timeBag.finishGlobalStage("Waiting for Full message");

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
                        assert cctx.kernalContext().isStopping() || cctx.kernalContext().clientDisconnected();

                        return; // Node is stopping, no need to further process exchange.
                    }

                    assert resTopVer.equals(exchCtx.events().topologyVersion()) : "Unexpected result version [" +
                        "msgVer=" + resTopVer +
                        ", locVer=" + exchCtx.events().topologyVersion() + ']';
                }

                exchCtx.events().processEvents(this);

                if (localJoinExchange()) {
                    Set<Integer> noAffinityGroups = cctx.affinity().onLocalJoin(this, msg.joinedNodeAffinity(), resTopVer);

                    // Prevent cache usage by a user.
                    if (!noAffinityGroups.isEmpty()) {
                        List<GridCacheAdapter> closedCaches = cctx.cache().blockGateways(noAffinityGroups);

                        closedCaches.forEach(cache -> log.warning("Affinity for cache " + cache.context().name()
                            + " has not received from coordinator during local join. "
                            + " Probably cache is already stopped but not processed on local node yet."
                            + " Cache proxy will be closed for user interactions for safety."));
                    }
                }
                else {
                    if (exchCtx.events().hasServerLeft())
                        cctx.affinity().applyAffinityFromFullMessage(this, msg.idealAffinityDiff());
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
                cctx.affinity().onLocalJoin(this, msg.joinedNodeAffinity(), resTopVer);
            else if (forceAffReassignment)
                cctx.affinity().applyAffinityFromFullMessage(this, msg.idealAffinityDiff());

            timeBag.finishGlobalStage("Affinity recalculation");

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

            if (msg.rebalanced())
                markRebalanced();

            if (stateChangeExchange() && !F.isEmpty(msg.getErrorsMap()))
                cctx.kernalContext().state().onStateChangeError(msg.getErrorsMap(), exchActions.stateChangeRequest());

            if (firstDiscoEvt.type() == EVT_DISCOVERY_CUSTOM_EVT) {
                DiscoveryCustomMessage discoveryCustomMessage = ((DiscoveryCustomEvent)firstDiscoEvt).customMessage();

                if (discoveryCustomMessage instanceof SnapshotDiscoveryMessage
                    && ((SnapshotDiscoveryMessage)discoveryCustomMessage).needAssignPartitions())
                    markAffinityReassign();
            }

            onDone(resTopVer, null);
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

        // Reserve at least 2 threads for system operations.
        int parallelismLvl = U.availableThreadCount(cctx.kernalContext(), GridIoPolicy.SYSTEM_POOL, 2);

        try {
            Map<Integer, Map<Integer, Long>> partsSizes = msg.partitionSizes(cctx);

            doInParallel(
                parallelismLvl,
                cctx.kernalContext().getSystemExecutorService(),
                msg.partitions().keySet(), grpId -> {
                    CacheGroupContext grp = cctx.cache().cacheGroup(grpId);

                    if (grp != null) {
                        CachePartitionFullCountersMap cntrMap = msg.partitionUpdateCounters(grpId,
                            grp.topology().partitions());

                        grp.topology().update(resTopVer,
                            msg.partitions().get(grpId),
                            cntrMap,
                            msg.partsToReload(cctx.localNodeId(), grpId),
                            partsSizes.getOrDefault(grpId, Collections.emptyMap()),
                            null,
                            this,
                            msg.lostPartitions(grpId));
                    }
                    else {
                        GridDhtPartitionTopology top = cctx.exchange().clientTopology(grpId, events().discoveryCache());

                        CachePartitionFullCountersMap cntrMap = msg.partitionUpdateCounters(grpId,
                            top.partitions());

                        top.update(resTopVer,
                            msg.partitions().get(grpId),
                            cntrMap,
                            Collections.emptySet(),
                            null,
                            null,
                            this,
                            msg.lostPartitions(grpId));
                    }

                    return null;
                });
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }

        timeBag.finishGlobalStage("Full map updating");
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
                            assert msg.error() != null : msg;

                            // Try to revert all the changes that were done during initialization phase
                            cctx.affinity().forceCloseCaches(
                                GridDhtPartitionsExchangeFuture.this,
                                crd.isLocal(),
                                msg.exchangeActions()
                            );

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

                        cctx.affinity().onExchangeChangeAffinityMessage(GridDhtPartitionsExchangeFuture.this, msg);

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

            grp.affinity().idealAssignment(initialVersion(), affAssignment);

            grp.affinity().initialize(initialVersion(), affAssignment);

            cctx.exchange().exchangerUpdateHeartbeat();
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

        try {
            onDiscoveryEvent(new IgniteRunnable() {
                @Override public void run() {
                    if (isDone() || !enterBusy())
                        return;

                    try {
                        boolean crdChanged = false;
                        boolean allReceived = false;
                        boolean wasMerged = false;

                        ClusterNode crd0;

                        events().discoveryCache().updateAlives(node);

                        InitNewCoordinatorFuture newCrdFut0;

                        synchronized (mux) {
                            newCrdFut0 = newCrdFut;
                        }

                        if (newCrdFut0 != null)
                            newCrdFut0.onNodeLeft(node.id());

                        synchronized (mux) {
                            srvNodes.remove(node);

                            boolean rmvd = remaining.remove(node.id());

                            if (!rmvd) {
                                if (mergedJoinExchMsgs != null && mergedJoinExchMsgs.containsKey(node.id())) {
                                    if (mergedJoinExchMsgs.get(node.id()) == null) {
                                        mergedJoinExchMsgs.remove(node.id());

                                        wasMerged = true;
                                        rmvd = true;

                                        awaitMergedMsgs--;

                                        assert awaitMergedMsgs >= 0 : "exchFut=" + this + ", node=" + node;
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
                                    allReceived = rmvd && (remaining.isEmpty() && awaitMergedMsgs == 0);

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

                            if (crd0 == null)
                                finishState = new FinishState(null, initialVersion(), null);
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
                                        try {
                                            newCrdFut.init(GridDhtPartitionsExchangeFuture.this);
                                        }
                                        catch (Throwable t) {
                                            U.error(log, "Failed to initialize new coordinator future [topVer=" + initialVersion() + "]", t);

                                            cctx.kernalContext().failure().process(new FailureContext(FailureType.CRITICAL_ERROR, t));

                                            throw t;
                                        }

                                        newCrdFut.listen(new CI1<IgniteInternalFuture>() {
                                            @Override public void apply(IgniteInternalFuture fut) {
                                                if (isDone())
                                                    return;

                                                Lock lock = cctx.io().readLock();

                                                if (lock == null)
                                                    return;

                                                try {
                                                    onBecomeCoordinator((InitNewCoordinatorFuture)fut);
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
                                boolean wasMerged0 = wasMerged;

                                cctx.kernalContext().getSystemExecutorService().submit(new Runnable() {
                                    @Override public void run() {
                                        awaitSingleMapUpdates();

                                        if (wasMerged0)
                                            finishExchangeOnCoordinator(null);
                                        else
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
                    Map<Integer, CacheGroupAffinityMessage> joinedNodeAff = new ConcurrentHashMap<>();

                    // Reserve at least 2 threads for system operations.
                    int parallelismLvl = U.availableThreadCount(cctx.kernalContext(), GridIoPolicy.SYSTEM_POOL, 2);

                    try {
                        U.doInParallel(
                            parallelismLvl,
                            cctx.kernalContext().getSystemExecutorService(),
                            msgs.entrySet(),
                            entry -> {
                                this.msgs.put(entry.getKey().id(), entry.getValue());

                                GridDhtPartitionsSingleMessage msg = entry.getValue();

                                Collection<Integer> affReq = msg.cacheGroupsAffinityRequest();

                                if (!F.isEmpty(affReq)) {
                                    CacheGroupAffinityMessage.createAffinityMessages(
                                        cctx,
                                        fullMsg.resultTopologyVersion(),
                                        affReq,
                                        joinedNodeAff
                                    );
                                }

                                return null;
                            }
                        );
                    }
                    catch (IgniteCheckedException e) {
                        throw new IgniteException(e);
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

    /**
     * @return {@code True} if cluster fully rebalanced.
     */
    public boolean rebalanced() {
        return rebalancedInfo != null;
    }

    /**
     * @return {@code True} if cluster was fully rebalanced on previous topology.
     */
    public boolean wasRebalanced() {
        GridDhtPartitionsExchangeFuture prev = sharedContext().exchange().lastFinishedFuture();

        assert prev != this;

        return prev != null && prev.rebalanced();
    }

    /**
     * Sets cluster fully rebalanced flag.
     */
    private void markRebalanced() {
        assert !rebalanced();

        rebalancedInfo = new RebalancedInfo(cctx.affinity().idealPrimaryNodesForLocalBackups());
    }

    /**
     * Keeps cluster fully rebalanced flag.
     */
    private void keepRebalanced() {
        assert !rebalanced() && wasRebalanced();

        rebalancedInfo = sharedContext().exchange().lastFinishedFuture().rebalancedInfo;
    }

    /**
     * Marks this future as affinity reassign.
     */
    public void markAffinityReassign() {
        affinityReassign = true;
    }

    /**
     * @return True if some owned partition was reassigned, false otherwise.
     */
    public boolean affinityReassign() {
        return affinityReassign;
    }

    /**
     * Add or merge updates received from coordinator while exchange in progress.
     *
     * @param fullMsg Full message with exchangeId = null.
     * @return {@code True} if message should be ignored and processed after exchange is done.
     */
    public synchronized boolean addOrMergeDelayedFullMessage(ClusterNode node, GridDhtPartitionsFullMessage fullMsg) {
        assert fullMsg.exchangeId() == null : fullMsg.exchangeId();

        if (isDone())
            return false;

        GridDhtPartitionsFullMessage prev = delayedLatestMsg;

        if (prev == null) {
            delayedLatestMsg = fullMsg;

            listen(f -> {
                GridDhtPartitionsFullMessage msg;

                synchronized (this) {
                    msg = delayedLatestMsg;

                    delayedLatestMsg = null;
                }

                if (msg != null)
                    cctx.exchange().processFullPartitionUpdate(node, msg);
            });
        }
        else
            delayedLatestMsg.merge(fullMsg, cctx.discovery());

        return true;
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
            final ClusterNode crd;
            final Set<UUID> remaining;
            final InitNewCoordinatorFuture newCrdFut;

            synchronized (mux) {
                crd = this.crd;
                remaining = new HashSet<>(this.remaining);
                newCrdFut = this.newCrdFut;
            }

            if (newCrdFut != null)
                newCrdFut.addDiagnosticRequest(diagCtx);

            if (crd != null) {
                if (!crd.isLocal()) {
                    diagCtx.exchangeInfo(crd.id(), initialVersion(), "Exchange future waiting for coordinator " +
                        "response [crd=" + crd.id() + ", topVer=" + initialVersion() + ']');
                }
                else if (!remaining.isEmpty()) {
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
            ", rebalanced=" + rebalanced() +
            ", done=" + isDone() +
            ", newCrdFut=" + this.newCrdFut + ']';
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        Set<UUID> remaining;
        Set<UUID> mergedJoinExch;
        int awaitMergedMsgs;

        synchronized (mux) {
            remaining = new HashSet<>(this.remaining);
            mergedJoinExch = mergedJoinExchMsgs == null ? null : new HashSet<>(mergedJoinExchMsgs.keySet());
            awaitMergedMsgs = this.awaitMergedMsgs;
        }

        return S.toString(GridDhtPartitionsExchangeFuture.class, this,
            "evtLatch", evtLatch == null ? "null" : evtLatch.getCount(),
            "remaining", remaining,
            "mergedJoinExchMsgs", mergedJoinExch,
            "awaitMergedMsgs", awaitMergedMsgs,
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
     * @param grp Group.
     * @param part Partition.
     * @return {@code True} if partition has to be cleared before rebalance.
     */
    public boolean isClearingPartition(CacheGroupContext grp, int part) {
        if (!grp.persistenceEnabled())
            return false;

        synchronized (mux) {
            if (clearingPartitions == null)
                return false;

            Set<Integer> parts = clearingPartitions.get(grp.groupId());

            return parts != null && parts.contains(part);
        }
    }

    /**
     * Marks a partition for clearing before rebalance.
     * Fully cleared partitions should never be historically rebalanced.
     *
     * @param grp Group.
     * @param part Partition.
     */
    public void addClearingPartition(CacheGroupContext grp, int part) {
        if (!grp.persistenceEnabled())
            return;

        synchronized (mux) {
            clearingPartitions.computeIfAbsent(grp.groupId(), k -> new HashSet()).add(part);
        }
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

        /**
         * Cleans up resources to avoid excessive memory usage.
         */
        public void cleanUp() {
            if (msg != null)
                msg.cleanUp();
        }
    }

    /**
     *
     */
    public enum ExchangeType {
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

    /**
     *
     */
    private static class RebalancedInfo {
        /** Primary nodes for local backups for all registered partitioned caches. */
        private Set<ClusterNode> primaryNodes;

        /**
         * @param primaryNodes Primary nodes for local backups.
         */
        public RebalancedInfo(Set<ClusterNode> primaryNodes) {
            this.primaryNodes = primaryNodes;
        }
    }
}

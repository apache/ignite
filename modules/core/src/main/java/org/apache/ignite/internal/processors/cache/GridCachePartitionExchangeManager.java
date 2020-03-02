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

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.cluster.BaselineNode;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.cluster.ClusterGroupEmptyException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.ClusterActivationEvent;
import org.apache.ignite.events.ClusterStateChangeEvent;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.internal.IgniteClientDisconnectedCheckedException;
import org.apache.ignite.internal.IgniteDiagnosticAware;
import org.apache.ignite.internal.IgniteDiagnosticPrepareContext;
import org.apache.ignite.internal.IgniteFutureTimeoutCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.IgniteNeedReconnectException;
import org.apache.ignite.internal.NodeStoppingException;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.events.DiscoveryCustomEvent;
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.managers.discovery.DiscoveryLocalJoinData;
import org.apache.ignite.internal.managers.eventstorage.DiscoveryEventListener;
import org.apache.ignite.internal.managers.eventstorage.GridEventStorageManager;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.affinity.GridAffinityAssignmentCache;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.CachePartitionFullCountersMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.CachePartitionPartialCountersMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.ForceRebalanceExchangeTask;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionDemandLegacyMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionDemandMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionExchangeId;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionFullMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionSupplyMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsFullMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsSingleMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsSingleRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPreloaderAssignments;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.IgniteDhtPartitionHistorySuppliersMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.IgniteDhtPartitionsToReloadMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.PartitionsExchangeAware;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.RebalanceReassignExchangeTask;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.StopCachesOnClientReconnectExchangeTask;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.latch.ExchangeLatchManager;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridClientPartitionTopology;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionTopology;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxLocal;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteCacheSnapshotManager;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotDiscoveryMessage;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxManager;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.cluster.ChangeGlobalStateFinishMessage;
import org.apache.ignite.internal.processors.cluster.ChangeGlobalStateMessage;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.internal.processors.metric.impl.HistogramMetricImpl;
import org.apache.ignite.internal.processors.query.schema.SchemaNodeLeaveExchangeWorkerTask;
import org.apache.ignite.internal.processors.timeout.GridTimeoutObject;
import org.apache.ignite.internal.util.GridListSet;
import org.apache.ignite.internal.util.GridPartitionStateMap;
import org.apache.ignite.internal.util.GridStringBuilder;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.CI2;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.thread.IgniteThread;
import org.apache.ignite.transactions.TransactionState;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_DIAGNOSTIC_WARN_LIMIT;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_IO_DUMP_ON_TIMEOUT;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_PRELOAD_RESEND_TIMEOUT;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_THREAD_DUMP_ON_EXCHANGE_TIMEOUT;
import static org.apache.ignite.IgniteSystemProperties.getLong;
import static org.apache.ignite.events.EventType.EVT_CLUSTER_ACTIVATED;
import static org.apache.ignite.events.EventType.EVT_CLUSTER_DEACTIVATED;
import static org.apache.ignite.events.EventType.EVT_CLUSTER_STATE_CHANGED;
import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_JOINED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.failure.FailureType.CRITICAL_ERROR;
import static org.apache.ignite.failure.FailureType.SYSTEM_WORKER_TERMINATION;
import static org.apache.ignite.internal.GridTopic.TOPIC_CACHE;
import static org.apache.ignite.internal.IgniteFeatures.TRANSACTION_OWNER_THREAD_DUMP_PROVIDING;
import static org.apache.ignite.internal.IgniteFeatures.allNodesSupports;
import static org.apache.ignite.internal.events.DiscoveryCustomEvent.EVT_DISCOVERY_CUSTOM_EVT;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.SYSTEM_POOL;
import static org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion.NONE;
import static org.apache.ignite.internal.processors.cache.distributed.dht.preloader.CachePartitionPartialCountersMap.PARTIAL_COUNTERS_MAP_SINCE;
import static org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture.nextDumpTimeout;
import static org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPreloader.DFLT_PRELOAD_RESEND_TIMEOUT;
import static org.apache.ignite.internal.processors.metric.GridMetricManager.PME_DURATION;
import static org.apache.ignite.internal.processors.metric.GridMetricManager.PME_DURATION_HISTOGRAM;
import static org.apache.ignite.internal.processors.metric.GridMetricManager.PME_METRICS;
import static org.apache.ignite.internal.processors.metric.GridMetricManager.PME_OPS_BLOCKED_DURATION;
import static org.apache.ignite.internal.processors.metric.GridMetricManager.PME_OPS_BLOCKED_DURATION_HISTOGRAM;

/**
 * Partition exchange manager.
 */
public class GridCachePartitionExchangeManager<K, V> extends GridCacheSharedManagerAdapter<K, V> {
    /** Exchange history size. */
    private final int EXCHANGE_HISTORY_SIZE =
        IgniteSystemProperties.getInteger(IgniteSystemProperties.IGNITE_EXCHANGE_HISTORY_SIZE, 1_000);

    /** */
    private final long IGNITE_EXCHANGE_MERGE_DELAY =
        IgniteSystemProperties.getLong(IgniteSystemProperties.IGNITE_EXCHANGE_MERGE_DELAY, 0);

    /** */
    private final int DIAGNOSTIC_WARN_LIMIT = IgniteSystemProperties.getInteger(IGNITE_DIAGNOSTIC_WARN_LIMIT, 10);

    /** */
    private static final IgniteProductVersion EXCHANGE_PROTOCOL_2_SINCE = IgniteProductVersion.fromString("2.1.4");

    /** Atomic reference for pending partition resend timeout object. */
    private AtomicReference<ResendTimeoutObject> pendingResend = new AtomicReference<>();

    /** Partition resend timeout after eviction. */
    private final long partResendTimeout = getLong(IGNITE_PRELOAD_RESEND_TIMEOUT, DFLT_PRELOAD_RESEND_TIMEOUT);

    /** */
    private final ReadWriteLock busyLock = new ReentrantReadWriteLock();

    /** Last partition refresh. */
    private final AtomicLong lastRefresh = new AtomicLong(-1);

    /** */
    private final ActionLimiter<IgniteInternalTx> ltrDumpLimiter = new ActionLimiter<>(1);

    /** */
    @GridToStringInclude
    private ExchangeWorker exchWorker;

    /** */
    @GridToStringExclude
    private final ConcurrentMap<Integer, GridClientPartitionTopology> clientTops = new ConcurrentHashMap<>();

    /** Last initialized topology future. */
    @Nullable private volatile GridDhtPartitionsExchangeFuture lastInitializedFut;

    /** */
    private final AtomicReference<GridDhtPartitionsExchangeFuture> lastFinishedFut = new AtomicReference<>();

    /** */
    private final ConcurrentMap<AffinityTopologyVersion, AffinityReadyFuture> readyFuts =
        new ConcurrentSkipListMap<>();

    /** */
    private final ConcurrentNavigableMap<AffinityTopologyVersion, AffinityTopologyVersion> lastAffTopVers =
        new ConcurrentSkipListMap<>();

    /**
     * Latest started rebalance topology version but possibly not finished yet. Value {@code NONE}
     * means that previous rebalance is undefined and the new one should be initiated.
     *
     * Should not be used to determine latest rebalanced topology.
     */
    private volatile AffinityTopologyVersion rebTopVer = NONE;

    /** */
    private GridFutureAdapter<?> reconnectExchangeFut;

    /**
     * Partition map futures.
     * This set also contains already completed exchange futures to address race conditions when coordinator
     * leaves grid and new coordinator sends full partition message to a node which has not yet received
     * discovery event. In case if remote node will retry partition exchange, completed future will indicate
     * that full partition map should be sent to requesting node right away.
     */
    private ExchangeFutureSet exchFuts = new ExchangeFutureSet(EXCHANGE_HISTORY_SIZE);

    /** */
    private volatile IgniteCheckedException stopErr;

    /** */
    private long nextLongRunningOpsDumpTime;

    /** */
    private int longRunningOpsDumpStep;

    /** */
    private DateFormat dateFormat = new SimpleDateFormat("HH:mm:ss.SSS");

    /** Events received while cluster state transition was in progress. */
    private final List<PendingDiscoveryEvent> pendingEvts = new ArrayList<>();

    /** */
    private final GridFutureAdapter<?> crdInitFut = new GridFutureAdapter();

    /** For tests only. */
    private volatile AffinityTopologyVersion exchMergeTestWaitVer;

    /** For tests only. */
    private volatile List mergedEvtsForTest;

    /** Distributed latch manager. */
    private ExchangeLatchManager latchMgr;

    /** List of exchange aware components. */
    private final List<PartitionsExchangeAware> exchangeAwareComps = new ArrayList<>();

    /** Histogram of PME durations. */
    private volatile HistogramMetricImpl durationHistogram;

    /** Histogram of blocking PME durations. */
    private volatile HistogramMetricImpl blockingDurationHistogram;

    /** Delay before rebalancing code is start executing after exchange completion. For tests only. */
    private volatile long rebalanceDelay;

    /** Discovery listener. */
    private final DiscoveryEventListener discoLsnr = new DiscoveryEventListener() {
        @Override public void onEvent(DiscoveryEvent evt, DiscoCache cache) {
            if (!enterBusy())
                return;

            try {
                if (evt.type() == EVT_DISCOVERY_CUSTOM_EVT &&
                    (((DiscoveryCustomEvent)evt).customMessage() instanceof ChangeGlobalStateMessage)) {
                    ChangeGlobalStateMessage stateChangeMsg =
                        (ChangeGlobalStateMessage)((DiscoveryCustomEvent)evt).customMessage();

                    if (stateChangeMsg.exchangeActions() == null)
                        return;

                    onDiscoveryEvent(evt, cache);

                    return;
                }
                if (evt.type() == EVT_DISCOVERY_CUSTOM_EVT &&
                    (((DiscoveryCustomEvent)evt).customMessage() instanceof ChangeGlobalStateFinishMessage)) {
                    ChangeGlobalStateFinishMessage stateFinishMsg =
                        (ChangeGlobalStateFinishMessage)((DiscoveryCustomEvent)evt).customMessage();

                    if (stateFinishMsg.clusterActive()) {
                        for (PendingDiscoveryEvent pendingEvt : pendingEvts) {
                            if (log.isDebugEnabled())
                                log.debug("Process pending event: " + pendingEvt.event());

                            onDiscoveryEvent(pendingEvt.event(), pendingEvt.discoCache());
                        }
                    }
                    else {
                        for (PendingDiscoveryEvent pendingEvt : pendingEvts)
                            processEventInactive(pendingEvt.event(), pendingEvt.discoCache());
                    }

                    pendingEvts.clear();

                    return;
                }

                if (cache.state().transition() &&
                    (evt.type() == EVT_NODE_LEFT || evt.type() == EVT_NODE_JOINED || evt.type() == EVT_NODE_FAILED)
                ) {
                    if (log.isDebugEnabled())
                        log.debug("Adding pending event: " + evt);

                    pendingEvts.add(new PendingDiscoveryEvent(evt, cache));
                }
                else if (cache.state().active())
                    onDiscoveryEvent(evt, cache);
                else
                    processEventInactive(evt, cache);

                notifyNodeFail(evt);
            }
            finally {
                leaveBusy();
            }
        }
    };

    /**
     * @param evt Event.
     */
    private void notifyNodeFail(DiscoveryEvent evt) {
        if (evt.type() == EVT_NODE_LEFT || evt.type() == EVT_NODE_FAILED) {
            final ClusterNode n = evt.eventNode();

            assert cctx.discovery().node(n.id()) == null;

            for (GridDhtPartitionsExchangeFuture f : exchFuts.values())
                f.onNodeLeft(n);
        }
    }

    /**
     * @param evt Event.
     * @param cache Discovery data cache.
     */
    private void processEventInactive(DiscoveryEvent evt, DiscoCache cache) {
        // Clear local join caches context.
        cctx.cache().localJoinCachesContext();

        if (log.isDebugEnabled())
            log.debug("Ignore event, cluster is inactive: " + evt);
   }

    /** {@inheritDoc} */
    @Override protected void start0() throws IgniteCheckedException {
        super.start0();

        exchWorker = new ExchangeWorker();

        latchMgr = new ExchangeLatchManager(cctx.kernalContext());

        cctx.gridEvents().addDiscoveryEventListener(discoLsnr, EVT_NODE_JOINED, EVT_NODE_LEFT, EVT_NODE_FAILED,
            EVT_DISCOVERY_CUSTOM_EVT);

        cctx.io().addCacheHandler(0, GridDhtPartitionsSingleMessage.class,
            new MessageHandler<GridDhtPartitionsSingleMessage>() {
                @Override public void onMessage(final ClusterNode node, final GridDhtPartitionsSingleMessage msg) {
                    GridDhtPartitionExchangeId exchangeId = msg.exchangeId();

                    if (exchangeId != null) {
                        GridDhtPartitionsExchangeFuture fut = exchangeFuture(exchangeId);

                        boolean fastReplied = fut.fastReplyOnSingleMessage(node, msg);

                        if (fastReplied) {
                            if (log.isInfoEnabled())
                                log.info("Fast replied to single message " +
                                    "[exchId=" + exchangeId + ", nodeId=" + node.id() + "]");

                            return;
                        }
                    }

                    preprocessSingleMessage(node, msg);
                }
            });

        cctx.io().addCacheHandler(0, GridDhtPartitionsFullMessage.class,
            new MessageHandler<GridDhtPartitionsFullMessage>() {
                @Override public void onMessage(ClusterNode node, GridDhtPartitionsFullMessage msg) {
                    if (msg.exchangeId() == null) {
                        GridDhtPartitionsExchangeFuture currentExchange = lastTopologyFuture();

                        if (currentExchange != null && currentExchange.addOrMergeDelayedFullMessage(node, msg)) {
                            if (log.isInfoEnabled())
                                log.info("Delay process full message without exchange id (there is exchange in progress) [nodeId=" + node.id() + "]");

                            return;
                        }
                    }

                    processFullPartitionUpdate(node, msg);
                }
            });

        cctx.io().addCacheHandler(0, GridDhtPartitionsSingleRequest.class,
            new MessageHandler<GridDhtPartitionsSingleRequest>() {
                @Override public void onMessage(ClusterNode node, GridDhtPartitionsSingleRequest msg) {
                    processSinglePartitionRequest(node, msg);
                }
            });

        if (!cctx.kernalContext().clientNode()) {
            for (int cnt = 0; cnt < cctx.gridConfig().getRebalanceThreadPoolSize(); cnt++) {
                final int idx = cnt;

                cctx.io().addOrderedCacheGroupHandler(cctx, rebalanceTopic(cnt), new CI2<UUID, GridCacheGroupIdMessage>() {
                    @Override public void apply(final UUID id, final GridCacheGroupIdMessage m) {
                        if (!enterBusy())
                            return;

                        try {
                            CacheGroupContext grp = cctx.cache().cacheGroup(m.groupId());

                            if (grp != null) {
                                if (m instanceof GridDhtPartitionSupplyMessage) {
                                    grp.preloader().handleSupplyMessage(id, (GridDhtPartitionSupplyMessage)m);

                                    return;
                                }
                                else if (m instanceof GridDhtPartitionDemandMessage) {
                                    grp.preloader().handleDemandMessage(idx, id, (GridDhtPartitionDemandMessage)m);

                                    return;
                                }
                                else if (m instanceof GridDhtPartitionDemandLegacyMessage) {
                                    grp.preloader().handleDemandMessage(idx, id,
                                        new GridDhtPartitionDemandMessage((GridDhtPartitionDemandLegacyMessage)m));

                                    return;
                                }
                                else
                                    U.error(log, "Unsupported message type: " + m.getClass().getName());
                            }

                            U.warn(log, "Cache group with id=" + m.groupId() + " is stopped or absent");
                        }
                        finally {
                            leaveBusy();
                        }
                    }
                });
            }
        }

        MetricRegistry mreg = cctx.kernalContext().metric().registry(PME_METRICS);

        mreg.register(PME_DURATION,
            () -> currentPMEDuration(false),
            "Current PME duration in milliseconds.");

        mreg.register(PME_OPS_BLOCKED_DURATION,
            () -> currentPMEDuration(true),
            "Current PME cache operations blocked duration in milliseconds.");

        durationHistogram = mreg.findMetric(PME_DURATION_HISTOGRAM);
        blockingDurationHistogram = mreg.findMetric(PME_OPS_BLOCKED_DURATION_HISTOGRAM);
    }

    /**
     * Preprocess {@code msg} which was sended by {@code node}.
     *
     * @param node Cluster node.
     * @param msg Message.
     */
    private void preprocessSingleMessage(ClusterNode node, GridDhtPartitionsSingleMessage msg) {
        if (!crdInitFut.isDone() && !msg.restoreState()) {
            GridDhtPartitionExchangeId exchId = msg.exchangeId();

            if (log.isInfoEnabled()) {
                log.info("Waiting for coordinator initialization [node=" + node.id() +
                    ", nodeOrder=" + node.order() +
                    ", ver=" + (exchId != null ? exchId.topologyVersion() : null) + ']');
            }

            crdInitFut.listen(new CI1<IgniteInternalFuture>() {
                @Override public void apply(IgniteInternalFuture fut) {
                    processSinglePartitionUpdate(node, msg);
                }
            });

            return;
        }

        processSinglePartitionUpdate(node, msg);
    }

    /**
     *
     */
    public void onCoordinatorInitialized() {
        crdInitFut.onDone();
    }

    /**
     * Callback for local join event (needed since regular event for local join is not generated).
     *
     * @param evt Event.
     * @param cache Cache.
     */
    public void onLocalJoin(DiscoveryEvent evt, DiscoCache cache) {
        discoLsnr.onEvent(evt, cache);
    }

    /**
     * @param evt Event.
     * @param cache Discovery data cache.
     */
    private void onDiscoveryEvent(DiscoveryEvent evt, DiscoCache cache) {
        ClusterNode loc = cctx.localNode();

        assert evt.type() == EVT_NODE_JOINED || evt.type() == EVT_NODE_LEFT || evt.type() == EVT_NODE_FAILED ||
            evt.type() == EVT_DISCOVERY_CUSTOM_EVT;

        final ClusterNode n = evt.eventNode();

        GridDhtPartitionExchangeId exchId = null;
        GridDhtPartitionsExchangeFuture exchFut = null;

        if (evt.type() != EVT_DISCOVERY_CUSTOM_EVT) {
            assert evt.type() != EVT_NODE_JOINED || n.isLocal() || n.order() > loc.order() :
                "Node joined with smaller-than-local " +
                    "order [newOrder=" + n.order() + ", locOrder=" + loc.order() + ", evt=" + evt + ']';

            exchId = exchangeId(n.id(), affinityTopologyVersion(evt), evt);

            ExchangeActions exchActs = null;

            boolean locJoin = evt.type() == EVT_NODE_JOINED && evt.eventNode().isLocal();

            if (locJoin) {
                LocalJoinCachesContext locJoinCtx = cctx.cache().localJoinCachesContext();

                if (locJoinCtx != null) {
                    exchActs = new ExchangeActions();

                    exchActs.localJoinContext(locJoinCtx);
                }
            }

            if (!n.isClient() && !n.isDaemon())
                exchActs = cctx.kernalContext().state().autoAdjustExchangeActions(exchActs);

            exchFut = exchangeFuture(exchId, evt, cache, exchActs, null);
        }
        else {
            DiscoveryCustomMessage customMsg = ((DiscoveryCustomEvent)evt).customMessage();

            if (customMsg instanceof ChangeGlobalStateMessage) {
                ChangeGlobalStateMessage stateChangeMsg = (ChangeGlobalStateMessage)customMsg;

                ExchangeActions exchActions = stateChangeMsg.exchangeActions();

                if (exchActions != null) {
                    exchId = exchangeId(n.id(), affinityTopologyVersion(evt), evt);

                    exchFut = exchangeFuture(exchId, evt, cache, exchActions, null);

                    exchFut.listen(f -> onClusterStateChangeFinish(f, exchActions));
                }
            }
            else if (customMsg instanceof DynamicCacheChangeBatch) {
                DynamicCacheChangeBatch batch = (DynamicCacheChangeBatch)customMsg;

                ExchangeActions exchActions = batch.exchangeActions();

                if (exchActions != null) {
                    exchId = exchangeId(n.id(), affinityTopologyVersion(evt), evt);

                    exchFut = exchangeFuture(exchId, evt, cache, exchActions, null);
                }
            }
            else if (customMsg instanceof CacheAffinityChangeMessage) {
                CacheAffinityChangeMessage msg = (CacheAffinityChangeMessage)customMsg;

                if (msg.exchangeId() == null) {
                    if (msg.exchangeNeeded()) {
                        exchId = exchangeId(n.id(), affinityTopologyVersion(evt), evt);

                        exchFut = exchangeFuture(exchId, evt, cache, null, msg);
                    }
                }
                else if (msg.exchangeId().topologyVersion().topologyVersion() >= cctx.discovery().localJoinEvent().topologyVersion())
                    exchangeFuture(msg.exchangeId(), null, null, null, null)
                        .onAffinityChangeMessage(evt.eventNode(), msg);
            }
            else if (customMsg instanceof DynamicCacheChangeFailureMessage) {
                DynamicCacheChangeFailureMessage msg = (DynamicCacheChangeFailureMessage) customMsg;

                if (msg.exchangeId().topologyVersion().topologyVersion() >=
                    affinityTopologyVersion(cctx.discovery().localJoinEvent()).topologyVersion())
                    exchangeFuture(msg.exchangeId(), null, null, null, null)
                        .onDynamicCacheChangeFail(evt.eventNode(), msg);
            }
            else if (customMsg instanceof SnapshotDiscoveryMessage
                && ((SnapshotDiscoveryMessage) customMsg).needExchange()) {
                exchId = exchangeId(n.id(), affinityTopologyVersion(evt), evt);

                exchFut = exchangeFuture(exchId, evt, null, null, null);
            }
            else if (customMsg instanceof WalStateAbstractMessage
                && ((WalStateAbstractMessage)customMsg).needExchange()) {
                exchId = exchangeId(n.id(), affinityTopologyVersion(evt), evt);

                exchFut = exchangeFuture(exchId, evt, null, null, null);
            }
            else {
                // Process event as custom discovery task if needed.
                CachePartitionExchangeWorkerTask task =
                    cctx.cache().exchangeTaskForCustomDiscoveryMessage(customMsg);

                if (task != null)
                    exchWorker.addCustomTask(task);
            }
        }

        if (exchId != null) {
            if (log.isDebugEnabled())
                log.debug("Discovery event (will start exchange): " + exchId);

            // Event callback - without this callback future will never complete.
            exchFut.onEvent(exchId, evt, cache);

            // Start exchange process.
            addFuture(exchFut);
        }
        else {
            if (log.isDebugEnabled())
                log.debug("Do not start exchange for discovery event: " + evt);
        }

        notifyNodeFail(evt);

        // Notify indexing engine about node leave so that we can re-map coordinator accordingly.
        if (evt.type() == EVT_NODE_LEFT || evt.type() == EVT_NODE_FAILED) {
            exchWorker.addCustomTask(new SchemaNodeLeaveExchangeWorkerTask(evt.eventNode()));
            exchWorker.addCustomTask(new WalStateNodeLeaveExchangeTask(evt.eventNode()));
        }
    }

    /** */
    private void onClusterStateChangeFinish(IgniteInternalFuture<AffinityTopologyVersion> fut, ExchangeActions exchActions) {
        A.notNull(exchActions, "exchActions");

        GridEventStorageManager evtMngr = cctx.kernalContext().event();

        if (exchActions.activate() && evtMngr.isRecordable(EVT_CLUSTER_ACTIVATED) ||
            exchActions.deactivate() && evtMngr.isRecordable(EVT_CLUSTER_DEACTIVATED) ||
            exchActions.changedClusterState() && evtMngr.isRecordable(EVT_CLUSTER_STATE_CHANGED)
        ) {
            List<Event> evts = new ArrayList<>(2);

            ClusterNode locNode = cctx.kernalContext().discovery().localNode();

            Collection<BaselineNode> bltNodes = cctx.kernalContext().cluster().get().currentBaselineTopology();

            boolean collectionUsed = false;

            if (exchActions.activate() && evtMngr.isRecordable(EVT_CLUSTER_ACTIVATED)) {
                assert !exchActions.deactivate() : exchActions;

                collectionUsed = true;

                evts.add(new ClusterActivationEvent(locNode, "Cluster activated.", EVT_CLUSTER_ACTIVATED, bltNodes));
            }

            if (exchActions.deactivate() && evtMngr.isRecordable(EVT_CLUSTER_DEACTIVATED)) {
                assert !exchActions.activate() : exchActions;

                collectionUsed = true;

                evts.add(new ClusterActivationEvent(locNode, "Cluster deactivated.", EVT_CLUSTER_DEACTIVATED, bltNodes));
            }

            if (exchActions.changedClusterState() && evtMngr.isRecordable(EVT_CLUSTER_STATE_CHANGED)) {
                StateChangeRequest req = exchActions.stateChangeRequest();

                if (collectionUsed && bltNodes != null)
                    bltNodes = new ArrayList<>(bltNodes);

                evts.add(new ClusterStateChangeEvent(req.prevState(), req.state(), bltNodes, locNode, "Cluster state changed."));
            }

            A.notEmpty(evts, "events " + exchActions);

            cctx.kernalContext().getSystemExecutorService()
                .submit(() -> evts.forEach(e -> cctx.kernalContext().event().record(e)));
        }
    }

    /**
     * @param task Task to run in exchange worker thread.
     */
    void addCustomTask(CachePartitionExchangeWorkerTask task) {
        assert task != null;

        exchWorker.addCustomTask(task);
    }

    /**
     * @return Reconnect partition exchange future.
     */
    public IgniteInternalFuture<?> reconnectExchangeFuture() {
        return reconnectExchangeFut;
    }

    /**
     * @return Initial exchange ID.
     */
    private GridDhtPartitionExchangeId initialExchangeId() {
        DiscoveryEvent discoEvt = cctx.discovery().localJoinEvent();

        assert discoEvt != null;

        final AffinityTopologyVersion startTopVer = affinityTopologyVersion(discoEvt);

        assert discoEvt.topologyVersion() == startTopVer.topologyVersion();

        return exchangeId(cctx.localNode().id(), startTopVer, discoEvt);
    }

    /**
     * @param active Cluster state.
     * @param reconnect Reconnect flag.
     * @return Topology version of local join exchange if cluster is active.
     *         Topology version NONE if cluster is not active or reconnect.
     * @throws IgniteCheckedException If failed.
     */
    public AffinityTopologyVersion onKernalStart(boolean active, boolean reconnect) throws IgniteCheckedException {
        for (ClusterNode n : cctx.discovery().remoteNodes())
            cctx.versions().onReceived(n.id(), n.metrics().getLastDataVersion());

        DiscoveryLocalJoinData locJoin = cctx.discovery().localJoin();

        GridDhtPartitionsExchangeFuture fut = null;

        if (reconnect)
            reconnectExchangeFut = new GridFutureAdapter<>();

        if (active) {
            DiscoveryEvent discoEvt = locJoin.event();
            DiscoCache discoCache = locJoin.discoCache();

            GridDhtPartitionExchangeId exchId = initialExchangeId();

            fut = exchangeFuture(
                exchId,
                reconnect ? null : discoEvt,
                reconnect ? null : discoCache,
                null,
                null);
        }
        else if (reconnect)
            reconnectExchangeFut.onDone();

        new IgniteThread(cctx.igniteInstanceName(), "exchange-worker", exchWorker).start();

        if (reconnect) {
            if (fut != null) {
                fut.listen(new CI1<IgniteInternalFuture<AffinityTopologyVersion>>() {
                    @Override public void apply(IgniteInternalFuture<AffinityTopologyVersion> fut) {
                        try {
                            fut.get();

                            for (CacheGroupContext grp : cctx.cache().cacheGroups())
                                grp.preloader().onInitialExchangeComplete(null);

                            reconnectExchangeFut.onDone();
                        }
                        catch (IgniteCheckedException e) {
                            for (CacheGroupContext grp : cctx.cache().cacheGroups())
                                grp.preloader().onInitialExchangeComplete(e);

                            reconnectExchangeFut.onDone(e);
                        }
                    }
                });
            }
        }
        else if (fut != null) {
            if (log.isDebugEnabled())
                log.debug("Beginning to wait on local exchange future: " + fut);

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
                catch (IgniteNeedReconnectException e) {
                    throw e;
                }
                catch (Exception e) {
                    if (fut.reconnectOnError(e))
                        throw new IgniteNeedReconnectException(cctx.localNode(), e);

                    throw e;
                }
            }

            for (CacheGroupContext grp : cctx.cache().cacheGroups()) {
                if (locJoin.joinTopologyVersion().equals(grp.localStartVersion()))
                    grp.preloader().onInitialExchangeComplete(null);
            }

            if (log.isDebugEnabled())
                log.debug("Finished waiting for initial exchange: " + fut.exchangeId());

            return fut.initialVersion();
        }

        return NONE;
    }

    /**
     * @param ver Node version.
     * @return Supported exchange protocol version.
     */
    public static int exchangeProtocolVersion(IgniteProductVersion ver) {
        if (ver.compareToIgnoreTimestamp(EXCHANGE_PROTOCOL_2_SINCE) >= 0)
            return 2;

        return 1;
    }

    /**
     * @param idx Index.
     * @return Topic for index.
     */
    public static Object rebalanceTopic(int idx) {
        return TOPIC_CACHE.topic("Rebalance", idx);
    }

    /** {@inheritDoc} */
    @Override protected void onKernalStop0(boolean cancel) {
        exchWorker.onKernalStop();

        cctx.gridEvents().removeDiscoveryEventListener(discoLsnr);

        cctx.io().removeHandler(false, 0, GridDhtPartitionsSingleMessage.class);
        cctx.io().removeHandler(false, 0, GridDhtPartitionsFullMessage.class);
        cctx.io().removeHandler(false, 0, GridDhtPartitionsSingleRequest.class);

        stopErr = cctx.kernalContext().clientDisconnected() ?
            new IgniteClientDisconnectedCheckedException(cctx.kernalContext().cluster().clientReconnectFuture(),
                "Client node disconnected: " + cctx.igniteInstanceName()) :
            new NodeStoppingException("Node is stopping: " + cctx.igniteInstanceName());

        // Stop exchange worker
        U.cancel(exchWorker);

        if (log.isDebugEnabled())
            log.debug("Before joining on exchange worker: " + exchWorker);

        U.join(exchWorker, log);

        // Finish all exchange futures.
        ExchangeFutureSet exchFuts0 = exchFuts;

        for (CachePartitionExchangeWorkerTask task : exchWorker.futQ) {
            if (task instanceof GridDhtPartitionsExchangeFuture)
                ((GridDhtPartitionsExchangeFuture)task).onDone(stopErr);
        }

        if (exchFuts0 != null) {
            for (GridDhtPartitionsExchangeFuture f : exchFuts.values())
                f.onDone(stopErr);
        }

        for (AffinityReadyFuture f : readyFuts.values())
            f.onDone(stopErr);

        GridDhtPartitionsExchangeFuture lastFut = lastInitializedFut;

        if (lastFut != null)
            lastFut.onDone(stopErr);

        if (!cctx.kernalContext().clientNode()) {
            for (int cnt = 0; cnt < cctx.gridConfig().getRebalanceThreadPoolSize(); cnt++)
                cctx.io().removeOrderedHandler(true, rebalanceTopic(cnt));
        }

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

        exchFuts.clear();
    }

    /**
     * @param grpId Cache group ID.
     * @return Topology.
     */
    @Nullable public GridDhtPartitionTopology clientTopologyIfExists(int grpId) {
        return clientTops.get(grpId);
    }

    /**
     * @param grpId Cache group ID.
     * @param discoCache Discovery data cache.
     * @return Topology.
     */
    public GridDhtPartitionTopology clientTopology(int grpId, DiscoCache discoCache) {
        GridClientPartitionTopology top = clientTops.get(grpId);

        if (top != null)
            return top;

        CacheGroupDescriptor grpDesc = cctx.affinity().cacheGroups().get(grpId);

        assert grpDesc != null : grpId;

        CacheConfiguration<?, ?> ccfg = grpDesc.config();

        AffinityFunction aff = ccfg.getAffinity();

        Object affKey = cctx.kernalContext().affinity().similaryAffinityKey(aff,
            ccfg.getNodeFilter(),
            ccfg.getBackups(),
            aff.partitions());

        GridClientPartitionTopology old = clientTops.putIfAbsent(grpId,
            top = new GridClientPartitionTopology(cctx, discoCache, grpId, aff.partitions(), affKey));

        return old != null ? old : top;
    }

    /**
     * @return Collection of client topologies.
     */
    public Collection<GridClientPartitionTopology> clientTopologies() {
        return clientTops.values();
    }

    /**
     * @param grpId Cache group ID.
     * @return Client partition topology.
     */
    public GridClientPartitionTopology clearClientTopology(int grpId) {
        return clientTops.remove(grpId);
    }

    /**
     * @return Topology version of latest completed partition exchange.
     */
    public AffinityTopologyVersion readyAffinityVersion() {
        return exchFuts.readyTopVer();
    }

    /**
     * @return Latest rebalance topology version or {@code NONE} if there is no info.
     */
    public AffinityTopologyVersion rebalanceTopologyVersion() {
        return rebTopVer;
    }

    /**
     * @return Last initialized topology future.
     */
    public GridDhtPartitionsExchangeFuture lastTopologyFuture() {
        return lastInitializedFut;
    }

    /**
     * @return Last finished topology future.
     */
    @Nullable public GridDhtPartitionsExchangeFuture lastFinishedFuture() {
        return lastFinishedFut.get();
    }

    /**
     * @param fut Finished future.
     */
    public void lastFinishedFuture(GridDhtPartitionsExchangeFuture fut) {
        assert fut != null && fut.isDone() : fut;

        while (true) {
            GridDhtPartitionsExchangeFuture cur = lastFinishedFut.get();

            if (fut.topologyVersion() != null && (cur == null || fut.topologyVersion().compareTo(cur.topologyVersion()) > 0)) {
                if (lastFinishedFut.compareAndSet(cur, fut))
                    break;
            }
            else
                break;
        }
    }

    /**
     * @param ver Topology version.
     * @return Future or {@code null} is future is already completed.
     */
    @NotNull public IgniteInternalFuture<AffinityTopologyVersion> affinityReadyFuture(AffinityTopologyVersion ver) {
        GridDhtPartitionsExchangeFuture lastInitializedFut0 = lastInitializedFut;

        if (lastInitializedFut0 != null && lastInitializedFut0.initialVersion().compareTo(ver) == 0
            && lastInitializedFut0.changedAffinity()) {
            if (log.isTraceEnabled())
                log.trace("Return lastInitializedFut for topology ready future " +
                    "[ver=" + ver + ", fut=" + lastInitializedFut0 + ']');

            return lastInitializedFut0;
        }

        AffinityTopologyVersion topVer = exchFuts.readyTopVer();

        if (topVer.compareTo(ver) >= 0) {
            if (log.isTraceEnabled())
                log.trace("Return finished future for topology ready future [ver=" + ver + ", topVer=" + topVer + ']');

            return new GridFinishedFuture<>(topVer);
        }

        GridFutureAdapter<AffinityTopologyVersion> fut = F.addIfAbsent(readyFuts, ver,
            new AffinityReadyFuture(ver));

        if (log.isDebugEnabled())
            log.debug("Created topology ready future [ver=" + ver + ", fut=" + fut + ']');

        topVer = exchFuts.readyTopVer();

        if (topVer.compareTo(ver) >= 0) {
            if (log.isTraceEnabled())
                log.trace("Completing created topology ready future " +
                    "[ver=" + topVer + ", topVer=" + topVer + ", fut=" + fut + ']');

            fut.onDone(topVer);
        }
        else if (stopErr != null)
            fut.onDone(stopErr);

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
    public List<GridDhtPartitionsExchangeFuture> exchangeFutures() {
        return exchFuts.values();
    }

    /**
     * @return {@code True} if pending future queue contains exchange task.
     */
    public boolean hasPendingExchange() {
        return exchWorker.hasPendingExchange();
    }

    /**
     * @return {@code True} if pending future queue contains server exchange task.
     */
    public boolean hasPendingServerExchange() {
        return exchWorker.hasPendingServerExchange();
    }

    /**
     *
     * @param topVer Topology version.
     * @return Last topology version before the provided one when affinity was modified.
     */
    public AffinityTopologyVersion lastAffinityChangedTopologyVersion(AffinityTopologyVersion topVer) {
        if (topVer.topologyVersion() <= 0)
            return topVer;

        AffinityTopologyVersion lastAffTopVer = lastAffTopVers.get(topVer);

        return lastAffTopVer != null ? lastAffTopVer : topVer;
    }

    /**
     *
     * @param topVer Topology version.
     * @param lastAffTopVer Last topology version before the provided one when affinity was modified.
     * @return {@code True} if data was modified.
     */
    public boolean lastAffinityChangedTopologyVersion(AffinityTopologyVersion topVer, AffinityTopologyVersion lastAffTopVer) {
        assert lastAffTopVer.compareTo(topVer) <= 0;

        if (lastAffTopVer.topologyVersion() <= 0 || lastAffTopVer.equals(topVer))
            return false;

        while (true) {
            AffinityTopologyVersion old = lastAffTopVers.putIfAbsent(topVer, lastAffTopVer);

            if (old == null)
                return true;

            if (lastAffTopVer.compareTo(old) < 0) {
                if (lastAffTopVers.replace(topVer, old, lastAffTopVer))
                    return true;
            }
            else
                return false;
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
     * @param exchId Exchange ID.
     */
    public void forceReassign(GridDhtPartitionExchangeId exchId) {
        exchWorker.forceReassign(exchId);
    }

    /**
     * @param exchId Exchange ID.
     * @return Rebalance future.
     */
    public IgniteInternalFuture<Boolean> forceRebalance(GridDhtPartitionExchangeId exchId) {
        return exchWorker.forceRebalance(exchId);
    }

    /**
     * @param caches Caches to stop.
     * @return Future that will be completed when caches are stopped from the exchange thread.
     */
    public IgniteInternalFuture<Void> deferStopCachesOnClientReconnect(Collection<GridCacheAdapter> caches) {
        assert cctx.discovery().localNode().isClient();

        return exchWorker.deferStopCachesOnClientReconnect(caches);
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
     * Registers component that will be notified on every partition map exchange.
     *
     * @param comp Component to be registered.
     */
    public void registerExchangeAwareComponent(PartitionsExchangeAware comp) {
        exchangeAwareComps.add(comp);
    }

    /**
     * Removes exchange aware component from list of listeners.
     *
     * @param comp Component to be registered.
     */
    public void unregisterExchangeAwareComponent(PartitionsExchangeAware comp) {
        exchangeAwareComps.remove(comp);
    }

    /**
     * @return List of registered exchange listeners.
     */
    public List<PartitionsExchangeAware> exchangeAwareComponents() {
        return U.sealList(exchangeAwareComps);
    }

    /**
     * Partition refresh callback for selected cache groups.
     * For coordinator causes {@link GridDhtPartitionsFullMessage FullMessages} send,
     * for non coordinator -  {@link GridDhtPartitionsSingleMessage SingleMessages} send
     *
     * @param grps Cache groups for partitions refresh.
     */
    public void refreshPartitions(@NotNull Collection<CacheGroupContext> grps) {
        // TODO https://issues.apache.org/jira/browse/IGNITE-6857
        if (cctx.snapshot().snapshotOperationInProgress()) {
            if (log.isDebugEnabled())
                log.debug("Schedule resend parititions due to snapshot in progress");

            scheduleResendPartitions();

            return;
        }

        if (grps.isEmpty()) {
            if (log.isDebugEnabled())
                log.debug("Skip partitions refresh, there are no cache groups for partition refresh.");

            return;
        }

        ClusterNode oldest = cctx.discovery().oldestAliveServerNode(NONE);

        if (oldest == null) {
            if (log.isDebugEnabled())
                log.debug("Skip partitions refresh, there are no server nodes [loc=" + cctx.localNodeId() + ']');

            return;
        }

        if (log.isDebugEnabled()) {
            log.debug("Refreshing partitions [oldest=" + oldest.id() + ", loc=" + cctx.localNodeId() +
                ", cacheGroups= " + grps + ']');
        }

        // If this is the oldest node.
        if (oldest.id().equals(cctx.localNodeId())) {
            // Check rebalance state & send CacheAffinityChangeMessage if need.
            for (CacheGroupContext grp : cctx.cache().cacheGroups()) {
                if (!grp.isLocal()) {
                    GridDhtPartitionTopology top = grp.topology();

                    if (top != null)
                        cctx.affinity().checkRebalanceState(top, grp.groupId());
                }
            }

            GridDhtPartitionsExchangeFuture lastFut = lastInitializedFut;

            // No need to send to nodes which did not finish their first exchange.
            AffinityTopologyVersion rmtTopVer =
                lastFut != null ?
                    (lastFut.isDone() ? lastFut.topologyVersion() : lastFut.initialVersion())
                    : AffinityTopologyVersion.NONE;

            Collection<ClusterNode> rmts = cctx.discovery().remoteAliveNodesWithCaches(rmtTopVer);

            if (log.isDebugEnabled())
                log.debug("Refreshing partitions from oldest node: " + cctx.localNodeId());

            sendAllPartitions(rmts, rmtTopVer, grps);
        }
        else {
            if (log.isDebugEnabled())
                log.debug("Refreshing local partitions from non-oldest node: " +
                    cctx.localNodeId());

            sendLocalPartitions(oldest, null, grps);
        }
    }

    /**
     * Partition refresh callback.
     * For coordinator causes {@link GridDhtPartitionsFullMessage FullMessages} send,
     * for non coordinator -  {@link GridDhtPartitionsSingleMessage SingleMessages} send
     */
    public void refreshPartitions() { refreshPartitions(cctx.cache().cacheGroups()); }

    /**
     * @param nodes Target Nodes.
     * @param msgTopVer Topology version. Will be added to full message.
     * @param grps Selected cache groups.
     */
    private void sendAllPartitions(
        Collection<ClusterNode> nodes,
        AffinityTopologyVersion msgTopVer,
        Collection<CacheGroupContext> grps
    ) {
        long time = System.currentTimeMillis();

        GridDhtPartitionsFullMessage m = createPartitionsFullMessage(true, false, null, null, null, null, grps);

        m.topologyVersion(msgTopVer);

        if (log.isInfoEnabled()) {
            long latency = System.currentTimeMillis() - time;

            if (latency > 50 || log.isDebugEnabled()) {
                log.info("Finished full message creation [msgTopVer=" + msgTopVer + ", groups=" + grps +
                    ", latency=" + latency + "ms]");
            }
        }

        if (log.isTraceEnabled())
            log.trace("Sending all partitions [nodeIds=" + U.nodeIds(nodes) + ", cacheGroups=" + grps +
                ", msg=" + m + ']');

        time = System.currentTimeMillis();

        Collection<ClusterNode> failedNodes = U.newHashSet(nodes.size());

        for (ClusterNode node : nodes) {
            try {
                assert !node.equals(cctx.localNode());

                cctx.io().sendNoRetry(node, m, SYSTEM_POOL);
            }
            catch (ClusterTopologyCheckedException ignore) {
                if (log.isDebugEnabled()) {
                    log.debug("Failed to send partition update to node because it left grid (will ignore) " +
                        "[node=" + node.id() + ", msg=" + m + ']');
                }
            }
            catch (IgniteCheckedException e) {
                failedNodes.add(node);

                U.warn(log, "Failed to send partitions full message [node=" + node + ", err=" + e + ']', e);
            }
        }

        if (log.isInfoEnabled()) {
            long latency = System.currentTimeMillis() - time;

            if (latency > 50 || log.isDebugEnabled()) {
                log.info("Finished sending full message [msgTopVer=" + msgTopVer + ", groups=" + grps +
                    (failedNodes.isEmpty() ? "" : (", skipped=" + U.nodeIds(failedNodes))) +
                    ", latency=" + latency + "ms]");
            }
        }
    }

    /**
     * Creates partitions full message for all cache groups.
     *
     * @param compress {@code True} if possible to compress message (properly work only if prepareMarshall/
     * finishUnmarshall methods are called).
     * @param newCntrMap {@code True} if possible to use {@link CachePartitionFullCountersMap}.
     * @param exchId Non-null exchange ID if message is created for exchange.
     * @param lastVer Last version.
     * @param partHistSuppliers Partition history suppliers map.
     * @param partsToReload Partitions to reload map.
     * @return Message.
     */
    public GridDhtPartitionsFullMessage createPartitionsFullMessage(
        boolean compress,
        boolean newCntrMap,
        @Nullable final GridDhtPartitionExchangeId exchId,
        @Nullable GridCacheVersion lastVer,
        @Nullable IgniteDhtPartitionHistorySuppliersMap partHistSuppliers,
        @Nullable IgniteDhtPartitionsToReloadMap partsToReload
    ) {
        Collection<CacheGroupContext> grps = cctx.cache().cacheGroups();

        return createPartitionsFullMessage(compress, newCntrMap, exchId, lastVer, partHistSuppliers, partsToReload, grps);
    }

    /**
     * Creates partitions full message for selected cache groups.
     *
     * @param compress {@code True} if possible to compress message (properly work only if prepareMarshall/
     *     finishUnmarshall methods are called).
     * @param newCntrMap {@code True} if possible to use {@link CachePartitionFullCountersMap}.
     * @param exchId Non-null exchange ID if message is created for exchange.
     * @param lastVer Last version.
     * @param partHistSuppliers Partition history suppliers map.
     * @param partsToReload Partitions to reload map.
     * @param grps Selected cache groups.
     * @return Message.
     */
    public GridDhtPartitionsFullMessage createPartitionsFullMessage(
        boolean compress,
        boolean newCntrMap,
        @Nullable final GridDhtPartitionExchangeId exchId,
        @Nullable GridCacheVersion lastVer,
        @Nullable IgniteDhtPartitionHistorySuppliersMap partHistSuppliers,
        @Nullable IgniteDhtPartitionsToReloadMap partsToReload,
        Collection<CacheGroupContext> grps
    ) {
        AffinityTopologyVersion ver = exchId != null ? exchId.topologyVersion() : AffinityTopologyVersion.NONE;

        final GridDhtPartitionsFullMessage m =
            new GridDhtPartitionsFullMessage(exchId, lastVer, ver, partHistSuppliers, partsToReload);

        m.compressed(compress);

        final Map<Object, T2<Integer, GridDhtPartitionFullMap>> dupData = new HashMap<>();

        Map<Integer, Map<Integer, Long>> partsSizes = new HashMap<>();

        for (CacheGroupContext grp : grps) {
            if (!grp.isLocal()) {
                if (exchId != null) {
                    AffinityTopologyVersion startTopVer = grp.localStartVersion();

                    if (startTopVer.compareTo(exchId.topologyVersion()) > 0)
                        continue;
                }

                GridAffinityAssignmentCache affCache = grp.affinity();

                GridDhtPartitionFullMap locMap = grp.topology().partitionMap(true);

                if (locMap != null)
                    addFullPartitionsMap(m, dupData, compress, grp.groupId(), locMap, affCache.similarAffinityKey());

                Map<Integer, Long> partSizesMap = grp.topology().globalPartSizes();

                if (!partSizesMap.isEmpty())
                    partsSizes.put(grp.groupId(), partSizesMap);

                if (exchId != null) {
                    CachePartitionFullCountersMap cntrsMap = grp.topology().fullUpdateCounters();

                    if (newCntrMap)
                        m.addPartitionUpdateCounters(grp.groupId(), cntrsMap);
                    else {
                        m.addPartitionUpdateCounters(grp.groupId(),
                            CachePartitionFullCountersMap.toCountersMap(cntrsMap));
                    }
                }
            }
        }

        // It is important that client topologies be added after contexts.
        for (GridClientPartitionTopology top : cctx.exchange().clientTopologies()) {
            GridDhtPartitionFullMap map = top.partitionMap(true);

            if (map != null)
                addFullPartitionsMap(m, dupData, compress, top.groupId(), map, top.similarAffinityKey());

            if (exchId != null) {
                CachePartitionFullCountersMap cntrsMap = top.fullUpdateCounters();

                if (newCntrMap)
                    m.addPartitionUpdateCounters(top.groupId(), cntrsMap);
                else
                    m.addPartitionUpdateCounters(top.groupId(), CachePartitionFullCountersMap.toCountersMap(cntrsMap));

                Map<Integer, Long> partSizesMap = top.globalPartSizes();

                if (!partSizesMap.isEmpty())
                    partsSizes.put(top.groupId(), partSizesMap);
            }
        }

        if (!partsSizes.isEmpty())
            m.partitionSizes(cctx, partsSizes);

        return m;
    }

    /**
     * @param m Message.
     * @param dupData Duplicated data map.
     * @param compress {@code True} if need check for duplicated partition state data.
     * @param grpId Cache group ID.
     * @param map Map to add.
     * @param affKey Cache affinity key.
     */
    private void addFullPartitionsMap(GridDhtPartitionsFullMessage m,
        Map<Object, T2<Integer, GridDhtPartitionFullMap>> dupData,
        boolean compress,
        Integer grpId,
        GridDhtPartitionFullMap map,
        Object affKey) {
        assert map != null;

        Integer dupDataCache = null;

        if (compress && affKey != null && !m.containsGroup(grpId)) {
            T2<Integer, GridDhtPartitionFullMap> state0 = dupData.get(affKey);

            if (state0 != null && state0.get2().partitionStateEquals(map)) {
                GridDhtPartitionFullMap map0 = new GridDhtPartitionFullMap(map.nodeId(),
                    map.nodeOrder(),
                    map.updateSequence());

                for (Map.Entry<UUID, GridDhtPartitionMap> e : map.entrySet())
                    map0.put(e.getKey(), e.getValue().emptyCopy());

                map = map0;

                dupDataCache = state0.get1();
            }
            else
                dupData.put(affKey, new T2<>(grpId, map));
        }

        m.addFullPartitionsMap(grpId, map, dupDataCache);
    }

    /**
     * @param node Destination cluster node.
     * @param id Exchange ID.
     * @param grps Cache groups for send partitions.
     */
    private void sendLocalPartitions(
        ClusterNode node,
        @Nullable GridDhtPartitionExchangeId id,
        @NotNull Collection<CacheGroupContext> grps
    ) {
        GridDhtPartitionsSingleMessage m =
            createPartitionsSingleMessage(id,
                cctx.kernalContext().clientNode(),
                false,
                node.version().compareToIgnoreTimestamp(PARTIAL_COUNTERS_MAP_SINCE) >= 0,
                null,
                grps);

        if (log.isTraceEnabled())
            log.trace("Sending local partitions [nodeId=" + node.id() + ", msg=" + m + ']');

        try {
            cctx.io().sendNoRetry(node, m, SYSTEM_POOL);
        }
        catch (ClusterTopologyCheckedException ignore) {
            if (log.isDebugEnabled())
                log.debug("Failed to send partition update to node because it left grid (will ignore) [node=" +
                    node.id() + ", msg=" + m + ']');
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Failed to send local partition map to node [node=" + node + ", exchId=" + id + ']', e);
        }
    }

    /**
     * Creates partitions single message for all cache groups.
     *
     * @param exchangeId Exchange ID.
     * @param clientOnlyExchange Client exchange flag.
     * @param sndCounters {@code True} if need send partition update counters.
     * @param newCntrMap {@code True} if possible to use {@link CachePartitionPartialCountersMap}.
     * @return Message.
     */
    public GridDhtPartitionsSingleMessage createPartitionsSingleMessage(
        @Nullable GridDhtPartitionExchangeId exchangeId,
        boolean clientOnlyExchange,
        boolean sndCounters,
        boolean newCntrMap,
        ExchangeActions exchActions
    ) {
        Collection<CacheGroupContext> grps = cctx.cache().cacheGroups();

        return createPartitionsSingleMessage(exchangeId, clientOnlyExchange, sndCounters, newCntrMap, exchActions, grps);
    }

    /**
     * Creates partitions single message for selected cache groups.
     *
     * @param exchangeId Exchange ID.
     * @param clientOnlyExchange Client exchange flag.
     * @param sndCounters {@code True} if need send partition update counters.
     * @param newCntrMap {@code True} if possible to use {@link CachePartitionPartialCountersMap}.
     * @param grps Selected cache groups.
     * @return Message.
     */
    public GridDhtPartitionsSingleMessage createPartitionsSingleMessage(
        @Nullable GridDhtPartitionExchangeId exchangeId,
        boolean clientOnlyExchange,
        boolean sndCounters,
        boolean newCntrMap,
        ExchangeActions exchActions,
        Collection<CacheGroupContext> grps
    ) {
        GridDhtPartitionsSingleMessage m = new GridDhtPartitionsSingleMessage(exchangeId,
            clientOnlyExchange,
            cctx.versions().last(),
            true);

        Map<Object, T2<Integer, GridPartitionStateMap>> dupData = new HashMap<>();

        for (CacheGroupContext grp : grps) {
            if (!grp.isLocal() && (exchActions == null || !exchActions.cacheGroupStopping(grp.groupId()))) {
                GridDhtPartitionMap locMap = grp.topology().localPartitionMap();

                addPartitionMap(m,
                    dupData,
                    true,
                    grp.groupId(),
                    locMap,
                    grp.affinity().similarAffinityKey());

                if (sndCounters) {
                    CachePartitionPartialCountersMap cntrsMap = grp.topology().localUpdateCounters(true);

                    m.addPartitionUpdateCounters(grp.groupId(),
                        newCntrMap ? cntrsMap : CachePartitionPartialCountersMap.toCountersMap(cntrsMap));
                }

                m.addPartitionSizes(grp.groupId(), grp.topology().partitionSizes());
            }
        }

        for (GridClientPartitionTopology top : clientTops.values()) {
            if (m.partitions() != null && m.partitions().containsKey(top.groupId()))
                continue;

            GridDhtPartitionMap locMap = top.localPartitionMap();

            addPartitionMap(m,
                dupData,
                true,
                top.groupId(),
                locMap,
                top.similarAffinityKey());

            if (sndCounters) {
                CachePartitionPartialCountersMap cntrsMap = top.localUpdateCounters(true);

                m.addPartitionUpdateCounters(top.groupId(),
                    newCntrMap ? cntrsMap : CachePartitionPartialCountersMap.toCountersMap(cntrsMap));
            }

            m.addPartitionSizes(top.groupId(), top.partitionSizes());
        }

        return m;
    }

    /**
     * @param m Message.
     * @param dupData Duplicated data map.
     * @param compress {@code True} if need check for duplicated partition state data.
     * @param cacheId Cache ID.
     * @param map Map to add.
     * @param affKey Cache affinity key.
     */
    private void addPartitionMap(GridDhtPartitionsSingleMessage m,
        Map<Object, T2<Integer, GridPartitionStateMap>> dupData,
        boolean compress,
        Integer cacheId,
        GridDhtPartitionMap map,
        Object affKey) {
        Integer dupDataCache = null;

        if (compress) {
            T2<Integer, GridPartitionStateMap> state0 = dupData.get(affKey);

            if (state0 != null && state0.get2().equals(map.map())) {
                dupDataCache = state0.get1();

                map = map.emptyCopy();
            }
            else
                dupData.put(affKey, new T2<>(cacheId, map.map()));
        }

        m.addLocalPartitionMap(cacheId, map, dupDataCache);
    }

    /**
     * @param nodeId Cause node ID.
     * @param topVer Topology version.
     * @param evt Event.
     * @return Exchange ID instance.
     */
    private GridDhtPartitionExchangeId exchangeId(UUID nodeId, AffinityTopologyVersion topVer, DiscoveryEvent evt) {
        return new GridDhtPartitionExchangeId(nodeId, evt, topVer);
    }

    /**
     * Gets exchange future by exchange id.
     *
     * @param exchId Exchange id.
     */
    private GridDhtPartitionsExchangeFuture exchangeFuture(@NotNull GridDhtPartitionExchangeId exchId) {
        return exchangeFuture(exchId, null, null, null, null);
    }

    /**
     * @param exchId Exchange ID.
     * @param discoEvt Discovery event.
     * @param cache Discovery data cache.
     * @param exchActions Cache change actions.
     * @param affChangeMsg Affinity change message.
     * @return Exchange future.
     */
    private GridDhtPartitionsExchangeFuture exchangeFuture(
        @NotNull GridDhtPartitionExchangeId exchId,
        @Nullable DiscoveryEvent discoEvt,
        @Nullable DiscoCache cache,
        @Nullable ExchangeActions exchActions,
        @Nullable CacheAffinityChangeMessage affChangeMsg
    ) {
        GridDhtPartitionsExchangeFuture fut;

        GridDhtPartitionsExchangeFuture old = exchFuts.addx(
            fut = new GridDhtPartitionsExchangeFuture(cctx, busyLock, exchId, exchActions, affChangeMsg));

        if (old != null) {
            fut = old;

            if (exchActions != null)
                fut.exchangeActions(exchActions);

            if (affChangeMsg != null)
                fut.affinityChangeMessage(affChangeMsg);
        }

        if (discoEvt != null)
            fut.onEvent(exchId, discoEvt, cache);

        if (stopErr != null)
            fut.onDone(stopErr);

        return fut;
    }

    /**
     * @param topVer Exchange result topology version.
     * @param initTopVer Exchange initial version.
     * @param err Error.
     */
    public void onExchangeDone(AffinityTopologyVersion topVer, AffinityTopologyVersion initTopVer, @Nullable Throwable err) {
        assert topVer != null || err != null;
        assert initTopVer != null;

        if (log.isDebugEnabled())
            log.debug("Exchange done [topVer=" + topVer + ", err=" + err + ']');

        if (err == null)
            exchFuts.readyTopVer(topVer);

        completeAffReadyFuts(err == null ? topVer : initTopVer, err);

        ExchangeFutureSet exchFuts0 = exchFuts;

        if (exchFuts0 != null) {
            int skipped = 0;

            for (GridDhtPartitionsExchangeFuture fut : exchFuts0.values()) {
                if (initTopVer.compareTo(fut.exchangeId().topologyVersion()) < 0)
                    continue;

                skipped++;

                if (skipped > 10)
                    fut.cleanUp();
            }
        }
    }

    /** */
    private void completeAffReadyFuts(AffinityTopologyVersion topVer, @Nullable Throwable err) {
        for (Map.Entry<AffinityTopologyVersion, AffinityReadyFuture> entry : readyFuts.entrySet()) {
            if (entry.getKey().compareTo(topVer) <= 0) {
                if (err == null) {
                    if (log.isDebugEnabled())
                        log.debug("Completing created topology ready future " +
                            "[ver=" + topVer + ", fut=" + entry.getValue() + ']');

                    entry.getValue().onDone(topVer);
                }
                else {
                    if (log.isDebugEnabled())
                        log.debug("Completing created topology ready future with error " +
                            "[ver=" + entry.getKey() + ", fut=" + entry.getValue() + ']');

                    entry.getValue().onDone(err);
                }
            }
        }
    }

    /**
     * @param fut Future.
     * @return {@code True} if added.
     */
    private boolean addFuture(GridDhtPartitionsExchangeFuture fut) {
        if (fut.onAdded()) {
            exchWorker.addExchangeFuture(fut);

            return true;
        }

        return false;
    }

    /**
     * @param node Sender cluster node.
     * @param msg Message.
     */
    public void processFullPartitionUpdate(ClusterNode node, GridDhtPartitionsFullMessage msg) {
        if (!enterBusy())
            return;

        try {
            if (msg.exchangeId() == null) {
                if (log.isDebugEnabled())
                    log.debug("Received full partition update [node=" + node.id() + ", msg=" + msg + ']');

                boolean updated = false;

                Map<Integer, Map<Integer, Long>> partsSizes = msg.partitionSizes(cctx);

                for (Map.Entry<Integer, GridDhtPartitionFullMap> entry : msg.partitions().entrySet()) {
                    Integer grpId = entry.getKey();

                    CacheGroupContext grp = cctx.cache().cacheGroup(grpId);

                    GridDhtPartitionTopology top = null;

                    if (grp == null)
                        top = clientTops.get(grpId);
                    else if (!grp.isLocal())
                        top = grp.topology();

                    if (top != null) {
                        updated |= top.update(null,
                            entry.getValue(),
                            null,
                            msg.partsToReload(cctx.localNodeId(), grpId),
                            partsSizes.getOrDefault(grpId, Collections.emptyMap()),
                            msg.topologyVersion(),
                            null
                        );
                    }
                }

                if (!cctx.kernalContext().clientNode() && updated) {
                    if (log.isDebugEnabled())
                        log.debug("Refresh partitions due to topology update");

                    refreshPartitions();
                }

                boolean hasMovingParts = false;

                for (CacheGroupContext grp : cctx.cache().cacheGroups()) {
                    if (!grp.isLocal() && grp.topology().hasMovingPartitions()) {
                        hasMovingParts = true;

                        break;
                    }
                }

                if (!hasMovingParts)
                    cctx.database().releaseHistoryForPreloading();
            }
            else
                exchangeFuture(msg.exchangeId(), null, null, null, null).onReceiveFullMessage(node, msg);
        }
        finally {
            leaveBusy();
        }
    }

    /**
     * @param node Sender cluster node.
     * @param msg Message.
     */
    private void processSinglePartitionUpdate(final ClusterNode node, final GridDhtPartitionsSingleMessage msg) {
        if (!enterBusy())
            return;

        try {
            if (msg.exchangeId() == null) {
                if (log.isDebugEnabled())
                    log.debug("Received local partition update [nodeId=" + node.id() + ", parts=" +
                        msg + ']');

                boolean updated = false;

                for (Map.Entry<Integer, GridDhtPartitionMap> entry : msg.partitions().entrySet()) {
                    Integer grpId = entry.getKey();

                    CacheGroupContext grp = cctx.cache().cacheGroup(grpId);

                    if (grp != null &&
                        grp.localStartVersion().compareTo(entry.getValue().topologyVersion()) > 0)
                        continue;

                    GridDhtPartitionTopology top = null;

                    if (grp == null)
                        top = clientTops.get(grpId);
                    else if (!grp.isLocal())
                        top = grp.topology();

                    if (top != null) {
                        updated |= top.update(null, entry.getValue(), false);

                        cctx.affinity().checkRebalanceState(top, grpId);
                    }
                }

                if (updated) {
                    if (log.isDebugEnabled())
                        log.debug("Partitions have been scheduled to resend [reason=Single update from " + node.id() + "]");

                    scheduleResendPartitions();
                }
            }
            else {
                GridDhtPartitionsExchangeFuture exchFut = exchangeFuture(msg.exchangeId());

                if (log.isTraceEnabled())
                    log.trace("Notifying exchange future about single message: " + exchFut);

                if (msg.client()) {
                    AffinityTopologyVersion initVer = exchFut.initialVersion();
                    AffinityTopologyVersion readyVer = readyAffinityVersion();

                    if (initVer.compareTo(readyVer) < 0 && !exchFut.isDone()) {
                        U.warn(log, "Client node tries to connect but its exchange " +
                            "info is cleaned up from exchange history. " +
                            "Consider increasing 'IGNITE_EXCHANGE_HISTORY_SIZE' property " +
                            "or start clients in smaller batches. " +
                            "Current settings and versions: " +
                            "[IGNITE_EXCHANGE_HISTORY_SIZE=" + EXCHANGE_HISTORY_SIZE + ", " +
                            "initVer=" + initVer + ", " +
                            "readyVer=" + readyVer + "]."
                        );

                        exchFut.forceClientReconnect(node, msg);

                        return;
                    }
                }

                exchFut.onReceiveSingleMessage(node, msg);
            }
        }
        finally {
            leaveBusy();
        }
    }

    /**
     * @param node Sender cluster node.
     * @param msg Message.
     */
    private void processSinglePartitionRequest(ClusterNode node, GridDhtPartitionsSingleRequest msg) {
        if (!enterBusy())
            return;

        try {
            final GridDhtPartitionsExchangeFuture exchFut = exchangeFuture(msg.exchangeId(),
                null,
                null,
                null,
                null);

            exchFut.onReceivePartitionRequest(node, msg);
        }
        finally {
            leaveBusy();
        }
    }

    /**
     * @return Latch manager instance.
     */
    public ExchangeLatchManager latch() {
        return latchMgr;
    }

    /**
     * @param exchFut Optional current exchange future.
     * @throws Exception If failed.
     */
    public void dumpDebugInfo(@Nullable GridDhtPartitionsExchangeFuture exchFut) throws Exception {
        AffinityTopologyVersion exchTopVer = exchFut != null ? exchFut.initialVersion() : null;

        U.warn(diagnosticLog, "Ready affinity version: " + exchFuts.readyTopVer());

        U.warn(diagnosticLog, "Last exchange future: " + lastInitializedFut);

        exchWorker.dumpExchangeDebugInfo();

        if (!readyFuts.isEmpty()) {
            int warningsLimit = IgniteSystemProperties.getInteger(IGNITE_DIAGNOSTIC_WARN_LIMIT, 5);

            U.warn(diagnosticLog, "First " + warningsLimit + " pending affinity ready futures [total=" +
                readyFuts.size() + ']');

            if (warningsLimit > 0) {
                int cnt = 0;

                for (AffinityReadyFuture fut : readyFuts.values()) {
                    U.warn(diagnosticLog, ">>> " + fut);

                    if (++cnt == warningsLimit)
                        break;
                }
            }
        }

        IgniteDiagnosticPrepareContext diagCtx = cctx.kernalContext().cluster().diagnosticEnabled() ?
            new IgniteDiagnosticPrepareContext(cctx.localNodeId()) : null;

        if (diagCtx != null && exchFut != null)
            exchFut.addDiagnosticRequest(diagCtx);

        ExchangeFutureSet exchFuts = this.exchFuts;

        if (exchFuts != null) {
            U.warn(diagnosticLog, "Last " + DIAGNOSTIC_WARN_LIMIT + " exchange futures (total: " +
                exchFuts.size() + "):");

            if (DIAGNOSTIC_WARN_LIMIT > 0) {
                int cnt = 0;

                for (GridDhtPartitionsExchangeFuture fut : exchFuts.values()) {
                    U.warn(diagnosticLog, ">>> " + fut.shortInfo());

                    if (++cnt == DIAGNOSTIC_WARN_LIMIT)
                        break;
                }
            }
        }

        U.warn(diagnosticLog, "Latch manager state: " + latchMgr);

        dumpPendingObjects(exchTopVer, diagCtx);

        for (CacheGroupContext grp : cctx.cache().cacheGroups()) {
            GridCachePreloader preloader = grp.preloader();

            if (preloader != null)
                preloader.dumpDebugInfo();
        }

        cctx.affinity().dumpDebugInfo();

        StringBuilder pendingMsgs = new StringBuilder();

        cctx.io().dumpPendingMessages(pendingMsgs);

        if (pendingMsgs.length() > 0 && diagnosticLog.isInfoEnabled())
            diagnosticLog.info(pendingMsgs.toString());

        if (IgniteSystemProperties.getBoolean(IGNITE_IO_DUMP_ON_TIMEOUT, false))
            cctx.gridIO().dumpStats();

        if (IgniteSystemProperties.getBoolean(IGNITE_THREAD_DUMP_ON_EXCHANGE_TIMEOUT, false))
            U.dumpThreads(diagnosticLog);

        if (diagCtx != null)
            diagCtx.send(cctx.kernalContext(), null);
    }

    /**
     * Builds warning string for long running transaction.
     *
     * @param tx Transaction.
     * @param curTime Current timestamp.
     * @return Warning string.
     */
    private String longRunningTransactionWarning(IgniteInternalTx tx, long curTime) {
        GridStringBuilder warning = new GridStringBuilder()
            .a(">>> Transaction [startTime=")
            .a(formatTime(tx.startTime()))
            .a(", curTime=")
            .a(formatTime(curTime));

        if (tx instanceof GridNearTxLocal) {
            GridNearTxLocal nearTxLoc = (GridNearTxLocal)tx;

            long sysTimeCurr = nearTxLoc.systemTimeCurrent();

            //in some cases totalTimeMillis can be less than systemTimeMillis, as they are calculated with different precision
            long userTime = Math.max(curTime - nearTxLoc.startTime() - sysTimeCurr, 0);

            warning.a(", systemTime=")
                .a(sysTimeCurr)
                .a(", userTime=")
                .a(userTime);
        }

        warning.a(", tx=")
            .a(tx)
            .a("]");

        return warning.toString();
    }

    /**
     * @param timeout Operation timeout.
     * @return {@code True} if found long running operations.
     */
    private boolean dumpLongRunningOperations0(long timeout) {
        long curTime = U.currentTimeMillis();

        boolean found = false;

        IgniteTxManager tm = cctx.tm();

        GridCacheMvccManager mvcc = cctx.mvcc();

        final IgniteDiagnosticPrepareContext diagCtx = cctx.kernalContext().cluster().diagnosticEnabled() ?
            new IgniteDiagnosticPrepareContext(cctx.localNodeId()) : null;

        if (tm != null) {
            WarningsGroup warnings = new WarningsGroup("First %d long running transactions [total=%d]",
                diagnosticLog, DIAGNOSTIC_WARN_LIMIT);

            synchronized (ltrDumpLimiter) {
                for (IgniteInternalTx tx : tm.activeTransactions()) {
                    if (curTime - tx.startTime() > timeout) {
                        found = true;

                        if (warnings.canAddMessage()) {
                            warnings.add(longRunningTransactionWarning(tx, curTime));

                            if (cctx.tm().txOwnerDumpRequestsAllowed()
                                && !Optional.ofNullable(cctx.kernalContext().config().isClientMode()).orElse(false)
                                && tx.local()
                                && tx.state() == TransactionState.ACTIVE
                                && ltrDumpLimiter.allowAction(tx))
                                dumpLongRunningTransaction(tx);
                        }
                        else
                            warnings.incTotal();
                    }
                }

                ltrDumpLimiter.trim();
            }

            warnings.printToLog();
        }

        if (mvcc != null) {
            WarningsGroup activeWarnings = new WarningsGroup("First %d long running cache futures [total=%d]",
                diagnosticLog, DIAGNOSTIC_WARN_LIMIT);

            for (GridCacheFuture<?> fut : mvcc.activeFutures()) {
                if (curTime - fut.startTime() > timeout) {
                    found = true;

                    if (activeWarnings.canAddMessage()) {
                        activeWarnings.add(">>> Future [startTime=" + formatTime(fut.startTime()) +
                            ", curTime=" + formatTime(curTime) + ", fut=" + fut + ']');
                    }
                    else
                        activeWarnings.incTotal();

                    if (diagCtx != null && fut instanceof IgniteDiagnosticAware)
                        ((IgniteDiagnosticAware)fut).addDiagnosticRequest(diagCtx);
                }
            }

            activeWarnings.printToLog();

            WarningsGroup atomicWarnings = new WarningsGroup("First %d long running cache futures [total=%d]",
                diagnosticLog, DIAGNOSTIC_WARN_LIMIT);

            for (GridCacheFuture<?> fut : mvcc.atomicFutures()) {
                if (curTime - fut.startTime() > timeout) {
                    found = true;

                    if (atomicWarnings.canAddMessage()) {
                        atomicWarnings.add(">>> Future [startTime=" + formatTime(fut.startTime()) +
                            ", curTime=" + formatTime(curTime) + ", fut=" + fut + ']');
                    }
                    else
                        atomicWarnings.incTotal();

                    if (diagCtx != null && fut instanceof IgniteDiagnosticAware)
                        ((IgniteDiagnosticAware)fut).addDiagnosticRequest(diagCtx);
                }
            }

            atomicWarnings.printToLog();
        }

        if (diagCtx != null && !diagCtx.empty()) {
            try {
                cctx.kernalContext().closure().runLocal(new Runnable() {
                    @Override public void run() {
                        diagCtx.send(cctx.kernalContext(), null);
                    }
                }, SYSTEM_POOL);
            }
            catch (IgniteCheckedException e) {
                U.error(diagnosticLog, "Failed to submit diagnostic closure: " + e, e);
            }
        }

        return found;
    }

    /**
     * Dumps long running transaction. If transaction is active and is not near, sends compute request
     * to near node to get the stack trace of transaction owner thread.
     *
     * @param tx Transaction.
     */
    private void dumpLongRunningTransaction(IgniteInternalTx tx) {
        Collection<UUID> masterNodeIds = tx.masterNodeIds();

        if (masterNodeIds.size() == 1) {
            UUID nearNodeId = masterNodeIds.iterator().next();

            long txOwnerThreadId = tx.threadId();

            Ignite ignite = cctx.kernalContext().grid();

            ClusterGroup nearNode = ignite.cluster().forNodeId(nearNodeId);

            String txRequestInfo = String.format(
                "[xidVer=%s, nodeId=%s]",
                tx.xidVersion().toString(),
                nearNodeId.toString()
            );

            if (allNodesSupports(nearNode.nodes(), TRANSACTION_OWNER_THREAD_DUMP_PROVIDING)) {
                IgniteCompute compute = ignite.compute(ignite.cluster().forNodeId(nearNodeId));

                try {
                    compute
                        .callAsync(new FetchActiveTxOwnerTraceClosure(txOwnerThreadId))
                        .listen(new IgniteInClosure<IgniteFuture<String>>() {
                            @Override public void apply(IgniteFuture<String> strIgniteFut) {
                                String traceDump = null;

                                try {
                                    traceDump = strIgniteFut.get();
                                }
                                catch (ClusterGroupEmptyException e) {
                                    U.error(
                                        diagnosticLog,
                                        "Could not get thread dump from transaction owner because near node " +
                                                "is out of topology now. " + txRequestInfo
                                    );
                                }
                                catch (Exception e) {
                                    U.error(
                                        diagnosticLog,
                                        "Could not get thread dump from transaction owner near node " + txRequestInfo,
                                        e
                                    );
                                }

                                if (traceDump != null) {
                                    U.warn(
                                        diagnosticLog,
                                        String.format(
                                            "Dumping the near node thread that started transaction %s\n%s",
                                            txRequestInfo,
                                            traceDump
                                        )
                                    );
                                }
                            }
                        });
                }
                catch (Exception e) {
                    U.error(diagnosticLog, "Could not send dump request to transaction owner near node " + txRequestInfo, e);
                }
            }
            else {
                U.warn(
                    diagnosticLog,
                    "Could not send dump request to transaction owner near node: node does not support this feature. " +
                        txRequestInfo
                );
            }
        }
    }

    /**
     * @param timeout Operation timeout.
     */
    public void dumpLongRunningOperations(long timeout) {
        try {
            GridDhtPartitionsExchangeFuture lastFut = lastInitializedFut;

            // If exchange is in progress it will dump all hanging operations if any.
            if (lastFut != null && !lastFut.isDone())
                return;

            if (U.currentTimeMillis() < nextLongRunningOpsDumpTime)
                return;

            if (dumpLongRunningOperations0(timeout)) {
                nextLongRunningOpsDumpTime = U.currentTimeMillis() + nextDumpTimeout(longRunningOpsDumpStep++, timeout);

                if (IgniteSystemProperties.getBoolean(IGNITE_THREAD_DUMP_ON_EXCHANGE_TIMEOUT, false)) {
                    U.warn(diagnosticLog, "Found long running cache operations, dump threads.");

                    U.dumpThreads(diagnosticLog);
                }

                if (IgniteSystemProperties.getBoolean(IGNITE_IO_DUMP_ON_TIMEOUT, false)) {
                    U.warn(diagnosticLog, "Found long running cache operations, dump IO statistics.");

                // Dump IO manager statistics.
                if (IgniteSystemProperties.getBoolean(IgniteSystemProperties.IGNITE_IO_DUMP_ON_TIMEOUT, false))
                    cctx.gridIO().dumpStats();}
            }
            else {
                nextLongRunningOpsDumpTime = 0;
                longRunningOpsDumpStep = 0;
            }
        }
        catch (Exception e) {
            U.error(diagnosticLog, "Failed to dump debug information: " + e, e);
        }
    }

    /**
     * @param time Time.
     * @return Time string.
     */
    private String formatTime(long time) {
        return dateFormat.format(new Date(time));
    }

    /**
     * Check if provided task from exchange queue is exchange task.
     *
     * @param task Task.
     * @return {@code True} if this is exchange task.
     */
    private static boolean isExchangeTask(CachePartitionExchangeWorkerTask task) {
        return task instanceof GridDhtPartitionsExchangeFuture ||
            task instanceof RebalanceReassignExchangeTask ||
            task instanceof ForceRebalanceExchangeTask;
    }

    /**
     * @param exchTopVer Exchange topology version.
     * @param diagCtx Diagnostic request.
     */
    private void dumpPendingObjects(@Nullable AffinityTopologyVersion exchTopVer,
        @Nullable IgniteDiagnosticPrepareContext diagCtx) {
        IgniteTxManager tm = cctx.tm();

        if (tm != null) {
            boolean first = true;

            for (IgniteInternalTx tx : tm.activeTransactions()) {
                if (first) {
                    U.warn(diagnosticLog, "Pending transactions:");

                    first = false;
                }

                if (exchTopVer != null) {
                    U.warn(diagnosticLog, ">>> [txVer=" + tx.topologyVersionSnapshot() +
                        ", exchWait=" + tm.needWaitTransaction(tx, exchTopVer) +
                        ", tx=" + tx + ']');
                }
                else
                    U.warn(diagnosticLog, ">>> [txVer=" + tx.topologyVersionSnapshot() + ", tx=" + tx + ']');
            }
        }

        GridCacheMvccManager mvcc = cctx.mvcc();

        if (mvcc != null) {
            boolean first = true;

            for (GridCacheExplicitLockSpan lockSpan : mvcc.activeExplicitLocks()) {
                if (first) {
                    U.warn(diagnosticLog, "Pending explicit locks:");

                    first = false;
                }

                U.warn(diagnosticLog, ">>> " + lockSpan);
            }

            first = true;

            for (GridCacheFuture<?> fut : mvcc.activeFutures()) {
                if (first) {
                    U.warn(diagnosticLog, "Pending cache futures:");

                    first = false;
                }

                dumpDiagnosticInfo(fut, diagCtx);
            }

            first = true;

            for (GridCacheFuture<?> fut : mvcc.atomicFutures()) {
                if (first) {
                    U.warn(diagnosticLog, "Pending atomic cache futures:");

                    first = false;
                }

                dumpDiagnosticInfo(fut, diagCtx);
            }

            first = true;

            for (IgniteInternalFuture<?> fut : mvcc.dataStreamerFutures()) {
                if (first) {
                    U.warn(diagnosticLog, "Pending data streamer futures:");

                    first = false;
                }

                dumpDiagnosticInfo(fut, diagCtx);
            }

            if (tm != null) {
                first = true;

                for (IgniteInternalFuture<?> fut : tm.deadlockDetectionFutures()) {
                    if (first) {
                        U.warn(diagnosticLog, "Pending transaction deadlock detection futures:");

                        first = false;
                    }

                    dumpDiagnosticInfo(fut, diagCtx);
                }
            }
        }

        int affDumpCnt = 0;

        for (CacheGroupContext grp : cctx.cache().cacheGroups()) {
            if (grp.isLocal())
                continue;

            GridCachePreloader preloader = grp.preloader();

            if (preloader != null)
                preloader.dumpDebugInfo();

            GridAffinityAssignmentCache aff = grp.affinity();

            if (aff != null && affDumpCnt < 5) {
                if (aff.dumpDebugInfo())
                    affDumpCnt++;
            }
        }

        cctx.kernalContext().coordinators().dumpDebugInfo(diagnosticLog, diagCtx);
    }

    /**
     * Logs the future and add diagnostic info closure.
     *
     * @param fut Future.
     * @param ctx Diagnostic prepare context.
     */
    private void dumpDiagnosticInfo(IgniteInternalFuture<?> fut,
        @Nullable IgniteDiagnosticPrepareContext ctx) {
        U.warn(diagnosticLog, ">>> " + fut);

        if (ctx != null && fut instanceof IgniteDiagnosticAware)
            ((IgniteDiagnosticAware)fut).addDiagnosticRequest(ctx);
    }

    /**
     * For testing only.
     *
     * @param exchMergeTestWaitVer Version to wait for.
     * @param mergedEvtsForTest List to collect discovery events with merged exchanges.
     */
    public void mergeExchangesTestWaitVersion(
        AffinityTopologyVersion exchMergeTestWaitVer,
        @Nullable List mergedEvtsForTest
    ) {
        this.exchMergeTestWaitVer = exchMergeTestWaitVer;
        this.mergedEvtsForTest = mergedEvtsForTest;
    }

    /**
     * @param delay Rebalance delay.
     */
    public void rebalanceDelay(long delay) {
        this.rebalanceDelay = delay;
    }

    /**
     * For testing only.
     *
     * @return Current version to wait for.
     */
    public AffinityTopologyVersion mergeExchangesTestWaitVersion() {
        return exchMergeTestWaitVer;
    }

    /**
     * @param curFut Current exchange future.
     * @param msg Message.
     * @return {@code True} if node is stopping.
     * @throws IgniteInterruptedCheckedException If interrupted.
     */
    public boolean mergeExchanges(final GridDhtPartitionsExchangeFuture curFut, GridDhtPartitionsFullMessage msg)
        throws IgniteInterruptedCheckedException {
        AffinityTopologyVersion resVer = msg.resultTopologyVersion();

        if (exchWorker.waitForExchangeFuture(resVer))
            return true;

        for (CachePartitionExchangeWorkerTask task : exchWorker.futQ) {
            if (task instanceof GridDhtPartitionsExchangeFuture) {
                GridDhtPartitionsExchangeFuture fut = (GridDhtPartitionsExchangeFuture) task;

                if (fut.initialVersion().compareTo(resVer) > 0) {
                    if (log.isInfoEnabled()) {
                        log.info("Merge exchange future on finish stop [curFut=" + curFut.initialVersion() +
                            ", resVer=" + resVer +
                            ", nextFutVer=" + fut.initialVersion() + ']');
                    }

                    break;
                }

                if (log.isInfoEnabled()) {
                    log.info("Merge exchange future on finish [curFut=" + curFut.initialVersion() +
                        ", mergedFut=" + fut.initialVersion() +
                        ", evt=" + IgniteUtils.gridEventName(fut.firstEvent().type()) +
                        ", evtNode=" + fut.firstEvent().eventNode().id()+
                        ", evtNodeClient=" + fut.firstEvent().eventNode().isClient() + ']');
                }

                DiscoveryEvent evt = fut.firstEvent();

                curFut.context().events().addEvent(fut.initialVersion(),
                    fut.firstEvent(),
                    fut.firstEventCache());

                if (evt.type() == EVT_NODE_JOINED) {
                    final GridDhtPartitionsSingleMessage pendingMsg = fut.mergeJoinExchangeOnDone(curFut);

                    if (pendingMsg != null) {
                        if (log.isInfoEnabled()) {
                            log.info("Merged join exchange future on finish, will reply to node [" +
                                "curFut=" + curFut.initialVersion() +
                                ", mergedFut=" + fut.initialVersion() +
                                ", evtNode=" + evt.eventNode().id() + ']');
                        }

                        curFut.waitAndReplyToNode(evt.eventNode().id(), pendingMsg);
                    }
                }
            }
        }

        ExchangeDiscoveryEvents evts = curFut.context().events();

        assert evts.topologyVersion().equals(resVer) : "Invalid exchange merge result [ver=" + evts.topologyVersion()
            + ", expVer=" + resVer + ']';

        return false;
    }

    /**
     * @param curFut Current active exchange future.
     * @return {@code False} if need wait messages for merged exchanges.
     */
    public boolean mergeExchangesOnCoordinator(
        GridDhtPartitionsExchangeFuture curFut,
        @Nullable AffinityTopologyVersion threshold
    ) {
        if (IGNITE_EXCHANGE_MERGE_DELAY > 0) {
            try {
                U.sleep(IGNITE_EXCHANGE_MERGE_DELAY);
            }
            catch (IgniteInterruptedCheckedException e) {
                U.warn(log, "Failed to wait for exchange merge, thread interrupted: " + e);

                return true;
            }
        }

        AffinityTopologyVersion exchMergeTestWaitVer = this.exchMergeTestWaitVer;

        if (exchMergeTestWaitVer != null)
            waitForTestVersion(exchMergeTestWaitVer, curFut);

        synchronized (curFut.mutex()) {
            int awaited = 0;

            for (CachePartitionExchangeWorkerTask task : exchWorker.futQ) {
                if (task instanceof GridDhtPartitionsExchangeFuture) {
                    GridDhtPartitionsExchangeFuture fut = (GridDhtPartitionsExchangeFuture)task;

                    DiscoveryEvent evt = fut.firstEvent();

                    if (threshold != null && fut.initialVersion().compareTo(threshold) > 0) {
                        if (log.isInfoEnabled())
                            log.info("Stop merge, threshold is exceed: " + evt + ", threshold = " + threshold);

                        break;
                    }

                    if (evt.type() == EVT_DISCOVERY_CUSTOM_EVT) {
                        if (log.isInfoEnabled())
                            log.info("Stop merge, custom event found: " + evt);

                        break;
                    }

                    if (!fut.changedAffinity()) {
                        if (log.isInfoEnabled())
                            log.info("Stop merge, no-affinity exchange found: " + evt);

                        break;
                    }

                    ClusterNode node = evt.eventNode();

                    if (!curFut.context().supportsMergeExchanges(node)) {
                        if (log.isInfoEnabled())
                            log.info("Stop merge, node does not support merge: " + node);

                        break;
                    }

                    if (evt.type() == EVT_NODE_JOINED && cctx.cache().hasCachesReceivedFromJoin(node)) {
                        if (log.isInfoEnabled())
                            log.info("Stop merge, received caches from node: " + node);

                        break;
                    }

                    if (log.isInfoEnabled()) {
                        log.info("Merge exchange future [curFut=" + curFut.initialVersion() +
                            ", mergedFut=" + fut.initialVersion() +
                            ", evt=" + IgniteUtils.gridEventName(fut.firstEvent().type()) +
                            ", evtNode=" + fut.firstEvent().eventNode().id() +
                            ", evtNodeClient=" + fut.firstEvent().eventNode().isClient() + ']');
                    }

                    addDiscoEvtForTest(fut.firstEvent());

                    curFut.context().events().addEvent(fut.initialVersion(),
                        fut.firstEvent(),
                        fut.firstEventCache());

                    if (evt.type() == EVT_NODE_JOINED) {
                        if (fut.mergeJoinExchange(curFut))
                            awaited++;
                    }
                }
                else {
                    if (!task.skipForExchangeMerge()) {
                        if (log.isInfoEnabled())
                            log.info("Stop merge, custom task found: " + task);

                        break;
                    }
                }
            }

            return awaited == 0;
        }
    }

    /**
     * For testing purposes. Stores discovery events with merged exchanges to enable examining them later.
     *
     * @param discoEvt Discovery event.
     */
    private void addDiscoEvtForTest(DiscoveryEvent discoEvt) {
        List mergedEvtsForTest = this.mergedEvtsForTest;

        if (mergedEvtsForTest != null)
            mergedEvtsForTest.add(discoEvt);
    }

    /**
     * For testing purposes. Method allows to wait for an exchange future of specific version
     * to appear in exchange worker queue.
     *
     * @param exchMergeTestWaitVer Topology Version to wait for.
     * @param curFut Current Exchange Future.
     */
    private void waitForTestVersion(AffinityTopologyVersion exchMergeTestWaitVer, GridDhtPartitionsExchangeFuture curFut) {
        if (log.isInfoEnabled()) {
            log.info("Exchange merge test, waiting for version [exch=" + curFut.initialVersion() +
                ", waitVer=" + exchMergeTestWaitVer + ']');
        }

        long end = U.currentTimeMillis() + 10_000;

        while (U.currentTimeMillis() < end) {
            boolean found = false;

            for (CachePartitionExchangeWorkerTask task : exchWorker.futQ) {
                if (task instanceof GridDhtPartitionsExchangeFuture) {
                    GridDhtPartitionsExchangeFuture fut = (GridDhtPartitionsExchangeFuture)task;

                    if (exchMergeTestWaitVer.equals(fut.initialVersion())) {
                        if (log.isInfoEnabled())
                            log.info("Exchange merge test, found awaited version: " + exchMergeTestWaitVer);

                        found = true;

                        break;
                    }
                }
            }

            if (found)
                break;
            else {
                try {
                    U.sleep(100);
                }
                catch (IgniteInterruptedCheckedException e) {
                    break;
                }
            }
        }

        this.exchMergeTestWaitVer = null;
    }

    /**
     * Invokes {@link GridWorker#updateHeartbeat()} for exchange worker.
     */
    public void exchangerUpdateHeartbeat() {
        exchWorker.updateHeartbeat();
    }

    /**
     * Invokes {@link GridWorker#blockingSectionBegin()} for exchange worker.
     * Should be called from exchange worker thread.
     */
    public void exchangerBlockingSectionBegin() {
        if (currentThreadIsExchanger())
            exchWorker.blockingSectionBegin();
    }

    /**
     * Invokes {@link GridWorker#blockingSectionEnd()} for exchange worker.
     * Should be called from exchange worker thread.
     */
    public void exchangerBlockingSectionEnd() {
        if (currentThreadIsExchanger())
            exchWorker.blockingSectionEnd();
    }

    /** */
    private boolean currentThreadIsExchanger() {
        return exchWorker != null && Thread.currentThread() == exchWorker.runner();
    }

    /** */
    public boolean affinityChanged(AffinityTopologyVersion from, AffinityTopologyVersion to) {
        if (lastAffinityChangedTopologyVersion(to).compareTo(from) >= 0)
            return false;

        Collection<GridDhtPartitionsExchangeFuture> history = exchFuts.values();

        boolean fromFound = false;

        for (GridDhtPartitionsExchangeFuture fut : history) {
            if (!fromFound) {
                int cmp = fut.initialVersion().compareTo(from);

                if (cmp > 0) // We don't have history, so return true for safety
                    return true;
                else if (cmp == 0)
                    fromFound = true;
                else if (fut.isDone() && fut.topologyVersion().compareTo(from) >= 0)
                    return true; // Temporary solution for merge exchange case
            }
            else {
                if (fut.changedAffinity())
                    return true;

                if (fut.initialVersion().compareTo(to) >= 0)
                    return false;
            }
        }

        return true;
    }

    /**
     * @param blocked {@code True} if take into account only cache operations blocked PME.
     * @return Gets execution duration for current partition map exchange in milliseconds. {@code 0} If there is no
     * running PME or {@code blocked} was set to {@code true} and current PME don't block cache operations.
     */
    private long currentPMEDuration(boolean blocked) {
        GridDhtPartitionsExchangeFuture fut = lastTopologyFuture();

        return fut == null ? 0 : fut.currentPMEDuration(blocked);
    }

    /** @return Histogram of PME durations metric. */
    public HistogramMetricImpl durationHistogram() {
        return durationHistogram;
    }

    /** @return Histogram of blocking PME durations metric. */
    public HistogramMetricImpl blockingDurationHistogram() {
        return blockingDurationHistogram;
    }

    /**
     * Exchange future thread. All exchanges happen only by one thread and next
     * exchange will not start until previous one completes.
     */
    private class ExchangeWorker extends GridWorker {
        /** Future queue. */
        private final LinkedBlockingDeque<CachePartitionExchangeWorkerTask> futQ =
            new LinkedBlockingDeque<>();

        /** */
        private AffinityTopologyVersion lastFutVer;

        /** Busy flag used as performance optimization to stop current preloading. */
        private volatile boolean busy;

        /** */
        private boolean crd;

        /** */
        private boolean stop;

        /** Indicates that worker terminates because the node needs to reconnect to the cluster. */
        private boolean reconnectNeeded;

        /**
         * Constructor.
         */
        private ExchangeWorker() {
            super(cctx.igniteInstanceName(), "partition-exchanger", GridCachePartitionExchangeManager.this.log,
                cctx.kernalContext().workersRegistry());
        }

        /**
         * @param exchId Exchange ID.
         */
        void forceReassign(GridDhtPartitionExchangeId exchId) {
            if (!hasPendingExchange() && !busy)
                futQ.add(new RebalanceReassignExchangeTask(exchId));
        }

        /**
         * @param exchId Exchange ID.
         * @return Rebalance future.
         */
        IgniteInternalFuture<Boolean> forceRebalance(GridDhtPartitionExchangeId exchId) {
            GridCompoundFuture<Boolean, Boolean> fut = new GridCompoundFuture<>(CU.boolReducer());

            futQ.add(new ForceRebalanceExchangeTask(exchId, fut));

            return fut;
        }

        /**
         * @param caches Caches to stop.
         */
        IgniteInternalFuture<Void> deferStopCachesOnClientReconnect(Collection<GridCacheAdapter> caches) {
            StopCachesOnClientReconnectExchangeTask task = new StopCachesOnClientReconnectExchangeTask(caches);

            futQ.add(task);

            return task;
        }

        /**
         * @param exchFut Exchange future.
         */
        void addExchangeFuture(GridDhtPartitionsExchangeFuture exchFut) {
            assert exchFut != null;

            futQ.offer(exchFut);

            synchronized (this) {
                lastFutVer = exchFut.initialVersion();

                notifyAll();
            }

            if (log.isDebugEnabled())
                log.debug("Added exchange future to exchange worker: " + exchFut);
        }

        /**
         *
         */
        private void onKernalStop() {
            synchronized (this) {
                stop = true;

                notifyAll();
            }
        }

        /**
         * @param resVer Version to wait for.
         * @return {@code True} if node is stopping.
         * @throws IgniteInterruptedCheckedException If interrupted.
         */
        private boolean waitForExchangeFuture(AffinityTopologyVersion resVer) throws IgniteInterruptedCheckedException {
            synchronized (this) {
                while (!stop && lastFutVer.compareTo(resVer) < 0)
                    U.wait(this);

                return stop;
            }
        }

        /**
         * @param resVer Exchange result version.
         * @param exchFut Exchange future.
         * @throws IgniteInterruptedCheckedException If interrupted.
         */
        private void removeMergedFutures(AffinityTopologyVersion resVer, GridDhtPartitionsExchangeFuture exchFut)
            throws IgniteInterruptedCheckedException {
            if (resVer.compareTo(exchFut.initialVersion()) != 0) {
                waitForExchangeFuture(resVer);

                for (CachePartitionExchangeWorkerTask task : futQ) {
                    if (task instanceof GridDhtPartitionsExchangeFuture) {
                        GridDhtPartitionsExchangeFuture fut0 = (GridDhtPartitionsExchangeFuture)task;

                        if (resVer.compareTo(fut0.initialVersion()) >= 0) {
                            fut0.finishMerged(resVer, exchFut);

                            futQ.remove(fut0);
                        }
                        else
                            break;
                    }
                }
            }
        }

        /**
         * Add custom exchange task.
         *
         * @param task Task.
         */
        void addCustomTask(CachePartitionExchangeWorkerTask task) {
            assert task != null;

            assert !isExchangeTask(task);

            futQ.offer(task);
        }

        /**
         * Process custom exchange task.
         *
         * @param task Task.
         */
        void processCustomTask(CachePartitionExchangeWorkerTask task) {
            assert !isExchangeTask(task);

            try {
                cctx.cache().processCustomExchangeTask(task);
            }
            catch (Exception e) {
                U.error(log, "Failed to process custom exchange task: " + task, e);
            }
        }

        /**
         * @return Whether pending exchange future exists.
         */
        boolean hasPendingExchange() {
            if (!futQ.isEmpty()) {
                for (CachePartitionExchangeWorkerTask task : futQ) {
                    if (isExchangeTask(task))
                        return true;
                }
            }

            return false;
        }

        /**
         * @return Whether pending exchange future triggered by non client node exists.
         */
        boolean hasPendingServerExchange() {
            if (!futQ.isEmpty()) {
                for (CachePartitionExchangeWorkerTask task : futQ) {
                    if (task instanceof GridDhtPartitionsExchangeFuture) {
                        // First event is enough to check,
                        // because only current exchange future can have multiple discovery events (exchange merge).
                        ClusterNode triggeredBy = ((GridDhtPartitionsExchangeFuture) task).firstEvent().eventNode();

                        if (!triggeredBy.isClient())
                            return true;
                    }
                }
            }

            return false;
        }

        /**
         * Dump debug info.
         */
        void dumpExchangeDebugInfo() {
            U.warn(log, "First " + DIAGNOSTIC_WARN_LIMIT + " pending exchange futures [total=" + futQ.size() + ']');

            if (DIAGNOSTIC_WARN_LIMIT > 0) {
                int cnt = 0;

                for (CachePartitionExchangeWorkerTask task : futQ) {
                    if (task instanceof GridDhtPartitionsExchangeFuture) {
                        U.warn(log, ">>> " + ((GridDhtPartitionsExchangeFuture)task).shortInfo());

                        if (++cnt == DIAGNOSTIC_WARN_LIMIT)
                            break;
                    }
                }
            }
        }

        /** {@inheritDoc} */
        @Override protected void body() throws InterruptedException, IgniteInterruptedCheckedException {
            Throwable err = null;

            try {
                body0();
            }
            catch (InterruptedException | IgniteInterruptedCheckedException e) {
                if (!stop)
                    err = e;
            }
            catch (Throwable e) {
                if (!(e == stopErr || (stop && (X.hasCause(e, IgniteInterruptedCheckedException.class)))))
                    err = e;
            }
            finally {
                if (err == null && !stop && !reconnectNeeded)
                    err = new IllegalStateException("Thread " + name() + " is terminated unexpectedly");

                if (err instanceof OutOfMemoryError)
                    cctx.kernalContext().failure().process(new FailureContext(CRITICAL_ERROR, err));
                else if (err != null)
                    cctx.kernalContext().failure().process(new FailureContext(SYSTEM_WORKER_TERMINATION, err));
                else
                    // In case of reconnectNeeded == true, prevent general-case termination handling.
                    cancel();
            }
        }

        /**
         *
         */
        private void body0() throws InterruptedException, IgniteCheckedException {
            long timeout = cctx.gridConfig().getNetworkTimeout();

            long cnt = 0;

            while (!isCancelled()) {
                onIdle();

                cnt++;

                CachePartitionExchangeWorkerTask task = null;

                try {
                    boolean preloadFinished = true;

                    for (CacheGroupContext grp : cctx.cache().cacheGroups()) {
                        if (grp.isLocal())
                            continue;

                        preloadFinished &= grp.preloader() != null && grp.preloader().syncFuture().isDone();

                        if (!preloadFinished)
                            break;
                    }

                    // If not first preloading and no more topology events present.
                    if (!cctx.kernalContext().clientNode() && !hasPendingExchange() && preloadFinished)
                        timeout = cctx.gridConfig().getNetworkTimeout();

                    // After workers line up and before preloading starts we initialize all futures.
                    if (log.isTraceEnabled()) {
                        Collection<IgniteInternalFuture> unfinished = new HashSet<>();

                        for (GridDhtPartitionsExchangeFuture fut : exchFuts.values()) {
                            if (!fut.isDone())
                                unfinished.add(fut);
                        }

                        log.trace("Before waiting for exchange futures [futs" + unfinished + ", worker=" + this + ']');
                    }

                    // Take next exchange future.
                    if (isCancelled())
                        Thread.currentThread().interrupt();

                    updateHeartbeat();

                    task = futQ.poll(timeout, MILLISECONDS);

                    updateHeartbeat();

                    if (task == null)
                        continue; // Main while loop.

                    if (!isExchangeTask(task)) {
                        processCustomTask(task);

                        continue;
                    }

                    busy = true;

                    Map<Integer, GridDhtPreloaderAssignments> assignsMap = null;

                    boolean forcePreload = false;

                    GridDhtPartitionExchangeId exchId;

                    GridDhtPartitionsExchangeFuture exchFut = null;

                    AffinityTopologyVersion resVer = null;

                    try {
                        if (isCancelled())
                            break;

                        if (task instanceof RebalanceReassignExchangeTask)
                            exchId = ((RebalanceReassignExchangeTask) task).exchangeId();
                        else if (task instanceof ForceRebalanceExchangeTask) {
                            forcePreload = true;

                            timeout = 0; // Force refresh.

                            exchId = ((ForceRebalanceExchangeTask)task).exchangeId();
                        }
                        else {
                            assert task instanceof GridDhtPartitionsExchangeFuture : task;

                            exchFut = (GridDhtPartitionsExchangeFuture)task;

                            exchId = exchFut.exchangeId();

                            lastInitializedFut = exchFut;

                            boolean newCrd = false;

                            if (!crd) {
                                List<ClusterNode> srvNodes = exchFut.firstEventCache().serverNodes();

                                crd = newCrd = !srvNodes.isEmpty() && srvNodes.get(0).isLocal();
                            }

                            if (!exchFut.changedAffinity()) {
                                GridDhtPartitionsExchangeFuture lastFut = lastFinishedFut.get();

                                if (lastFut != null) {
                                    if (!lastFut.changedAffinity()) {
                                        // If lastFut corresponds to merged exchange, it is essential to use
                                        // topologyVersion() instead of initialVersion() - nodes joined in this PME
                                        // will have DiscoCache only for the last version.
                                        AffinityTopologyVersion lastAffVer = cctx.exchange()
                                            .lastAffinityChangedTopologyVersion(lastFut.topologyVersion());

                                        cctx.exchange().lastAffinityChangedTopologyVersion(exchFut.initialVersion(),
                                            lastAffVer);
                                    }
                                    else
                                        cctx.exchange().lastAffinityChangedTopologyVersion(exchFut.initialVersion(),
                                            lastFut.topologyVersion());
                                }
                            }

                            exchFut.timeBag().finishGlobalStage("Waiting in exchange queue");

                            exchFut.init(newCrd);

                            int dumpCnt = 0;

                            long waitStartNanos = System.nanoTime();

                            // Call rollback logic only for client node, for server nodes
                            // rollback logic is in GridDhtPartitionsExchangeFuture.
                            boolean txRolledBack = !cctx.localNode().isClient();

                            IgniteConfiguration cfg = cctx.gridConfig();

                            final long dumpTimeout = 2 * cfg.getNetworkTimeout();

                            long nextDumpTime = 0;

                            while (true) {
                                // Read txTimeoutOnPME from configuration after every iteration.
                                long curTimeout = cfg.getTransactionConfiguration().getTxTimeoutOnPartitionMapExchange();

                                try {
                                    long exchTimeout = curTimeout > 0 && !txRolledBack
                                        ? Math.min(curTimeout, dumpTimeout)
                                        : dumpTimeout;

                                    blockingSectionBegin();

                                    try {
                                        resVer = exchFut.get(exchTimeout, TimeUnit.MILLISECONDS);
                                    } finally {
                                        blockingSectionEnd();
                                    }

                                    onIdle();

                                    break;
                                }
                                catch (IgniteFutureTimeoutCheckedException ignored) {
                                    updateHeartbeat();

                                    if (nextDumpTime <= U.currentTimeMillis()) {
                                        U.warn(diagnosticLog, "Failed to wait for partition map exchange [" +
                                            "topVer=" + exchFut.initialVersion() +
                                            ", node=" + cctx.localNodeId() + "]. " +
                                            (curTimeout <= 0 && !txRolledBack ? "Consider changing " +
                                            "TransactionConfiguration.txTimeoutOnPartitionMapExchange" +
                                            " to non default value to avoid this message. " : "") +
                                            "Dumping pending objects that might be the cause: ");

                                        try {
                                            dumpDebugInfo(exchFut);
                                        }
                                        catch (Exception e) {
                                            U.error(diagnosticLog, "Failed to dump debug information: " + e, e);
                                        }

                                        nextDumpTime = U.currentTimeMillis() + nextDumpTimeout(dumpCnt++, dumpTimeout);
                                    }

                                    long passedMillis = U.millisSinceNanos(waitStartNanos);

                                    if (!txRolledBack && curTimeout > 0 && passedMillis >= curTimeout) {
                                        txRolledBack = true; // Try automatic rollback only once.

                                        cctx.tm().rollbackOnTopologyChange(exchFut.initialVersion());
                                    }
                                }
                                catch (Exception e) {
                                    if (exchFut.reconnectOnError(e))
                                        throw new IgniteNeedReconnectException(cctx.localNode(), e);

                                    throw e;
                                }
                            }

                            removeMergedFutures(resVer, exchFut);

                            if (log.isTraceEnabled())
                                log.trace("After waiting for exchange future [exchFut=" + exchFut + ", worker=" +
                                    this + ']');

                            if (exchFut.exchangeId().nodeId().equals(cctx.localNodeId()))
                                lastRefresh.compareAndSet(-1, U.currentTimeMillis());

                            // Just pick first worker to do this, so we don't
                            // invoke topology callback more than once for the
                            // same event.

                            boolean changed = false;

                            for (CacheGroupContext grp : cctx.cache().cacheGroups()) {
                                if (grp.isLocal())
                                    continue;

                                if (grp.preloader().rebalanceRequired(rebTopVer, exchFut))
                                    rebTopVer = NONE;

                                changed |= grp.topology().afterExchange(exchFut);
                            }

                            if (!cctx.kernalContext().clientNode() && changed && !hasPendingServerExchange()) {
                                if (log.isDebugEnabled())
                                    log.debug("Refresh partitions due to mapping was changed");

                                refreshPartitions();
                            }
                        }

                        // Schedule rebalance if force rebalance or force reassign occurs.
                        if (exchFut == null)
                            rebTopVer = NONE;

                        if (!cctx.kernalContext().clientNode() && rebTopVer.equals(NONE)) {
                            if (rebalanceDelay > 0)
                                U.sleep(rebalanceDelay);

                            assignsMap = new HashMap<>();

                            IgniteCacheSnapshotManager snp = cctx.snapshot();

                            for (final CacheGroupContext grp : cctx.cache().cacheGroups()) {
                                long delay = grp.config().getRebalanceDelay();

                                boolean disableRebalance = snp.partitionsAreFrozen(grp);

                                GridDhtPreloaderAssignments assigns = null;

                                // Don't delay for dummy reassigns to avoid infinite recursion.
                                if ((delay == 0 || forcePreload) && !disableRebalance)
                                    assigns = grp.preloader().generateAssignments(exchId, exchFut);

                                assignsMap.put(grp.groupId(), assigns);

                                if (resVer == null && !grp.isLocal())
                                    resVer = grp.topology().readyTopologyVersion();
                            }
                        }

                        if (resVer == null)
                            resVer = exchId.topologyVersion();
                    }
                    finally {
                        // Must flip busy flag before assignments are given to demand workers.
                        busy = false;
                    }

                    if (assignsMap != null && rebTopVer.equals(NONE)) {
                        int size = assignsMap.size();

                        NavigableMap<Integer, List<Integer>> orderMap = new TreeMap<>();

                        for (Map.Entry<Integer, GridDhtPreloaderAssignments> e : assignsMap.entrySet()) {
                            int grpId = e.getKey();

                            CacheGroupContext grp = cctx.cache().cacheGroup(grpId);

                            int order = grp.config().getRebalanceOrder();

                            if (orderMap.get(order) == null)
                                orderMap.put(order, new ArrayList<Integer>(size));

                            orderMap.get(order).add(grpId);
                        }

                        Runnable r = null;

                        List<String> rebList = new LinkedList<>();

                        boolean assignsCancelled = false;

                        GridCompoundFuture<Boolean, Boolean> forcedRebFut = null;

                        if (task instanceof ForceRebalanceExchangeTask)
                            forcedRebFut = ((ForceRebalanceExchangeTask)task).forcedRebalanceFuture();

                        for (Integer order : orderMap.descendingKeySet()) {
                            for (Integer grpId : orderMap.get(order)) {
                                CacheGroupContext grp = cctx.cache().cacheGroup(grpId);

                                GridDhtPreloaderAssignments assigns = assignsMap.get(grpId);

                                if (assigns != null)
                                    assignsCancelled |= assigns.cancelled();

                                Runnable cur = grp.preloader().addAssignments(assigns,
                                    forcePreload,
                                    cnt,
                                    r,
                                    forcedRebFut);

                                if (cur != null) {
                                    rebList.add(grp.cacheOrGroupName());

                                    r = cur;
                                }
                            }
                        }

                        if (forcedRebFut != null)
                            forcedRebFut.markInitialized();

                        if (assignsCancelled || hasPendingServerExchange()) {
                            U.log(log, "Skipping rebalancing (obsolete exchange ID) " +
                                "[top=" + resVer + ", evt=" + exchId.discoveryEventName() +
                                ", node=" + exchId.nodeId() + ']');
                        }
                        else if (r != null) {
                            Collections.reverse(rebList);

                            U.log(log, "Rebalancing scheduled [order=" + rebList +
                                ", top=" + resVer + ", force=" + (exchFut == null) +
                                ", evt=" + exchId.discoveryEventName() +
                                ", node=" + exchId.nodeId() + ']');

                            rebTopVer = resVer;

                            // Start rebalancing cache groups chain. Each group will be rebalanced
                            // sequentially one by one e.g.:
                            // ignite-sys-cache -> cacheGroupR1 -> cacheGroupP2 -> cacheGroupR3
                            r.run();
                        }
                        else
                            U.log(log, "Skipping rebalancing (nothing scheduled) " +
                                "[top=" + resVer + ", force=" + (exchFut == null) +
                                ", evt=" + exchId.discoveryEventName() +
                                ", node=" + exchId.nodeId() + ']');
                    }
                    else
                        U.log(log, "Skipping rebalancing (no affinity changes) " +
                            "[top=" + resVer +
                            ", rebTopVer=" + rebTopVer +
                            ", evt=" + exchId.discoveryEventName() +
                            ", evtNode=" + exchId.nodeId() +
                            ", client=" + cctx.kernalContext().clientNode() + ']');
                }
                catch (IgniteInterruptedCheckedException e) {
                    throw e;
                }
                catch (IgniteClientDisconnectedCheckedException | IgniteNeedReconnectException e) {
                    if (cctx.discovery().reconnectSupported()) {
                        U.warn(log, "Local node failed to complete partition map exchange due to " +
                            "exception, will try to reconnect to cluster: " + e.getMessage(), e);

                        cctx.discovery().reconnect();

                        reconnectNeeded = true;
                    }
                    else
                        U.warn(log, "Local node received IgniteClientDisconnectedCheckedException or " +
                            " IgniteNeedReconnectException exception but doesn't support reconnect, stopping node: " +
                            e.getMessage(), e);

                    return;
                }
                catch (IgniteCheckedException e) {
                    U.error(log, "Failed to wait for completion of partition map exchange " +
                        "(preloading will not start): " + task, e);

                    throw e;
                }
            }
        }
    }

    /**
     * Partition resend timeout object.
     */
    private class ResendTimeoutObject implements GridTimeoutObject {
        /** Timeout ID. */
        private final IgniteUuid timeoutId = IgniteUuid.randomUuid();

        /** Logger. */
        protected final IgniteLogger log;

        /** Timeout start time. */
        private final long createTime = U.currentTimeMillis();

        /** Started flag. */
        private AtomicBoolean started = new AtomicBoolean();

        /**
         *
         */
        private ResendTimeoutObject() {
            this.log = cctx.logger(getClass());
        }

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
            cctx.kernalContext().closure().runLocalSafe(new Runnable() {
                @Override public void run() {
                    if (!busyLock.readLock().tryLock())
                        return;

                    try {
                        if (started.compareAndSet(false, true)) {
                            if (log.isDebugEnabled())
                                log.debug("Refresh partitions due to scheduled timeout");

                            refreshPartitions();
                        }
                    }
                    finally {
                        busyLock.readLock().unlock();

                        cctx.time().removeTimeoutObject(ResendTimeoutObject.this);

                        pendingResend.compareAndSet(ResendTimeoutObject.this, null);
                    }
                }
            });
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

        /** */
        private final int histSize;

        /** */
        private final AtomicReference<AffinityTopologyVersion> readyTopVer =
            new AtomicReference<>(NONE);

        /**
         * Creates ordered, not strict list set.
         *
         * @param histSize Max history size.
         */
        private ExchangeFutureSet(int histSize) {
            super((f1, f2) -> {
                AffinityTopologyVersion t1 = f1.exchangeId().topologyVersion();
                AffinityTopologyVersion t2 = f2.exchangeId().topologyVersion();

                assert t1.topologyVersion() > 0;
                assert t2.topologyVersion() > 0;

                // Reverse order.
                return t2.compareTo(t1);
            }, /*not strict*/false);

            this.histSize = histSize;
        }

        /**
         * @param fut Future to add.
         * @return {@code True} if added.
         */
        @Override public synchronized GridDhtPartitionsExchangeFuture addx(
            GridDhtPartitionsExchangeFuture fut) {
            GridDhtPartitionsExchangeFuture cur = super.addx(fut);

            while (size() > histSize) {
                GridDhtPartitionsExchangeFuture last = last();

                if (!last.isDone() || Objects.equals(last.initialVersion(), readyTopVer()))
                    break;

                removeLast();
            }

            // Return the value in the set.
            return cur == null ? fut : cur;
        }

        /**
         * @return Ready top version.
         */
        public AffinityTopologyVersion readyTopVer() {
            return readyTopVer.get();
        }

        /**
         * @param readyTopVersion Ready top version.
         * @return {@code true} if version was set and {@code false} otherwise.
         */
        public boolean readyTopVer(AffinityTopologyVersion readyTopVersion) {
            while (true) {
                AffinityTopologyVersion readyVer = readyTopVer.get();

                if (readyVer.compareTo(readyTopVersion) >= 0)
                    return false;

                if (readyTopVer.compareAndSet(readyVer, readyTopVersion))
                    return true;
            }
        }

        /** {@inheritDoc} */
        @Nullable @Override public synchronized GridDhtPartitionsExchangeFuture removex(
            GridDhtPartitionsExchangeFuture val) {

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

        /**
         * @param nodeId Sender node ID.
         * @param msg Message.
         */
        @Override public void apply(UUID nodeId, M msg) {
            ClusterNode node = cctx.node(nodeId);

            if (node == null) {
                if (log.isTraceEnabled())
                    log.trace("Received message from failed node [node=" + nodeId + ", msg=" + msg + ']');

                return;
            }

            if (log.isTraceEnabled())
                log.trace("Received message from node [node=" + nodeId + ", msg=" + msg + ']');

            onMessage(node, msg);
        }

        /**
         * @param node Sender cluster node.
         * @param msg Message.
         */
        protected abstract void onMessage(ClusterNode node, M msg);
    }

    /**
     * Affinity ready future.
     */
    private class AffinityReadyFuture extends GridFutureAdapter<AffinityTopologyVersion> {
        /** */
        @GridToStringInclude
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

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(AffinityReadyFuture.class, this, super.toString());
        }
    }

    /**
     * Class to print only limited number of warnings.
     */
    private static class WarningsGroup {
        /** */
        private final IgniteLogger log;

        /** */
        private final int warningsLimit;

        /** */
        private final String title;

        /** */
        private final List<String> messages = new ArrayList<>();

        /** */
        private int warningsTotal = 0;

        /**
         * @param log Target logger.
         * @param warningsLimit Warnings limit.
         */
        private WarningsGroup(String title, IgniteLogger log, int warningsLimit) {
            this.title = title;

            this.log = log;

            this.warningsLimit = warningsLimit;
        }

        /**
         * @param msg Warning message.
         * @return {@code true} if message is added to list.
         */
        private boolean add(String msg) {
            boolean added = false;

            if (canAddMessage()) {
                messages.add(msg);

                added = true;
            }

            warningsTotal++;

            return added;
        }

        /**
         * @return {@code true} if messages list size less than limit.
         */
        private boolean canAddMessage() {
            return warningsTotal < warningsLimit;
        }

        /**
         * Increase total number of warnings.
         */
        private void incTotal() {
            warningsTotal++;
        }

        /**
         * Print warnings block title and messages.
         */
        private void printToLog() {
            if (warningsTotal > 0) {
                U.warn(log, String.format(title, warningsLimit, warningsTotal));

                for (String message : messages)
                    U.warn(log, message);
            }
        }
    }

    /**
     * Class to limit action count for unique objects.
     * <p>
     * NO guarantees of thread safety are provided.
     */
    private static class ActionLimiter<T> {
        /** */
        private final int limit;

        /**
         * Internal storage of objects and counters for each of object.
         */
        private final Map<T, AtomicInteger> actionsCnt = new HashMap<>();

        /**
         * Set of active objects.
         */
        private final Set<T> activeObjects = new HashSet<>();

        /**
         * @param limit Limit.
         */
        private ActionLimiter(int limit) {
            this.limit = limit;
        }

        /**
         * Shows if action is allowed for the given object. Adds this object to internal set of active
         * objects that are still in use.
         *
         * @param obj object.
         */
        boolean allowAction(T obj) {
            activeObjects.add(obj);

            int cnt = actionsCnt.computeIfAbsent(obj, o -> new AtomicInteger(0))
                .incrementAndGet();

            return (cnt <= limit);
        }

        /**
         * Removes old objects from limiter's internal storage. All objects that are contained in internal
         * storage but not in set of active objects, are assumed as 'old'. This method is to be called
         * after processing of collection of objects to purge limiter's internal storage.
         */
        void trim() {
            actionsCnt.keySet().removeIf(key -> !activeObjects.contains(key));

            activeObjects.clear();
        }
    }
}

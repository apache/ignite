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

package org.apache.ignite.internal.processors.cache.mvcc;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.failure.FailureType;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteDiagnosticPrepareContext;
import org.apache.ignite.internal.IgniteFutureTimeoutCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.NodeStoppingException;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.managers.communication.GridMessageListener;
import org.apache.ignite.internal.managers.discovery.CustomEventListener;
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.managers.eventstorage.DiscoveryEventListener;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.DynamicCacheChangeBatch;
import org.apache.ignite.internal.processors.cache.DynamicCacheChangeRequest;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.mvcc.msg.MvccAckRequestQueryCntr;
import org.apache.ignite.internal.processors.cache.mvcc.msg.MvccAckRequestQueryId;
import org.apache.ignite.internal.processors.cache.mvcc.msg.MvccAckRequestTx;
import org.apache.ignite.internal.processors.cache.mvcc.msg.MvccActiveQueriesMessage;
import org.apache.ignite.internal.processors.cache.mvcc.msg.MvccFutureResponse;
import org.apache.ignite.internal.processors.cache.mvcc.msg.MvccMessage;
import org.apache.ignite.internal.processors.cache.mvcc.msg.MvccQuerySnapshotRequest;
import org.apache.ignite.internal.processors.cache.mvcc.msg.MvccRecoveryFinishedMessage;
import org.apache.ignite.internal.processors.cache.mvcc.msg.MvccSnapshotResponse;
import org.apache.ignite.internal.processors.cache.mvcc.msg.MvccTxSnapshotRequest;
import org.apache.ignite.internal.processors.cache.mvcc.txlog.TxKey;
import org.apache.ignite.internal.processors.cache.mvcc.txlog.TxLog;
import org.apache.ignite.internal.processors.cache.mvcc.txlog.TxState;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.persistence.DatabaseLifecycleListener;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.IgniteCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.tree.mvcc.data.MvccDataRow;
import org.apache.ignite.internal.processors.cache.tree.mvcc.search.MvccLinkAwareSearchRow;
import org.apache.ignite.internal.util.GridAtomicLong;
import org.apache.ignite.internal.util.GridLongList;
import org.apache.ignite.internal.util.GridSpinBusyLock;
import org.apache.ignite.internal.util.future.GridCompoundIdentityFuture;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.lang.GridClosureException;
import org.apache.ignite.internal.util.lang.GridCursor;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteNodeValidationResult;
import org.apache.ignite.thread.IgniteThread;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT;
import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_JOINED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.internal.GridTopic.TOPIC_CACHE_COORDINATOR;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.SYSTEM_POOL;
import static org.apache.ignite.internal.processors.cache.mvcc.MvccCoordinatorChangeAware.ID_FILTER;
import static org.apache.ignite.internal.processors.cache.mvcc.MvccQueryTracker.MVCC_TRACKER_ID_NA;
import static org.apache.ignite.internal.processors.cache.mvcc.MvccUtils.MVCC_COUNTER_NA;
import static org.apache.ignite.internal.processors.cache.mvcc.MvccUtils.MVCC_CRD_COUNTER_NA;
import static org.apache.ignite.internal.processors.cache.mvcc.MvccUtils.MVCC_HINTS_BIT_OFF;
import static org.apache.ignite.internal.processors.cache.mvcc.MvccUtils.MVCC_INITIAL_CNTR;
import static org.apache.ignite.internal.processors.cache.mvcc.MvccUtils.MVCC_READ_OP_CNTR;
import static org.apache.ignite.internal.processors.cache.mvcc.MvccUtils.MVCC_START_CNTR;
import static org.apache.ignite.internal.processors.cache.mvcc.MvccUtils.MVCC_START_OP_CNTR;
import static org.apache.ignite.internal.processors.cache.mvcc.MvccUtils.belongToSameTx;
import static org.apache.ignite.internal.processors.cache.mvcc.MvccUtils.compare;
import static org.apache.ignite.internal.processors.cache.mvcc.MvccUtils.hasNewVersion;
import static org.apache.ignite.internal.processors.cache.mvcc.MvccUtils.isVisible;
import static org.apache.ignite.internal.processors.cache.mvcc.MvccUtils.noCoordinatorError;
import static org.apache.ignite.internal.processors.cache.mvcc.txlog.TxLog.TX_LOG_CACHE_ID;
import static org.apache.ignite.internal.processors.cache.mvcc.txlog.TxLog.TX_LOG_CACHE_NAME;
import static org.apache.ignite.internal.processors.cache.persistence.CacheDataRowAdapter.RowData.KEY_ONLY;

/**
 * MVCC processor.
 */
@SuppressWarnings("unchecked")
public class MvccProcessorImpl extends GridProcessorAdapter implements MvccProcessor, DatabaseLifecycleListener {
    /** */
    private static final boolean FORCE_MVCC =
        IgniteSystemProperties.getBoolean(IgniteSystemProperties.IGNITE_FORCE_MVCC_MODE_IN_TESTS, false);

    /** */
    private static final IgniteProductVersion MVCC_SUPPORTED_SINCE = IgniteProductVersion.fromString("2.7.0");

    /** */
    private static final Waiter LOCAL_TRANSACTION_MARKER = new LocalTransactionMarker();

    /** For tests only. */
    private static IgniteClosure<Collection<ClusterNode>, ClusterNode> crdC;

    /**
     * For testing only.
     *
     * @param crdC Closure assigning coordinator.
     */
    static void coordinatorAssignClosure(IgniteClosure<Collection<ClusterNode>, ClusterNode> crdC) {
        MvccProcessorImpl.crdC = crdC;
    }

    /** */
    private volatile MvccCoordinator curCrd = MvccCoordinator.UNASSIGNED_COORDINATOR;

    /** */
    private TxLog txLog;

    /** */
    private List<GridWorker> vacuumWorkers;

    /** */
    private BlockingQueue<VacuumTask> cleanupQueue;

    /**
     * Vacuum mutex. Prevents concurrent vacuum while start/stop operations
     */
    private final Object mux = new Object();

    /** */
    private final GridAtomicLong futIdCntr = new GridAtomicLong(0);

    /** */
    private final GridAtomicLong mvccCntr = new GridAtomicLong(MVCC_START_CNTR);

    /** */
    private final GridAtomicLong committedCntr = new GridAtomicLong(MVCC_INITIAL_CNTR);

    /**
     * Contains active transactions on mvcc coordinator. Key is mvcc counter.
     * Access is protected by "this" monitor.
     */
    private final Map<Long, ActiveTx> activeTxs = new HashMap<>();

    /** Active query trackers. */
    private final Map<Long, MvccQueryTracker> activeTrackers = new ConcurrentHashMap<>();

    /** */
    private final Map<UUID, Map<Long, MvccSnapshotResponseListener>> snapLsnrs = new ConcurrentHashMap<>();

    /** */
    private final Map<Long, WaitAckFuture> ackFuts = new ConcurrentHashMap<>();

    /** */
    private final Map<TxKey, Waiter> waitMap = new ConcurrentHashMap<>();

    /** */
    private final ActiveQueries activeQueries = new ActiveQueries();

    /** */
    private final PreviousQueries prevQueries = new PreviousQueries();

    /** */
    private final GridFutureAdapter<Void> initFut = new GridFutureAdapter<>();

    /** Flag whether at least one cache with {@code CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT} mode is registered. */
    private volatile boolean mvccEnabled;

    /** Flag whether all nodes in cluster support MVCC. */
    private volatile boolean mvccSupported = true;

    /** */
    private volatile AffinityTopologyVersion readyVer = AffinityTopologyVersion.NONE;

    /**
     * Maps failed node id to votes accumulator for that node.
     */
    private final ConcurrentHashMap<UUID, RecoveryBallotBox> recoveryBallotBoxes = new ConcurrentHashMap<>();

    /** */
    private final GridSpinBusyLock busyLock = new GridSpinBusyLock();

    /** */
    private final DiscoveryEventListener discoLsnr;

    /** */
    private final GridMessageListener msgLsnr;

    /** */
    private final CustomEventListener customLsnr;

    /** State mutex. */
    private final Object stateMux = new Object();

    /**
     * @param ctx Context.
     */
    public MvccProcessorImpl(GridKernalContext ctx) {
        super(ctx);

        ctx.internalSubscriptionProcessor().registerDatabaseListener(this);

        discoLsnr = this::onDiscovery;
        msgLsnr = new MvccMessageListener();
        customLsnr = new CustomEventListener<DynamicCacheChangeBatch>() {
            @Override public void onCustomEvent(AffinityTopologyVersion topVer, ClusterNode snd,
                DynamicCacheChangeBatch msg) {
                checkMvccCacheStarted(msg);
            }
        };
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        ctx.event().addDiscoveryEventListener(discoLsnr, EVT_NODE_FAILED, EVT_NODE_LEFT, EVT_NODE_JOINED);

        ctx.io().addMessageListener(TOPIC_CACHE_COORDINATOR, msgLsnr);

        ctx.discovery().setCustomEventListener(DynamicCacheChangeBatch.class, customLsnr);
    }

    /** {@inheritDoc} */
    @Override public boolean mvccEnabled() {
        return mvccEnabled;
    }

    /** {@inheritDoc} */
    @Override public void preProcessCacheConfiguration(CacheConfiguration ccfg) {
        if (FORCE_MVCC && ccfg.getAtomicityMode() == TRANSACTIONAL && !CU.isSystemCache(ccfg.getName())) {
            ccfg.setAtomicityMode(TRANSACTIONAL_SNAPSHOT);
            //noinspection unchecked
            ccfg.setNearConfiguration(null);
        }

        if (ccfg.getAtomicityMode() == TRANSACTIONAL_SNAPSHOT) {
            if (!mvccSupported)
                throw new IgniteException("Cannot start MVCC transactional cache. " +
                    "MVCC is unsupported by the cluster.");

            mvccEnabled = true;
        }
    }

    /** {@inheritDoc} */
    @Override public void validateCacheConfiguration(CacheConfiguration ccfg) {
        if (ccfg.getAtomicityMode() == TRANSACTIONAL_SNAPSHOT) {
            if (!mvccSupported)
                throw new IgniteException("Cannot start MVCC transactional cache. " +
                    "MVCC is unsupported by the cluster.");

            mvccEnabled = true;
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override public IgniteNodeValidationResult validateNode(ClusterNode node) {
        if (mvccEnabled && node.version().compareToIgnoreTimestamp(MVCC_SUPPORTED_SINCE) < 0) {
            String errMsg = "Failed to add node to topology. MVCC is enabled on the cluster, but " +
                "the node doesn't support MVCC [nodeId=" + node.id() + ']';

            return new IgniteNodeValidationResult(node.id(), errMsg);
        }

        return null;
    }

    /** {@inheritDoc} */
    @Override public void ensureStarted() throws IgniteCheckedException {
        if (!ctx.clientNode()) {
            assert mvccEnabled && mvccSupported;

            synchronized (mux) {
                if (txLog == null)
                    txLog = new TxLog(ctx, ctx.cache().context().database());
            }

            startVacuumWorkers();

            if (log.isInfoEnabled())
                log.info("Mvcc processor started.");
        }
    }

    /** {@inheritDoc} */
    @Override public void onCacheStop(final GridCacheContext cctx) {
        if (cctx.mvccEnabled() && txLog != null) {
            assert mvccEnabled && mvccSupported;

            boolean hasMvccCaches = ctx.cache().cacheDescriptors().values().stream()
                .anyMatch(c -> c.cacheConfiguration().getAtomicityMode() == TRANSACTIONAL_SNAPSHOT);

            if (!hasMvccCaches)
                stopTxLog();
        }
    }


    /** {@inheritDoc} */
    @Override public void beforeStop(IgniteCacheDatabaseSharedManager mgr) {
        stopTxLog();
    }

    /** {@inheritDoc} */
    @Override public void stopTxLog() {
        stopVacuumWorkers();

        txLog = null;

        mvccEnabled = false;
    }

    /** {@inheritDoc} */
    @Override public void onInitDataRegions(IgniteCacheDatabaseSharedManager mgr) throws IgniteCheckedException {
        // We have to always init txLog data region.
        DataStorageConfiguration dscfg = dataStorageConfiguration();

        mgr.addDataRegion(
            dscfg,
            createTxLogRegion(dscfg),
            CU.isPersistenceEnabled(ctx.config()));
    }

    /** {@inheritDoc} */
    @Override public void beforeResumeWalLogging(IgniteCacheDatabaseSharedManager mgr) throws IgniteCheckedException {
        // In case of blt changed we should re-init TX_LOG cache.
        txLogPageStoreInit(mgr);
    }

    /** {@inheritDoc} */
    @Override public void beforeBinaryMemoryRestore(IgniteCacheDatabaseSharedManager mgr) throws IgniteCheckedException {
        txLogPageStoreInit(mgr);
    }

    /** {@inheritDoc} */
    @Override public void afterBinaryMemoryRestore(IgniteCacheDatabaseSharedManager mgr,
        GridCacheDatabaseSharedManager.RestoreBinaryState restoreState) throws IgniteCheckedException {

        boolean hasMvccCaches = ctx.cache().persistentCaches().stream()
            .anyMatch(c -> c.cacheConfiguration().getAtomicityMode() == TRANSACTIONAL_SNAPSHOT);

        if (hasMvccCaches) {
            txLog = new TxLog(ctx, mgr);

            mvccEnabled = true;
        }
    }

    /**
     * @param mgr Database shared manager.
     * @throws IgniteCheckedException If failed.
     */
    private void txLogPageStoreInit(IgniteCacheDatabaseSharedManager mgr) throws IgniteCheckedException {
        assert CU.isPersistenceEnabled(ctx.config());

        //noinspection ConstantConditions
        ctx.cache().context().pageStore().initialize(TX_LOG_CACHE_ID, 0,
            TX_LOG_CACHE_NAME, mgr.dataRegion(TX_LOG_CACHE_NAME).memoryMetrics().totalAllocatedPages());
    }

    /** {@inheritDoc} */
    @Override public void onExchangeDone(DiscoCache discoCache) {
        assert discoCache != null && readyVer.compareTo(discoCache.version()) < 0;

        MvccCoordinator curCrd0 = curCrd;

        if (curCrd0.disconnected())
            return; // Nothing to do.

        assert curCrd0.topologyVersion().initialized();

        if (curCrd0.initialized() && curCrd0.local())
            cleanupOrphanedServerTransactions(discoCache.serverNodes());

        if (!curCrd0.initialized() && coordinatorChanged(curCrd0, readyVer, discoCache.version()))
            initialize(curCrd0);
    }

    /** {@inheritDoc} */
    @Override public void onLocalJoin(DiscoveryEvent evt, DiscoCache discoCache) {
        assert evt.type() == EVT_NODE_JOINED && evt.eventNode().isLocal();

        checkMvccSupported(discoCache.allNodes());

        onCoordinatorChanged(discoCache.version(), discoCache.allNodes(), false);
    }

    /** {@inheritDoc} */
    @Override public void onDisconnected(IgniteFuture<?> reconnectFut) {
        MvccCoordinator curCrd0 = curCrd;

        if (!curCrd0.disconnected()) {
            // Notify all listeners waiting for a snapshot.
            onCoordinatorFailed(curCrd0.nodeId());

            synchronized (stateMux) {
                curCrd = MvccCoordinator.DISCONNECTED_COORDINATOR;
            }

            readyVer = AffinityTopologyVersion.NONE;
        }
    }

    /** {@inheritDoc} */
    @Override public void onKernalStop(boolean cancel) {
        busyLock.block();

        try {
            ctx.io().removeMessageListener(TOPIC_CACHE_COORDINATOR, msgLsnr);

            ctx.event().removeDiscoveryEventListener(discoLsnr, EVT_NODE_FAILED, EVT_NODE_LEFT, EVT_NODE_JOINED);
        }
        finally {
            // Cleanup pending futures.
            MvccCoordinator curCrd0 = curCrd;

            if (curCrd0.nodeId() != null) //Skip if coordinator is unassigned or disconnected.
                onCoordinatorFailed(curCrd0.nodeId());
        }
    }

    /**
     * Discovery listener. Note: initial join event is handled by {@link MvccProcessorImpl#onLocalJoin}
     * method.
     *
     * @param evt Discovery event.
     */
    private void onDiscovery(DiscoveryEvent evt, DiscoCache discoCache) {
        assert evt.type() == EVT_NODE_FAILED
            || evt.type() == EVT_NODE_LEFT
            || evt.type() == EVT_NODE_JOINED;

        UUID nodeId = evt.eventNode().id();
        AffinityTopologyVersion topVer = discoCache.version();
        List<ClusterNode> nodes = discoCache.allNodes();

        checkMvccSupported(nodes);

        MvccCoordinator curCrd0 = curCrd;

        if (evt.type() == EVT_NODE_JOINED) {
            if (curCrd0.disconnected()) // Handle join event only if coordinator has not been elected yet.
                onCoordinatorChanged(topVer, nodes, true);
        }
        else if (Objects.equals(nodeId, curCrd0.nodeId())) {
            // 1. Notify all listeners waiting for a snapshot.
            onCoordinatorFailed(nodeId);

            // 2. Process coordinator change.
            onCoordinatorChanged(topVer, nodes, true);
        }
        // Process node left event on the current mvcc coordinator.
        else if (curCrd0.local()) {
            // 1. Notify active queries.
            activeQueries.onNodeFailed(nodeId);

            // 2. Notify previous queries.
            prevQueries.onNodeFailed(nodeId);

            if (mvccEnabled) {
                // 3. Recover transactions started by the failed node.
                recoveryBallotBoxes.forEach((nearNodeId, ballotBox) -> {
                    // Put synthetic vote from another failed node
                    ballotBox.vote(nodeId);

                    tryFinishRecoveryVoting(nearNodeId, ballotBox);
                });

                if (evt.eventNode().isClient()) {
                    RecoveryBallotBox ballotBox = recoveryBallotBoxes
                        .computeIfAbsent(nodeId, uuid -> new RecoveryBallotBox());

                    ballotBox.voters(evt.topologyNodes().stream()
                        // Nodes not supporting MVCC will never send votes to us. So, filter them away.
                        .filter(this::supportsMvcc)
                        .map(ClusterNode::id)
                        .collect(Collectors.toList()));

                    tryFinishRecoveryVoting(nodeId, ballotBox);
                }
            }
        }
    }

    /** */
    private void onCoordinatorFailed(UUID nodeId) {
        // 1. Notify all listeners waiting for a snapshot.
        Map<Long, MvccSnapshotResponseListener> map = snapLsnrs.remove(nodeId);

        if (map != null) {
            ClusterTopologyCheckedException ex = new ClusterTopologyCheckedException("Failed to request mvcc " +
                "version, coordinator left: " + nodeId);

            MvccSnapshotResponseListener lsnr;

            for (Long id : map.keySet()) {
                if ((lsnr = map.remove(id)) != null)
                    lsnr.onError(ex);
            }
        }

        // 2. Notify acknowledge futures.
        for (WaitAckFuture fut : ackFuts.values())
            fut.onNodeLeft(nodeId);
    }

    /**
     * Coordinator change callback. Performs all needed actions for handling new coordinator assignment.
     *
     * @param sndQrys {@code True} if it is need to collect/send an active queries list.
     */
    private void onCoordinatorChanged(AffinityTopologyVersion topVer, Collection<ClusterNode> nodes, boolean sndQrys) {
        MvccCoordinator newCrd = pickMvccCoordinator(nodes, topVer);

        synchronized (stateMux) {
            if (ctx.clientDisconnected())
                return;

            if (newCrd.disconnected()) {
                curCrd = newCrd;

                return;
            }

            assert newCrd.topologyVersion().compareTo(curCrd.topologyVersion()) > 0;

            curCrd = newCrd;
        }

        processActiveQueries(nodes, newCrd, sndQrys);
    }

    /** */
    private void processActiveQueries(Collection<ClusterNode> nodes, MvccCoordinator newCrd, boolean sndQrys) {
        GridLongList qryIds = sndQrys ? new GridLongList(Stream.concat(activeTrackers.values().stream(),
            ctx.cache().context().tm().activeTransactions().stream()
                .filter(tx -> tx.near() && tx.local()))
            .mapToLong(q -> ((MvccCoordinatorChangeAware)q).onMvccCoordinatorChange(newCrd))
            .filter(ID_FILTER).toArray()) : new GridLongList();

        if (newCrd.local()) {
            prevQueries.addActiveQueries(ctx.localNodeId(), qryIds);

            prevQueries.init(nodes, ctx.discovery()::alive);
        }
        else if (sndQrys) {
            ctx.getSystemExecutorService().submit(() -> {
                try {
                    sendMessage(newCrd.nodeId(), new MvccActiveQueriesMessage(qryIds));
                }
                catch (IgniteCheckedException e) {
                    U.error(log, "Failed to send active queries to mvcc coordinator: " + e);
                }
            });
        }
    }

    /**
     * @param currCrd Current Mvcc coordinator.
     * @param from Start topology version.
     * @param to End topology version
     * @return {@code True} if coordinator was changed between two passed topology versions.
     */
    private boolean coordinatorChanged(MvccCoordinator currCrd, AffinityTopologyVersion from,
        AffinityTopologyVersion to) {
        return from.compareTo(currCrd.topologyVersion()) < 0
            && to.compareTo(currCrd.topologyVersion()) >= 0;
    }

    /**
     * Cleans up active transactions lost near node which is server. Executed on coordinator.
     *
     * @param liveSrvs Live server nodes at the moment of cleanup.
     */
    private void cleanupOrphanedServerTransactions(Collection<ClusterNode> liveSrvs) {
        Set<UUID> ids = liveSrvs.stream()
            .map(ClusterNode::id)
            .collect(Collectors.toSet());

        List<Long> forRmv = new ArrayList<>();

        synchronized (this) {
            for (Map.Entry<Long, ActiveTx> entry : activeTxs.entrySet()) {
                // If node started tx is not known as live then remove such tx from active list
                ActiveTx activeTx = entry.getValue();

                if (activeTx.getClass() == ActiveServerTx.class && !ids.contains(activeTx.nearNodeId))
                    forRmv.add(entry.getKey());
            }
        }

        for (Long txCntr : forRmv)
            // Committed counter is increased because it is not known if transaction was committed or not and we must
            // bump committed counter for committed transaction as it is used in (read-only) query snapshot.
            onTxDone(txCntr, true);
    }

    /**
     * Initializes a new coordinator.
     */
    private void initialize(MvccCoordinator curCrd0) {
        readyVer = curCrd0.topologyVersion();

        curCrd0.initialized(true);

        // Complete init future if local node is a new coordinator. All previous txs have been already completed here.
        if (curCrd0.local())
            ctx.closure().runLocalSafe(initFut::onDone);
    }

    /** {@inheritDoc} */
    @Override @NotNull public MvccCoordinator currentCoordinator() {
        return curCrd;
    }

    /** {@inheritDoc} */
    @Override public byte state(MvccVersion ver) {
        return state(ver.coordinatorVersion(), ver.counter());
    }

    /** {@inheritDoc} */
    @Override public byte state(long crdVer, long cntr) {
        assert txLog != null && mvccEnabled : mvccEnabled;

        try {
            return txLog.get(crdVer, cntr);
        }
        catch (IgniteCheckedException e) {
            ctx.failure().process(new FailureContext(FailureType.CRITICAL_ERROR, e));
        }

        return TxState.NA;
    }

    /** {@inheritDoc} */
    @Override public void updateState(MvccVersion ver, byte state) {
        updateState(ver, state, true);
    }

    /** {@inheritDoc} */
    @Override public void updateState(MvccVersion ver, byte state, boolean primary) {
        assert mvccEnabled;
        assert txLog != null || waitMap.isEmpty();

        // txLog may not exist if node is non-affinity for any mvcc-cache.
        if (txLog == null)
            return;

        try {
            txLog.put(new TxKey(ver.coordinatorVersion(), ver.counter()), state, primary);
        }
        catch (IgniteCheckedException e) {
            ctx.failure().process(new FailureContext(FailureType.CRITICAL_ERROR, e));
        }
    }

    /** {@inheritDoc} */
    @Override public void registerLocalTransaction(long crd, long cntr) {
        Waiter old = waitMap.putIfAbsent(new TxKey(crd, cntr), LOCAL_TRANSACTION_MARKER);

        assert old == null || old.hasLocalTransaction();
    }

    /** {@inheritDoc} */
    @Override public boolean hasLocalTransaction(long crd, long cntr) {
        Waiter waiter = waitMap.get(new TxKey(crd, cntr));

        return waiter != null && waiter.hasLocalTransaction();
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Void> waitForLock(GridCacheContext cctx, MvccVersion waiterVer,
        MvccVersion blockerVer) {
        TxKey key = new TxKey(blockerVer.coordinatorVersion(), blockerVer.counter());

        LockFuture fut = new LockFuture(cctx.ioPolicy(), waiterVer);

        Waiter waiter = waitMap.merge(key, fut, Waiter::concat);

        if (!waiter.hasLocalTransaction() && (waiter = waitMap.remove(key)) != null)
            waiter.run(ctx);
        else {
            DeadlockDetectionManager.DelayedDeadlockComputation delayedComputation
                = ctx.cache().context().deadlockDetectionMgr().initDelayedComputation(waiterVer, blockerVer);

            if (delayedComputation != null)
                fut.listen(fut0 -> delayedComputation.cancel());
        }

        return fut;
    }

    /** {@inheritDoc} */
    @Override public void releaseWaiters(MvccVersion locked) {
        Waiter waiter = waitMap.remove(new TxKey(locked.coordinatorVersion(), locked.counter()));

        if (waiter != null)
            waiter.run(ctx);
    }

    /** {@inheritDoc} */
    @Override public void addQueryTracker(MvccQueryTracker tracker) {
        assert tracker.id() != MVCC_TRACKER_ID_NA;

        activeTrackers.putIfAbsent(tracker.id(), tracker);
    }

    /** {@inheritDoc} */
    @Override public void removeQueryTracker(Long id) {
        activeTrackers.remove(id);
    }

    /** {@inheritDoc} */
    @Override public MvccSnapshot requestWriteSnapshotLocal() {
        if (!currentCoordinator().local() || !initFut.isDone())
            return null;

        return assignTxSnapshot(0L, ctx.localNodeId(), false);
    }

    /** {@inheritDoc} */
    @Override public MvccSnapshot requestReadSnapshotLocal() {
        if (!currentCoordinator().local() || !initFut.isDone())
            return null;

        return activeQueries.assignQueryCounter(ctx.localNodeId(), 0L);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<MvccSnapshot> requestReadSnapshotAsync() {
        MvccSnapshotFuture fut = new MvccSnapshotFuture();

        requestReadSnapshotAsync(currentCoordinator(), fut);

        return fut;
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<MvccSnapshot> requestWriteSnapshotAsync() {
        MvccSnapshotFuture fut = new MvccSnapshotFuture();

        requestWriteSnapshotAsync(currentCoordinator(), fut);

        return fut;
    }

    /** {@inheritDoc} */
    @Override public void requestReadSnapshotAsync(MvccCoordinator crd, MvccSnapshotResponseListener lsnr) {
        requestSnapshotAsync(crd, lsnr, true);
    }

    /** {@inheritDoc} */
    @Override public void requestWriteSnapshotAsync(MvccCoordinator crd, MvccSnapshotResponseListener lsnr) {
        requestSnapshotAsync(crd, lsnr, false);
    }

    /** */
    private void requestSnapshotAsync(MvccCoordinator crd, MvccSnapshotResponseListener lsnr, boolean forRead) {
        if (crd.disconnected()) {
            lsnr.onError(noCoordinatorError());

            return;
        }

        if (!busyLock.enterBusy()) {
            lsnr.onError(new NodeStoppingException("Failed to request snapshot (Node is stopping)."));

            return;
        }

        try {
            if (ctx.localNodeId().equals(crd.nodeId())) {
                if (!initFut.isDone()) {
                    // Wait for the local coordinator init.
                    initFut.listen(new IgniteInClosure<IgniteInternalFuture>() {
                        @Override public void apply(IgniteInternalFuture fut) {
                            if (forRead)
                                lsnr.onResponse(activeQueries.assignQueryCounter(ctx.localNodeId(), 0L));
                            else
                                lsnr.onResponse(assignTxSnapshot(0L, ctx.localNodeId(), false));
                        }
                    });
                }
                else if (forRead)
                    lsnr.onResponse(activeQueries.assignQueryCounter(ctx.localNodeId(), 0L));
                else
                    lsnr.onResponse(assignTxSnapshot(0L, ctx.localNodeId(), false));

                return;
            }

            // Send request to the remote coordinator.
            UUID nodeId = crd.nodeId();

            long id = futIdCntr.incrementAndGet();

            Map<Long, MvccSnapshotResponseListener> map = snapLsnrs.get(nodeId), map0;

            if (map == null && (map0 = snapLsnrs.putIfAbsent(nodeId, map = new ConcurrentHashMap<>())) != null)
                map = map0;

            map.put(id, lsnr);

            try {
                sendMessage(nodeId, forRead ? new MvccQuerySnapshotRequest(id) : new MvccTxSnapshotRequest(id));
            }
            catch (IgniteCheckedException e) {
                if (map.remove(id) != null)
                    lsnr.onError(e);
            }
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Void> ackTxCommit(MvccSnapshot updateVer) {
        assert updateVer != null;

        MvccCoordinator crd = curCrd;

        if (crd.disconnected() || crd.version() != updateVer.coordinatorVersion())
            return new GridFinishedFuture<>();

        return sendTxCommit(crd, new MvccAckRequestTx(futIdCntr.incrementAndGet(), updateVer.counter()));
    }

    /** {@inheritDoc} */
    @Override public void ackTxRollback(MvccVersion updateVer) {
        assert updateVer != null;

        MvccCoordinator crd = curCrd;

        if (crd.disconnected() || crd.version() != updateVer.coordinatorVersion())
            return;

        MvccAckRequestTx msg = new MvccAckRequestTx((long)-1, updateVer.counter());

        msg.skipResponse(true);

        try {
            sendMessage(crd.nodeId(), msg);
        }
        catch (ClusterTopologyCheckedException e) {
            if (log.isDebugEnabled())
                log.debug("Failed to send tx rollback ack, node left [msg=" + msg + ", node=" + crd.nodeId() + ']');
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Failed to send tx rollback ack [msg=" + msg + ", node=" + crd.nodeId() + ']', e);
        }
    }

    /** {@inheritDoc} */
    @Override public void ackQueryDone(MvccSnapshot snapshot, long qryId) {
        MvccCoordinator crd = currentCoordinator();

        if (crd.disconnected() || snapshot == null)
            return;

        if (crd.version() != snapshot.coordinatorVersion()
            || !sendQueryDone(crd, new MvccAckRequestQueryCntr(queryTrackCounter(snapshot)))) {
            Message msg = new MvccAckRequestQueryId(qryId);

            do {
                crd = currentCoordinator();
            }
            while (!sendQueryDone(crd, msg));
        }
    }

    /** {@inheritDoc} */
    // TODO: Proper use of diagnostic context.
    @Override public void dumpDebugInfo(IgniteLogger log, @Nullable IgniteDiagnosticPrepareContext diagCtx) {
        boolean first = true;

        for (Map<Long, MvccSnapshotResponseListener> map : snapLsnrs.values()) {
            if (first) {
                U.warn(log, "Pending mvcc listener: ");

                first = false;
            }

            for (MvccSnapshotResponseListener lsnr : map.values())
                U.warn(log, ">>> " + lsnr.toString());
        }

        first = true;

        for (WaitAckFuture waitAckFut : ackFuts.values()) {
            if (first) {
                U.warn(log, "Pending mvcc wait ack futures: ");

                first = false;
            }

            U.warn(log, ">>> " + waitAckFut.toString());
        }
    }

    /**
     * Removes all less or equals to the given one records from Tx log.
     *
     * @param ver Version.
     * @throws IgniteCheckedException If fails.
     */
    void removeUntil(MvccVersion ver) throws IgniteCheckedException {
        txLog.removeUntil(ver.coordinatorVersion(), ver.counter());
    }

    /**
     * TODO IGNITE-7966
     *
     * @return Data region configuration.
     */
    private DataRegionConfiguration createTxLogRegion(DataStorageConfiguration dscfg) {
        DataRegionConfiguration cfg = new DataRegionConfiguration();

        cfg.setName(TX_LOG_CACHE_NAME);
        cfg.setInitialSize(dscfg.getSystemRegionInitialSize());
        cfg.setMaxSize(dscfg.getSystemRegionMaxSize());
        cfg.setPersistenceEnabled(CU.isPersistenceEnabled(dscfg));
        cfg.setLazyMemoryAllocation(false);

        return cfg;
    }

    /**
     * @return Data storage configuration.
     */
    private DataStorageConfiguration dataStorageConfiguration() {
        return ctx.config().getDataStorageConfiguration();
    }

    /**
     * Picks mvcc coordinator from the given list of nodes.
     *
     * @return Chosen mvcc coordinator.
     */
    private @NotNull MvccCoordinator pickMvccCoordinator(Collection<ClusterNode> nodes, AffinityTopologyVersion topVer) {
        ClusterNode crdNode = null;

        if (crdC != null) {
            crdNode = crdC.apply(nodes);

            if (crdNode != null && log.isInfoEnabled())
                log.info("Assigned coordinator using test closure: " + crdNode.id());
        }
        else {
            // Expect nodes are sorted by order.
            for (ClusterNode node : nodes) {
                if (!node.isClient() && supportsMvcc(node)) {
                    crdNode = node;

                    break;
                }
            }
        }

        MvccCoordinator crd = crdNode != null ? new MvccCoordinator(topVer, crdNode.id(), coordinatorVersion(crdNode),
            crdNode.isLocal()) : MvccCoordinator.DISCONNECTED_COORDINATOR;

        if (crd.disconnected())
            U.warn(log, "New mvcc coordinator was not assigned [topVer=" + topVer + ']');
        else if (log.isInfoEnabled())
            log.info("Assigned mvcc coordinator [crd=" + crd + ']');

        return crd;
    }

    /**
     * @param crdNode Assigned coordinator node.
     * @return Coordinator version.
     */
    private long coordinatorVersion(ClusterNode crdNode) {
        return crdNode.order() + ctx.discovery().gridStartTime();
    }

    /** */
    private void checkMvccSupported(Collection<ClusterNode> nodes) {
        if (mvccEnabled) {
            assert mvccSupported;

            return;
        }

        boolean res = true, was = mvccSupported;

        for (ClusterNode node : nodes) {
            if (!supportsMvcc(node)) {
                res = false;

                break;
            }
        }

        if (was != res)
            mvccSupported = res;
    }

    /** */
    private boolean supportsMvcc(ClusterNode node) {
        return node.version().compareToIgnoreTimestamp(MVCC_SUPPORTED_SINCE) >= 0;
    }

    /** */
    private void checkMvccCacheStarted(DynamicCacheChangeBatch cacheMsg) {
        if (!mvccEnabled) {
            for (DynamicCacheChangeRequest req : cacheMsg.requests()) {
                CacheConfiguration ccfg = req.startCacheConfiguration();

                if (ccfg == null)
                    continue;

                if (ccfg.getAtomicityMode() == TRANSACTIONAL_SNAPSHOT) {
                    assert mvccSupported;

                    mvccEnabled = true;
                }
            }
        }
    }

    /** */
    private MvccSnapshotResponse assignTxSnapshot(long futId, UUID nearId, boolean client) {
        assert initFut.isDone() && curCrd.local();

        MvccSnapshotResponse res = new MvccSnapshotResponse();

        long ver, cleanup, tracking;

        synchronized (this) {
            ver = mvccCntr.incrementAndGet();
            tracking = ver;
            cleanup = committedCntr.get() + 1;

            for (Map.Entry<Long, ActiveTx> entry : activeTxs.entrySet()) {
                cleanup = Math.min(entry.getValue().tracking, cleanup);
                tracking = Math.min(entry.getKey(), tracking);

                res.addTx(entry.getKey());
            }

            ActiveTx activeTx = client ? new ActiveTx(tracking, nearId) : new ActiveServerTx(tracking, nearId);

            boolean add = activeTxs.put(ver, activeTx) == null;

            assert add : ver;
        }

        long minQry = activeQueries.minimalQueryCounter();

        if (minQry != -1)
            cleanup = Math.min(cleanup, minQry);

        cleanup = prevQueries.done() ? cleanup - 1 : MVCC_COUNTER_NA;

        res.init(futId, curCrd.version(), ver, MVCC_START_OP_CNTR, cleanup, tracking);

        return res;
    }

    /** */
    private void onTxDone(Long txCntr, boolean increaseCommittedCntr) {
        assert initFut.isDone();

        synchronized (this) {
            activeTxs.remove(txCntr);

            if (increaseCommittedCntr)
                committedCntr.setIfGreater(txCntr);
        }
    }

    /**
     * @param mvccCntr Query counter.
     */
    private void onQueryDone(UUID nodeId, Long mvccCntr) {
        activeQueries.onQueryDone(nodeId, mvccCntr);
    }

    /**
     * @param mvccVer Read version.
     * @return Tracker counter.
     */
    private long queryTrackCounter(MvccSnapshot mvccVer) {
        long trackCntr = mvccVer.counter();

        MvccLongList txs = mvccVer.activeTransactions();

        int size = txs.size();

        for (int i = 0; i < size; i++) {
            long txVer = txs.get(i);

            if (txVer < trackCntr)
                trackCntr = txVer;
        }

        return trackCntr;
    }

    /**
     * Launches vacuum workers and scheduler.
     */
    void startVacuumWorkers() {
        assert !ctx.clientNode();

        synchronized (mux) {
            if (vacuumWorkers == null) {
                assert cleanupQueue == null;

                cleanupQueue = new LinkedBlockingQueue<>();

                vacuumWorkers = new ArrayList<>(ctx.config().getMvccVacuumThreadCount() + 1);

                vacuumWorkers.add(new VacuumScheduler(ctx, log, this));

                for (int i = 0; i < ctx.config().getMvccVacuumThreadCount(); i++)
                    vacuumWorkers.add(new VacuumWorker(ctx, log, cleanupQueue));

                for (GridWorker worker : vacuumWorkers)
                    new IgniteThread(worker).start();

                return;
            }
        }

        U.warn(log, "Attempting to start active vacuum.");
    }

    /**
     * Stops vacuum worker and scheduler.
     */
    void stopVacuumWorkers() {
        if (!ctx.clientNode()) {
            List<GridWorker> workers;
            BlockingQueue<VacuumTask> queue;

            synchronized (mux) {
                workers = vacuumWorkers;
                queue = cleanupQueue;

                vacuumWorkers = null;
                cleanupQueue = null;
            }

            if (workers == null) {
                if (log.isDebugEnabled() && mvccEnabled())
                    log.debug("Attempting to stop inactive vacuum.");

                return;
            }

            assert queue != null;

            // Stop vacuum workers outside mutex to prevent deadlocks.
            U.cancel(workers);
            U.join(workers, log);

            if (!queue.isEmpty()) {
                IgniteCheckedException ex = vacuumCancelledException();

                for (VacuumTask task : queue)
                    task.onDone(ex);
            }
        }
    }

    /**
     * Runs vacuum process.
     *
     * @return {@code Future} with {@link VacuumMetrics}.
     */
    IgniteInternalFuture<VacuumMetrics> runVacuum() {
        assert !ctx.clientNode();

        MvccCoordinator crd0 = currentCoordinator();

        if (!crd0.initialized() || Thread.currentThread().isInterrupted())
            return new GridFinishedFuture<>(new VacuumMetrics());

        final GridFutureAdapter<VacuumMetrics> res = new GridFutureAdapter<>();

        // TODO IGNITE-8974 create special method for getting cleanup version only.
        MvccSnapshot snapshot = requestWriteSnapshotLocal();

        if (snapshot != null)
            continueRunVacuum(res, snapshot);
        else
            requestWriteSnapshotAsync(crd0, new MvccSnapshotResponseListener() {
                @Override public void onResponse(MvccSnapshot s) {
                    continueRunVacuum(res, s);
                }

                @Override public void onError(IgniteCheckedException e) {
                    if (!(e instanceof ClusterTopologyCheckedException))
                        completeWithException(res, e);
                    else {
                        if (log.isDebugEnabled())
                            log.debug("Vacuum failed to receive an Mvcc snapshot. " +
                                "Need to retry on the stable topology. " + e.getMessage());

                        res.onDone(new VacuumMetrics());
                    }
                }
            });

        return res;
    }

    /**
     * @param res Result.
     * @param snapshot Snapshot.
     */
    private void continueRunVacuum(GridFutureAdapter<VacuumMetrics> res, MvccSnapshot snapshot) {
        ackTxCommit(snapshot)
            .listen(new IgniteInClosure<IgniteInternalFuture>() {
                @Override public void apply(IgniteInternalFuture fut) {
                    Throwable err;

                    if ((err = fut.error()) != null) {
                        U.error(log, "Vacuum error.", err);

                        res.onDone(err);
                    }
                    else if (snapshot.cleanupVersion() <= MVCC_COUNTER_NA)
                        res.onDone(new VacuumMetrics());
                    else {
                        try {
                            if (log.isDebugEnabled())
                                log.debug("Started vacuum with cleanup version=" + snapshot.cleanupVersion() + '.');

                            synchronized (mux) {
                                if (cleanupQueue == null) {
                                    res.onDone(vacuumCancelledException());

                                    return;
                                }

                                GridCompoundIdentityFuture<VacuumMetrics> res0 =
                                    new GridCompoundIdentityFuture<VacuumMetrics>(new VacuumMetricsReducer()) {
                                        /** {@inheritDoc} */
                                        @Override protected void logError(IgniteLogger log, String msg, Throwable e) {
                                            // no-op
                                        }

                                        /** {@inheritDoc} */
                                        @Override protected void logDebug(IgniteLogger log, String msg) {
                                            // no-op
                                        }
                                    };

                                for (CacheGroupContext grp : ctx.cache().cacheGroups()) {
                                    if (grp.mvccEnabled()) {
                                        grp.topology().readLock();

                                        try {
                                            for (GridDhtLocalPartition part : grp.topology().localPartitions()) {
                                                VacuumTask task = new VacuumTask(snapshot, part);

                                                cleanupQueue.offer(task);

                                                res0.add(task);
                                            }
                                        }
                                        finally {
                                            grp.topology().readUnlock();
                                        }
                                    }
                                }

                                res0.markInitialized();

                                res0.listen(future -> {
                                    VacuumMetrics metrics = null; Throwable ex = null;

                                    try {
                                        metrics = future.get();

                                        txLog.removeUntil(snapshot.coordinatorVersion(), snapshot.cleanupVersion());

                                        if (log.isDebugEnabled())
                                            log.debug("Vacuum completed. " + metrics);
                                    } catch (Throwable e) {
                                        if (X.hasCause(e, NodeStoppingException.class)) {
                                            if (log.isDebugEnabled())
                                                log.debug("Cannot complete vacuum (node is stopping).");

                                            metrics = new VacuumMetrics();
                                        } else
                                            ex = new GridClosureException(e);
                                    }

                                    res.onDone(metrics, ex);
                                });
                            }

                        }
                        catch (Throwable e) {
                            completeWithException(res, e);
                        }
                    }
                }
            });
    }

    /** */
    private void completeWithException(GridFutureAdapter fut, Throwable e) {
        fut.onDone(e);

        if (e instanceof Error)
            throw (Error)e;
    }

    /** */
    @NotNull private IgniteCheckedException vacuumCancelledException() {
        return new NodeStoppingException("Operation has been cancelled (node is stopping).");
    }

    /** */
    @NotNull private IgniteInternalFuture<Void> sendTxCommit(MvccCoordinator crd, MvccAckRequestTx msg) {
        if (!busyLock.enterBusy())
            return new GridFinishedFuture<>();

        WaitAckFuture fut = null;

        try {
            fut = new WaitAckFuture(msg.futureId(), crd.nodeId(), true);

            ackFuts.put(fut.id, fut);

            sendMessage(crd.nodeId(), msg);
        }
        catch (IgniteCheckedException e) {
            if (fut != null && ackFuts.remove(fut.id) != null) {
                if (e instanceof ClusterTopologyCheckedException) {
                    if (log.isDebugEnabled())
                        log.debug("Failed to send tx ack, node left [crd=" + crd + ", msg=" + msg + ']');

                    fut.onDone(); // No need to ack, finish without error.
                }
                else
                    fut.onDone(e);
            }
        }
        finally {
            busyLock.leaveBusy();
        }

        return fut != null ? fut : new GridFinishedFuture<>();
    }

    /**
     * @param crd Mvcc coordinator.
     * @param msg Message.
     * @return {@code True} if no need to resend the message to a new coordinator.
     */
    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    private boolean sendQueryDone(MvccCoordinator crd, Message msg) {
        if (crd.disconnected())
            return true; // no need to send ack;

        try {
            sendMessage(crd.nodeId(), msg);

            return true;
        }
        catch (ClusterTopologyCheckedException e) {
            if (log.isDebugEnabled())
                log.debug("Failed to send query ack, node left [crd=" + crd + ", msg=" + msg + ']');

            MvccCoordinator crd0 = currentCoordinator();

            // Coordinator is unassigned or still the same.
            return crd0.disconnected() || crd.version() == crd0.version();
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Failed to send query ack [crd=" + crd + ", msg=" + msg + ']', e);

            return true;
        }
    }

    /**
     * Send IO message.
     *
     * @param nodeId Node ID.
     * @param msg Message.
     */
    private void sendMessage(UUID nodeId, Message msg) throws IgniteCheckedException {
        ctx.io().sendToGridTopic(nodeId, TOPIC_CACHE_COORDINATOR, msg, SYSTEM_POOL);
    }

    /**
     * @param nodeId Sender node ID.
     * @param msg Message.
     */
    private void processCoordinatorTxSnapshotRequest(UUID nodeId, MvccTxSnapshotRequest msg) {
        ClusterNode node = ctx.discovery().node(nodeId);

        if (node == null) {
            if (log.isDebugEnabled())
                log.debug("Ignore tx snapshot request processing, node left [msg=" + msg + ", node=" + nodeId + ']');

            return;
        }

        MvccSnapshotResponse res = assignTxSnapshot(msg.futureId(), nodeId, node.isClient());

        boolean finishFailed = true;

        try {
            sendMessage(node.id(), res);

            finishFailed = false;
        }
        catch (ClusterTopologyCheckedException e) {
            if (log.isDebugEnabled())
                log.debug("Failed to send tx snapshot response, node left [msg=" + msg + ", node=" + nodeId + ']');
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Failed to send tx snapshot response [msg=" + msg + ", node=" + nodeId + ']', e);
        }

        if (finishFailed)
            onTxDone(res.counter(), false);
    }

    /**
     * @param nodeId Sender node ID.
     * @param msg Message.
     */
    private void processCoordinatorQuerySnapshotRequest(UUID nodeId, MvccQuerySnapshotRequest msg) {
        ClusterNode node = ctx.discovery().node(nodeId);

        if (node == null) {
            if (log.isDebugEnabled())
                log.debug("Ignore query counter request processing, node left [msg=" + msg + ", node=" + nodeId + ']');

            return;
        }

        MvccSnapshotResponse res = activeQueries.assignQueryCounter(nodeId, msg.futureId());

        try {
            sendMessage(node.id(), res);
        }
        catch (ClusterTopologyCheckedException e) {
            if (log.isDebugEnabled())
                log.debug("Failed to send query counter response, node left [msg=" + msg + ", node=" + nodeId + ']');
        }
        catch (IgniteCheckedException e) {
            onQueryDone(nodeId, res.tracking());

            U.error(log, "Failed to send query counter response [msg=" + msg + ", node=" + nodeId + ']', e);
        }
    }

    /**
     * @param nodeId Sender node ID.
     * @param msg Message.
     */
    private void processCoordinatorSnapshotResponse(UUID nodeId, MvccSnapshotResponse msg) {
        Map<Long, MvccSnapshotResponseListener> map = snapLsnrs.get(nodeId);

        MvccSnapshotResponseListener lsnr;

        if (map != null && (lsnr = map.remove(msg.futureId())) != null)
            lsnr.onResponse(msg);
        else {
            if (ctx.discovery().alive(nodeId))
                U.warn(log, "Failed to find query version future [node=" + nodeId + ", msg=" + msg + ']');
            else if (log.isDebugEnabled())
                log.debug("Failed to find query version future [node=" + nodeId + ", msg=" + msg + ']');
        }
    }

    /**
     * @param nodeId Node ID.
     * @param msg Message.
     */
    private void processCoordinatorQueryAckRequest(UUID nodeId, MvccAckRequestQueryCntr msg) {
        onQueryDone(nodeId, msg.counter());
    }

    /**
     * @param nodeId Node ID.
     * @param msg Message.
     */
    private void processNewCoordinatorQueryAckRequest(UUID nodeId, MvccAckRequestQueryId msg) {
        prevQueries.onQueryDone(nodeId, msg.queryTrackerId());
    }

    /**
     * @param nodeId Sender node ID.
     * @param msg Message.
     */
    private void processCoordinatorTxAckRequest(UUID nodeId, MvccAckRequestTx msg) {
        onTxDone(msg.txCounter(), msg.futureId() >= 0);

        if (msg.queryCounter() != MVCC_COUNTER_NA)
            onQueryDone(nodeId, msg.queryCounter());
        else if (msg.queryTrackerId() != MVCC_TRACKER_ID_NA)
            prevQueries.onQueryDone(nodeId, msg.queryTrackerId());

        if (!msg.skipResponse()) {
            try {
                sendMessage(nodeId, new MvccFutureResponse(msg.futureId()));
            }
            catch (ClusterTopologyCheckedException e) {
                if (log.isDebugEnabled())
                    log.debug("Failed to send tx ack response, node left [msg=" + msg + ", node=" + nodeId + ']');
            }
            catch (IgniteCheckedException e) {
                U.error(log, "Failed to send tx ack response [msg=" + msg + ", node=" + nodeId + ']', e);
            }
        }
    }

    /**
     * @param nodeId Sender node ID.
     * @param msg Message.
     */
    private void processCoordinatorAckResponse(UUID nodeId, MvccFutureResponse msg) {
        WaitAckFuture fut = ackFuts.remove(msg.futureId());

        if (fut != null)
            fut.onResponse();
        else {
            if (ctx.discovery().alive(nodeId))
                U.warn(log, "Failed to find tx ack future [node=" + nodeId + ", msg=" + msg + ']');
            else if (log.isDebugEnabled())
                log.debug("Failed to find tx ack future [node=" + nodeId + ", msg=" + msg + ']');
        }
    }

    /**
     * @param nodeId Node ID.
     * @param msg Message.
     */
    private void processActiveQueriesMessage(UUID nodeId, MvccActiveQueriesMessage msg) {
        GridLongList queryIds = msg.activeQueries();

        assert queryIds != null;

        prevQueries.addActiveQueries(nodeId, queryIds);
    }

    /**
     *
     */
    private class ActiveQueries {
        /** */
        private final Map<UUID, TreeMap<Long, AtomicInteger>> activeQueries = new HashMap<>();

        /** */
        private Long minQry;

        /** */
        private synchronized long minimalQueryCounter() {
            return minQry == null ? -1 : minQry;
        }

        /** */
        private synchronized MvccSnapshotResponse assignQueryCounter(UUID nodeId, long futId) {
            MvccSnapshotResponse res = new MvccSnapshotResponse();

            long ver, tracking;

            synchronized (MvccProcessorImpl.this) {
                ver = committedCntr.get();
                tracking = ver;

                for (Long txVer : activeTxs.keySet()) {
                    if (txVer < ver) {
                        tracking = Math.min(txVer, tracking);
                        res.addTx(txVer);
                    }
                }
            }

            TreeMap<Long, AtomicInteger> nodeMap = activeQueries.get(nodeId);

            if (nodeMap == null) {
                activeQueries.put(nodeId, nodeMap = new TreeMap<>());

                nodeMap.put(tracking, new AtomicInteger(1));
            }
            else {
                AtomicInteger cntr = nodeMap.get(tracking);

                if (cntr == null)
                    nodeMap.put(tracking, new AtomicInteger(1));
                else
                    cntr.incrementAndGet();
            }

            if (minQry == null)
                minQry = tracking;

            res.init(futId, curCrd.version(), ver, MVCC_READ_OP_CNTR, MVCC_COUNTER_NA, tracking);

            return res;
        }

        /** */
        private synchronized void onQueryDone(UUID nodeId, Long ver) {
            TreeMap<Long, AtomicInteger> nodeMap = activeQueries.get(nodeId);

            if (nodeMap == null)
                return;

            assert minQry != null;

            AtomicInteger cntr = nodeMap.get(ver);

            assert cntr != null && cntr.get() > 0 : "onQueryDone ver=" + ver;

            if (cntr.decrementAndGet() == 0) {
                nodeMap.remove(ver);

                if (nodeMap.isEmpty())
                    activeQueries.remove(nodeId);

                if (ver.equals(minQry))
                    minQry = activeMinimal();
            }
        }

        /** */
        private synchronized void onNodeFailed(UUID nodeId) {
            activeQueries.remove(nodeId);

            minQry = activeMinimal();
        }

        /** */
        private Long activeMinimal() {
            Long min = null;

            for (TreeMap<Long, AtomicInteger> s : activeQueries.values()) {
                Long first = s.firstKey();

                if (min == null || first < min)
                    min = first;
            }

            return min;
        }
    }

    /**
     *
     */
    private class WaitAckFuture extends MvccFuture<Void> {
        /** */
        private final long id;

        /** */
        final boolean ackTx;

        /**
         * @param id Future ID.
         * @param nodeId Coordinator node ID.
         * @param ackTx {@code True} if ack tx commit, {@code false} if waits for previous txs.
         */
        WaitAckFuture(long id, UUID nodeId, boolean ackTx) {
            super(nodeId);

            this.id = id;
            this.ackTx = ackTx;
        }

        /**
         *
         */
        void onResponse() {
            onDone();
        }

        /**
         * @param nodeId Failed node ID.
         */
        void onNodeLeft(UUID nodeId) {
            if (crdId.equals(nodeId) && ackFuts.remove(id) != null)
                onDone();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(WaitAckFuture.class, this, super.toString());
        }
    }

    /** {@inheritDoc} */
    @Override public Optional<? extends MvccVersion> checkWaiting(MvccVersion mvccVer) {
        return waitMap.entrySet().stream()
            .filter(e -> e.getValue().lockFuture(mvccVer) != null)
            .map(Map.Entry::getKey)
            .map(key -> new MvccVersionImpl(key.major(), key.minor(), 0))
            .findAny();
    }

    /** {@inheritDoc} */
    @Override public void failWaiter(MvccVersion mvccVer, Exception e) {
        waitMap.values().stream()
            .map(w -> w.lockFuture(mvccVer))
            .filter(Objects::nonNull)
            .findAny()
            .ifPresent(w -> w.onDone(e));
    }

    /**
     *
     */
    private class MvccMessageListener implements GridMessageListener {
        /** {@inheritDoc} */
        @Override public void onMessage(UUID nodeId, Object msg, byte plc) {
            MvccMessage msg0 = (MvccMessage)msg;

            if (msg0.waitForCoordinatorInit() && !initFut.isDone()) {
                initFut.listen(new IgniteInClosure<IgniteInternalFuture<Void>>() {
                    @Override public void apply(IgniteInternalFuture<Void> fut) {
                        assert curCrd.local();

                        processMessage(nodeId, msg);
                    }
                });
            }
            else
                processMessage(nodeId, msg);
        }

        /**
         * Processes mvcc message.
         *
         * @param nodeId Node id.
         * @param msg Message.
         */
        private void processMessage(UUID nodeId, Object msg) {
            if (msg instanceof MvccTxSnapshotRequest)
                processCoordinatorTxSnapshotRequest(nodeId, (MvccTxSnapshotRequest)msg);
            else if (msg instanceof MvccAckRequestTx)
                processCoordinatorTxAckRequest(nodeId, (MvccAckRequestTx)msg);
            else if (msg instanceof MvccFutureResponse)
                processCoordinatorAckResponse(nodeId, (MvccFutureResponse)msg);
            else if (msg instanceof MvccAckRequestQueryCntr)
                processCoordinatorQueryAckRequest(nodeId, (MvccAckRequestQueryCntr)msg);
            else if (msg instanceof MvccQuerySnapshotRequest)
                processCoordinatorQuerySnapshotRequest(nodeId, (MvccQuerySnapshotRequest)msg);
            else if (msg instanceof MvccSnapshotResponse)
                processCoordinatorSnapshotResponse(nodeId, (MvccSnapshotResponse)msg);
            else if (msg instanceof MvccAckRequestQueryId)
                processNewCoordinatorQueryAckRequest(nodeId, (MvccAckRequestQueryId)msg);
            else if (msg instanceof MvccActiveQueriesMessage)
                processActiveQueriesMessage(nodeId, (MvccActiveQueriesMessage)msg);
            else if (msg instanceof MvccRecoveryFinishedMessage)
                processRecoveryFinishedMessage(nodeId, ((MvccRecoveryFinishedMessage)msg));
            else
                U.warn(log, "Unexpected message received [node=" + nodeId + ", msg=" + msg + ']');
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "MvccMessageListener[]";
        }
    }

    /**
     * Accumulates transaction recovery votes for a node left the cluster.
     * Transactions started by the left node are considered not active
     * when each cluster server node aknowledges that is has finished transactions for the left node.
     */
    private static class RecoveryBallotBox {
        /** */
        private List<UUID> voters;

        /** */
        private final Set<UUID> ballots = new HashSet<>();

        /**
         * @param voters Nodes which can have transaction started by the left node.
         */
        private synchronized void voters(List<UUID> voters) {
            this.voters = voters;
        }

        /**
         * @param nodeId Voting node id.
         *
         */
        private synchronized void vote(UUID nodeId) {
            ballots.add(nodeId);
        }

        /**
         * @return {@code True} if all nodes expected to vote done it.
         */
        private synchronized boolean isVotingDone() {
            if (voters == null)
                return false;

            return ballots.containsAll(voters);
        }
    }

    /**
     * Process message that one node has finished with transactions for the left node.
     * @param nodeId Node sent the message.
     * @param msg Message.
     */
    private void processRecoveryFinishedMessage(UUID nodeId, MvccRecoveryFinishedMessage msg) {
        if (!mvccEnabled)
            return;

        UUID nearNodeId = msg.nearNodeId();

        RecoveryBallotBox ballotBox = recoveryBallotBoxes.computeIfAbsent(nearNodeId, uuid -> new RecoveryBallotBox());

        ballotBox.vote(nodeId);

        tryFinishRecoveryVoting(nearNodeId, ballotBox);
    }

    /**
     * Finishes recovery on coordinator by removing transactions started by the left node
     * @param nearNodeId Left node.
     * @param ballotBox Votes accumulator for the left node.
     */
    private void tryFinishRecoveryVoting(UUID nearNodeId, RecoveryBallotBox ballotBox) {
        if (ballotBox.isVotingDone()) {
            List<Long> recoveredTxs;

            synchronized (this) {
                recoveredTxs = activeTxs.entrySet().stream()
                    .filter(e -> e.getValue().nearNodeId.equals(nearNodeId))
                    .map(Map.Entry::getKey)
                    .collect(Collectors.toList());
            }

            // Committed counter is increased because it is not known if transaction was committed or not and we must
            // bump committed counter for committed transaction as it is used in (read-only) query snapshot.
            recoveredTxs.forEach(txCntr -> onTxDone(txCntr, true));

            recoveryBallotBoxes.remove(nearNodeId);
        }
    }

    /** */
    private interface Waiter {
        /**
         * @param ctx Grid kernal context.
         */
        void run(GridKernalContext ctx);

        /**
         * @param other Another waiter.
         * @return New compound waiter.
         */
        Waiter concat(Waiter other);

        /**
         * @return {@code True} if there is an active local transaction
         */
        boolean hasLocalTransaction();

        /**
         * @return {@code True} if it is a compound waiter.
         */
        boolean compound();

        /**
         * @param checkedVer Version of transaction checking for wait.
         * @return Lock future corresponding to checked transaction or {@code null} if it is not waiting.
         */
        @Nullable GridFutureAdapter<?> lockFuture(MvccVersion checkedVer);
    }

    /** */
    private static class LockFuture extends GridFutureAdapter<Void> implements Waiter, Runnable {
        /** */
        private final byte plc;

        /** */
        private final MvccVersion waitingTxVer;

        /**
         * @param plc Pool policy.
         * @param waitingTxVer Waiting tx version.
         */
        LockFuture(byte plc, MvccVersion waitingTxVer) {
            this.plc = plc;
            this.waitingTxVer = waitingTxVer;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            onDone();
        }

        /** {@inheritDoc} */
        @Override public void run(GridKernalContext ctx) {
            try {
                if (!isDone())
                    ctx.pools().poolForPolicy(plc).execute(this);
            }
            catch (IgniteCheckedException e) {
                U.error(ctx.log(LockFuture.class), e);
            }
        }

        /** {@inheritDoc} */
        @Override public Waiter concat(Waiter other) {
            return new CompoundWaiterNoLocal(this, other);
        }

        /** {@inheritDoc} */
        @Override public boolean hasLocalTransaction() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public boolean compound() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public GridFutureAdapter<?> lockFuture(MvccVersion checkedVer) {
            return belongToSameTx(waitingTxVer, checkedVer) ? this : null;
        }
    }

    /** */
    private static class LocalTransactionMarker implements Waiter {
        /** {@inheritDoc} */
        @Override public void run(GridKernalContext ctx) {
            // No-op
        }

        /** {@inheritDoc} */
        @Override public Waiter concat(Waiter other) {
            return new CompoundWaiter(other);
        }

        /** {@inheritDoc} */
        @Override public boolean hasLocalTransaction() {
            return true;
        }

        /** {@inheritDoc} */
        @Override public boolean compound() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public GridFutureAdapter<?> lockFuture(MvccVersion checkedVer) {
            return null;
        }
    }

    /** */
    private static class CompoundWaiter implements Waiter {
        /** */
        private final Object inner;

        /**
         * @param waiter Waiter to wrap.
         */
        private CompoundWaiter(Waiter waiter) {
            inner = waiter.compound() ? ((CompoundWaiter)waiter).inner : waiter;
        }

        /**
         * @param first First waiter.
         * @param second Second waiter.
         */
        private CompoundWaiter(Waiter first, Waiter second) {
            ArrayList<Waiter> list = new ArrayList<>();

            add(list, first);
            add(list, second);

            inner = list;
        }

        /** */
        private void add(List<Waiter> to, Waiter waiter) {
            if (!waiter.compound())
                to.add(waiter);
            else if (((CompoundWaiter)waiter).inner.getClass() == ArrayList.class)
                to.addAll((List<Waiter>)((CompoundWaiter)waiter).inner);
            else
                to.add((Waiter)((CompoundWaiter)waiter).inner);
        }

        /** {@inheritDoc} */
        @Override public void run(GridKernalContext ctx) {
            if (inner.getClass() == ArrayList.class) {
                for (Waiter waiter : (List<Waiter>)inner)
                    waiter.run(ctx);
            }
            else
                ((Waiter)inner).run(ctx);
        }

        /** {@inheritDoc} */
        @Override public Waiter concat(Waiter other) {
            return new CompoundWaiter(this, other);
        }

        /** {@inheritDoc} */
        @Override public boolean hasLocalTransaction() {
            return true;
        }

        /** {@inheritDoc} */
        @Override public boolean compound() {
            return true;
        }

        /** {@inheritDoc} */
        @Override public GridFutureAdapter<?> lockFuture(MvccVersion checkedVer) {
            if (inner.getClass() == ArrayList.class) {
                for (Waiter waiter : (List<Waiter>)inner) {
                    GridFutureAdapter<?> waitFut;

                    if ((waitFut = waiter.lockFuture(checkedVer)) != null)
                        return waitFut;
                }

                return null;
            }
            else
                return ((Waiter)inner).lockFuture(checkedVer);
        }
    }

    /** */
    private static class CompoundWaiterNoLocal extends CompoundWaiter {
        /**
         * @param first First waiter.
         * @param second Second waiter.
         */
        private CompoundWaiterNoLocal(Waiter first, Waiter second) {
            super(first, second);
        }

        /** {@inheritDoc} */
        @Override public Waiter concat(Waiter other) {
            return new CompoundWaiterNoLocal(this, other);
        }

        /** {@inheritDoc} */
        @Override public boolean hasLocalTransaction() {
            return false;
        }
    }

    /**
     * Mvcc garbage collection scheduler.
     */
    private static class VacuumScheduler extends GridWorker {
        /** */
        private static final long VACUUM_TIMEOUT = 60_000;

        /** */
        private final long interval;

        /** */
        private final MvccProcessorImpl prc;

        /**
         * @param ctx Kernal context.
         * @param log Logger.
         * @param prc Mvcc processor.
         */
        VacuumScheduler(GridKernalContext ctx, IgniteLogger log, MvccProcessorImpl prc) {
            super(ctx.igniteInstanceName(), "vacuum-scheduler", log);

            this.interval = ctx.config().getMvccVacuumFrequency();
            this.prc = prc;
        }

        /** {@inheritDoc} */
        @Override protected void body() throws InterruptedException, IgniteInterruptedCheckedException {
            U.sleep(interval); // initial delay

            while (!isCancelled()) {
                long nextScheduledTime = U.currentTimeMillis() + interval;

                try {
                    IgniteInternalFuture<VacuumMetrics> fut = prc.runVacuum();

                    if (log.isDebugEnabled())
                        log.debug("Vacuum started by scheduler.");

                    while (true) {
                        try {
                            fut.get(VACUUM_TIMEOUT);

                            break;
                        }
                        catch (IgniteFutureTimeoutCheckedException e) {
                            U.warn(log, "Failed to wait for vacuum complete. Consider increasing vacuum workers count.");
                        }
                    }
                }
                catch (IgniteInterruptedCheckedException e) {
                    throw e; // Cancelled.
                }
                catch (Throwable e) {
                    if (e instanceof Error)
                        throw (Error) e;

                    if (log.isDebugEnabled())
                        U.warn(log, "Failed to perform vacuum.", e);
                }

                long delay = nextScheduledTime - U.currentTimeMillis();

                if (delay > 0)
                    U.sleep(delay);
            }
        }
    }

    /**
     * Vacuum worker.
     */
    private static class VacuumWorker extends GridWorker {
        /** */
        private final BlockingQueue<VacuumTask> cleanupQueue;

        /**
         * @param ctx Kernal context.
         * @param log Logger.
         * @param cleanupQueue Cleanup tasks queue.
         */
        VacuumWorker(GridKernalContext ctx, IgniteLogger log, BlockingQueue<VacuumTask> cleanupQueue) {
            super(ctx.igniteInstanceName(), "vacuum-cleaner", log);

            this.cleanupQueue = cleanupQueue;
        }

        /** {@inheritDoc} */
        @Override protected void body() throws InterruptedException, IgniteInterruptedCheckedException {
            while (!isCancelled()) {
                VacuumTask task = cleanupQueue.take();

                try {
                    switch (task.part().state()) {
                        case EVICTED:
                        case RENTING:
                            task.onDone(new VacuumMetrics());

                            break;
                        case MOVING:
                            task.part().group().preloader().rebalanceFuture().listen(f -> cleanupQueue.add(task));

                            break;
                        case OWNING:
                            task.onDone(processPartition(task));

                            break;
                        case LOST:
                            task.onDone(new IgniteCheckedException("Partition is lost."));

                            break;
                    }
                }
                catch (IgniteInterruptedCheckedException e) {
                    task.onDone(e);

                    throw e; // Cancelled.
                }
                catch (Throwable e) {
                    task.onDone(e);

                    if (X.hasCause(e, NodeStoppingException.class)) {
                        // Thereis no need for further processing of vacuum tasks.
                        return;
                    }

                    if (e instanceof Error)
                        throw (Error) e;
                }
            }
        }

        /**
         * Process partition.
         *
         * @param task VacuumTask.
         * @throws IgniteCheckedException If failed.
         */
        private VacuumMetrics processPartition(VacuumTask task) throws IgniteCheckedException {
            long startNanoTime = System.nanoTime();

            GridDhtLocalPartition part = task.part();

            VacuumMetrics metrics = new VacuumMetrics();

            if (!part.reserve())
                return metrics;

            int curCacheId = CU.UNDEFINED_CACHE_ID;

            try {
                KeyCacheObject prevKey = null;

                Object rest = null;

                List<MvccLinkAwareSearchRow> cleanupRows = null;

                MvccSnapshot snapshot = task.snapshot();

                GridCacheContext cctx = null;

                boolean shared = part.group().sharedGroup();

                if (!shared && (cctx = F.first(part.group().caches())) == null)
                    return metrics;

                GridCursor<? extends CacheDataRow> cursor = part.dataStore().cursor(KEY_ONLY);

                while (cursor.next()) {
                    if (isCancelled())
                        throw new IgniteInterruptedCheckedException("Operation has been cancelled.");

                    MvccDataRow row = (MvccDataRow)cursor.get();

                    if (prevKey == null)
                        prevKey = row.key();

                    if (cctx == null) { // Shared group.
                        cctx = part.group().shared().cacheContext(curCacheId = row.cacheId());

                        if (cctx == null)
                            continue;
                    }

                    if ((shared && curCacheId != row.cacheId()) || !prevKey.equals(row.key())) {
                        if (rest != null || !F.isEmpty(cleanupRows))
                            cleanup(part, prevKey, cleanupRows, rest, cctx, metrics);

                        cleanupRows = null;

                        rest = null;

                        if (shared && curCacheId != row.cacheId()) {
                            cctx = part.group().shared().cacheContext(curCacheId = row.cacheId());

                            if (cctx == null)
                                continue;
                        }

                        prevKey = row.key();
                    }

                    if (canClean(row, snapshot, cctx))
                        cleanupRows = addRow(cleanupRows, row);
                    else if (actualize(cctx, row, snapshot))
                        rest = addRest(rest, row);

                    metrics.addScannedRowsCount(1);
                }

                if (rest != null || !F.isEmpty(cleanupRows))
                    cleanup(part, prevKey, cleanupRows, rest, cctx, metrics);

                metrics.addSearchNanoTime(System.nanoTime() - startNanoTime - metrics.cleanupNanoTime());

                return metrics;
            }
            finally {
                part.release();
            }
        }

        /** */
        @SuppressWarnings("unchecked")
        @NotNull private Object addRest(@Nullable Object rest, MvccDataRow row) {
            if (rest == null)
                rest = row;
            else if (rest.getClass() == ArrayList.class)
                ((List)rest).add(row);
            else {
                ArrayList list = new ArrayList();

                list.add(rest);
                list.add(row);

                rest = list;
            }

            return rest;
        }

        /**
         * @param rows Collection of rows.
         * @param row Row to add.
         * @return Collection of rows.
         */
        @NotNull private List<MvccLinkAwareSearchRow> addRow(@Nullable List<MvccLinkAwareSearchRow> rows, MvccDataRow row) {
            if (rows == null)
                rows = new ArrayList<>();

            rows.add(new MvccLinkAwareSearchRow(row.cacheId(), row.key(), row.mvccCoordinatorVersion(),
                row.mvccCounter(), row.mvccOperationCounter(), row.link()));

            return rows;
        }

        /**
         * @param row Mvcc row to check.
         * @param snapshot Cleanup version to compare with.
         * @param cctx Cache context.
         */
        private boolean canClean(MvccDataRow row, MvccSnapshot snapshot, GridCacheContext cctx) {
            // Row can be safely cleaned if it has ABORTED min version or COMMITTED and less than cleanup one max version.
            return compare(row, snapshot.coordinatorVersion(), snapshot.cleanupVersion()) <= 0
                && hasNewVersion(row) && MvccUtils.compareNewVersion(row, snapshot.coordinatorVersion(), snapshot.cleanupVersion()) <= 0
                && MvccUtils.state(cctx, row.newMvccCoordinatorVersion(), row.newMvccCounter(),
                row.newMvccOperationCounter() | (row.newMvccTxState() << MVCC_HINTS_BIT_OFF)) == TxState.COMMITTED
                || MvccUtils.state(cctx, row.mvccCoordinatorVersion(), row.mvccCounter(),
                row.mvccOperationCounter() | (row.mvccTxState() << MVCC_HINTS_BIT_OFF)) == TxState.ABORTED;
        }

        /** */
        private boolean actualize(GridCacheContext cctx, MvccDataRow row,
            MvccSnapshot snapshot) throws IgniteCheckedException {
            return isVisible(cctx, snapshot, row.mvccCoordinatorVersion(), row.mvccCounter(), row.mvccOperationCounter(), false)
                && (row.mvccTxState() == TxState.NA || (row.newMvccCoordinatorVersion() != MVCC_CRD_COUNTER_NA && row.newMvccTxState() == TxState.NA));
        }

        /**
         * @param part Local partition.
         * @param key Key.
         * @param cleanupRows Cleanup rows.
         * @param cctx Cache context.
         * @param metrics Vacuum metrics.
         * @throws IgniteCheckedException If failed.
         */
        private void cleanup(GridDhtLocalPartition part, KeyCacheObject key, List<MvccLinkAwareSearchRow> cleanupRows,
            Object rest, GridCacheContext cctx, VacuumMetrics metrics) throws IgniteCheckedException {
            assert key != null && cctx != null && (!F.isEmpty(cleanupRows) || rest != null);

            cctx.gate().enter();

            try {
                long cleanupStartNanoTime = System.nanoTime();

                GridCacheEntryEx entry = cctx.cache().entryEx(key);

                while (true) {
                    entry.lockEntry();

                    if (!entry.obsolete())
                        break;

                    entry.unlockEntry();

                    entry = cctx.cache().entryEx(key);
                }

                int cleaned = 0;

                try {
                    cctx.shared().database().checkpointReadLock();

                    try {
                        if (cleanupRows != null)
                            cleaned = part.dataStore().cleanup(cctx, cleanupRows);

                        if (rest != null) {
                            if (rest.getClass() == ArrayList.class) {
                                for (MvccDataRow row : ((List<MvccDataRow>) rest))
                                    part.dataStore().updateTxState(cctx, row);
                            }
                            else
                                part.dataStore().updateTxState(cctx, (MvccDataRow)rest);
                        }
                    }
                    finally {
                        cctx.shared().database().checkpointReadUnlock();
                    }
                }
                finally {
                    entry.unlockEntry();

                    cctx.evicts().touch(entry);

                    metrics.addCleanupNanoTime(System.nanoTime() - cleanupStartNanoTime);
                    metrics.addCleanupRowsCnt(cleaned);
                }
            }
            finally {
                cctx.gate().leave();
            }
        }
    }

    /** */
    private static class ActiveTx {
        /** */
        private final long tracking;

        /** */
        private final UUID nearNodeId;

        /** */
        private ActiveTx(long tracking, UUID nearNodeId) {
            this.tracking = tracking;
            this.nearNodeId = nearNodeId;
        }
    }

    /** */
    private static class ActiveServerTx extends ActiveTx {
        /** */
        private ActiveServerTx(long tracking, UUID nearNodeId) {
            super(tracking, nearNodeId);
        }
    }
}

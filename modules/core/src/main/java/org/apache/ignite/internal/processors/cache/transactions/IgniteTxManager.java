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

package org.apache.ignite.internal.processors.cache.transactions;

import java.io.Externalizable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Predicate;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteClientDisconnectedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.TransactionConfiguration;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.failure.FailureType;
import org.apache.ignite.internal.IgniteFeatures;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.cluster.DistributedTransactionConfiguration;
import org.apache.ignite.internal.managers.communication.GridIoPolicy;
import org.apache.ignite.internal.managers.communication.GridMessageListener;
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.managers.eventstorage.DiscoveryEventListener;
import org.apache.ignite.internal.managers.eventstorage.HighPriorityListener;
import org.apache.ignite.internal.managers.systemview.walker.TransactionViewWalker;
import org.apache.ignite.internal.pagemem.wal.record.DataEntry;
import org.apache.ignite.internal.pagemem.wal.record.TxRecord;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheObjectsReleaseFuture;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.GridCacheEntryRemovedException;
import org.apache.ignite.internal.processors.cache.GridCacheMapEntry;
import org.apache.ignite.internal.processors.cache.GridCacheMessage;
import org.apache.ignite.internal.processors.cache.GridCacheMvccCandidate;
import org.apache.ignite.internal.processors.cache.GridCacheReturnCompletableWrapper;
import org.apache.ignite.internal.processors.cache.GridCacheSharedManagerAdapter;
import org.apache.ignite.internal.processors.cache.GridCacheVersionedFuture;
import org.apache.ignite.internal.processors.cache.GridDeferredAckMessageSender;
import org.apache.ignite.internal.processors.cache.TxTimeoutOnPartitionMapExchangeChangeMessage;
import org.apache.ignite.internal.processors.cache.distributed.GridCacheMappedVersion;
import org.apache.ignite.internal.processors.cache.distributed.GridCacheTxRecoveryFuture;
import org.apache.ignite.internal.processors.cache.distributed.GridDistributedCacheEntry;
import org.apache.ignite.internal.processors.cache.distributed.GridDistributedLockCancelledException;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxLocal;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxOnePhaseCommitAckRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxRemote;
import org.apache.ignite.internal.processors.cache.distributed.dht.colocated.GridDhtColocatedLockFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtInvalidPartitionException;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearCacheAdapter;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearCacheEntry;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearLockFuture;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearOptimisticTxPrepareFuture;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxLocal;
import org.apache.ignite.internal.processors.cache.persistence.wal.WALPointer;
import org.apache.ignite.internal.processors.cache.transactions.TxDeadlockDetection.TxDeadlockFuture;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.cluster.BaselineTopology;
import org.apache.ignite.internal.processors.metric.impl.HitRateMetric;
import org.apache.ignite.internal.processors.timeout.GridTimeoutProcessor;
import org.apache.ignite.internal.transactions.IgniteTxOptimisticCheckedException;
import org.apache.ignite.internal.transactions.IgniteTxRollbackCheckedException;
import org.apache.ignite.internal.transactions.IgniteTxTimeoutCheckedException;
import org.apache.ignite.internal.util.GridBoundedConcurrentOrderedMap;
import org.apache.ignite.internal.util.TimeBag;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.lang.GridPlainRunnable;
import org.apache.ignite.internal.util.lang.IgnitePair;
import org.apache.ignite.internal.util.lang.gridfunc.ReadOnlyCollectionView2X;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteOutClosure;
import org.apache.ignite.lang.IgniteReducer;
import org.apache.ignite.spi.systemview.view.TransactionView;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.transactions.TransactionState;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentLinkedHashMap;

import static java.util.Objects.nonNull;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_DEFERRED_ONE_PHASE_COMMIT_ACK_REQUEST_BUFFER_SIZE;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_DEFERRED_ONE_PHASE_COMMIT_ACK_REQUEST_TIMEOUT;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_DUMP_TX_COLLISIONS_INTERVAL;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_LONG_OPERATIONS_DUMP_TIMEOUT;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_MAX_COMPLETED_TX_COUNT;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_SLOW_TX_WARN_TIMEOUT;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_TX_DEADLOCK_DETECTION_MAX_ITERS;
import static org.apache.ignite.configuration.TransactionConfiguration.TX_AWARE_QUERIES_SUPPORTED_MODES;
import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_JOINED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.events.EventType.EVT_TX_STARTED;
import static org.apache.ignite.internal.GridTopic.TOPIC_TX;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.SYSTEM_POOL;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.READ;
import static org.apache.ignite.internal.processors.cache.GridCacheUtils.isNearEnabled;
import static org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx.FinalizationStatus.RECOVERY_FINISH;
import static org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx.FinalizationStatus.USER_FINISH;
import static org.apache.ignite.internal.processors.security.SecurityUtils.securitySubjectId;
import static org.apache.ignite.internal.util.GridConcurrentFactory.newMap;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionState.ACTIVE;
import static org.apache.ignite.transactions.TransactionState.COMMITTED;
import static org.apache.ignite.transactions.TransactionState.COMMITTING;
import static org.apache.ignite.transactions.TransactionState.MARKED_ROLLBACK;
import static org.apache.ignite.transactions.TransactionState.PREPARED;
import static org.apache.ignite.transactions.TransactionState.PREPARING;
import static org.apache.ignite.transactions.TransactionState.ROLLED_BACK;
import static org.apache.ignite.transactions.TransactionState.ROLLING_BACK;
import static org.apache.ignite.transactions.TransactionState.SUSPENDED;
import static org.apache.ignite.transactions.TransactionState.UNKNOWN;
import static org.jsr166.ConcurrentLinkedHashMap.QueuePolicy.PER_SEGMENT_Q;

/**
 * Cache transaction manager.
 */
public class IgniteTxManager extends GridCacheSharedManagerAdapter {
    /** */
    public static final String TXS_MON_LIST = "transactions";

    /** */
    public static final String TXS_MON_LIST_DESC = "Running transactions";

    /** Default maximum number of transactions that have completed. */
    public static final int DFLT_MAX_COMPLETED_TX_CNT = 262144; // 2^18

    /** @see IgniteSystemProperties#IGNITE_SLOW_TX_WARN_TIMEOUT */
    public static final int DFLT_SLOW_TX_WARN_TIMEOUT = 0;

    /** @see IgniteSystemProperties#IGNITE_TX_DEADLOCK_DETECTION_MAX_ITERS */
    public static final int DFLT_TX_DEADLOCK_DETECTION_MAX_ITERS = 1000;

    /** @see IgniteSystemProperties#IGNITE_DEFERRED_ONE_PHASE_COMMIT_ACK_REQUEST_TIMEOUT */
    public static final int DFLT_DEFERRED_ONE_PHASE_COMMIT_ACK_REQUEST_TIMEOUT = 500;

    /** @see IgniteSystemProperties#IGNITE_DEFERRED_ONE_PHASE_COMMIT_ACK_REQUEST_BUFFER_SIZE */
    public static final int DFLT_DEFERRED_ONE_PHASE_COMMIT_ACK_REQUEST_BUFFER_SIZE = 256;

    /** @see IgniteSystemProperties#IGNITE_TRANSACTION_TIME_DUMP_SAMPLES_PER_SECOND_LIMIT */
    public static final int DFLT_TRANSACTION_TIME_DUMP_SAMPLES_PER_SECOND_LIMIT = 5;

    /** Slow tx warn timeout (initialized to 0). */
    private static final int SLOW_TX_WARN_TIMEOUT =
        Integer.getInteger(IGNITE_SLOW_TX_WARN_TIMEOUT, DFLT_SLOW_TX_WARN_TIMEOUT);

    /** Returns {@code true} if transaction has completed states. */
    public static final Predicate<TxRecord> COMPLETED_TX_STATES =
        txRec -> txRec.state() == COMMITTED || txRec.state() == ROLLED_BACK;

    /** Returns {@code true} if transaction has prepared states. */
    public static final Predicate<TxRecord> PREPARED_TX_STATES =
        txRec -> txRec.state() == PREPARED || txRec.state() == PREPARING;

    /** Uncommited tx states. */
    private Set<GridCacheVersion> uncommitedTx = new HashSet<>();

    /** One phase commit deferred ack request timeout. */
    public static final int DEFERRED_ONE_PHASE_COMMIT_ACK_REQUEST_TIMEOUT =
        Integer.getInteger(IGNITE_DEFERRED_ONE_PHASE_COMMIT_ACK_REQUEST_TIMEOUT,
            DFLT_DEFERRED_ONE_PHASE_COMMIT_ACK_REQUEST_TIMEOUT);

    /** One phase commit deferred ack request buffer size. */
    private static final int DEFERRED_ONE_PHASE_COMMIT_ACK_REQUEST_BUFFER_SIZE =
        Integer.getInteger(IGNITE_DEFERRED_ONE_PHASE_COMMIT_ACK_REQUEST_BUFFER_SIZE,
            DFLT_DEFERRED_ONE_PHASE_COMMIT_ACK_REQUEST_BUFFER_SIZE);

    /** @see IgniteSystemProperties#IGNITE_TX_OWNER_DUMP_REQUESTS_ALLOWED */
    public static final boolean DFLT_TX_OWNER_DUMP_REQUESTS_ALLOWED = true;

    /** @see IgniteSystemProperties#IGNITE_DUMP_TX_COLLISIONS_INTERVAL */
    public static final int DFLT_DUMP_TX_COLLISIONS_INTERVAL = 1000;

    /** Lower tx collisions queue size threshold. */
    private static final int COLLISIONS_QUEUE_THRESHOLD = 100;

    /** Deadlock detection maximum iterations. */
    final int deadlockMaxIters =
        IgniteSystemProperties.getInteger(IGNITE_TX_DEADLOCK_DETECTION_MAX_ITERS, DFLT_TX_DEADLOCK_DETECTION_MAX_ITERS);

    /** Committing transactions. */
    private final ThreadLocal<IgniteInternalTx> threadCtx = new ThreadLocal<>();

    /** Topology version should be used when mapping internal tx. */
    private final ThreadLocal<AffinityTopologyVersion> txTop = new ThreadLocal<>();

    /** Per-thread transaction map. */
    private final ConcurrentMap<Long, IgniteInternalTx> threadMap = newMap();

    /** Per-thread system transaction map. */
    private final ConcurrentMap<TxThreadKey, IgniteInternalTx> sysThreadMap = newMap();

    /** Per-ID map. */
    private final ConcurrentMap<GridCacheVersion, IgniteInternalTx> idMap = newMap();

    /** Per-ID map for near transactions. */
    private final ConcurrentMap<GridCacheVersion, IgniteInternalTx> nearIdMap = newMap();

    /** Deadlock detection futures. */
    private final ConcurrentMap<Long, TxDeadlockFuture> deadlockDetectFuts = new ConcurrentHashMap<>();

    /** TX handler. */
    private IgniteTxHandler txHnd;

    /** Committed local transactions. */
    private final GridBoundedConcurrentOrderedMap<GridCacheVersion, Boolean> completedVersSorted =
        new GridBoundedConcurrentOrderedMap<>(
            Integer.getInteger(IGNITE_MAX_COMPLETED_TX_COUNT, DFLT_MAX_COMPLETED_TX_CNT));

    /** Committed local transactions. */
    private final ConcurrentLinkedHashMap<GridCacheVersion, Object> completedVersHashMap =
        new ConcurrentLinkedHashMap<>(
            Integer.getInteger(IGNITE_MAX_COMPLETED_TX_COUNT, DFLT_MAX_COMPLETED_TX_CNT),
            0.75f,
            Runtime.getRuntime().availableProcessors() * 2,
            Integer.getInteger(IGNITE_MAX_COMPLETED_TX_COUNT, DFLT_MAX_COMPLETED_TX_CNT),
            PER_SEGMENT_Q);

    /** Pending one phase commit ack requests sender. */
    private GridDeferredAckMessageSender deferredAckMsgSnd;

    /** Slow tx warn timeout. */
    private int slowTxWarnTimeout = SLOW_TX_WARN_TIMEOUT;

    /** */
    private final TxDumpsThrottling txDumpsThrottling = new TxDumpsThrottling();

    /**
     * Near version to DHT version map. Note that we initialize to 5K size from get go,
     * to avoid future map resizings.
     */
    private final ConcurrentMap<GridCacheVersion, GridCacheVersion> mappedVers =
        new ConcurrentHashMap<>(5120);

    /** TxDeadlock detection. */
    private TxDeadlockDetection txDeadlockDetection;

    /**
     * Indicates whether {@code suspend()} and {@code resume()} operations are supported for pessimistic transactions
     * cluster wide.
     */
    private volatile boolean suspendResumeForPessimisticSupported;

    /** The futures for changing transaction timeout on partition map exchange. */
    private final ConcurrentMap<UUID, TxTimeoutOnPartitionMapExchangeChangeFuture> txTimeoutOnPartitionMapExchangeFuts =
        new ConcurrentHashMap<>();

    /** Timeout operations. */
    private final Map<String, GridTimeoutProcessor.CancelableTask> timeoutOperations = new HashMap<>();

    {
        timeoutOperations.put(IGNITE_LONG_OPERATIONS_DUMP_TIMEOUT, null);
        timeoutOperations.put(IGNITE_DUMP_TX_COLLISIONS_INTERVAL, null);
    }

    /** Key collisions info holder. */
    private volatile KeyCollisionsHolder keyCollisionsInfo;

    /** Distributed transaction configuration. */
    private DistributedTransactionConfiguration distributedTransactionConfiguration;

    /** Transforms transaction message. */
    private volatile BiFunction<GridCacheMessage, IgniteInternalTx, GridCacheMessage> txMsgTransform;

    /** Callback invoked on transaction commit. */
    private volatile Consumer<IgniteInternalTx> onCommitCallback;

    /** {@inheritDoc} */
    @Override protected void onKernalStop0(boolean cancel) {
        cctx.gridIO().removeMessageListener(TOPIC_TX);

        Exception err = new IgniteCheckedException("Operation has been cancelled (node is stopping).");

        for (TxTimeoutOnPartitionMapExchangeChangeFuture fut : txTimeoutOnPartitionMapExchangeFuts.values())
            fut.onDone(err);
    }

    /** {@inheritDoc} */
    @Override protected void start0() {
        txHnd = new IgniteTxHandler(cctx);

        deferredAckMsgSnd = new GridDeferredAckMessageSender<GridCacheVersion>(cctx.time(), cctx.kernalContext().closure()) {
            @Override public int getTimeout() {
                return DEFERRED_ONE_PHASE_COMMIT_ACK_REQUEST_TIMEOUT;
            }

            @Override public int getBufferSize() {
                return DEFERRED_ONE_PHASE_COMMIT_ACK_REQUEST_BUFFER_SIZE;
            }

            @Override public void finish(UUID nodeId, Collection<GridCacheVersion> vers) {
                GridDhtTxOnePhaseCommitAckRequest ackReq = new GridDhtTxOnePhaseCommitAckRequest(vers);

                cctx.kernalContext().gateway().readLock();

                try {
                    cctx.io().send(nodeId, ackReq, GridIoPolicy.SYSTEM_POOL);
                }
                catch (ClusterTopologyCheckedException ignored) {
                    if (log.isDebugEnabled())
                        log.debug("Failed to send one phase commit ack to backup node because it left grid: " + nodeId);
                }
                catch (IgniteCheckedException e) {
                    log.error("Failed to send one phase commit ack to backup node [backup=" + nodeId + ']', e);
                }
                finally {
                    cctx.kernalContext().gateway().readUnlock();
                }
            }
        };

        cctx.gridEvents().addDiscoveryEventListener(new TransactionRecoveryListener(), EVT_NODE_FAILED, EVT_NODE_LEFT);

        cctx.gridEvents().addDiscoveryEventListener(
            (DiscoveryEvent evt, DiscoCache discoCache) -> {
                if (evt.type() == EVT_NODE_FAILED || evt.type() == EVT_NODE_LEFT) {
                    UUID nodeId = evt.eventNode().id();

                    for (TxDeadlockFuture fut : deadlockDetectFuts.values())
                        fut.onNodeLeft(nodeId);

                    for (Map.Entry<GridCacheVersion, Object> entry : completedVersHashMap.entrySet()) {
                        Object obj = entry.getValue();

                        if (obj instanceof GridCacheReturnCompletableWrapper &&
                            nodeId.equals(((GridCacheReturnCompletableWrapper)obj).nodeId()))
                            removeTxReturn(entry.getKey());
                    }
                }

                suspendResumeForPessimisticSupported = IgniteFeatures.allNodesSupports(
                    cctx.discovery().remoteNodes(), IgniteFeatures.SUSPEND_RESUME_PESSIMISTIC_TX);
            },
            EVT_NODE_FAILED, EVT_NODE_LEFT, EVT_NODE_JOINED);

        txDeadlockDetection = new TxDeadlockDetection(cctx);

        cctx.gridIO().addMessageListener(TOPIC_TX, new DeadlockDetectionListener());

        cctx.txMetrics().onTxManagerStarted();

        keyCollisionsInfo = new KeyCollisionsHolder();

        distributedTransactionConfiguration = new DistributedTransactionConfiguration(cctx.kernalContext(), log,
                (String name, Long oldVal, Long newVal) -> {
                    if (!Objects.equals(oldVal, newVal)) {
                        scheduleDumpTask(
                                IGNITE_LONG_OPERATIONS_DUMP_TIMEOUT,
                                () -> cctx.kernalContext().closure().runLocalSafe(
                                        () -> cctx.kernalContext().cache().context().exchange().dumpLongRunningOperations(newVal)),
                                newVal);
                    }
                },
                (String name, Integer oldVal, Integer newVal) -> {
                    if (!Objects.equals(oldVal, newVal)) {
                        scheduleDumpTask(
                                IGNITE_DUMP_TX_COLLISIONS_INTERVAL,
                                this::collectTxCollisionsInfo,
                                newVal);
                    }
                });

        cctx.kernalContext().systemView().registerView(TXS_MON_LIST, TXS_MON_LIST_DESC,
            new TransactionViewWalker(),
            new ReadOnlyCollectionView2X<>(idMap.values(), nearIdMap.values()),
            TransactionView::new);
    }

    /** {@inheritDoc} */
    @Override protected void onKernalStart0(boolean active) {
        suspendResumeForPessimisticSupported = IgniteFeatures.allNodesSupports(
            cctx.discovery().remoteNodes(), IgniteFeatures.SUSPEND_RESUME_PESSIMISTIC_TX);
    }

    /**
     * @param cacheId Cache ID.
     */
    public void rollbackTransactionsForCache(int cacheId) {
        rollbackTransactionsForCache(cacheId, nearIdMap);

        rollbackTransactionsForCache(cacheId, idMap);
    }

    /**
     * @param cacheToStop Cache to stop.
     */
    public void rollbackTransactionsForStoppingCache(int cacheToStop) {
        GridCompoundFuture<IgniteInternalTx, IgniteInternalTx> compFut = new GridCompoundFuture<>();

        Collection<IgniteInternalTx> active = activeTransactions();

        for (IgniteInternalTx tx : active) {
            IgniteTxState state = tx.txState();

            Collection<IgniteTxEntry> txEntries =
                state instanceof IgniteTxStateImpl ? ((IgniteTxStateImpl)state).allEntriesCopy() : state.allEntries();

            for (IgniteTxEntry e : txEntries) {
                if (e.context().cacheId() == cacheToStop) {
                    compFut.add(failTxOnPreparing(tx));

                    break;
                }
            }
        }

        compFut.markInitialized();

        try {
            compFut.get();
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Error occurred during tx rollback.", e);
        }
    }

    /**
     * This method allows to roll back the transaction during partition map exchange related to destroying a cache(s).
     * Semantically, this method is equivalent to two subsequent calls:
     * <pre>
     *     tx.rollbackAsync();
     *     tx.currentPrepareFuture().onDone(new IgniteTxRollbackCheckedException())
     * </pre>
     *
     * It is assumed that the given transaction did not acquire any locks.
     *
     * @param tx Transaction.
     * @return Rollback future.
     */
    private IgniteInternalFuture<IgniteInternalTx> failTxOnPreparing(IgniteInternalTx tx) {
        IgniteInternalFuture<IgniteInternalTx> rollbackFut = tx.rollbackAsync();

        IgniteInternalFuture prepFut = tx.currentPrepareFuture();

        if (prepFut != null) {
            assert prepFut instanceof GridFutureAdapter :
                "It is assumed that prepare future should extend GridFutureAdapter class [prepFut=" + prepFut + ']';

            ((GridFutureAdapter)prepFut).onDone(
                new IgniteTxRollbackCheckedException(
                    "Failed to prepare the transaction, due to the transaction is marked as rolled back " +
                        "[tx=" + CU.txString(tx) + ']'));
        }

        return rollbackFut;
    }

    /**
     * Rollback transactions blocking partition map exchange.
     *
     * @param topVer Initial exchange version.
     */
    public void rollbackOnTopologyChange(AffinityTopologyVersion topVer) {
        for (IgniteInternalTx tx : activeTransactions()) {
            if (tx.local() && tx.near() && needWaitTransaction(tx, topVer)) {
                U.warn(log, "The transaction was forcibly rolled back on partition map exchange because a timeout is " +
                    "reached: [tx=" + CU.txString(tx) + ", topVer=" + topVer + ']');

                ((GridNearTxLocal)tx).rollbackNearTxLocalAsync(false, false);
            }
        }
    }

    /**
     * @param cacheId Cache ID.
     * @param txMap Transactions map.
     */
    private void rollbackTransactionsForCache(int cacheId, ConcurrentMap<?, IgniteInternalTx> txMap) {
        for (Map.Entry<?, IgniteInternalTx> e : txMap.entrySet()) {
            IgniteInternalTx tx = e.getValue();

            for (IgniteTxEntry entry : tx.allEntries()) {
                if (entry.cacheId() == cacheId) {
                    rollbackTx(tx, false, false);

                    break;
                }
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void onDisconnected(IgniteFuture reconnectFut) {
        for (IgniteInternalTx tx : idMap.values()) {
            rollbackTx(tx, true, false);

            tx.state(ROLLING_BACK);
            tx.state(ROLLED_BACK);
        }

        for (IgniteInternalTx tx : nearIdMap.values()) {
            rollbackTx(tx, true, false);

            tx.state(ROLLING_BACK);
            tx.state(ROLLED_BACK);
        }

        IgniteClientDisconnectedException err =
            new IgniteClientDisconnectedException(reconnectFut, "Client node disconnected.");

        for (TxDeadlockFuture fut : deadlockDetectFuts.values())
            fut.onDone(err);

        for (TxTimeoutOnPartitionMapExchangeChangeFuture fut : txTimeoutOnPartitionMapExchangeFuts.values())
            fut.onDone(err);
    }

    /**
     * @return TX handler.
     */
    public IgniteTxHandler txHandler() {
        return txHnd;
    }

    /**
     * Sets if dump requests from local node to near node are allowed, when long-running transaction
     * is found. If allowed, the compute request to near node will be made to get thread dump of transaction
     * owner thread.
     *
     * @return <code>true</code> if allowed, <code>false</code> otherwise.
     */
    public boolean txOwnerDumpRequestsAllowed() {
        return distributedTransactionConfiguration.txOwnerDumpRequestsAllowed();
    }

    /**
     * Threshold timeout for long transactions, if transaction exceeds it, it will be dumped in log with
     * information about how much time did it spent in system time (time while aquiring locks, preparing,
     * commiting, etc) and user time (time when client node runs some code while holding transaction and not
     * waiting it). Equals 0 if not set. No transactions are dumped in log if this parameter is not set.
     *
     * @return Threshold timeout in milliseconds.
     */
    public long longTransactionTimeDumpThreshold() {
        return distributedTransactionConfiguration.longTransactionTimeDumpThreshold();
    }

    /**
     * The coefficient for samples of completed transactions that will be dumped in log.
     */
    public double transactionTimeDumpSamplesCoefficient() {
        return distributedTransactionConfiguration.transactionTimeDumpSamplesCoefficient();
    }

    /**
     * The limit of samples of completed transactions that will be dumped in log per second, if
     * {@link #transactionTimeDumpSamplesCoefficient} is above <code>0.0</code>. Must be integer value
     * greater than <code>0</code>.
     */
    public int transactionTimeDumpSamplesPerSecondLimit() {
        return distributedTransactionConfiguration.longTransactionTimeDumpSamplesPerSecondLimit();
    }

    /**
     * Invalidates transaction.
     *
     * @param tx Transaction.
     */
    public void salvageTx(IgniteInternalTx tx) {
        salvageTx(tx, USER_FINISH);
    }

    /**
     * Invalidates transaction.
     *
     * @param tx Transaction.
     * @param status Finalization status.
     */
    private void salvageTx(IgniteInternalTx tx, IgniteInternalTx.FinalizationStatus status) {
        assert tx != null;

        TransactionState state = tx.state();

        if (state == ACTIVE || state == PREPARING || state == PREPARED || state == MARKED_ROLLBACK) {
            if (!tx.markFinalizing(status)) {
                if (log.isInfoEnabled())
                    log.info("Will not try to commit invalidate transaction (could not mark finalized): " + tx);

                return;
            }

            tx.salvageTx();

            if (log.isInfoEnabled())
                log.info("Invalidated transaction because originating node left grid: " + CU.txString(tx));
        }
    }

    /**
     * Prints out memory stats to standard out.
     * <p>
     * USE ONLY FOR MEMORY PROFILING DURING TESTS.
     */
    @Override public void printMemoryStats() {
        X.println(">>> ");
        X.println(">>> Transaction manager memory stats [igniteInstanceName=" + cctx.igniteInstanceName() + ']');
        X.println(">>>   threadMapSize: " + threadMap.size());
        X.println(">>>   idMap [size=" + idMap.size() + ']');
        X.println(">>>   nearIdMap [size=" + nearIdMap.size() + ']');
        X.println(">>>   completedVersSortedSize: " + completedVersSorted.size());
        X.println(">>>   completedVersHashMapSize: " + completedVersHashMap.sizex());
    }

    /**
     * @return Thread map size.
     */
    public int threadMapSize() {
        return threadMap.size();
    }

    /**
     * @return ID map size.
     */
    public int idMapSize() {
        return idMap.size();
    }

    /**
     * @return Committed versions size.
     */
    public int completedVersionsSize() {
        return completedVersHashMap.size();
    }

    /**
     *
     * @param tx Transaction to check.
     * @return {@code True} if transaction has been committed or rolled back,
     *      {@code false} otherwise.
     */
    private boolean isCompleted(IgniteInternalTx tx) {
        boolean completed = completedVersHashMap.containsKey(tx.xidVersion());

        // Need check that for tx rollback message was not received before lock.
        // This could happen on timeout or async rollback.
        if (!completed && tx.local() && tx.dht())
            return completedVersHashMap.containsKey(tx.nearXidVersion());

        return completed;
    }

    /**
     * @param implicit {@code True} if transaction is implicit.
     * @param implicitSingle Implicit-with-single-key flag.
     * @param concurrency Concurrency.
     * @param isolation Isolation.
     * @param timeout transaction timeout.
     * @param txSize Expected transaction size.
     * @param lb Label.
     * @return New transaction.
     */
    public GridNearTxLocal newTx(
        boolean implicit,
        boolean implicitSingle,
        @Nullable GridCacheContext sysCacheCtx,
        TransactionConcurrency concurrency,
        TransactionIsolation isolation,
        long timeout,
        boolean storeEnabled,
        int txSize,
        @Nullable String lb
    ) {
        assert sysCacheCtx == null || sysCacheCtx.systemTx();

        int taskNameHash = cctx.kernalContext().job().currentTaskNameHash();

        GridNearTxLocal tx = new GridNearTxLocal(
            cctx,
            implicit,
            implicitSingle,
            sysCacheCtx != null,
            sysCacheCtx != null ? sysCacheCtx.ioPolicy() : SYSTEM_POOL,
            concurrency,
            isolation,
            timeout,
            storeEnabled,
            txSize,
            securitySubjectId(cctx),
            taskNameHash,
            lb,
            txDumpsThrottling
        );

        if (tx.system()) {
            AffinityTopologyVersion topVer = cctx.tm().lockedTopologyVersion(Thread.currentThread().getId(), tx);

            // If there is another system transaction in progress, use its topology version to prevent deadlock.
            if (topVer != null)
                tx.topologyVersion(topVer);
        }

        return onCreated(sysCacheCtx, tx);
    }

    /**
     * @param cacheCtx Cache context.
     * @param tx Created transaction.
     * @return Started transaction.
     */
    @Nullable public <T extends IgniteInternalTx> T onCreated(@Nullable GridCacheContext cacheCtx, T tx) {
        ConcurrentMap<GridCacheVersion, IgniteInternalTx> txIdMap = transactionMap(tx);

        // Start clean.
        resetContext();

        if (isCompleted(tx)) {
            if (log.isDebugEnabled())
                log.debug("Attempt to create a completed transaction (will ignore): " + tx);

            return null;
        }

        IgniteInternalTx t;

        if ((t = txIdMap.putIfAbsent(tx.xidVersion(), tx)) == null) {
            if (tx.local() && !tx.dht()) {
                assert tx instanceof GridNearTxLocal : tx;

                if (!tx.implicit()) {
                    if (cacheCtx == null || !cacheCtx.systemTx())
                        threadMap.put(tx.threadId(), tx);
                    else
                        sysThreadMap.put(new TxThreadKey(tx.threadId(), cacheCtx.cacheId()), tx);
                }

                ((IgniteTxAdapter)tx).recordStateChangedEvent(EVT_TX_STARTED);
            }

            // Handle mapped versions.
            if (tx instanceof GridCacheMappedVersion) {
                GridCacheMappedVersion mapped = (GridCacheMappedVersion)tx;

                GridCacheVersion from = mapped.mappedVersion();

                if (from != null)
                    mappedVers.put(from, tx.xidVersion());

                if (log.isDebugEnabled())
                    log.debug("Added transaction version mapping [from=" + from + ", to=" + tx.xidVersion() +
                        ", tx=" + tx + ']');
            }
        }
        else {
            if (log.isDebugEnabled())
                log.debug("Attempt to create an existing transaction (will ignore) [newTx=" + tx + ", existingTx=" +
                    t + ']');

            return null;
        }

        if (log.isDebugEnabled())
            log.debug("Transaction created: " + tx);

        return tx;
    }

    /**
     * Creates a future that will wait for all ongoing transactions that maybe affected by topology update
     * to be finished. This set of transactions include
     * <ul>
     *     <li/> All {@link TransactionConcurrency#PESSIMISTIC} transactions with topology version
     *     less or equal to {@code topVer}.
     *     <li/> {@link TransactionConcurrency#OPTIMISTIC} transactions in PREPARING state with topology
     *     version less or equal to {@code topVer} and having transaction key with entry that belongs to
     *     one of partitions in {@code parts}.
     * </ul>
     *
     * @param topVer Topology version.
     * @return Future that will be completed when all ongoing transactions are finished.
     */
    public IgniteInternalFuture<Boolean> finishLocalTxs(AffinityTopologyVersion topVer) {
        GridCompoundFuture<IgniteInternalTx, Boolean> res =
            new CacheObjectsReleaseFuture<>(
                "LocalTx",
                topVer,
                new IgniteReducer<IgniteInternalTx, Boolean>() {
                    @Override public boolean collect(IgniteInternalTx e) {
                        return true;
                    }

                    @Override public Boolean reduce() {
                        return true;
                    }
                });

        for (IgniteInternalTx tx : activeTransactions()) {
            if (needWaitTransaction(tx, topVer))
                res.add(tx.finishFuture());
        }

        res.markInitialized();

        return res;
    }

    /**
     * Creates a future that will wait for all transactions with failed primary recovery.
     *
     * @param topVer Topology version.
     * @param node Failed node.
     * @return Future that will be completed when all affected transactions are recovered.
     */
    public IgniteInternalFuture<Boolean> recoverLocalTxs(AffinityTopologyVersion topVer, ClusterNode node) {
        GridCompoundFuture<IgniteInternalTx, Boolean> res =
            new CacheObjectsReleaseFuture<>(
                "TxRecovery",
                topVer,
                new IgniteReducer<IgniteInternalTx, Boolean>() {
                    @Override public boolean collect(IgniteInternalTx e) {
                        return true;
                    }

                    @Override public Boolean reduce() {
                        return true;
                    }
                });

        for (IgniteInternalTx tx : activeTransactions()) {
            if (tx.transactionNodes() != null && tx.transactionNodes().containsKey(node.id()) // One of tx primaries failed.
                && (tx.dht() // Local node is a primary (not on originating) or backup.
                // Local node is a primary (on originating).
                || (tx.near() && tx.local() && ((GridNearTxLocal)tx).colocatedLocallyMapped()))) {
                assert needWaitTransaction(tx, topVer);

                res.add(tx.finishFuture());
            }
        }

        res.markInitialized();

        return res;
    }

    /**
     * Creates a future that will wait for finishing all tx updates on backups after all local transactions are finished.
     * NOTE:
     * As we send finish request to backup nodes after transaction successfully completed on primary node
     * it's important to ensure that all updates from primary to backup are finished or at least remote transaction has created
     * on backup node.
     *
     * @param finishLocTxsFut Local transactions finish future.
     * @param topVer Topology version.
     * @return Future that will be completed when all ongoing transactions are finished.
     */
    public IgniteInternalFuture<?> finishAllTxs(IgniteInternalFuture<?> finishLocTxsFut, AffinityTopologyVersion topVer) {
        final GridCompoundFuture finishAllTxsFut = new CacheObjectsReleaseFuture("AllTx", topVer);

        // After finishing all local updates, wait for finishing all tx updates on backups.
        finishLocTxsFut.listen(() -> {
            finishAllTxsFut.add(cctx.mvcc().finishRemoteTxs(topVer));
            finishAllTxsFut.markInitialized();
        });

        return finishAllTxsFut;
    }

    /**
     * @param tx Transaction.
     * @param topVer Exchange version.
     * @return {@code True} if need wait transaction for exchange.
     */
    public boolean needWaitTransaction(IgniteInternalTx tx, AffinityTopologyVersion topVer) {
        AffinityTopologyVersion txTopVer = tx.topologyVersionSnapshot();

        return txTopVer != null && txTopVer.compareTo(topVer) < 0;
    }

    /**
     * Transaction start callback (has to do with when any operation was
     * performed on this transaction).
     *
     * @param tx Started transaction.
     * @return {@code True} if transaction is not in completed set.
     */
    public boolean onStarted(IgniteInternalTx tx) {
        assert tx.state() == ACTIVE || tx.state() == SUSPENDED || tx.isRollbackOnly() :
            "Invalid transaction state [locId=" + cctx.localNodeId() + ", tx=" + tx + ']';

        if (isCompleted(tx)) {
            ConcurrentMap<GridCacheVersion, IgniteInternalTx> txIdMap = transactionMap(tx);

            txIdMap.remove(tx.xidVersion(), tx);

            if (log.isDebugEnabled())
                log.debug("Attempt to start a completed transaction (will ignore): " + tx);

            return false;
        }

        if (log.isDebugEnabled())
            log.debug("Transaction started: " + tx);

        return true;
    }

    /**
     * @param from Near version.
     * @return DHT version for a near version.
     */
    public GridCacheVersion mappedVersion(GridCacheVersion from) {
        GridCacheVersion to = mappedVers.get(from);

        if (log.isDebugEnabled())
            log.debug("Found mapped version [from=" + from + ", to=" + to);

        return to;
    }

    /**
     *
     * @param ver Alternate version.
     * @param tx Transaction.
     */
    public void addAlternateVersion(GridCacheVersion ver, IgniteInternalTx tx) {
        if (idMap.putIfAbsent(ver, tx) == null)
            if (log.isDebugEnabled())
                log.debug("Registered alternate transaction version [ver=" + ver + ", tx=" + tx + ']');
    }

    /**
     * @return Local transaction.
     */
    @Nullable public IgniteTxLocalAdapter localTx() {
        IgniteTxLocalAdapter tx = tx();

        return tx != null && tx.local() ? tx : null;
    }

    /**
     * @param cctx Cache context.
     * @return Transaction for current thread.
     */
    public GridNearTxLocal threadLocalTx(GridCacheContext cctx) {
        IgniteInternalTx tx = tx(cctx, Thread.currentThread().getId());

        if (tx != null && tx.local() && (!tx.dht() || tx.colocated()) && !tx.implicit()) {
            assert tx instanceof GridNearTxLocal : tx;

            return (GridNearTxLocal)tx;
        }

        return null;
    }

    /**
     * Sets transaction for current thread.
     * @return Previously associated transaction.
     */
    public IgniteInternalTx tx(IgniteInternalTx tx) {
        long key = Thread.currentThread().getId();

        return tx == null ? threadMap.remove(key) : threadMap.put(key, tx);
    }

    /**
     * @return Transaction for current thread.
     */
    public <T extends IgniteInternalTx> T tx() {
        IgniteInternalTx tx = txContext();

        return tx != null ? (T)tx : (T)tx(null, Thread.currentThread().getId());
    }

    /**
     * @param threadId Thread ID.
     * @param ignore Transaction to ignore.
     * @return Not null topology version if current thread holds lock preventing topology change.
     */
    @Nullable public AffinityTopologyVersion lockedTopologyVersion(long threadId, IgniteInternalTx ignore) {
        IgniteInternalTx tx = threadMap.get(threadId);

        if (tx != null) {
            AffinityTopologyVersion topVer = tx.topologyVersionSnapshot();

            if (topVer != null)
                return topVer;
        }

        if (!sysThreadMap.isEmpty()) {
            for (GridCacheContext cacheCtx : cctx.cache().context().cacheContexts()) {
                if (!cacheCtx.systemTx())
                    continue;

                tx = sysThreadMap.get(new TxThreadKey(threadId, cacheCtx.cacheId()));

                if (tx != null && tx != ignore) {
                    AffinityTopologyVersion topVer = tx.topologyVersionSnapshot();

                    if (topVer != null)
                        return topVer;
                }
            }
        }

        return txTop.get();
    }

    /**
     * @param topVer Locked topology version.
     * @return {@code True} if topology hint was set.
     */
    public boolean setTxTopologyHint(@Nullable AffinityTopologyVersion topVer) {
        if (topVer == null)
            txTop.set(null);
        else {
            if (txTop.get() == null) {
                txTop.set(topVer);

                return true;
            }
        }

        return false;
    }

    /**
     * @return User transaction for current thread.
     */
    @Nullable public GridNearTxLocal userTx() {
        IgniteInternalTx tx = txContext();

        if (activeUserTx(tx))
            return (GridNearTxLocal)tx;

        tx = tx(null, Thread.currentThread().getId());

        if (activeUserTx(tx))
            return (GridNearTxLocal)tx;

        return null;
    }

    /**
     * @param cctx Cache context.
     * @return User transaction for current thread.
     */
    @Nullable GridNearTxLocal userTx(GridCacheContext cctx) {
        IgniteInternalTx tx = tx(cctx, Thread.currentThread().getId());

        if (activeUserTx(tx))
            return (GridNearTxLocal)tx;

        return null;
    }

    /**
     * @param tx Transaction.
     * @return {@code True} if given transaction is explicitly started user transaction.
     */
    private boolean activeUserTx(@Nullable IgniteInternalTx tx) {
        if (tx != null && tx.user() && tx.state() == ACTIVE) {
            assert tx instanceof GridNearTxLocal : tx;

            return true;
        }

        return false;
    }

    /**
     * @param cctx Cache context.
     * @param threadId Id of thread for transaction.
     * @return Transaction for thread with given ID.
     */
    private <T> T tx(GridCacheContext cctx, long threadId) {
        if (cctx == null || !cctx.systemTx())
            return (T)threadMap.get(threadId);

        TxThreadKey key = new TxThreadKey(threadId, cctx.cacheId());

        return (T)sysThreadMap.get(key);
    }

    /**
     * @return {@code True} if current thread is currently within transaction.
     */
    public boolean inUserTx() {
        return userTx() != null;
    }

    /**
     * @param txId Transaction ID.
     * @return Transaction with given ID.
     */
    @Nullable public <T extends IgniteInternalTx> T tx(GridCacheVersion txId) {
        return (T)idMap.get(txId);
    }

    /**
     * @param txId Transaction ID.
     * @return Transaction with given ID.
     */
    @Nullable public <T extends IgniteInternalTx> T nearTx(GridCacheVersion txId) {
        return (T)nearIdMap.get(txId);
    }

    /**
     * Handles prepare stage.
     *
     * @param tx Transaction to prepare.
     * @param entries Entries to lock or {@code null} if use default {@link IgniteInternalTx#optimisticLockEntries()}.
     * @throws IgniteCheckedException If preparation failed.
     */
    public void prepareTx(IgniteInternalTx tx, @Nullable Collection<IgniteTxEntry> entries) throws IgniteCheckedException {
        if (tx.state() == MARKED_ROLLBACK) {
            if (tx.remainingTime() == -1)
                throw new IgniteTxTimeoutCheckedException("Transaction timed out: " + this);

            throw new IgniteCheckedException("Transaction is marked for rollback: " + tx);
        }

        // One-phase commit tx cannot timeout on prepare because it is expected to be committed.
        if (tx.remainingTime() == -1 && !tx.onePhaseCommit()) {
            tx.setRollbackOnly();

            throw new IgniteTxTimeoutCheckedException("Transaction timed out: " + this);
        }

        if (tx.pessimistic() && tx.local())
            return; // Nothing else to do in pessimistic mode.

        // Optimistic or remote tx.
        assert tx.optimistic() || !tx.local();

        if (!lockMultiple(tx, entries != null ? entries : tx.optimisticLockEntries())) {
            tx.setRollbackOnly();

            throw new IgniteTxOptimisticCheckedException("Failed to prepare transaction (lock conflict): " + tx);
        }
    }

    /**
     * @param tx Transaction.
     */
    private void removeObsolete(IgniteInternalTx tx) {
        Collection<IgniteTxEntry> entries = tx.local() ? tx.allEntries() : tx.writeEntries();

        if (F.isEmpty(entries))
            return;

        cctx.database().checkpointReadLock();

        try {
            for (IgniteTxEntry entry : entries) {
                GridCacheEntryEx cached = entry.cached();

                GridCacheContext cacheCtx = entry.context();

                if (cached == null)
                    cached = cacheCtx.cache().peekEx(entry.key());

                if (cached.detached())
                    continue;

                try {
                    if (cached.obsolete() || cached.markObsoleteIfEmpty(tx.xidVersion()))
                        cacheCtx.cache().removeEntry(cached);

                    if (!tx.near() && isNearEnabled(cacheCtx)) {
                        GridNearCacheAdapter near = cacheCtx.isNear() ? cacheCtx.near() : cacheCtx.dht().near();

                        GridNearCacheEntry e = near.peekExx(entry.key());

                        if (e != null && e.markObsoleteIfEmpty(null))
                            near.removeEntry(e);
                    }
                }
                catch (IgniteCheckedException e) {
                    U.error(log, "Failed to remove obsolete entry from cache: " + cached, e);
                }
            }
        }
        finally {
            cctx.database().checkpointReadUnlock();
        }

    }

    /**
     * @param min Minimum version.
     * @return Pair [committed, rolledback] - never {@code null}, elements potentially empty,
     *      but also never {@code null}.
     */
    public IgnitePair<Collection<GridCacheVersion>> versions(GridCacheVersion min) {
        Collection<GridCacheVersion> committed = null;
        Collection<GridCacheVersion> rolledback = null;

        for (Map.Entry<GridCacheVersion, Boolean> e : completedVersSorted.tailMap(min, true).entrySet()) {
            if (e.getValue()) {
                if (committed == null)
                    committed = new ArrayList<>();

                committed.add(e.getKey());
            }
            else {
                if (rolledback == null)
                    rolledback = new ArrayList<>();

                rolledback.add(e.getKey());
            }
        }

        return new IgnitePair<>(
            committed == null ? Collections.emptyList() : committed,
            rolledback == null ? Collections.emptyList() : rolledback);
    }

    /**
     * Peeks completed versions history map to find out whether transaction was committed or rolled back
     * in the recent past.
     *
     * @param xid Transaction XID version.
     * @return <code>true</code> if transaction was committed, <code>false</code> if transaction was rolled back,
     * <code>null</code> if information is missed in history.
     */
    public Boolean peekCompletedVersionsHistory(GridCacheVersion xid) {
        Object o = completedVersHashMap.get(xid);

        return (o instanceof Boolean) ? (Boolean)o : null;
    }

    /**
     * @return Collection of active transactions.
     */
    public Collection<IgniteInternalTx> activeTransactions() {
        return F.concat(false, idMap.values(), nearIdMap.values());
    }

    /**
     * @param tx Tx to remove.
     */
    public void removeCommittedTx(IgniteInternalTx tx) {
        completedVersHashMap.remove(tx.xidVersion(), true);

        if (tx.needsCompletedVersions())
            completedVersSorted.remove(tx.xidVersion(), true);
    }

    /**
     * @param tx Committed transaction.
     */
    public void addCommittedTx(IgniteInternalTx tx) {
        addCommittedTx(tx, tx.xidVersion(), tx.nearXidVersion());
    }

    /**
     * @param tx Committed transaction.
     */
    public void addCommittedTxReturn(IgniteInternalTx tx, GridCacheReturnCompletableWrapper ret) {
        addCommittedTxReturn(tx.nearXidVersion(), ret);
    }

    /**
     * @param tx Committed transaction.
     * @return If transaction was not already present in completed set.
     */
    public boolean addRolledbackTx(IgniteInternalTx tx) {
        return addRolledbackTx(tx, tx.xidVersion());
    }

    /**
     * @param tx Tx.
     * @param xidVer Completed transaction version.
     * @param nearXidVer Optional near transaction ID.
     * @return If transaction was not already present in completed set.
     */
    public boolean addCommittedTx(
        IgniteInternalTx tx,
        GridCacheVersion xidVer,
        @Nullable GridCacheVersion nearXidVer
    ) {
        if (nearXidVer != null)
            xidVer = new CommittedVersion(xidVer, nearXidVer);

        Object committed0 = completedVersHashMap.putIfAbsent(xidVer, true);

        if (committed0 == null && (tx == null || tx.needsCompletedVersions())) {
            Boolean b = completedVersSorted.putIfAbsent(xidVer, true);

            assert b == null;
        }

        boolean committed = committed0 != null && !committed0.equals(Boolean.FALSE);

        return committed0 == null || committed;
    }

    /**
     * @param xidVer Completed transaction version.
     * @param retVal Invoke result.
     */
    private void addCommittedTxReturn(
        GridCacheVersion xidVer,
        GridCacheReturnCompletableWrapper retVal
    ) {
        assert retVal != null;

        Object prev = completedVersHashMap.putIfAbsent(xidVer, retVal);

        assert prev == null || Boolean.FALSE.equals(prev) : prev; // Can be rolled back.
    }

    /**
     * @param tx Tx.
     * @param xidVer Completed transaction version.
     * @return If transaction was not already present in completed set.
     */
    public boolean addRolledbackTx(
        IgniteInternalTx tx,
        GridCacheVersion xidVer
    ) {
        Object committed0 = completedVersHashMap.putIfAbsent(xidVer, false);

        if (committed0 == null && (tx == null || tx.needsCompletedVersions())) {
            Boolean b = completedVersSorted.putIfAbsent(xidVer, false);

            assert b == null;
        }

        boolean committed = committed0 != null && !committed0.equals(Boolean.FALSE);

        return committed0 == null || !committed;
    }

    /**
     * @param xidVer xidVer Completed transaction version.
     * @return Tx result.
     */
    public GridCacheReturnCompletableWrapper getCommittedTxReturn(GridCacheVersion xidVer) {
        Object retVal = completedVersHashMap.get(xidVer);

        // Will gain true in regular case or GridCacheReturn in onePhaseCommit case.
        if (!Boolean.TRUE.equals(retVal)) {
            assert !Boolean.FALSE.equals(retVal); // Method should be used only after 'committed' checked.

            GridCacheReturnCompletableWrapper res = (GridCacheReturnCompletableWrapper)retVal;

            removeTxReturn(xidVer);

            return res;
        }
        else
            return null;
    }

    /**
     * @param xidVer xidVer Completed transaction version.
     */
    public void removeTxReturn(GridCacheVersion xidVer) {
        Object prev = completedVersHashMap.get(xidVer);

        if (prev instanceof GridCacheReturnCompletableWrapper)
            completedVersHashMap.replace(xidVer, prev, true);
    }

    /**
     * @param tx Transaction.
     */
    private void processCompletedEntries(IgniteInternalTx tx) {
        if (tx.needsCompletedVersions()) {
            GridCacheVersion min = minVersion(tx.readEntries(), tx.xidVersion(), tx);

            min = minVersion(tx.writeEntries(), min, tx);

            assert min != null;

            IgnitePair<Collection<GridCacheVersion>> versPair = versions(min);

            tx.completedVersions(min, versPair.get1(), versPair.get2());
        }
    }

    /**
     * Collects versions for all pending locks for all entries within transaction
     *
     * @param dhtTxLoc Transaction being committed.
     */
    private void collectPendingVersions(GridDhtTxLocal dhtTxLoc) {
        if (dhtTxLoc.needsCompletedVersions()) {
            if (log.isDebugEnabled())
                log.debug("Checking for pending locks with version less then tx version: " + dhtTxLoc);

            Set<GridCacheVersion> vers = new LinkedHashSet<>();

            collectPendingVersions(dhtTxLoc.readEntries(), dhtTxLoc.xidVersion(), vers);
            collectPendingVersions(dhtTxLoc.writeEntries(), dhtTxLoc.xidVersion(), vers);

            if (!vers.isEmpty())
                dhtTxLoc.pendingVersions(vers);
        }
    }

    /**
     * Gets versions of all not acquired locks for collection of tx entries that are less than base version.
     *
     * @param entries Tx entries to process.
     * @param baseVer Base version to compare with.
     * @param vers Collection of versions that will be populated.
     */
    @SuppressWarnings("TypeMayBeWeakened")
    private void collectPendingVersions(Iterable<IgniteTxEntry> entries,
        GridCacheVersion baseVer, Set<GridCacheVersion> vers) {

        // The locks are not released yet, so we can safely list pending candidates versions.
        for (IgniteTxEntry txEntry : entries) {
            GridCacheEntryEx cached = txEntry.cached();

            try {
                // If check should be faster than exception handling.
                if (!cached.obsolete()) {
                    for (GridCacheMvccCandidate cand : cached.localCandidates()) {
                        if (!cand.owner() && cand.version().compareTo(baseVer) < 0) {
                            if (log.isDebugEnabled())
                                log.debug("Adding candidate version to pending set: " + cand);

                            vers.add(cand.version());
                        }
                    }
                }
            }
            catch (GridCacheEntryRemovedException ignored) {
                if (log.isDebugEnabled())
                    log.debug("There are no pending locks for entry (entry was deleted in transaction): " + txEntry);
            }
        }
    }

    /**
     * Go through all candidates for entries involved in transaction and find their min
     * version. We know that these candidates will commit after this transaction, and
     * therefore we can grab the min version, so we can send all committed and rolled
     * back versions from min to current to remote nodes for re-ordering.
     *
     * @param entries Entries.
     * @param min Min version so far.
     * @param tx Transaction.
     * @return Minimal available version.
     */
    private GridCacheVersion minVersion(Iterable<IgniteTxEntry> entries, GridCacheVersion min,
        IgniteInternalTx tx) {
        for (IgniteTxEntry txEntry : entries) {
            GridCacheEntryEx cached = txEntry.cached();

            // We are assuming that this method is only called on commit. In that
            // case, if lock is held, entry can never be removed.
            assert txEntry.isRead() || !cached.obsolete(tx.xidVersion()) :
                "Invalid obsolete version for transaction [entry=" + cached + ", tx=" + tx + ']';

            for (GridCacheMvccCandidate cand : cached.remoteMvccSnapshot())
                if (min == null || cand.version().isLess(min))
                    min = cand.version();
        }

        return min;
    }

    /**
     * @param tx Transaction.
     * @return {@code True} if transaction read entries should be unlocked.
     */
    private boolean unlockReadEntries(IgniteInternalTx tx) {
        if (tx.pessimistic())
            return !tx.readCommitted();
        else
            return tx.serializable();
    }

    /**
     * Commits a transaction.
     *
     * @param tx Transaction to commit.
     * @throws IgniteCheckedException If failed.
     */
    public void commitTx(IgniteInternalTx tx) throws IgniteCheckedException {
        assert tx != null;
        assert tx.state() == COMMITTING : "Invalid transaction state for commit from tm [state=" + tx.state() +
            ", expected=COMMITTING, tx=" + tx + ']';

        if (log.isDebugEnabled())
            log.debug("Committing from TM [locNodeId=" + cctx.localNodeId() + ", tx=" + tx + ']');

        /*
         * Note that write phase is handled by transaction adapter itself,
         * so we don't do it here.
         */

        Object committed0 = completedVersHashMap.get(tx.xidVersion());

        boolean committed = committed0 != null && !committed0.equals(Boolean.FALSE);

        // 1. Make sure that committed version has been recorded.
        if (!(committed || tx.writeSet().isEmpty() || tx.isSystemInvalidate())) {
            uncommitTx(tx);

            tx.errorWhenCommitting();

            throw new IgniteCheckedException("Missing commit version (consider increasing " +
                IGNITE_MAX_COMPLETED_TX_COUNT + " system property) [ver=" + tx.xidVersion() +
                ", committed0=" + committed0 +
                ", tx=" + tx.getClass().getSimpleName() + ']');
        }

        Consumer<IgniteInternalTx> onCommitCallback0 = onCommitCallback;

        if (onCommitCallback0 != null)
            onCommitCallback0.accept(tx);

        ConcurrentMap<GridCacheVersion, IgniteInternalTx> txIdMap = transactionMap(tx);

        if (txIdMap.remove(tx.xidVersion(), tx)) {
            // 2. Must process completed entries before unlocking!
            processCompletedEntries(tx);

            if (tx instanceof GridDhtTxLocal) {
                GridDhtTxLocal dhtTxLoc = (GridDhtTxLocal)tx;

                collectPendingVersions(dhtTxLoc);
            }

            // 3. Unlock write resources.
            unlockMultiple(tx, tx.writeEntries());

            // 4. Unlock read resources if required.
            if (unlockReadEntries(tx))
                unlockMultiple(tx, tx.readEntries());

            // 5. Notify evictions.
            notifyEvictions(tx);

            // 6. Remove obsolete entries from cache.
            removeObsolete(tx);

            // 7. Remove from per-thread storage.
            clearThreadMap(tx);

            // 8. Unregister explicit locks.
            if (!tx.alternateVersions().isEmpty()) {
                for (GridCacheVersion ver : tx.alternateVersions())
                    idMap.remove(ver);
            }

            // 9. Remove Near-2-DHT mappings.
            if (tx instanceof GridCacheMappedVersion) {
                GridCacheVersion mapped = ((GridCacheMappedVersion)tx).mappedVersion();

                if (mapped != null)
                    mappedVers.remove(mapped);
            }

            // 10. Clear context.
            resetContext();

            // 11. Update metrics.
            if (!tx.dht() && tx.local()) {
                if (!tx.system())
                    cctx.txMetrics().onTxCommit();

                writeStatistics(tx, true);

                tx.txState().onTxEnd(cctx, tx, true);
            }

            if (slowTxWarnTimeout > 0 && tx.local() &&
                U.currentTimeMillis() - tx.startTime() > slowTxWarnTimeout)
                U.warn(log, "Slow transaction detected [tx=" + tx +
                    ", slowTxWarnTimeout=" + slowTxWarnTimeout + ']');

            if (log.isDebugEnabled())
                log.debug("Committed from TM [locNodeId=" + cctx.localNodeId() + ", tx=" + tx + ']');
        }
        else if (log.isDebugEnabled())
            log.debug("Did not commit from TM (was already committed): " + tx);
    }

    /**
     * Rolls back a transaction.
     *
     * @param tx Transaction to rollback.
     * @param clearThreadMap {@code True} if need remove tx from thread map.
     * @param skipCompletedVers {@code True} if tx should skip adding itself to completed versions map on finish.
     */
    public void rollbackTx(IgniteInternalTx tx, boolean clearThreadMap, boolean skipCompletedVers) {
        assert tx != null;

        if (log.isDebugEnabled())
            log.debug("Rolling back from TM [locNodeId=" + cctx.localNodeId() + ", tx=" + tx + ']');

        // 1. Record transaction version to avoid duplicates.
        if (!skipCompletedVers)
            addRolledbackTx(tx);

        ConcurrentMap<GridCacheVersion, IgniteInternalTx> txIdMap = transactionMap(tx);

        if (txIdMap.remove(tx.xidVersion(), tx)) {
            // 2. Unlock write resources.
            unlockMultiple(tx, tx.writeEntries());

            // 3. Unlock read resources if required.
            if (unlockReadEntries(tx))
                unlockMultiple(tx, tx.readEntries());

            // 4. Notify evictions.
            notifyEvictions(tx);

            // 5. Remove obsolete entries.
            removeObsolete(tx);

            // 6. Remove from per-thread storage.
            if (clearThreadMap)
                clearThreadMap(tx);

            // 7. Unregister explicit locks.
            if (!tx.alternateVersions().isEmpty())
                for (GridCacheVersion ver : tx.alternateVersions())
                    idMap.remove(ver);

            // 8. Remove Near-2-DHT mappings.
            if (tx instanceof GridCacheMappedVersion)
                mappedVers.remove(((GridCacheMappedVersion)tx).mappedVersion());

            // 9. Clear context.
            resetContext();

            // 10. Update metrics.
            if (!tx.dht() && tx.local()) {
                if (!tx.system())
                    cctx.txMetrics().onTxRollback();

                writeStatistics(tx, false);

                tx.txState().onTxEnd(cctx, tx, false);
            }

            if (log.isDebugEnabled())
                log.debug("Rolled back from TM: " + tx);
        }
        else if (log.isDebugEnabled())
            log.debug("Did not rollback from TM (was already rolled back): " + tx);
    }

    /**
     * Fast finish transaction. Can be used only if no locks were acquired.
     *
     * @param tx Transaction to finish.
     * @param commit {@code True} if transaction is committed, {@code false} if rolled back.
     * @param clearThreadMap {@code True} if need remove tx from thread map.
     */
    public void fastFinishTx(GridNearTxLocal tx, boolean commit, boolean clearThreadMap) {
        assert tx != null;
        assert tx.writeMap().isEmpty();
        assert tx.optimistic() || tx.readMap().isEmpty();

        ConcurrentMap<GridCacheVersion, IgniteInternalTx> txIdMap = transactionMap(tx);

        if (txIdMap.remove(tx.xidVersion(), tx)) {
            // 1. Notify evictions.
            notifyEvictions(tx);

            // 2. Evict near entries.
            if (!tx.readMap().isEmpty()) {
                for (IgniteTxEntry entry : tx.readMap().values())
                    tx.evictNearEntry(entry, false);
            }

            // 3. Remove obsolete entries.
            removeObsolete(tx);

            // 4. Remove from per-thread storage.
            if (clearThreadMap)
                clearThreadMap(tx);

            // 5. Clear context.
            resetContext();

            // 6. Update metrics.
            if (!tx.dht() && tx.local()) {
                if (!tx.system()) {
                    if (commit)
                        cctx.txMetrics().onTxCommit();
                    else
                        cctx.txMetrics().onTxRollback();
                }

                writeStatistics(tx, commit);

                tx.txState().onTxEnd(cctx, tx, commit);
            }
        }
    }

    /**
     * Tries to minimize damage from partially-committed transaction.
     *
     * @param tx Tx to uncommit.
     */
    void uncommitTx(IgniteInternalTx tx) {
        assert tx != null;

        if (log.isDebugEnabled())
            log.debug("Uncommiting from TM: " + tx);

        ConcurrentMap<GridCacheVersion, IgniteInternalTx> txIdMap = transactionMap(tx);

        if (txIdMap.remove(tx.xidVersion(), tx)) {
            // 1. Unlock write resources.
            unlockMultiple(tx, tx.writeEntries());

            // 2. Unlock read resources if required.
            if (unlockReadEntries(tx))
                unlockMultiple(tx, tx.readEntries());

            // 3. Notify evictions.
            notifyEvictions(tx);

            // 4. Remove from per-thread storage.
            clearThreadMap(tx);

            // 5. Unregister explicit locks.
            if (!tx.alternateVersions().isEmpty()) {
                for (GridCacheVersion ver : tx.alternateVersions())
                    idMap.remove(ver);
            }

            // 6. Remove Near-2-DHT mappings.
            if (tx instanceof GridCacheMappedVersion)
                mappedVers.remove(((GridCacheMappedVersion)tx).mappedVersion());

            // 7. Clear context.
            resetContext();

            if (log.isDebugEnabled())
                log.debug("Uncommitted from TM: " + tx);
        }
        else if (log.isDebugEnabled())
            log.debug("Did not uncommit from TM (was already committed or rolled back): " + tx);
    }

    /**
     * @param tx Transaction to clear.
     */
    public void clearThreadMap(IgniteInternalTx tx) {
        if (tx.local() && !tx.dht()) {
            assert tx instanceof GridNearTxLocal : tx;

            if (!tx.system())
                threadMap.remove(tx.threadId(), tx);
            else {
                Integer cacheId = tx.txState().firstCacheId();

                if (cacheId != null)
                    sysThreadMap.remove(new TxThreadKey(tx.threadId(), cacheId), tx);
                else {
                    for (Iterator<IgniteInternalTx> it = sysThreadMap.values().iterator(); it.hasNext(); ) {
                        IgniteInternalTx txx = it.next();

                        if (tx == txx) {
                            it.remove();

                            break;
                        }
                    }
                }
            }
        }
    }

    /**
     * Enters system section for thread local near tx, if it is present.
     * In this section system time for this transaction is counted.
     */
    public void enterNearTxSystemSection() {
        GridNearTxLocal tx = threadLocalTx(null);

        if (tx != null)
            tx.enterSystemSection();
    }

    /**
     * Leaves system section for thread local near tx, if it is present.
     */
    public void leaveNearTxSystemSection() {
        GridNearTxLocal tx = threadLocalTx(null);

        if (tx != null)
            tx.leaveSystemSection();
    }

    /**
     * Gets transaction ID map depending on transaction type.
     *
     * @param tx Transaction.
     * @return Transaction map.
     */
    private ConcurrentMap<GridCacheVersion, IgniteInternalTx> transactionMap(IgniteInternalTx tx) {
        return (tx.near() && !tx.local()) ? nearIdMap : idMap;
    }

    /**
     * @param tx Transaction to notify evictions for.
     */
    private void notifyEvictions(IgniteInternalTx tx) {
        if (tx.internal())
            return;

        for (IgniteTxEntry txEntry : tx.allEntries())
            txEntry.cached().context().evicts().touch(txEntry, tx.local());
    }

    /**
     * Callback invoked whenever a member of a transaction acquires
     * lock ownership.
     *
     * @param entry Cache entry.
     * @param owner Candidate that won ownership.
     * @return {@code True} if transaction was notified, {@code false} otherwise.
     */
    public boolean onOwnerChanged(GridCacheEntryEx entry, GridCacheMvccCandidate owner) {
        // We only care about acquired locks.
        if (owner != null) {
            IgniteTxAdapter tx = entry.isNear() ? nearTx(owner.version()) : tx(owner.version());

            if (tx != null) {
                if (!tx.local()) {
                    if (log.isDebugEnabled())
                        log.debug("Found transaction for owner changed event [owner=" + owner + ", entry=" + entry +
                            ", tx=" + tx + ']');

                    tx.onOwnerChanged(entry, owner);

                    return true;
                }
                else if (log.isDebugEnabled())
                    log.debug("Ignoring local transaction for owner change event: " + tx);
            }
            else if (log.isDebugEnabled())
                log.debug("Transaction not found for owner changed event [owner=" + owner + ", entry=" + entry + ']');
        }

        return false;
    }

    /**
     * @param tx Transaction.
     * @param entries Entries to lock.
     * @return {@code True} if all keys were locked.
     * @throws IgniteCheckedException If lock has been cancelled.
     */
    private boolean lockMultiple(IgniteInternalTx tx, Iterable<IgniteTxEntry> entries)
        throws IgniteCheckedException {
        assert tx.optimistic() || !tx.local();

        long remainingTime = tx.remainingTime();

        // For serializable transactions, failure to acquire lock means
        // that there is a serializable conflict. For all other isolation levels,
        // we wait for the lock.
        long timeout = remainingTime < 0 ? 0 : remainingTime;

        GridCacheVersion serOrder = (tx.serializable() && tx.optimistic()) ? tx.nearXidVersion() : null;

        for (IgniteTxEntry txEntry1 : entries) {
            // Check if this entry was prepared before.
            if (!txEntry1.markPrepared() || txEntry1.explicitVersion() != null)
                continue;

            GridCacheContext cacheCtx = txEntry1.context();

            while (true) {
                cctx.database().checkpointReadLock();

                try {
                    GridCacheEntryEx entry1 = txEntry1.cached();

                    assert entry1 != null : txEntry1;
                    assert !entry1.detached() : "Expected non-detached entry for near transaction " +
                        "[locNodeId=" + cctx.localNodeId() + ", entry=" + entry1 + ']';

                    GridCacheVersion serReadVer = txEntry1.entryReadVersion();

                    assert serReadVer == null || (tx.optimistic() && tx.serializable()) : txEntry1;

                    boolean read = serOrder != null && txEntry1.op() == READ;

                    entry1.unswap();

                    if (!entry1.tmLock(tx, timeout, serOrder, serReadVer, read)) {
                        // Unlock locks locked so far.
                        for (IgniteTxEntry txEntry2 : entries) {
                            if (txEntry2 == txEntry1)
                                break;

                            txUnlock(tx, txEntry2);
                        }

                        return false;
                    }

                    break;
                }
                catch (GridCacheEntryRemovedException ignored) {
                    if (log.isDebugEnabled())
                        log.debug("Got removed entry in TM lockMultiple(..) method (will retry): " + txEntry1);

                    try {
                        // Renew cache entry.
                        txEntry1.cached(cacheCtx.cache().entryEx(txEntry1.key(), tx.topologyVersion()));
                    }
                    catch (GridDhtInvalidPartitionException e) {
                        assert tx.dht() : "Received invalid partition for non DHT transaction [tx=" +
                            tx + ", invalidPart=" + e.partition() + ']';

                        // If partition is invalid, we ignore this entry.
                        tx.addInvalidPartition(cacheCtx.cacheId(), e.partition());

                        break;
                    }
                }
                catch (GridDistributedLockCancelledException ignore) {
                    tx.setRollbackOnly();

                    throw new IgniteCheckedException("Entry lock has been cancelled for transaction: " + tx);
                }
                finally {
                    cctx.database().checkpointReadUnlock();
                }
            }
        }

        return true;
    }

    /**
     * @param tx Transaction.
     * @param txEntry Entry to unlock.
     */
    private void txUnlock(IgniteInternalTx tx, IgniteTxEntry txEntry) {
        while (true) {
            try {
                GridCacheEntryEx entry = txEntry.cached();

                assert entry != null;

                if (entry.detached())
                    break;

                entry.txUnlock(tx);

                break;
            }
            catch (GridCacheEntryRemovedException ignored) {
                if (log.isDebugEnabled())
                    log.debug("Got removed entry in TM txUnlock(..) method (will retry): " + txEntry);

                try {
                    txEntry.cached(txEntry.context().cache().entryEx(txEntry.key(), tx.topologyVersion()));
                }
                catch (GridDhtInvalidPartitionException e) {
                    return; // Ignore and proceed to next lock.
                }
            }
        }
    }

    /**
     * @param tx Owning transaction.
     * @param entries Entries to unlock.
     */
    private void unlockMultiple(IgniteInternalTx tx, Iterable<IgniteTxEntry> entries) {
        for (IgniteTxEntry txEntry : entries)
            txUnlock(tx, txEntry);
    }

    /**
     * @param tx Committing transaction.
     */
    public void txContext(IgniteInternalTx tx) {
        threadCtx.set(tx);
    }

    /**
     * @return Currently committing transaction.
     */
    private IgniteInternalTx txContext() {
        return threadCtx.get();
    }

    /**
     * Commit ended.
     */
    public void resetContext() {
        threadCtx.set(null);
    }

    /**
     * @param slowTxWarnTimeout Slow tx warn timeout.
     */
    public void slowTxWarnTimeout(int slowTxWarnTimeout) {
        this.slowTxWarnTimeout = slowTxWarnTimeout;
    }

    /**
     * @return Long operations dump timeout.
     */
    public long longOperationsDumpTimeout() {
        return distributedTransactionConfiguration.longOperationsDumpTimeout();
    }

    /**
     * @param longOpsDumpTimeout Long operations dump timeout.
     */
    public void longOperationsDumpTimeout(long longOpsDumpTimeout) {
        distributedTransactionConfiguration.updateLongOperationsDumpTimeoutLocal(longOpsDumpTimeout);

        scheduleDumpTask(
            IGNITE_LONG_OPERATIONS_DUMP_TIMEOUT,
            () -> cctx.kernalContext().closure().runLocalSafe(
                (GridPlainRunnable)() -> cctx.kernalContext().cache().context().exchange().dumpLongRunningOperations(longOpsDumpTimeout)),
            longOpsDumpTimeout);
    }

    /**
     * Schedule tx collisions collection task.
     *
     * @param timeout Sets tx key collisions analysis interval.
     **/
    void txCollisionsDumpInterval(int timeout) {
        distributedTransactionConfiguration.updateCollisionsDumpIntervalLocal(timeout);

        scheduleDumpTask(
            IGNITE_DUMP_TX_COLLISIONS_INTERVAL,
            this::collectTxCollisionsInfo,
            collisionsDumpInterval());
    }

    /**
     * Scheduling tasks for dumping long operations. Closes current task
     * (if any) and if the {@code timeout > 0} schedules a new task
     * with a new timeout, delay and start period equal to
     * {@code timeout}, otherwise task is deleted.
     *
     * @param taskKey Appropriate key in {@link IgniteTxManager#timeoutOperations}
     * @param r Task.
     * @param timeout Long operations dump timeout.
     */
    void scheduleDumpTask(String taskKey, Runnable r, long timeout) {
        if (isStopping())
            return;

        GridTimeoutProcessor.CancelableTask longOpDumpTask;

        GridTimeoutProcessor timeoutProc = cctx.kernalContext().timeout();

        synchronized (timeoutOperations) {
            GridTimeoutProcessor.CancelableTask task = timeoutOperations.get(taskKey);

            if (nonNull(task))
                task.close();

            longOpDumpTask = timeout > 0 ? timeoutProc.schedule(r, timeout, timeout) : null;

            timeoutOperations.put(taskKey, longOpDumpTask);
        }
    }

    /**
     * Checks if transactions with given near version ID was prepared or committed.
     *
     * @param nearVer Near version ID.
     * @param txNum Number of transactions.
     * @return Future for flag indicating if transactions were prepared or committed or {@code null} for success future.
     */
    @Nullable public IgniteInternalFuture<Boolean> txsPreparedOrCommitted(GridCacheVersion nearVer, int txNum) {
        return txsPreparedOrCommitted(nearVer, txNum, null, null);
    }

    /**
     * @param xidVer Version.
     * @return Future for flag indicating if transactions was committed.
     */
    public IgniteInternalFuture<Boolean> txCommitted(GridCacheVersion xidVer) {
        final GridFutureAdapter<Boolean> resFut = new GridFutureAdapter<>();

        final IgniteInternalTx tx = cctx.tm().tx(xidVer);

        if (tx != null) {
            assert tx.near() && tx.local() : tx;

            if (log.isDebugEnabled())
                log.debug("Found near transaction, will wait for completion: " + tx);

            tx.finishFuture().listen(() -> {
                    TransactionState state = tx.state();

                    if (log.isDebugEnabled())
                        log.debug("Near transaction finished with state: " + state);

                    resFut.onDone(state == COMMITTED);
                }
            );

            return resFut;
        }

        boolean committed = false;

        for (Map.Entry<GridCacheVersion, Object> entry : completedVersHashMap.entrySet()) {
            if (entry.getKey() instanceof CommittedVersion) {
                CommittedVersion comm = (CommittedVersion)entry.getKey();

                if (comm.nearVer.equals(xidVer)) {
                    committed = !entry.getValue().equals(Boolean.FALSE);

                    break;
                }
            }
        }

        if (log.isDebugEnabled())
            log.debug("Near transaction committed: " + committed);

        resFut.onDone(committed);

        return resFut;
    }

    /**
     * @param nearVer Near version.
     * @return Finish future for related remote transactions.
     */
    @SuppressWarnings("unchecked")
    public IgniteInternalFuture<?> remoteTxFinishFuture(GridCacheVersion nearVer) {
        GridCompoundFuture<Void, Void> fut = new GridCompoundFuture<>();

        for (final IgniteInternalTx tx : activeTransactions()) {
            if (!tx.local() && nearVer.equals(tx.nearXidVersion()))
                fut.add((IgniteInternalFuture)tx.finishFuture());
        }

        fut.markInitialized();

        return fut;
    }

    /**
     * @param nearVer Near version ID.
     * @param txNum Number of transactions.
     * @param fut Result future.
     * @param processedVers Processed versions.
     * @return Future for flag indicating if transactions were prepared or committed or {@code null} for success future.
     */
    @Nullable private IgniteInternalFuture<Boolean> txsPreparedOrCommitted(final GridCacheVersion nearVer,
        int txNum,
        @Nullable GridFutureAdapter<Boolean> fut,
        @Nullable Collection<GridCacheVersion> processedVers
    ) {
        for (final IgniteInternalTx tx : activeTransactions()) {
            if (nearVer.equals(tx.nearXidVersion())) {
                IgniteInternalFuture<?> prepFut = tx.currentPrepareFuture();

                if (prepFut != null && !prepFut.isDone()) {
                    if (log.isDebugEnabled())
                        log.debug("Transaction is preparing (will wait): " + tx);

                    final GridFutureAdapter<Boolean> fut0 = fut != null ? fut : new GridFutureAdapter<>();

                    final int txNum0 = txNum;

                    final Collection<GridCacheVersion> processedVers0 = processedVers;

                    prepFut.listen(() -> {
                        if (log.isDebugEnabled())
                            log.debug("Transaction prepare future finished: " + tx);

                        IgniteInternalFuture<Boolean> f = txsPreparedOrCommitted(nearVer,
                            txNum0,
                            fut0,
                            processedVers0);

                        assert f == fut0;
                    });

                    return fut0;
                }

                TransactionState state = tx.state();

                if (state == PREPARED || state == COMMITTING || state == COMMITTED) {
                    if (state == PREPARED)
                        tx.markFinalizing(RECOVERY_FINISH); // Prevents concurrent rollback.

                    if (--txNum == 0) {
                        if (fut != null)
                            fut.onDone(true);

                        return fut;
                    }
                }
                else {
                    if (tx.setRollbackOnly() || tx.state() == UNKNOWN) {
                        tx.rollbackAsync();

                        if (log.isDebugEnabled())
                            log.debug("Transaction was not prepared (rolled back): " + tx);

                        if (fut == null)
                            fut = new GridFutureAdapter<>();

                        fut.onDone(false);

                        return fut;
                    }
                    else {
                        if (tx.state() == COMMITTED) {
                            if (--txNum == 0) {
                                if (fut != null)
                                    fut.onDone(true);

                                return fut;
                            }
                        }
                        else {
                            if (log.isDebugEnabled())
                                log.debug("Transaction is not prepared: " + tx);

                            if (fut == null)
                                fut = new GridFutureAdapter<>();

                            fut.onDone(false);

                            return fut;
                        }
                    }
                }

                if (processedVers == null)
                    processedVers = U.newHashSet(txNum);

                processedVers.add(tx.xidVersion());
            }
        }

        // Not all transactions were found. Need to scan committed versions to check
        // if transaction was already committed.
        for (Map.Entry<GridCacheVersion, Object> e : completedVersHashMap.entrySet()) {
            if (e.getValue().equals(Boolean.FALSE))
                continue;

            GridCacheVersion ver = e.getKey();

            if (processedVers != null && processedVers.contains(ver))
                continue;

            if (ver instanceof CommittedVersion) {
                CommittedVersion commitVer = (CommittedVersion)ver;

                if (commitVer.nearVer.equals(nearVer)) {
                    if (--txNum == 0) {
                        if (fut != null)
                            fut.onDone(true);

                        return fut;
                    }
                }
            }
        }

        if (fut == null)
            fut = new GridFutureAdapter<>();

        fut.onDone(false);

        return fut;
    }

    /**
     * Commits or rolls back prepared transaction.
     *
     * @param tx Transaction.
     * @param commit Whether transaction should be committed or rolled back.
     */
    public void finishTxOnRecovery(final IgniteInternalTx tx, boolean commit) {
        if (log.isInfoEnabled())
            log.info("Finishing prepared transaction [commit=" + commit + ", tx=" + tx + ']');

        // Transactions participating in recovery can be finished only by recovery consensus.
        assert tx.finalizationStatus() == RECOVERY_FINISH : tx;

        if (tx instanceof IgniteTxRemoteEx) {
            IgniteTxRemoteEx rmtTx = (IgniteTxRemoteEx)tx;

            rmtTx.doneRemote(tx.xidVersion(),
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.emptyList());
        }

        if (commit)
            tx.commitAsync().listen(new CommitListener(tx));
        else {
            if (!tx.local()) {
                // This tx was rolled back on recovery because of primary node fail, other backups may be not aware of it.
                TxCounters cnts = tx.txCounters(false);

                if (cnts != null)
                    // Skipping counters update to keep them the same everywhere without any sync.
                    // Tx counters will be finalized (gaps removed) on local txs recovery finish.
                    // Each node will have counters equals to latest successful transactions counters.
                    cnts.updateCounters().clear();
            }

            tx.rollbackAsync();
        }
    }

    /**
     * Commits transaction in case when node started transaction failed, but all related
     * transactions were prepared (invalidates transaction if it is not fully prepared).
     *
     * @param tx Transaction.
     * @param failedNodeIds Failed nodes IDs.
     */
    public IgniteInternalFuture<Boolean> commitIfPrepared(IgniteInternalTx tx, Set<UUID> failedNodeIds) {
        assert tx instanceof GridDhtTxLocal || tx instanceof GridDhtTxRemote : tx;
        assert !F.isEmpty(tx.transactionNodes()) : tx;
        assert tx.nearXidVersion() != null : tx;

        // Transaction will be completed by finish message.
        if (!tx.markFinalizing(RECOVERY_FINISH))
            return null;

        GridCacheTxRecoveryFuture fut = new GridCacheTxRecoveryFuture(
            cctx,
            tx,
            failedNodeIds,
            tx.transactionNodes());

        cctx.mvcc().addFuture(fut, fut.futureId());

        if (log.isInfoEnabled())
            log.info("Checking optimistic transaction state on remote nodes [tx=" + tx + ", fut=" + fut + ']');

        fut.prepare();

        return fut;
    }

    /**
     * @return {@code True} if deadlock detection is enabled.
     */
    public boolean deadlockDetectionEnabled() {
        return deadlockMaxIters > 0;
    }

    /**
     * Performs deadlock detection for given keys.
     *
     * @param tx Target tx.
     * @param keys Keys.
     * @return Detection result.
     */
    public IgniteInternalFuture<TxDeadlock> detectDeadlock(
        IgniteInternalTx tx,
        Set<IgniteTxKey> keys
    ) {
        return txDeadlockDetection.detectDeadlock(tx, keys);
    }

    /**
     * @param nodeId Node ID.
     * @param fut Future.
     * @param txKeys Tx keys.
     */
    void txLocksInfo(UUID nodeId, TxDeadlockFuture fut, Set<IgniteTxKey> txKeys) {
        ClusterNode node = cctx.node(nodeId);

        if (node == null) {
            if (log.isDebugEnabled())
                log.debug("Failed to finish deadlock detection, node left: " + nodeId);

            fut.onDone();

            return;
        }

        TxLocksRequest req = new TxLocksRequest(fut.futureId(), txKeys);

        try {
            if (!cctx.localNodeId().equals(nodeId))
                req.prepareMarshal(cctx);

            cctx.gridIO().sendToGridTopic(node, TOPIC_TX, req, SYSTEM_POOL);
        }
        catch (IgniteCheckedException e) {
            if (e instanceof ClusterTopologyCheckedException) {
                if (log.isDebugEnabled())
                    log.debug("Failed to finish deadlock detection, node left: " + nodeId);
            }
            else
                U.warn(log, "Failed to finish deadlock detection: " + e, e);

            fut.onDone();
        }
    }

    /**
     * @param tx Tx.
     * @param txKeys Tx keys.
     * @return {@code True} if key is involved into tx.
     */
    private boolean hasKeys(IgniteInternalTx tx, Collection<IgniteTxKey> txKeys) {
        for (IgniteTxKey key : txKeys) {
            if (tx.txState().entry(key) != null)
                return true;
        }

        return false;
    }

    /**
     * @param txKeys Tx keys.
     * @return Transactions locks and nodes.
     */
    private TxLocksResponse txLocksInfo(Collection<IgniteTxKey> txKeys) {
        TxLocksResponse res = new TxLocksResponse();

        Collection<IgniteInternalTx> txs = activeTransactions();

        for (IgniteInternalTx tx : txs) {
            boolean nearTxLoc = tx instanceof GridNearTxLocal;

            if (!(nearTxLoc || tx instanceof GridDhtTxLocal) || !hasKeys(tx, txKeys))
                continue;

            IgniteTxState state = tx.txState();

            assert state instanceof IgniteTxStateImpl || state instanceof IgniteTxImplicitSingleStateImpl;

            Collection<IgniteTxEntry> txEntries =
                state instanceof IgniteTxStateImpl ? ((IgniteTxStateImpl)state).allEntriesCopy() : state.allEntries();

            Set<IgniteTxKey> requestedKeys = null;

            // Try to get info about requested keys for detached entries in case of GridNearTxLocal transaction
            // in order to reduce amount of requests to remote nodes.
            if (nearTxLoc) {
                if (tx.pessimistic()) {
                    GridDhtColocatedLockFuture fut =
                        (GridDhtColocatedLockFuture)mvccFuture(tx, GridDhtColocatedLockFuture.class);

                    if (fut != null)
                        requestedKeys = fut.requestedKeys();

                    GridNearLockFuture nearFut = (GridNearLockFuture)mvccFuture(tx, GridNearLockFuture.class);

                    if (nearFut != null) {
                        Set<IgniteTxKey> nearRequestedKeys = nearFut.requestedKeys();

                        if (nearRequestedKeys != null)
                            requestedKeys = nearRequestedKeys;
                    }
                }
                else {
                    GridNearOptimisticTxPrepareFuture fut =
                        (GridNearOptimisticTxPrepareFuture)mvccFuture(tx, GridNearOptimisticTxPrepareFuture.class);

                    if (fut != null)
                        requestedKeys = fut.requestedKeys();
                }
            }

            for (IgniteTxEntry txEntry : txEntries) {
                IgniteTxKey txKey = txEntry.txKey();

                if (res.txLocks(txKey) == null) {
                    GridCacheMapEntry e = (GridCacheMapEntry)txEntry.cached();

                    List<GridCacheMvccCandidate> locs = e.mvccAllLocal();

                    if (locs != null) {
                        boolean owner = false;

                        for (GridCacheMvccCandidate loc : locs) {
                            if (!owner && loc.owner() && loc.tx())
                                owner = true;

                            if (!owner) // Skip all candidates in case when no tx that owns lock.
                                break;

                            if (loc.tx()) {
                                UUID nearNodeId = loc.otherNodeId();

                                GridCacheVersion txId = loc.otherVersion();

                                TxLock txLock = new TxLock(
                                    txId == null ? loc.version() : txId,
                                    nearNodeId == null ? loc.nodeId() : nearNodeId,
                                    // We can get outdated value of thread ID, but this value only for information here.
                                    loc.threadId(),
                                    loc.owner() ? TxLock.OWNERSHIP_OWNER : TxLock.OWNERSHIP_CANDIDATE);

                                res.addTxLock(txKey, txLock);
                            }
                        }
                    }
                    // Special case for optimal sequence of nodes processing.
                    else if (nearTxLoc && requestedKeys != null && requestedKeys.contains(txKey)) {
                        TxLock txLock = new TxLock(
                            tx.nearXidVersion(),
                            tx.nodeId(),
                            tx.threadId(),
                            TxLock.OWNERSHIP_REQUESTED);

                        res.addTxLock(txKey, txLock);
                    }
                    else
                        res.addKey(txKey);
                }
            }
        }

        return res;
    }

    /**
     * @param tx Tx. Must be instance of {@link GridNearTxLocal}.
     * @param cls Future class.
     * @return Cache future.
     */
    private IgniteInternalFuture mvccFuture(IgniteInternalTx tx, Class<? extends IgniteInternalFuture> cls) {
        assert tx instanceof GridNearTxLocal : tx;

        Collection<GridCacheVersionedFuture<?>> futs = cctx.mvcc().futuresForVersion(tx.nearXidVersion());

        if (futs != null) {
            for (GridCacheVersionedFuture<?> fut : futs) {
                if (fut.getClass().equals(cls))
                    return fut;
            }
        }

        return null;
    }

    /**
     * @param fut Future.
     */
    public void addFuture(TxDeadlockFuture fut) {
        TxDeadlockFuture old = deadlockDetectFuts.put(fut.futureId(), fut);

        assert old == null : old;
    }

    /**
     * @param futId Future ID.
     * @return Found future.
     */
    @Nullable public TxDeadlockFuture future(long futId) {
        return deadlockDetectFuts.get(futId);
    }

    /**
     * @param futId Future ID.
     */
    public void removeFuture(long futId) {
        deadlockDetectFuts.remove(futId);
    }

    /**
     * @param nodeId Node ID to send message to.
     * @param ver Version to ack.
     */
    public void sendDeferredAckResponse(UUID nodeId, GridCacheVersion ver) {
        deferredAckMsgSnd.sendDeferredAckMessage(nodeId, ver);
    }

    /**
     * @return Collection of active transaction deadlock detection futures.
     */
    public Collection<IgniteInternalFuture<?>> deadlockDetectionFutures() {
        Collection<? extends IgniteInternalFuture<?>> values = deadlockDetectFuts.values();

        return (Collection<IgniteInternalFuture<?>>)values;
    }

    /**
     * Suspends transaction.
     * Should not be used directly. Use tx.suspend() instead.
     *
     * @param tx Transaction to be suspended.
     *
     * @see #resumeTx(GridNearTxLocal, long)
     * @see GridNearTxLocal#suspend()
     * @see GridNearTxLocal#resume()
     * @throws IgniteCheckedException If failed to suspend transaction.
     */
    public void suspendTx(final GridNearTxLocal tx) throws IgniteCheckedException {
        assert tx != null && !tx.system() : tx;

        if (tx.concurrency == PESSIMISTIC && !suspendResumeForPessimisticSupported) {
            throw new IgniteCheckedException("Suspend operation cannot be called " +
                "because some nodes in the cluster don't support this feature.");
        }

        if (!tx.state(SUSPENDED)) {
            throw new IgniteCheckedException("Trying to suspend transaction with incorrect state "
                + "[expected=" + ACTIVE + ", actual=" + tx.state() + ']');
        }

        clearThreadMap(tx);
    }

    /**
     * Resume transaction in current thread.
     * Please don't use directly. Use tx.resume() instead.
     *
     * @param tx Transaction to be resumed.
     * @param threadId Thread id to restore.
     *
     * @see #suspendTx(GridNearTxLocal)
     * @see GridNearTxLocal#suspend()
     * @see GridNearTxLocal#resume()
     * @throws IgniteCheckedException If failed to resume tx.
     */
    public void resumeTx(GridNearTxLocal tx, long threadId) throws IgniteCheckedException {
        assert tx != null && !tx.system() : tx;

        if (!tx.state(ACTIVE)) {
            throw new IgniteCheckedException("Trying to resume transaction with incorrect state "
                + "[expected=" + SUSPENDED + ", actual=" + tx.state() + ']');
        }

        assert !threadMap.containsValue(tx) : tx;
        assert !haveSystemTxForThread(Thread.currentThread().getId());

        if (threadMap.putIfAbsent(threadId, tx) != null)
            throw new IgniteCheckedException("Thread already has started a transaction.");

        tx.threadId(threadId);
    }

    /**
     * @param threadId Thread id.
     * @return True if thread have system transaction. False otherwise.
     */
    private boolean haveSystemTxForThread(long threadId) {
        if (!sysThreadMap.isEmpty()) {
            for (GridCacheContext cacheCtx : cctx.cache().context().cacheContexts()) {
                if (!cacheCtx.systemTx())
                    continue;

                if (sysThreadMap.containsKey(new TxThreadKey(threadId, cacheCtx.cacheId())))
                    return true;
            }
        }

        return false;
    }

    /**
     * Logs Tx state to WAL if needed.
     *
     * @param tx Transaction.
     * @return WALPointer or {@code null} if nothing was logged.
     */
    @Nullable WALPointer logTxRecord(IgniteTxAdapter tx) {
        BaselineTopology baselineTop;

        // Log tx state change to WAL.
        if (cctx.wal() == null
            || (baselineTop = cctx.kernalContext().state().clusterState().baselineTopology()) == null
            || !baselineTop.consistentIds().contains(cctx.localNode().consistentId()))
            return null;

        Map<Short, Collection<Short>> nodes = tx.consistentIdMapper.mapToCompactIds(tx.topVer, tx.txNodes, baselineTop);

        TxRecord record = new TxRecord(tx.state(), tx.nearXidVersion(), tx.writeVersion(), nodes);

        try {
            return cctx.wal().log(record);
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Failed to log TxRecord: " + record, e);

            throw new IgniteException("Failed to log TxRecord: " + record, e);
        }
    }

    /**
     * Sets transaction timeout on partition map exchange.
     *
     * @param timeout Transaction timeout on partition map exchange in milliseconds.
     */
    public void setTxTimeoutOnPartitionMapExchange(long timeout) throws IgniteCheckedException {
        UUID reqId = UUID.randomUUID();

        TxTimeoutOnPartitionMapExchangeChangeFuture fut = new TxTimeoutOnPartitionMapExchangeChangeFuture(reqId);

        txTimeoutOnPartitionMapExchangeFuts.put(reqId, fut);

        TxTimeoutOnPartitionMapExchangeChangeMessage msg = new TxTimeoutOnPartitionMapExchangeChangeMessage(
            reqId, timeout);

        cctx.discovery().sendCustomEvent(msg);

        fut.get();
    }

    /**
     * Callback invoked from discovery thread when discovery custom message is received.
     *
     * @param msg Discovery message for changing transaction timeout on partition map exchange.
     */
    public void onTxTimeoutOnPartitionMapExchangeChange(TxTimeoutOnPartitionMapExchangeChangeMessage msg) {
        assert msg != null;

        if (msg.isInit()) {
            TransactionConfiguration cfg = cctx.kernalContext().config().getTransactionConfiguration();

            if (cfg.getTxTimeoutOnPartitionMapExchange() != msg.getTimeout())
                cfg.setTxTimeoutOnPartitionMapExchange(msg.getTimeout());
        }
        else {
            TxTimeoutOnPartitionMapExchangeChangeFuture fut = txTimeoutOnPartitionMapExchangeFuts.get(
                msg.getRequestId());

            if (fut != null)
                fut.onDone();
        }
    }

    /**
     * The task for changing transaction timeout on partition map exchange processed by exchange worker.
     *
     * @param msg Message.
     */
    public void processTxTimeoutOnPartitionMapExchangeChange(TxTimeoutOnPartitionMapExchangeChangeMessage msg) {
        assert msg != null;

        long timeout = cctx.kernalContext().config().getTransactionConfiguration().getTxTimeoutOnPartitionMapExchange();

        if (timeout != msg.getTimeout())
            cctx.kernalContext().config().getTransactionConfiguration().setTxTimeoutOnPartitionMapExchange(msg.getTimeout());
    }

    /**
     * Method checks that current thread does not have active transactions.
     * If transaction or topology lock is hold by current thread
     * exception {@link IgniteException} with given {@code errMsgConstructor} message will be thrown.
     *
     * @param errMsgConstructor Error message constructor.
     */
    public void checkEmptyTransactions(@NotNull IgniteOutClosure<String> errMsgConstructor) {
        if (userTx() != null || cctx.lockedTopologyVersion(null) != null)
            throw new IgniteException(errMsgConstructor.apply());
    }

    /**
     * Sets if dump requests from local node to near node are allowed, when long-running transaction
     * is found. If allowed, the compute request to near node will be made to get thread dump of transaction
     * owner thread. Also broadcasts this setting on other server nodes in cluster.
     *
     * @param allowed whether allowed
     */
    public void setTxOwnerDumpRequestsAllowedDistributed(boolean allowed) {
        try {
            distributedTransactionConfiguration.updateTxOwnerDumpRequestsAllowedAsync(allowed).get();
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /**
     * Sets threshold timeout in milliseconds for long transactions, if transaction exceeds it,
     * it will be dumped in log with information about how much time did
     * it spent in system time (time while aquiring locks, preparing, commiting, etc.)
     * and user time (time when client node runs some code while holding transaction).
     * Can be set to 0 - no transactions will be dumped in log in this case.
     *
     * @param threshold Threshold timeout in milliseconds.
     */
    public void longTransactionTimeDumpThresholdDistributed(long threshold) {
        assert threshold >= 0 : "Threshold timeout must be greater than or equal to 0.";

        try {
            distributedTransactionConfiguration.updateLongTransactionTimeDumpThresholdAsync(threshold).get();
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /**
     * Sets the coefficient for samples of long-running transactions that will be dumped in log, if
     * {@link #longTransactionTimeDumpThreshold} is set to non-zero value.
     *
     * @param coefficient Coefficient, must be value between 0.0 and 1.0 inclusively.
     */
    public void transactionTimeDumpSamplesCoefficientDistributed(double coefficient) {
        assert coefficient >= 0.0 && coefficient <= 1.0 : "Percentage value must be between 0.0 and 1.0 inclusively.";

        try {
            distributedTransactionConfiguration.updateTransactionTimeDumpSamplesCoefficientAsync(coefficient).get();
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /**
     * Sets the limit of samples of completed transactions that will be dumped in log per second,
     * if {@link #transactionTimeDumpSamplesCoefficient} is above <code>0.0</code>.
     * Must be integer value greater than <code>0</code>.
     *
     * @param limit Limit value.
     */
    public void longTransactionTimeDumpSamplesPerSecondLimit(int limit) {
        assert limit > 0 : "Limit value must be greater than 0.";

        try {
            distributedTransactionConfiguration.updateLongTransactionTimeDumpSamplesPerSecondLimitAsync(limit).get();
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /** {@inheritDoc} */
    @Override protected void stop0(boolean cancel) {
        super.stop0(cancel);

        synchronized (timeoutOperations) {
            timeoutOperations.forEach((k, v) -> {
                if (v != null)
                    v.close();
            });
        }
    }

    /**
     * Setting (for all nodes) a timeout (in millis) for printing long-running
     * transactions as well as transactions that cannot receive locks for all
     * their keys for a long time. Set less than or equal {@code 0} to disable.
     *
     * @param longOpsDumpTimeout Long operations dump timeout.
     */
    public void longOperationsDumpTimeoutDistributed(long longOpsDumpTimeout) {
        try {
            distributedTransactionConfiguration.updateLongOperationsDumpTimeoutAsync(longOpsDumpTimeout).get();
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /**
     * Returns tx keys collisions dump interval, for additional info check
     * {@link IgniteSystemProperties#IGNITE_DUMP_TX_COLLISIONS_INTERVAL} description.
     *
     * @return Collisions dump interval.
     */
    public int collisionsDumpInterval() {
        return distributedTransactionConfiguration.collisionsDumpInterval();
    }

    /**
     * Changes tx key collisions dump interval.
     * For additional info check {@link IgniteSystemProperties#IGNITE_DUMP_TX_COLLISIONS_INTERVAL} description.
     *
     * @param collisionsDumpInterval New collisions dump interval or negative for disabling.
     */
    public void collisionsDumpIntervalDistributed(int collisionsDumpInterval) {
        try {
            distributedTransactionConfiguration.updateCollisionsDumpIntervalAsync(collisionsDumpInterval).get();
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /**
     * Collect queue size per key collisions info.
     *
     * @param key Key.
     * @param queueSize Collisions queue size
     */
    public void pushCollidingKeysWithQueueSize(GridCacheMapEntry key, int queueSize) {
        keyCollisionsInfo.put(key, queueSize);
    }

    /** Wrapper for inner collect logic. */
    private void collectTxCollisionsInfo() {
        keyCollisionsInfo.collectInfo();
    }

    /**
     * Check local and remote candidates queue size.
     *
     * @param entry CacheEntry.
     */
    public void detectPossibleCollidingKeys(GridDistributedCacheEntry entry) {
        int qSize = entry.remoteMvccSnapshot().size();

        try {
            qSize += entry.localCandidates().size();
        }
        catch (GridCacheEntryRemovedException ignored) {
            // No-op, obsolete vers found.
        }

        if (qSize >= COLLISIONS_QUEUE_THRESHOLD)
            pushCollidingKeysWithQueueSize(entry, qSize);
    }

    /** @param transform Transaction message transformer. */
    public void txMessageTransformer(BiFunction<GridCacheMessage, IgniteInternalTx, GridCacheMessage> transform) {
        txMsgTransform = transform;
    }

    /** @param callback Callback invoked on transaction commit. */
    public void onCommitCallback(Consumer<IgniteInternalTx> callback) {
        onCommitCallback = callback;
    }

    /**
     * Sends transaction message after transforming it.
     *
     * @param nodeId Node ID to send message.
     * @param msg Original message to transform.
     * @param tx Transaction.
     * @param plc IO policy.
     */
    public void sendTransactionMessage(UUID nodeId, GridCacheMessage msg, IgniteInternalTx tx, byte plc) throws IgniteCheckedException {
        BiFunction<GridCacheMessage, IgniteInternalTx, GridCacheMessage> transform = txMsgTransform;

        if (transform != null)
            msg = transform.apply(msg, tx);

        cctx.io().send(nodeId, msg, plc);
    }

    /**
     * Sends transaction message after transforming it.
     *
     * @param n Node to send message.
     * @param msg Original message to transform.
     * @param tx Transaction.
     * @param plc IO policy.
     */
    public void sendTransactionMessage(ClusterNode n, GridCacheMessage msg, IgniteInternalTx tx, byte plc) throws IgniteCheckedException {
        sendTransactionMessage(n.id(), msg, tx, plc);
    }

    /** Tx key collisions info holder. */
    private final class KeyCollisionsHolder {
        /** Stripes count. */
        private final int stripesCnt = cctx.kernalContext().config().getSystemThreadPoolSize();

        /** Max objects per store. */
        private static final int MAX_OBJS = 5;

        /** Store for keys and collisions queue sizes. */
        private final Map<GridCacheMapEntry, Integer>[] stores = new LinkedHashMap[stripesCnt];

        /** Metric per cache store. */
        private final Map<GridCacheAdapter<?, ?>, List<Map.Entry<GridCacheMapEntry, Integer>>> metricPerCacheStore =
            new ConcurrentHashMap<>();

        /** Guard. */
        private final AtomicBoolean alreadyRun = new AtomicBoolean();

        /** Constructor. */
        private KeyCollisionsHolder() {
            for (int i = 0; i < stripesCnt; ++i) {
                stores[i] = new LinkedHashMap<GridCacheMapEntry, Integer>() {
                    /** {@inheritDoc} */
                    @Override protected boolean removeEldestEntry(Map.Entry<GridCacheMapEntry, Integer> eldest) {
                        return size() > MAX_OBJS;
                    }
                };
            }
        }

        /**
         * Stores keys and values.
         *
         * @param key Key to store.
         * @param val Value to store.
         */
        public void put(GridCacheMapEntry key, Integer val) {
            int stripeIdx = key.hashCode() & (stripesCnt - 1);

            synchronized (stores[stripeIdx]) {
                stores[stripeIdx].put(key, val);
            }
        }

        /** Print hot keys info. */
        private void collectInfo() {
            if (!alreadyRun.compareAndSet(false, true))
                return;

            metricPerCacheStore.clear();

            for (int i = 0; i < stripesCnt; ++i) {
                synchronized (stores[i]) {
                    Map<GridCacheMapEntry, Integer> store = stores[i];

                    if (store.isEmpty())
                        continue;

                    for (Map.Entry<GridCacheMapEntry, Integer> info : store.entrySet()) {
                        GridCacheAdapter<Object, Object> cacheCtx = info.getKey().context().cache();

                        if (cacheCtx != null)
                            metricPerCacheStore.computeIfAbsent(cacheCtx, k -> new ArrayList<>()).add(info);
                    }

                    store.clear();
                }
            }

            metricPerCacheStore.forEach((k, v) -> {
                if (k.metrics0().keyCollisionsInfo() == null) {
                    k.metrics0().keyCollisionsInfo(
                        () -> metricPerCacheStore.get(k)
                    );
                }
            });

            alreadyRun.getAndSet(false);
        }
    }

    /**
     * Writes transaction performance statistics.
     *
     * @param tx Transaction.
     * @param commited {@code True} if transaction commited.
     */
    private void writeStatistics(IgniteInternalTx tx, boolean commited) {
        if (!cctx.kernalContext().performanceStatistics().enabled() || tx.startTimeNanos() == 0)
            return;

        cctx.kernalContext().performanceStatistics().transaction(
            tx.txState().cacheIds(),
            tx.startTime(),
            System.nanoTime() - tx.startTimeNanos(),
            commited);
    }

    /**
     * Transactions recovery initialization runnable.
     */
    private final class TxRecoveryInitRunnable implements GridPlainRunnable {
        /** */
        private final ClusterNode node;

        /** Time bag to measure and store tx recovery stages times. */
        private final TimeBag timeBag = new TimeBag(log.isInfoEnabled());

        /** Prepared tx to be recovered count. */
        private final AtomicLong preparedTxCnt = new AtomicLong();

        /** Recovery finished future. */
        private final GridCompoundFuture<Boolean, ?> doneFut = new GridCompoundFuture<>();

        /**
         * @param node Failed node.
         */
        private TxRecoveryInitRunnable(ClusterNode node) {
            this.node = node;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            timeBag.finishGlobalStage("Started");

            try {
                cctx.kernalContext().gateway().readLock();
            }
            catch (IllegalStateException | IgniteClientDisconnectedException e) {
                if (log.isDebugEnabled())
                    log.debug("Failed to acquire kernal gateway [err=" + e + ']');

                return;
            }

            UUID evtNodeId = node.id();

            try {
                if (log.isDebugEnabled())
                    log.debug("Processing node failed event [locNodeId=" + cctx.localNodeId() +
                        ", failedNodeId=" + evtNodeId + ']');

                for (final IgniteInternalTx tx : activeTransactions()) {
                    if ((tx.near() && !tx.local() && tx.originatingNodeId().equals(evtNodeId))
                        || (tx.storeWriteThrough() && tx.masterNodeIds().contains(evtNodeId))) {
                        // Invalidate transactions.
                        salvageTx(tx, RECOVERY_FINISH);
                    }
                    else {
                        // Check prepare only if originating node ID failed. Otherwise, parent node will finish this tx.
                        if (tx.originatingNodeId().equals(evtNodeId)) {
                            if (tx.state() == PREPARED)
                                processPrepared(tx, evtNodeId);
                            else {
                                IgniteInternalFuture<?> prepFut = tx.currentPrepareFuture();

                                if (prepFut != null) {
                                    prepFut.listen(() -> {
                                        if (tx.state() == PREPARED)
                                            processPrepared(tx, evtNodeId);
                                            // If we could not mark tx as rollback, it means that transaction is being committed.
                                        else if (tx.state() == MARKED_ROLLBACK || tx.setRollbackOnly())
                                            tx.rollbackAsync();
                                    });
                                }
                                // If we could not mark tx as rollback, it means that transaction
                                // is either being committed or already marked as rollback.
                                else if (tx.state() == MARKED_ROLLBACK || tx.setRollbackOnly())
                                    tx.rollbackAsync();
                            }
                        }
                    }
                }

                timeBag.finishGlobalStage("Initialized");

                doneFut.markInitialized();

                if (log.isInfoEnabled() && preparedTxCnt.get() > 0)
                    doneFut.listen(this::finishAndRecordTimings);
            }
            finally {
                cctx.kernalContext().gateway().readUnlock();
            }
        }

        /**
         * @param tx Tx.
         * @param failedNode Failed node.
         */
        private void processPrepared(IgniteInternalTx tx, UUID failedNode) {
            IgniteInternalFuture<Boolean> fut = commitIfPrepared(tx, Collections.singleton(failedNode));

            if (fut != null)
                doneFut.add(fut);

            preparedTxCnt.incrementAndGet();
        }

        /**
         *
         */
        private void finishAndRecordTimings() {
            timeBag.finishGlobalStage("Finished");

            StringBuilder timingsToLog = new StringBuilder();

            timingsToLog.append("TxRecovery Status and Timings [txs=").append(preparedTxCnt.get());

            for (String stageTiming : timeBag.stagesTimings())
                timingsToLog.append(", ").append(stageTiming);

            timingsToLog.append("]");

            log.info(timingsToLog.toString());
        }
    }

    /**
     * Per-thread key for system transactions.
     */
    private static class TxThreadKey {
        /** Thread ID. */
        private final long threadId;

        /** Cache ID. */
        private final int cacheId;

        /**
         * @param threadId Thread ID.
         * @param cacheId Cache ID.
         */
        private TxThreadKey(long threadId, int cacheId) {
            this.threadId = threadId;
            this.cacheId = cacheId;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (!(o instanceof TxThreadKey))
                return false;

            TxThreadKey that = (TxThreadKey)o;

            return cacheId == that.cacheId && threadId == that.threadId;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            int res = (int)(threadId ^ (threadId >>> 32));

            res = 31 * res + cacheId;

            return res;
        }
    }

    /**
     *
     */
    private static class CommittedVersion extends GridCacheVersion {
        /** */
        private static final long serialVersionUID = 0L;

        /** Corresponding near version. Transient. */
        private GridCacheVersion nearVer;

        /**
         * Empty constructor required by {@link Externalizable}.
         */
        public CommittedVersion() {
            // No-op.
        }

        /**
         * @param ver Committed version.
         * @param nearVer Near transaction version.
         */
        private CommittedVersion(GridCacheVersion ver, GridCacheVersion nearVer) {
            super(ver.topologyVersion(), ver.order(), ver.nodeOrder(), ver.dataCenterId());

            assert nearVer != null;

            this.nearVer = nearVer;
        }
    }

    /**
     * Commit listener. Checks if commit succeeded and rollbacks if case of error.
     */
    private class CommitListener implements CI1<IgniteInternalFuture<IgniteInternalTx>> {
        /** */
        private static final long serialVersionUID = 0L;

        /** Transaction. */
        private final IgniteInternalTx tx;

        /**
         * @param tx Transaction.
         */
        private CommitListener(IgniteInternalTx tx) {
            this.tx = tx;
        }

        /** {@inheritDoc} */
        @Override public void apply(IgniteInternalFuture<IgniteInternalTx> t) {
            try {
                t.get();
            }
            catch (IgniteTxOptimisticCheckedException ignore) {
                if (log.isDebugEnabled())
                    log.debug("Optimistic failure while committing prepared transaction (will rollback): " +
                        tx);

                try {
                    tx.rollbackAsync();
                }
                catch (Throwable e) {
                    U.error(log, "Failed to automatically rollback transaction: " + tx, e);
                }
            }
            catch (IgniteCheckedException e) {
                U.error(log, "Failed to commit transaction during failover: " + tx, e);
            }
        }
    }

    /**
     * Transactions deadlock detection process message listener.
     */
    private class DeadlockDetectionListener implements GridMessageListener {
        /** {@inheritDoc} */
        @Override public void onMessage(UUID nodeId, Object msg, byte plc) {
            GridCacheMessage cacheMsg = (GridCacheMessage)msg;

            Throwable err = null;

            try {
                unmarshall(nodeId, cacheMsg);
            }
            catch (Exception e) {
                err = e;
            }

            if (err != null || cacheMsg.classError() != null) {
                try {
                    processFailedMessage(nodeId, cacheMsg, err);
                }
                catch (Throwable e) {
                    U.error(log, "Failed to process message [senderId=" + nodeId +
                        ", messageType=" + cacheMsg.getClass() + ']', e);

                    if (e instanceof Error)
                        throw (Error)e;
                }
            }
            else {
                if (log.isDebugEnabled())
                    log.debug("Message received [locNodeId=" + cctx.localNodeId() +
                        ", rmtNodeId=" + nodeId + ", msg=" + msg + ']');

                if (msg instanceof TxLocksRequest) {
                    TxLocksRequest req = (TxLocksRequest)msg;

                    TxLocksResponse res = txLocksInfo(req.txKeys());

                    res.futureId(req.futureId());

                    try {
                        if (!cctx.localNodeId().equals(nodeId))
                            res.prepareMarshal(cctx);

                        cctx.gridIO().sendToGridTopic(nodeId, TOPIC_TX, res, SYSTEM_POOL);
                    }
                    catch (ClusterTopologyCheckedException e) {
                        if (log.isDebugEnabled())
                            log.debug("Failed to send response, node failed: " + nodeId);
                    }
                    catch (IgniteCheckedException e) {
                        U.error(log, "Failed to send response to node [node=" + nodeId + ", res=" + res + ']', e);
                    }
                }
                else if (msg instanceof TxLocksResponse) {
                    TxLocksResponse res = (TxLocksResponse)msg;

                    long futId = res.futureId();

                    TxDeadlockFuture fut = future(futId);

                    if (fut != null)
                        fut.onResult(nodeId, res);
                    else
                        U.warn(log, "Unexpected response received " + res);
                }
                else
                    throw new IllegalArgumentException("Unknown message [msg=" + msg + ']');
            }
        }

        /**
         * @param nodeId Node ID.
         * @param msg Message.
         */
        private void processFailedMessage(UUID nodeId, GridCacheMessage msg, Throwable err) throws IgniteCheckedException {
            switch (msg.directType()) {
                case -24: {
                    TxLocksRequest req = (TxLocksRequest)msg;

                    TxLocksResponse res = new TxLocksResponse();

                    res.futureId(req.futureId());

                    try {
                        cctx.gridIO().sendToGridTopic(nodeId, TOPIC_TX, res, SYSTEM_POOL);
                    }
                    catch (ClusterTopologyCheckedException e) {
                        if (log.isDebugEnabled())
                            log.debug("Failed to send response, node failed: " + nodeId);
                    }
                    catch (IgniteCheckedException e) {
                        U.error(log, "Failed to send response to node (is node still alive?) [nodeId=" + nodeId +
                            ", res=" + res + ']', e);
                    }
                }

                break;

                case -23: {
                    TxLocksResponse res = (TxLocksResponse)msg;

                    TxDeadlockFuture fut = future(res.futureId());

                    if (fut == null) {
                        if (log.isDebugEnabled())
                            log.debug("Failed to find future for response [sender=" + nodeId + ", res=" + res + ']');

                        return;
                    }

                    if (err == null)
                        fut.onResult(nodeId, res);
                    else
                        fut.onDone(null, err);
                }

                break;

                default:
                    throw new IgniteCheckedException("Failed to process message. Unsupported direct type [msg=" +
                        msg + ']', msg.classError());
            }

        }

        /**
         * @param nodeId Sender node ID.
         * @param cacheMsg Message.
         */
        private void unmarshall(UUID nodeId, GridCacheMessage cacheMsg) {
            if (cctx.localNodeId().equals(nodeId))
                return;

            try {
                cacheMsg.finishUnmarshal(cctx, cctx.deploy().globalLoader());
            }
            catch (IgniteCheckedException e) {
                cacheMsg.onClassError(e);
            }
            catch (BinaryObjectException e) {
                cacheMsg.onClassError(new IgniteCheckedException(e));
            }
            catch (Error e) {
                if (cacheMsg.ignoreClassErrors() &&
                    X.hasCause(e, NoClassDefFoundError.class, UnsupportedClassVersionError.class)) {
                    cacheMsg.onClassError(
                        new IgniteCheckedException("Failed to load class during unmarshalling: " + e, e)
                    );
                }
                else
                    throw e;
            }
        }
    }

    /**
     * This class is used to store information about transaction time dump throttling.
     */
    public class TxDumpsThrottling {
        /** */
        private final AtomicInteger skippedTxCntr = new AtomicInteger();

        /** */
        private final HitRateMetric transactionHitRateCntr = new HitRateMetric("transactionHitRateCounter", null, 1000, 2);

        /**
         * Returns should we skip dumping the transaction in current moment.
         */
        public boolean skipCurrent() {
            boolean res = transactionHitRateCntr.value() >= transactionTimeDumpSamplesPerSecondLimit();

            if (!res) {
                int skipped = skippedTxCntr.getAndSet(0);

                //we should not log info about skipped dumps if skippedTxCounter was reset concurrently
                if (skipped > 0)
                    log.info("Transaction time dumps skipped because of log throttling: " + skipped);
            }

            return res;
        }

        /**
         * Should be called when we dump transaction to log.
         */
        public void dump() {
            transactionHitRateCntr.increment();
        }

        /**
         * Should be called when we skip transaction which we could dump to log because of throttling.
         */
        public void skip() {
            skippedTxCntr.incrementAndGet();
        }
    }

    /**
     * The future for changing transaction timeout on partition map exchange.
     */
    private class TxTimeoutOnPartitionMapExchangeChangeFuture extends GridFutureAdapter<Void> {
        /** */
        private final UUID id;

        /**
         * @param id Future ID.
         */
        private TxTimeoutOnPartitionMapExchangeChangeFuture(UUID id) {
            this.id = id;
        }

        /** {@inheritDoc} */
        @Override public boolean onDone(@Nullable Void res, @Nullable Throwable err) {
            txTimeoutOnPartitionMapExchangeFuts.remove(id, this);
            return super.onDone(res, err);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(TxTimeoutOnPartitionMapExchangeChangeFuture.class, this);
        }
    }

    /**
     * Starts the tx recovery.
     */
    private class TransactionRecoveryListener implements DiscoveryEventListener, HighPriorityListener {
        /** {@inheritDoc} */
        @Override public void onEvent(DiscoveryEvent evt, DiscoCache discoCache) {
            IgniteInternalFuture<?> recInitFut = cctx.kernalContext().closure().runLocalSafe(
                new TxRecoveryInitRunnable(evt.eventNode()));

            recInitFut.listen(() -> {
                if (recInitFut.error() != null)
                    cctx.kernalContext().failure().process(new FailureContext(FailureType.CRITICAL_ERROR, recInitFut.error()));
            });
        }

        /** {@inheritDoc} */
        @Override public int order() {
            return 0;
        }
    }

    /**
     * Collects tx states {@link TransactionState} for further correct restoring.
     *
     * @param txRec tx Record.
     */
    public void collectTxStates(final TxRecord txRec) {
        if (COMPLETED_TX_STATES.test(txRec))
            uncommitedTx.remove(txRec.nearXidVersion());
        else if (PREPARED_TX_STATES.test(txRec))
            uncommitedTx.add(txRec.nearXidVersion());
    }

    /**
     * @param dataEntry Processing entry.
     * @return {@code true} If entry contains not completed tx version.
     */
    public boolean uncommitedTx(final DataEntry dataEntry) {
        return uncommitedTx.contains(dataEntry.nearXidVersion());
    }

    /** Clears tx states collections. */
    public void clearUncommitedStates() {
        uncommitedTx = Collections.emptySet();
    }

    /**
     * Checks if transaction mode supported for transaction aware queries.
     * @param isolation Transaction isolation to check.
     */
    public static void ensureTransactionModeSupported(TransactionIsolation isolation) {
        if (TX_AWARE_QUERIES_SUPPORTED_MODES.contains(isolation))
            return;

        throw new IllegalStateException("Transaction isolation mode not supported for SQL queries: " + isolation);
    }
}

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
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteClientDisconnectedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.managers.communication.GridIoPolicy;
import org.apache.ignite.internal.managers.eventstorage.GridLocalEventListener;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.GridCacheEntryRemovedException;
import org.apache.ignite.internal.processors.cache.GridCacheMvccCandidate;
import org.apache.ignite.internal.processors.cache.GridCacheSharedManagerAdapter;
import org.apache.ignite.internal.processors.cache.distributed.GridCacheMappedVersion;
import org.apache.ignite.internal.processors.cache.distributed.GridCacheTxFinishSync;
import org.apache.ignite.internal.processors.cache.distributed.GridCacheTxRecoveryFuture;
import org.apache.ignite.internal.processors.cache.distributed.GridDistributedLockCancelledException;
import org.apache.ignite.internal.processors.cache.distributed.GridDistributedTxRemoteAdapter;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtInvalidPartitionException;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxLocal;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxRemote;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearCacheAdapter;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearCacheEntry;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxLocal;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.timeout.GridTimeoutObjectAdapter;
import org.apache.ignite.internal.transactions.IgniteTxOptimisticCheckedException;
import org.apache.ignite.internal.transactions.IgniteTxTimeoutCheckedException;
import org.apache.ignite.internal.util.GridBoundedConcurrentOrderedMap;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.lang.GridFunc;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteReducer;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.transactions.TransactionState;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentHashMap8;
import org.jsr166.ConcurrentLinkedDeque8;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_MAX_COMPLETED_TX_COUNT;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_SLOW_TX_WARN_TIMEOUT;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_TX_SALVAGE_TIMEOUT;
import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.internal.processors.cache.GridCacheUtils.isNearEnabled;
import static org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx.FinalizationStatus.RECOVERY_FINISH;
import static org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx.FinalizationStatus.USER_FINISH;
import static org.apache.ignite.internal.util.GridConcurrentFactory.newMap;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionState.ACTIVE;
import static org.apache.ignite.transactions.TransactionState.COMMITTED;
import static org.apache.ignite.transactions.TransactionState.COMMITTING;
import static org.apache.ignite.transactions.TransactionState.MARKED_ROLLBACK;
import static org.apache.ignite.transactions.TransactionState.PREPARED;
import static org.apache.ignite.transactions.TransactionState.PREPARING;
import static org.apache.ignite.transactions.TransactionState.ROLLED_BACK;
import static org.apache.ignite.transactions.TransactionState.UNKNOWN;

/**
 * Cache transaction manager.
 */
public class IgniteTxManager extends GridCacheSharedManagerAdapter {
    /** Default maximum number of transactions that have completed. */
    private static final int DFLT_MAX_COMPLETED_TX_CNT = 262144; // 2^18

    /** Slow tx warn timeout (initialized to 0). */
    private static final int SLOW_TX_WARN_TIMEOUT = Integer.getInteger(IGNITE_SLOW_TX_WARN_TIMEOUT, 0);

    /** Tx salvage timeout (default 3s). */
    private static final int TX_SALVAGE_TIMEOUT = Integer.getInteger(IGNITE_TX_SALVAGE_TIMEOUT, 100);

    /** Committing transactions. */
    private final ThreadLocal<IgniteInternalTx> threadCtx = new ThreadLocal<>();

    /** Per-thread transaction map. */
    private final ConcurrentMap<Long, IgniteInternalTx> threadMap = newMap();

    /** Per-thread system transaction map. */
    private final ConcurrentMap<TxThreadKey, IgniteInternalTx> sysThreadMap = newMap();

    /** Per-ID map. */
    private final ConcurrentMap<GridCacheVersion, IgniteInternalTx> idMap = newMap();

    /** Per-ID map for near transactions. */
    private final ConcurrentMap<GridCacheVersion, IgniteInternalTx> nearIdMap = newMap();

    /** TX handler. */
    private IgniteTxHandler txHandler;

    /** All transactions. */
    private final Queue<IgniteInternalTx> committedQ = new ConcurrentLinkedDeque8<>();

    /** Preparing transactions. */
    private final Queue<IgniteInternalTx> prepareQ = new ConcurrentLinkedDeque8<>();

    /** Minimum start version. */
    private final ConcurrentNavigableMap<GridCacheVersion, AtomicInt> startVerCnts =
        new ConcurrentSkipListMap<>();

    /** Committed local transactions. */
    private final GridBoundedConcurrentOrderedMap<GridCacheVersion, Boolean> completedVers =
        new GridBoundedConcurrentOrderedMap<>(Integer.getInteger(IGNITE_MAX_COMPLETED_TX_COUNT, DFLT_MAX_COMPLETED_TX_CNT));

    /** Transaction finish synchronizer. */
    private GridCacheTxFinishSync txFinishSync;

    /** For test purposes only. */
    private boolean finishSyncDisabled;

    /** Slow tx warn timeout. */
    private int slowTxWarnTimeout = SLOW_TX_WARN_TIMEOUT;

    /**
     * Near version to DHT version map. Note that we initialize to 5K size from get go,
     * to avoid future map resizings.
     */
    private final ConcurrentMap<GridCacheVersion, GridCacheVersion> mappedVers =
        new ConcurrentHashMap8<>(5120);

    /** {@inheritDoc} */
    @Override protected void onKernalStart0(boolean reconnect) {
        if (reconnect)
            return;

        cctx.gridEvents().addLocalEventListener(
            new GridLocalEventListener() {
                @Override public void onEvent(Event evt) {
                    assert evt instanceof DiscoveryEvent;
                    assert evt.type() == EVT_NODE_FAILED || evt.type() == EVT_NODE_LEFT;

                    DiscoveryEvent discoEvt = (DiscoveryEvent)evt;

                    cctx.time().addTimeoutObject(new NodeFailureTimeoutObject(discoEvt.eventNode().id()));

                    if (txFinishSync != null)
                        txFinishSync.onNodeLeft(discoEvt.eventNode().id());
                }
            },
            EVT_NODE_FAILED, EVT_NODE_LEFT);

        for (IgniteInternalTx tx : idMap.values()) {
            if ((!tx.local() || tx.dht()) && !cctx.discovery().aliveAll(tx.masterNodeIds())) {
                if (log.isDebugEnabled())
                    log.debug("Remaining transaction from left node: " + tx);

                salvageTx(tx, true, USER_FINISH);
            }
        }
    }

    /** {@inheritDoc} */
    @Override protected void start0() throws IgniteCheckedException {
        txFinishSync = new GridCacheTxFinishSync<>(cctx);

        txHandler = new IgniteTxHandler(cctx);
    }

    /** {@inheritDoc} */
    @Override public void onDisconnected(IgniteFuture reconnectFut) {
        txFinishSync.onDisconnected(reconnectFut);

        for (Map.Entry<Long, IgniteInternalTx> e : threadMap.entrySet())
            rollbackTx(e.getValue());
    }

    /**
     * @return TX handler.
     */
    public IgniteTxHandler txHandler() {
        return txHandler;
    }

    /**
     * Invalidates transaction.
     *
     * @param tx Transaction.
     * @return {@code True} if transaction was salvaged by this call.
     */
    public boolean salvageTx(IgniteInternalTx tx) {
        return salvageTx(tx, false, USER_FINISH);
    }

    /**
     * Invalidates transaction.
     *
     * @param tx Transaction.
     * @param warn {@code True} if warning should be logged.
     * @param status Finalization status.
     * @return {@code True} if transaction was salvaged by this call.
     */
    private boolean salvageTx(IgniteInternalTx tx, boolean warn, IgniteInternalTx.FinalizationStatus status) {
        assert tx != null;

        TransactionState state = tx.state();

        if (state == ACTIVE || state == PREPARING || state == PREPARED) {
            try {
                if (!tx.markFinalizing(status)) {
                    if (log.isDebugEnabled())
                        log.debug("Will not try to commit invalidate transaction (could not mark finalized): " + tx);

                    return false;
                }

                tx.systemInvalidate(true);

                tx.prepare();

                if (tx.state() == PREPARING) {
                    if (log.isDebugEnabled())
                        log.debug("Ignoring transaction in PREPARING state as it is currently handled " +
                            "by another thread: " + tx);

                    return false;
                }

                if (tx instanceof IgniteTxRemoteEx) {
                    IgniteTxRemoteEx rmtTx = (IgniteTxRemoteEx)tx;

                    rmtTx.doneRemote(tx.xidVersion(), Collections.<GridCacheVersion>emptyList(),
                        Collections.<GridCacheVersion>emptyList(), Collections.<GridCacheVersion>emptyList());
                }

                tx.commit();

                if (warn) {
                    // This print out cannot print any peer-deployed entity either
                    // directly or indirectly.
                    U.warn(log, "Invalidated transaction because originating node either " +
                        "crashed or left grid: " + CU.txString(tx));
                }
            }
            catch (IgniteTxOptimisticCheckedException ignore) {
                if (log.isDebugEnabled())
                    log.debug("Optimistic failure while invalidating transaction (will rollback): " +
                        tx.xidVersion());

                try {
                    tx.rollback();
                }
                catch (IgniteCheckedException e) {
                    U.error(log, "Failed to rollback transaction: " + tx.xidVersion(), e);
                }
            }
            catch (IgniteCheckedException e) {
                U.error(log, "Failed to invalidate transaction: " + tx, e);
            }
        }
        else if (state == MARKED_ROLLBACK) {
            try {
                tx.rollback();
            }
            catch (IgniteCheckedException e) {
                U.error(log, "Failed to rollback transaction: " + tx.xidVersion(), e);
            }
        }

        return true;
    }

    /**
     * Prints out memory stats to standard out.
     * <p>
     * USE ONLY FOR MEMORY PROFILING DURING TESTS.
     */
    @Override public void printMemoryStats() {
        IgniteInternalTx firstTx = committedQ.peek();

        int committedSize = committedQ.size();

        Map.Entry<GridCacheVersion, AtomicInt> startVerEntry = startVerCnts.firstEntry();

        GridCacheVersion minStartVer = null;
        long dur = 0;

        if (committedSize > 3000) {
            minStartVer = new GridCacheVersion(Integer.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE, Integer.MAX_VALUE, 0);

            IgniteInternalTx stuck = null;

            for (IgniteInternalTx tx : txs())
                if (tx.startVersion().isLess(minStartVer)) {
                    minStartVer = tx.startVersion();
                    dur = U.currentTimeMillis() - tx.startTime();

                    stuck = tx;
                }

            X.println("Stuck transaction: " + stuck);
        }

        X.println(">>> ");
        X.println(">>> Transaction manager memory stats [grid=" + cctx.gridName() + ']');
        X.println(">>>   threadMapSize: " + threadMap.size());
        X.println(">>>   idMap [size=" + idMap.size() + ", minStartVer=" + minStartVer + ", dur=" + dur + "ms]");
        X.println(">>>   committedQueue [size=" + committedSize +
            ", firstStartVersion=" + (firstTx == null ? "null" : firstTx.startVersion()) +
            ", firstEndVersion=" + (firstTx == null ? "null" : firstTx.endVersion()) + ']');
        X.println(">>>   prepareQueueSize: " + prepareQ.size());
        X.println(">>>   startVerCntsSize [size=" + startVerCnts.size() +
            ", firstVer=" + startVerEntry + ']');
        X.println(">>>   completedVersSize: " + completedVers.size());
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
     * @return Committed queue size.
     */
    public int commitQueueSize() {
        return committedQ.size();
    }

    /**
     * @return Prepare queue size.
     */
    public int prepareQueueSize() {
        return prepareQ.size();
    }

    /**
     * @return Start version counts.
     */
    public int startVersionCountsSize() {
        return startVerCnts.size();
    }

    /**
     * @return Committed versions size.
     */
    public int completedVersionsSize() {
        return completedVers.size();
    }

    /**
     *
     * @param tx Transaction to check.
     * @return {@code True} if transaction has been committed or rolled back,
     *      {@code false} otherwise.
     */
    public boolean isCompleted(IgniteInternalTx tx) {
        return completedVers.containsKey(tx.xidVersion());
    }

    /**
     * @param implicit {@code True} if transaction is implicit.
     * @param implicitSingle Implicit-with-single-key flag.
     * @param concurrency Concurrency.
     * @param isolation Isolation.
     * @param timeout transaction timeout.
     * @param txSize Expected transaction size.
     * @return New transaction.
     */
    public IgniteTxLocalAdapter newTx(
        boolean implicit,
        boolean implicitSingle,
        @Nullable GridCacheContext sysCacheCtx,
        TransactionConcurrency concurrency,
        TransactionIsolation isolation,
        long timeout,
        boolean storeEnabled,
        int txSize
    ) {
        assert sysCacheCtx == null || sysCacheCtx.systemTx();

        UUID subjId = null; // TODO GG-9141 how to get subj ID?

        int taskNameHash = cctx.kernalContext().job().currentTaskNameHash();

        GridNearTxLocal tx = new GridNearTxLocal(
            cctx,
            implicit,
            implicitSingle,
            sysCacheCtx != null,
            sysCacheCtx != null ? sysCacheCtx.ioPolicy() : GridIoPolicy.SYSTEM_POOL,
            concurrency,
            isolation,
            timeout,
            storeEnabled,
            txSize,
            subjId,
            taskNameHash);

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
            // Add both, explicit and implicit transactions.
            // Do not add remote and dht local transactions as remote node may have the same thread ID
            // and overwrite local transaction.
            if (tx.local() && !tx.dht()) {
                if (cacheCtx == null || !cacheCtx.systemTx())
                    threadMap.put(tx.threadId(), tx);
                else
                    sysThreadMap.put(new TxThreadKey(tx.threadId(), cacheCtx.cacheId()), tx);
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

        if (cctx.txConfig().isTxSerializableEnabled()) {
            AtomicInt next = new AtomicInt(1);

            boolean loop = true;

            while (loop) {
                AtomicInt prev = startVerCnts.putIfAbsent(tx.startVersion(), next);

                if (prev == null)
                    break; // Put succeeded - exit.

                // Previous value was 0, which means that it will be deleted
                // by another thread in "decrementStartVersionCount(..)" method.
                // In that case, we delete here too, so we can safely try again.
                for (;;) {
                    int p = prev.get();

                    assert p >= 0 : p;

                    if (p == 0) {
                        if (startVerCnts.remove(tx.startVersion(), prev))
                            if (log.isDebugEnabled())
                                log.debug("Removed count from onCreated callback: " + tx);

                        break; // Retry outer loop.
                    }

                    if (prev.compareAndSet(p, p + 1)) {
                        loop = false; // Increment succeeded - exit outer loop.

                        break;
                    }
                }
            }
        }

        if (tx.timeout() > 0) {
            cctx.time().addTimeoutObject(tx);

            if (log.isDebugEnabled())
                log.debug("Registered transaction with timeout processor: " + tx);
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
    public IgniteInternalFuture<Boolean> finishTxs(AffinityTopologyVersion topVer) {
        GridCompoundFuture<IgniteInternalTx, Boolean> res =
            new GridCompoundFuture<>(
                new IgniteReducer<IgniteInternalTx, Boolean>() {
                    @Override public boolean collect(IgniteInternalTx e) {
                        return true;
                    }

                    @Override public Boolean reduce() {
                        return true;
                    }
                });

        for (IgniteInternalTx tx : txs()) {
            // Must wait for all transactions, even for DHT local and DHT remote since preloading may acquire
            // values pending to be overwritten by prepared transaction.

            if (tx.concurrency() == PESSIMISTIC) {
                if (tx.topologyVersion().compareTo(AffinityTopologyVersion.ZERO) > 0
                    && tx.topologyVersion().compareTo(topVer) < 0)
                    // For PESSIMISTIC mode we must wait for all uncompleted txs
                    // as we do not know in advance which keys will participate in tx.
                    res.add(tx.finishFuture());
            }
            else if (tx.concurrency() == OPTIMISTIC) {
                // For OPTIMISTIC mode we wait only for txs in PREPARING state that
                // have keys for given partitions.
                TransactionState state = tx.state();
                AffinityTopologyVersion txTopVer = tx.topologyVersion();

                if ((state != ACTIVE && state != COMMITTED && state != ROLLED_BACK && state != UNKNOWN)
                    && txTopVer.compareTo(AffinityTopologyVersion.ZERO) > 0 && txTopVer.compareTo(topVer) < 0)
                    res.add(tx.finishFuture());
            }
        }

        res.markInitialized();

        return res;
    }

    /**
     * Transaction start callback (has to do with when any operation was
     * performed on this transaction).
     *
     * @param tx Started transaction.
     * @return {@code True} if transaction is not in completed set.
     */
    public boolean onStarted(IgniteInternalTx tx) {
        assert tx.state() == ACTIVE || tx.isRollbackOnly() : "Invalid transaction state [locId=" + cctx.localNodeId() +
            ", tx=" + tx + ']';

        if (isCompleted(tx)) {
            if (log.isDebugEnabled())
                log.debug("Attempt to start a completed transaction (will ignore): " + tx);

            return false;
        }

        if (log.isDebugEnabled())
            log.debug("Transaction started: " + tx);

        return true;
    }

    /**
     * Reverse mapped version look up.
     *
     * @param dhtVer Dht version.
     * @return Near version.
     */
    @Nullable public GridCacheVersion nearVersion(GridCacheVersion dhtVer) {
        IgniteInternalTx tx = idMap.get(dhtVer);

        if (tx != null)
            return tx.nearXidVersion();

        return null;
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
    @SuppressWarnings({"unchecked"})
    @Nullable public <T> T localTx() {
        IgniteInternalTx tx = tx();

        return tx != null && tx.local() ? (T)tx : null;
    }

    /**
     * @return Transaction for current thread.
     */
    @SuppressWarnings({"unchecked"})
    public <T> T threadLocalTx(GridCacheContext cctx) {
        IgniteInternalTx tx = tx(cctx, Thread.currentThread().getId());

        return tx != null && tx.local() && (!tx.dht() || tx.colocated()) && !tx.implicit() ? (T)tx : null;
    }

    /**
     * @return Transaction for current thread.
     */
    @SuppressWarnings({"unchecked", "RedundantCast"})
    public <T> T tx() {
        IgniteInternalTx tx = txContext();

        return tx != null ? (T)tx : (T)tx(null, Thread.currentThread().getId());
    }

    /**
     * @return Any transaction associated with the current thread.
     */
    public IgniteInternalTx anyActiveThreadTx(IgniteInternalTx ignore) {
        long threadId = Thread.currentThread().getId();

        IgniteInternalTx tx = threadMap.get(threadId);

        if (tx != null && tx.topologyVersionSnapshot() != null)
            return tx;

        for (GridCacheContext cacheCtx : cctx.cache().context().cacheContexts()) {
            if (!cacheCtx.systemTx())
                continue;

            tx = sysThreadMap.get(new TxThreadKey(threadId, cacheCtx.cacheId()));

            if (tx != null && tx != ignore && tx.topologyVersionSnapshot() != null)
                return tx;
        }

        return null;
    }

    /**
     * @return Local transaction.
     */
    @Nullable public IgniteInternalTx localTxx() {
        IgniteInternalTx tx = txx();

        return tx != null && tx.local() ? tx : null;
    }

    /**
     * @return Transaction for current thread.
     */
    @SuppressWarnings({"unchecked"})
    public IgniteInternalTx txx() {
        return tx();
    }

    /**
     * @return User transaction for current thread.
     */
    @Nullable public IgniteInternalTx userTx() {
        IgniteInternalTx tx = txContext();

        if (tx != null && tx.user() && tx.state() == ACTIVE)
            return tx;

        tx = tx(null, Thread.currentThread().getId());

        return tx != null && tx.user() && tx.state() == ACTIVE ? tx : null;
    }

    /**
     * @return User transaction for current thread.
     */
    @Nullable public IgniteInternalTx userTx(GridCacheContext cctx) {
        IgniteInternalTx tx = tx(cctx, Thread.currentThread().getId());

        return tx != null && tx.user() && tx.state() == ACTIVE ? tx : null;
    }

    /**
     * @return User transaction.
     */
    @SuppressWarnings({"unchecked"})
    @Nullable public <T extends IgniteTxLocalEx> T userTxx() {
        return (T)userTx();
    }

    /**
     * @param cctx Cache context.
     * @param threadId Id of thread for transaction.
     * @return Transaction for thread with given ID.
     */
    @SuppressWarnings({"unchecked"})
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
    @SuppressWarnings({"unchecked"})
    @Nullable public <T extends IgniteInternalTx> T tx(GridCacheVersion txId) {
        return (T)idMap.get(txId);
    }

    /**
     * @param txId Transaction ID.
     * @return Transaction with given ID.
     */
    @SuppressWarnings({"unchecked"})
    @Nullable public <T extends IgniteInternalTx> T nearTx(GridCacheVersion txId) {
        return (T)nearIdMap.get(txId);
    }

    /**
     * Handles prepare stage of 2PC.
     *
     * @param tx Transaction to prepare.
     * @throws IgniteCheckedException If preparation failed.
     */
    public void prepareTx(IgniteInternalTx tx) throws IgniteCheckedException {
        if (tx.state() == MARKED_ROLLBACK) {
            if (tx.timedOut())
                throw new IgniteTxTimeoutCheckedException("Transaction timed out: " + this);

            throw new IgniteCheckedException("Transaction is marked for rollback: " + tx);
        }

        if (tx.remainingTime() == 0) {
            tx.setRollbackOnly();

            throw new IgniteTxTimeoutCheckedException("Transaction timed out: " + this);
        }

        boolean txSerEnabled = cctx.txConfig().isTxSerializableEnabled();

        // Clean up committed transactions queue.
        if (tx.pessimistic() && tx.local()) {
            if (tx.enforceSerializable() && txSerEnabled) {
                for (Iterator<IgniteInternalTx> it = committedQ.iterator(); it.hasNext();) {
                    IgniteInternalTx committedTx = it.next();

                    assert committedTx != tx;

                    // Clean up.
                    if (isSafeToForget(committedTx))
                        it.remove();
                }
            }

            // Nothing else to do in pessimistic mode.
            return;
        }

        if (txSerEnabled && tx.optimistic() && tx.enforceSerializable()) {
            Set<IgniteTxKey> readSet = tx.readSet();
            Set<IgniteTxKey> writeSet = tx.writeSet();

            GridCacheVersion startTn = tx.startVersion();

            GridCacheVersion finishTn = cctx.versions().last();

            // Add future to prepare queue only on first prepare call.
            if (tx.markPreparing())
                prepareQ.offer(tx);

            // Check that our read set does not intersect with write set
            // of all transactions that completed their write phase
            // while our transaction was in read phase.
            for (Iterator<IgniteInternalTx> it = committedQ.iterator(); it.hasNext();) {
                IgniteInternalTx committedTx = it.next();

                assert committedTx != tx;

                // Clean up.
                if (isSafeToForget(committedTx)) {
                    it.remove();

                    continue;
                }

                GridCacheVersion tn = committedTx.endVersion();

                // We only care about transactions
                // with tn > startTn and tn <= finishTn
                if (tn.compareTo(startTn) <= 0 || tn.compareTo(finishTn) > 0)
                    continue;

                if (tx.serializable()) {
                    if (GridFunc.intersects(committedTx.writeSet(), readSet)) {
                        tx.setRollbackOnly();

                        throw new IgniteTxOptimisticCheckedException("Failed to prepare transaction " +
                            "(committed vs. read-set conflict): " + tx);
                    }
                }
            }

            // Check that our read and write sets do not intersect with write
            // sets of all active transactions.
            for (Iterator<IgniteInternalTx> iter = prepareQ.iterator(); iter.hasNext();) {
                IgniteInternalTx prepareTx = iter.next();

                if (prepareTx == tx)
                    // Skip yourself.
                    continue;

                // Optimistically remove completed transactions.
                if (prepareTx.done()) {
                    iter.remove();

                    if (log.isDebugEnabled())
                        log.debug("Removed finished transaction from active queue: " + prepareTx);

                    continue;
                }

                // Check if originating node left.
                if (cctx.discovery().node(prepareTx.nodeId()) == null) {
                    iter.remove();

                    rollbackTx(prepareTx);

                    if (log.isDebugEnabled())
                        log.debug("Removed and rolled back transaction because sender node left grid: " +
                            CU.txString(prepareTx));

                    continue;
                }

                if (tx.serializable() && !prepareTx.isRollbackOnly()) {
                    Set<IgniteTxKey> prepareWriteSet = prepareTx.writeSet();

                    if (GridFunc.intersects(prepareWriteSet, readSet, writeSet)) {
                        // Remove from active set.
                        iter.remove();

                        tx.setRollbackOnly();

                        throw new IgniteTxOptimisticCheckedException(
                            "Failed to prepare transaction (read-set/write-set conflict): " + tx);
                    }
                }
            }
        }

        // Optimistic.
        assert tx.optimistic() || !tx.local();

        if (!lockMultiple(tx, tx.optimisticLockEntries())) {
            tx.setRollbackOnly();

            throw new IgniteTxOptimisticCheckedException("Failed to prepare transaction (lock conflict): " + tx);
        }
    }

    /**
     * @param tx Transaction to check.
     * @return {@code True} if transaction can be discarded.
     */
    private boolean isSafeToForget(IgniteInternalTx tx) {
        Map.Entry<GridCacheVersion, AtomicInt> e = startVerCnts.firstEntry();

        if (e == null)
            return true;

        assert e.getValue().get() >= 0;

        return tx.endVersion().compareTo(e.getKey()) <= 0;
    }

    /**
     * Decrement start version count.
     *
     * @param tx Cache transaction.
     */
    private void decrementStartVersionCount(IgniteInternalTx tx) {
        AtomicInt cnt = startVerCnts.get(tx.startVersion());

        assert cnt != null : "Failed to find start version count for transaction [startVerCnts=" + startVerCnts +
            ", tx=" + tx + ']';

        assert cnt.get() > 0;

        if (cnt.decrementAndGet() == 0)
            if (startVerCnts.remove(tx.startVersion(), cnt))
                if (log.isDebugEnabled())
                    log.debug("Removed start version for transaction: " + tx);
    }

    /**
     * @param tx Transaction.
     */
    private void removeObsolete(IgniteInternalTx tx) {
        Collection<IgniteTxEntry> entries = tx.local() ? tx.allEntries() : tx.writeEntries();

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

                    if (e != null && e.markObsoleteIfEmpty(tx.xidVersion()))
                        near.removeEntry(e);
                }
            }
            catch (IgniteCheckedException e) {
                U.error(log, "Failed to remove obsolete entry from cache: " + cached, e);
            }
        }
    }

    /**
     * @param map Collection to copy.
     * @param expVal Values to copy.
     * @return Copy of the collection.
     */
    private Collection<GridCacheVersion> copyOf(Map<GridCacheVersion, Boolean> map, boolean expVal) {
        Collection<GridCacheVersion> l = new LinkedList<>();

        for (Map.Entry<GridCacheVersion, Boolean> e : map.entrySet()) {
            if (e.getValue() == expVal)
                l.add(e.getKey());
        }

        return l;
    }

    /**
     * Gets committed transactions starting from the given version (inclusive). // TODO: GG-4011: why inclusive?
     *
     * @param min Start (or minimum) version.
     * @return Committed transactions starting from the given version (non-inclusive).
     */
    public Collection<GridCacheVersion> committedVersions(GridCacheVersion min) {
        ConcurrentNavigableMap<GridCacheVersion, Boolean> tail
            = completedVers.tailMap(min, true);

        return F.isEmpty(tail) ? Collections.<GridCacheVersion>emptyList() : copyOf(tail, true);
    }

    /**
     * Gets rolledback transactions starting from the given version (inclusive). // TODO: GG-4011: why inclusive?
     *
     * @param min Start (or minimum) version.
     * @return Committed transactions starting from the given version (non-inclusive).
     */
    public Collection<GridCacheVersion> rolledbackVersions(GridCacheVersion min) {
        ConcurrentNavigableMap<GridCacheVersion, Boolean> tail
            = completedVers.tailMap(min, true);

        return F.isEmpty(tail) ? Collections.<GridCacheVersion>emptyList() : copyOf(tail, false);
    }

    /**
     * @param tx Tx to remove.
     */
    public void removeCommittedTx(IgniteInternalTx tx) {
        completedVers.remove(tx.xidVersion(), true);
    }

    /**
     * @param tx Committed transaction.
     * @return If transaction was not already present in committed set.
     */
    public boolean addCommittedTx(IgniteInternalTx tx) {
        boolean res = addCommittedTx(tx.xidVersion(), tx.nearXidVersion());

        if (!tx.local() && !tx.near() && tx.onePhaseCommit())
            addCommittedTx(tx.nearXidVersion(), null);

        return res;
    }

    /**
     * @param tx Committed transaction.
     * @return If transaction was not already present in committed set.
     */
    public boolean addRolledbackTx(IgniteInternalTx tx) {
        return addRolledbackTx(tx.xidVersion());
    }

    /**
     * @return Collection of active transactions.
     */
    public Collection<IgniteInternalTx> activeTransactions() {
        return F.concat(false, idMap.values(), nearIdMap.values());
    }

    /**
     * @param xidVer Completed transaction version.
     * @param nearXidVer Optional near transaction ID.
     * @return If transaction was not already present in completed set.
     */
    public boolean addCommittedTx(GridCacheVersion xidVer, @Nullable GridCacheVersion nearXidVer) {
        if (nearXidVer != null)
            xidVer = new CommittedVersion(xidVer, nearXidVer);

        Boolean committed = completedVers.putIfAbsent(xidVer, true);

        if (committed == null || committed) {
            if (log.isDebugEnabled())
                log.debug("Added transaction to committed version set: " + xidVer);

            return true;
        }
        else {
            if (log.isDebugEnabled())
                log.debug("Transaction is already present in rolled back version set: " + xidVer);

            return false;
        }
    }

    /**
     * @param xidVer Completed transaction version.
     * @return If transaction was not already present in completed set.
     */
    public boolean addRolledbackTx(GridCacheVersion xidVer) {
        Boolean committed = completedVers.putIfAbsent(xidVer, false);

        if (committed == null || !committed) {
            if (log.isDebugEnabled())
                log.debug("Added transaction to rolled back version set: " + xidVer);

            return true;
        }
        else {
            if (log.isDebugEnabled())
                log.debug("Transaction is already present in committed version set: " + xidVer);

            return false;
        }
    }

    /**
     * @param tx Transaction.
     */
    private void processCompletedEntries(IgniteInternalTx tx) {
        if (tx.needsCompletedVersions()) {
            GridCacheVersion min = minVersion(tx.readEntries(), tx.xidVersion(), tx);

            min = minVersion(tx.writeEntries(), min, tx);

            assert min != null;

            tx.completedVersions(min, committedVersions(min), rolledbackVersions(min));
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
     * Gets versions of all not acquired locks for collection of tx entries that are less then base version.
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
                // If check should be faster then exception handling.
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
     * therefore we can grab the min version so we can send all committed and rolled
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
     * Commits a transaction.
     *
     * @param tx Transaction to commit.
     */
    public void commitTx(IgniteInternalTx tx) {
        assert tx != null;
        assert tx.state() == COMMITTING : "Invalid transaction state for commit from tm [state=" + tx.state() +
            ", expected=COMMITTING, tx=" + tx + ']';

        if (log.isDebugEnabled())
            log.debug("Committing from TM [locNodeId=" + cctx.localNodeId() + ", tx=" + tx + ']');

        if (tx.timeout() > 0) {
            cctx.time().removeTimeoutObject(tx);

            if (log.isDebugEnabled())
                log.debug("Unregistered transaction with timeout processor: " + tx);
        }

        /*
         * Note that write phase is handled by transaction adapter itself,
         * so we don't do it here.
         */

        Boolean committed = completedVers.get(tx.xidVersion());

        // 1. Make sure that committed version has been recorded.
        if (!((committed != null && committed) || tx.writeSet().isEmpty() || tx.isSystemInvalidate())) {
            uncommitTx(tx);

            GridCacheVersion first = completedVers.isEmpty() ? null : completedVers.firstKey();
            GridCacheVersion last = completedVers.isEmpty() ? null : completedVers.lastKey();

            throw new IgniteException("Missing commit version (consider increasing " +
                IGNITE_MAX_COMPLETED_TX_COUNT + " system property) [ver=" + tx.xidVersion() + ", firstVer=" +
                first + ", lastVer=" + last + ", tx=" + tx.xid() + ']');
        }

        ConcurrentMap<GridCacheVersion, IgniteInternalTx> txIdMap = transactionMap(tx);

        if (txIdMap.remove(tx.xidVersion(), tx)) {
            // 2. Must process completed entries before unlocking!
            processCompletedEntries(tx);

            if (tx instanceof GridDhtTxLocal) {
                GridDhtTxLocal dhtTxLoc = (GridDhtTxLocal)tx;

                collectPendingVersions(dhtTxLoc);
            }

            // 4. Unlock write resources.
            unlockMultiple(tx, tx.writeEntries());

            // 5. For pessimistic transaction, unlock read resources if required.
            if (tx.pessimistic() && !tx.readCommitted())
                unlockMultiple(tx, tx.readEntries());

            // 6. Notify evictions.
            notifyEvitions(tx);

            // 7. Remove obsolete entries from cache.
            removeObsolete(tx);

            // 8. Assign transaction number at the end of transaction.
            tx.endVersion(cctx.versions().next(tx.topologyVersion()));

            // 9. Clean start transaction number for this transaction.
            if (cctx.txConfig().isTxSerializableEnabled())
                decrementStartVersionCount(tx);

            // 10. Add to committed queue only if it is possible
            //    that this transaction can affect other ones.
            if (cctx.txConfig().isTxSerializableEnabled() && tx.enforceSerializable() && !isSafeToForget(tx))
                committedQ.add(tx);

            // 11. Remove from per-thread storage.
            clearThreadMap(tx);

            // 12. Unregister explicit locks.
            if (!tx.alternateVersions().isEmpty()) {
                for (GridCacheVersion ver : tx.alternateVersions())
                    idMap.remove(ver);
            }

            // 13. Remove Near-2-DHT mappings.
            if (tx instanceof GridCacheMappedVersion) {
                GridCacheVersion mapped = ((GridCacheMappedVersion)tx).mappedVersion();

                if (mapped != null)
                    mappedVers.remove(mapped);
            }

            // 14. Clear context.
            resetContext();

            // 15. Update metrics.
            if (!tx.dht() && tx.local()) {
                if (!tx.system())
                    cctx.txMetrics().onTxCommit();

                for (int cacheId : tx.activeCacheIds()) {
                    GridCacheContext cacheCtx = cctx.cacheContext(cacheId);

                    if (cacheCtx.cache().configuration().isStatisticsEnabled())
                        // Convert start time from ms to ns.
                        cacheCtx.cache().metrics0().onTxCommit((U.currentTimeMillis() - tx.startTime()) * 1000);
                }
            }

            if (slowTxWarnTimeout > 0 && tx.local() &&
                U.currentTimeMillis() - tx.startTime() > slowTxWarnTimeout)
                U.warn(log, "Slow transaction detected [tx=" + tx +
                    ", slowTxWarnTimeout=" + slowTxWarnTimeout + ']') ;

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
     */
    public void rollbackTx(IgniteInternalTx tx) {
        assert tx != null;

        if (log.isDebugEnabled())
            log.debug("Rolling back from TM [locNodeId=" + cctx.localNodeId() + ", tx=" + tx + ']');

        // 1. Record transaction version to avoid duplicates.
        addRolledbackTx(tx);

        ConcurrentMap<GridCacheVersion, IgniteInternalTx> txIdMap = transactionMap(tx);

        if (txIdMap.remove(tx.xidVersion(), tx)) {
            // 2. Unlock write resources.
            unlockMultiple(tx, tx.writeEntries());

            // 3. For pessimistic transaction, unlock read resources if required.
            if (tx.pessimistic() && !tx.readCommitted())
                unlockMultiple(tx, tx.readEntries());

            // 4. Notify evictions.
            notifyEvitions(tx);

            // 5. Remove obsolete entries.
            removeObsolete(tx);

            // 6. Clean start transaction number for this transaction.
            if (cctx.txConfig().isTxSerializableEnabled())
                decrementStartVersionCount(tx);

            // 7. Remove from per-thread storage.
            clearThreadMap(tx);

            // 8. Unregister explicit locks.
            if (!tx.alternateVersions().isEmpty())
                for (GridCacheVersion ver : tx.alternateVersions())
                    idMap.remove(ver);

            // 9. Remove Near-2-DHT mappings.
            if (tx instanceof GridCacheMappedVersion)
                mappedVers.remove(((GridCacheMappedVersion)tx).mappedVersion());

            // 10. Clear context.
            resetContext();

            // 11. Update metrics.
            if (!tx.dht() && tx.local()) {
                if (!tx.system())
                    cctx.txMetrics().onTxRollback();

                for (int cacheId : tx.activeCacheIds()) {
                    GridCacheContext cacheCtx = cctx.cacheContext(cacheId);

                    if (cacheCtx.cache().configuration().isStatisticsEnabled())
                        // Convert start time from ms to ns.
                        cacheCtx.cache().metrics0().onTxRollback((U.currentTimeMillis() - tx.startTime()) * 1000);
                }
            }

            if (log.isDebugEnabled())
                log.debug("Rolled back from TM: " + tx);
        }
        else if (log.isDebugEnabled())
            log.debug("Did not rollback from TM (was already rolled back): " + tx);
    }

    /**
     * Tries to minimize damage from partially-committed transaction.
     *
     * @param tx Tx to uncommit.
     */
    public void uncommitTx(IgniteInternalTx tx) {
        assert tx != null;

        if (log.isDebugEnabled())
            log.debug("Uncommiting from TM: " + tx);

        ConcurrentMap<GridCacheVersion, IgniteInternalTx> txIdMap = transactionMap(tx);

        if (txIdMap.remove(tx.xidVersion(), tx)) {
            // 1. Unlock write resources.
            unlockMultiple(tx, tx.writeEntries());

            // 2. For pessimistic transaction, unlock read resources if required.
            if (tx.pessimistic() && !tx.readCommitted())
                unlockMultiple(tx, tx.readEntries());

            // 3. Notify evictions.
            notifyEvitions(tx);

            // 4. Clean start transaction number for this transaction.
            if (cctx.txConfig().isTxSerializableEnabled())
                decrementStartVersionCount(tx);

            // 5. Remove from per-thread storage.
            clearThreadMap(tx);

            // 6. Unregister explicit locks.
            if (!tx.alternateVersions().isEmpty())
                for (GridCacheVersion ver : tx.alternateVersions())
                    idMap.remove(ver);

            // 7. Remove Near-2-DHT mappings.
            if (tx instanceof GridCacheMappedVersion)
                mappedVers.remove(((GridCacheMappedVersion)tx).mappedVersion());

            // 8. Clear context.
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
    private void clearThreadMap(IgniteInternalTx tx) {
        if (tx.local() && !tx.dht()) {
            if (!tx.system())
                threadMap.remove(tx.threadId(), tx);
            else {
                Integer cacheId = F.first(tx.activeCacheIds());

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
    private void notifyEvitions(IgniteInternalTx tx) {
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
            IgniteTxAdapter tx = tx(owner.version());

            if (tx == null)
                tx = nearTx(owner.version());

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
     * Callback called by near finish future before sending near finish request to remote node. Will increment
     * per-thread counter so that further awaitAck call will wait for finish response.
     *
     * @param rmtNodeId Remote node ID for which finish request is being sent.
     * @param threadId Near tx thread ID.
     */
    public void beforeFinishRemote(UUID rmtNodeId, long threadId) {
        if (finishSyncDisabled)
            return;

        assert txFinishSync != null;

        txFinishSync.onFinishSend(rmtNodeId, threadId);
    }

    /**
     * Callback invoked when near finish response is received from remote node.
     *
     * @param rmtNodeId Remote node ID from which response is received.
     * @param threadId Near tx thread ID.
     */
    public void onFinishedRemote(UUID rmtNodeId, long threadId) {
        if (finishSyncDisabled)
            return;

        assert txFinishSync != null;

        txFinishSync.onAckReceived(rmtNodeId, threadId);
    }

    /**
     * Asynchronously waits for last finish request ack.
     *
     * @param rmtNodeId Remote node ID.
     * @param threadId Near tx thread ID.
     * @return {@code null} if ack was received or future that will be completed when ack is received.
     */
    @Nullable public IgniteInternalFuture<?> awaitFinishAckAsync(UUID rmtNodeId, long threadId) {
        if (finishSyncDisabled)
            return null;

        assert txFinishSync != null;

        return txFinishSync.awaitAckAsync(rmtNodeId, threadId);
    }

    /**
     * For test purposes only.
     *
     * @param finishSyncDisabled {@code True} if finish sync should be disabled.
     */
    public void finishSyncDisabled(boolean finishSyncDisabled) {
        this.finishSyncDisabled = finishSyncDisabled;
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

        long remainingTime = U.currentTimeMillis() - (tx.startTime() + tx.timeout());

        // For serializable transactions, failure to acquire lock means
        // that there is a serializable conflict. For all other isolation levels,
        // we wait for the lock.
        long timeout = tx.timeout() == 0 ? 0 : remainingTime;

        for (IgniteTxEntry txEntry1 : entries) {
            // Check if this entry was prepared before.
            if (!txEntry1.markPrepared() || txEntry1.explicitVersion() != null)
                continue;

            GridCacheContext cacheCtx = txEntry1.context();

            while (true) {
                try {
                    GridCacheEntryEx entry1 = txEntry1.cached();

                    assert !entry1.detached() : "Expected non-detached entry for near transaction " +
                        "[locNodeId=" + cctx.localNodeId() + ", entry=" + entry1 + ']';

                    if (!entry1.tmLock(tx, timeout)) {
                        // Unlock locks locked so far.
                        for (IgniteTxEntry txEntry2 : entries) {
                            if (txEntry2 == txEntry1)
                                break;

                            txEntry2.cached().txUnlock(tx);
                        }

                        return false;
                    }

                    entry1.unswap();

                    break;
                }
                catch (GridCacheEntryRemovedException ignored) {
                    if (log.isDebugEnabled())
                        log.debug("Got removed entry in TM lockMultiple(..) method (will retry): " + txEntry1);

                    try {
                        // Renew cache entry.
                        txEntry1.cached(cacheCtx.cache().entryEx(txEntry1.key()));
                    }
                    catch (GridDhtInvalidPartitionException e) {
                        assert tx.dht() : "Received invalid partition for non DHT transaction [tx=" +
                            tx + ", invalidPart=" + e.partition() + ']';

                        // If partition is invalid, we ignore this entry.
                        tx.addInvalidPartition(cacheCtx, e.partition());

                        break;
                    }
                }
                catch (GridDistributedLockCancelledException ignore) {
                    tx.setRollbackOnly();

                    throw new IgniteCheckedException("Entry lock has been cancelled for transaction: " + tx);
                }
            }
        }

        return true;
    }

    /**
     * @param tx Owning transaction.
     * @param entries Entries to unlock.
     */
    private void unlockMultiple(IgniteInternalTx tx, Iterable<IgniteTxEntry> entries) {
        for (IgniteTxEntry txEntry : entries) {
            GridCacheContext cacheCtx = txEntry.context();

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
                        log.debug("Got removed entry in TM unlockMultiple(..) method (will retry): " + txEntry);

                    // Renew cache entry.
                    txEntry.cached(cacheCtx.cache().entryEx(txEntry.key()));
                }
            }
        }
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
    @SuppressWarnings({"unchecked"})
    private IgniteInternalTx txContext() {
        return threadCtx.get();
    }

    /**
     * Gets version of transaction in tx context or {@code null}
     * if tx context is empty.
     * <p>
     * This is a convenience method provided mostly for debugging.
     *
     * @return Transaction version from transaction context.
     */
    @Nullable public GridCacheVersion txContextVersion() {
        IgniteInternalTx tx = txContext();

        return tx == null ? null : tx.xidVersion();
    }

    /**
     * Commit ended.
     */
    public void resetContext() {
        threadCtx.set(null);
    }

    /**
     * @return All transactions.
     */
    public Collection<IgniteInternalTx> txs() {
        return F.concat(false, idMap.values(), nearIdMap.values());
    }

    /**
     * @return Slow tx warn timeout.
     */
    public int slowTxWarnTimeout() {
        return slowTxWarnTimeout;
    }

    /**
     * @param slowTxWarnTimeout Slow tx warn timeout.
     */
    public void slowTxWarnTimeout(int slowTxWarnTimeout) {
        this.slowTxWarnTimeout = slowTxWarnTimeout;
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

            tx.finishFuture().listen(new CI1<IgniteInternalFuture<IgniteInternalTx>>() {
                @Override public void apply(IgniteInternalFuture<IgniteInternalTx> fut) {
                    TransactionState state = tx.state();

                    if (log.isDebugEnabled())
                        log.debug("Near transaction finished with state: " + state);

                    resFut.onDone(state == COMMITTED);
                }
            });

            return resFut;
        }

        Boolean committed = null;

        for (Map.Entry<GridCacheVersion, Boolean> entry : completedVers.entrySet()) {
            if (entry.getValue() == null)
                continue;

            if (entry.getKey() instanceof CommittedVersion) {
                CommittedVersion comm = (CommittedVersion)entry.getKey();

                if (comm.nearVer.equals(xidVer)) {
                    committed = entry.getValue();

                    break;
                }
            }
        }

        if (log.isDebugEnabled())
            log.debug("Near transaction committed: " + committed);

        resFut.onDone(committed != null && committed);

        return resFut;
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
        @Nullable Collection<GridCacheVersion> processedVers)
    {
        for (final IgniteInternalTx tx : txs()) {
            if (nearVer.equals(tx.nearXidVersion())) {
                TransactionState state = tx.state();

                IgniteInternalFuture<?> prepFut = tx.currentPrepareFuture();

                if (prepFut != null && !prepFut.isDone()) {
                    if (log.isDebugEnabled())
                        log.debug("Transaction is preparing (will wait): " + tx);

                    final GridFutureAdapter<Boolean> fut0 = fut != null ? fut : new GridFutureAdapter<Boolean>();

                    final int txNum0 = txNum;

                    final Collection<GridCacheVersion> processedVers0 = processedVers;

                    prepFut.listen(new CI1<IgniteInternalFuture<?>>() {
                        @Override public void apply(IgniteInternalFuture<?> prepFut) {
                            if (log.isDebugEnabled())
                                log.debug("Transaction prepare future finished: " + tx);

                            IgniteInternalFuture<Boolean> fut = txsPreparedOrCommitted(nearVer,
                                txNum0,
                                fut0,
                                processedVers0);

                            assert fut == fut0;
                        }
                    });

                    return fut0;
                }

                if (state == PREPARED || state == COMMITTING || state == COMMITTED) {
                    if (--txNum == 0) {
                        if (fut != null)
                            fut.onDone(true);

                        return fut;
                    }
                }
                else {
                    if (tx.state(MARKED_ROLLBACK) || tx.state() == UNKNOWN) {
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
                    processedVers = new HashSet<>(txNum, 1.0f);

                processedVers.add(tx.xidVersion());
            }
        }

        // Not all transactions were found. Need to scan committed versions to check
        // if transaction was already committed.
        for (Map.Entry<GridCacheVersion, Boolean> e : completedVers.entrySet()) {
            if (!e.getValue())
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
        if (log.isDebugEnabled())
            log.debug("Finishing prepared transaction [tx=" + tx + ", commit=" + commit + ']');

        if (!tx.markFinalizing(RECOVERY_FINISH)) {
            if (log.isDebugEnabled())
                log.debug("Will not try to commit prepared transaction (could not mark finalized): " + tx);

            return;
        }

        if (tx instanceof GridDistributedTxRemoteAdapter) {
            IgniteTxRemoteEx rmtTx = (IgniteTxRemoteEx)tx;

            rmtTx.doneRemote(tx.xidVersion(), Collections.<GridCacheVersion>emptyList(), Collections.<GridCacheVersion>emptyList(),
                Collections.<GridCacheVersion>emptyList());
        }

        if (commit)
            tx.commitAsync().listen(new CommitListener(tx));
        else
            tx.rollbackAsync();
    }

    /**
     * Commits transaction in case when node started transaction failed, but all related
     * transactions were prepared (invalidates transaction if it is not fully prepared).
     *
     * @param tx Transaction.
     */
    public void commitIfPrepared(IgniteInternalTx tx) {
        assert tx instanceof GridDhtTxLocal || tx instanceof GridDhtTxRemote  : tx;
        assert !F.isEmpty(tx.transactionNodes()) : tx;
        assert tx.nearXidVersion() != null : tx;

        GridCacheTxRecoveryFuture fut = new GridCacheTxRecoveryFuture(
            cctx,
            tx,
            tx.originatingNodeId(),
            tx.transactionNodes());

        cctx.mvcc().addFuture(fut);

        if (log.isDebugEnabled())
            log.debug("Checking optimistic transaction state on remote nodes [tx=" + tx + ", fut=" + fut + ']');

        fut.prepare();
    }

    /**
     * Timeout object for node failure handler.
     */
    private final class NodeFailureTimeoutObject extends GridTimeoutObjectAdapter {
        /** Left or failed node. */
        private final UUID evtNodeId;

        /**
         * @param evtNodeId Event node ID.
         */
        private NodeFailureTimeoutObject(UUID evtNodeId) {
            super(IgniteUuid.fromUuid(cctx.localNodeId()), TX_SALVAGE_TIMEOUT);

            this.evtNodeId = evtNodeId;
        }

        /** {@inheritDoc} */
        @Override public void onTimeout() {
            try {
                cctx.kernalContext().gateway().readLock();
            }
            catch (IllegalStateException | IgniteClientDisconnectedException e) {
                if (log.isDebugEnabled())
                    log.debug("Failed to acquire kernal gateway [err=" + e + ']');

                return;
            }

            try {
                if (log.isDebugEnabled())
                    log.debug("Processing node failed event [locNodeId=" + cctx.localNodeId() +
                        ", failedNodeId=" + evtNodeId + ']');

                for (final IgniteInternalTx tx : txs()) {
                    if ((tx.near() && !tx.local()) || (tx.storeUsed() && tx.masterNodeIds().contains(evtNodeId))) {
                        // Invalidate transactions.
                        salvageTx(tx, false, RECOVERY_FINISH);
                    }
                    else {
                        // Check prepare only if originating node ID failed. Otherwise parent node will finish this tx.
                        if (tx.originatingNodeId().equals(evtNodeId)) {
                            if (tx.state() == PREPARED)
                                commitIfPrepared(tx);
                            else {
                                IgniteInternalFuture<?> prepFut = tx.currentPrepareFuture();

                                if (prepFut != null) {
                                    prepFut.listen(new CI1<IgniteInternalFuture<?>>() {
                                        @Override public void apply(IgniteInternalFuture<?> fut) {
                                            if (tx.state() == PREPARED)
                                                commitIfPrepared(tx);
                                            else if (tx.setRollbackOnly())
                                                tx.rollbackAsync();
                                        }
                                    });
                                }
                                else {
                                    // If we could not mark tx as rollback, it means that transaction is being committed.
                                    if (tx.setRollbackOnly())
                                        tx.rollbackAsync();
                                }
                            }
                        }
                    }
                }
            }
            finally {
                cctx.kernalContext().gateway().readUnlock();
            }
        }
    }

    /**
     * Per-thread key for system transactions.
     */
    private static class TxThreadKey {
        /** Thread ID. */
        private long threadId;

        /** Cache ID. */
        private int cacheId;

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
            int result = (int)(threadId ^ (threadId >>> 32));

            result = 31 * result + cacheId;

            return result;
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
            super(ver.topologyVersion(), ver.globalTime(), ver.order(), ver.nodeOrder(), ver.dataCenterId());

            assert nearVer != null;

            this.nearVer = nearVer;
        }
    }

    /**
     * Atomic integer that compares only using references, not values.
     */
    private static final class AtomicInt extends AtomicInteger {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * @param initVal Initial value.
         */
        private AtomicInt(int initVal) {
            super(initVal);
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object obj) {
            // Reference only.
            return obj == this;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return super.hashCode();
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

                tx.rollbackAsync();
            }
            catch (IgniteCheckedException e) {
                U.error(log, "Failed to commit transaction during failover: " + tx, e);
            }
        }
    }
}

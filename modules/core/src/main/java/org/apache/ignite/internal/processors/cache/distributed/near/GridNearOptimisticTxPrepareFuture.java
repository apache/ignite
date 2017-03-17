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

package org.apache.ignite.internal.processors.cache.distributed.near;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterTopologyException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.GridCacheEntryRemovedException;
import org.apache.ignite.internal.processors.cache.GridCacheMvccCandidate;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.distributed.GridDistributedTxMapping;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxMapping;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxEntry;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxKey;
import org.apache.ignite.internal.processors.cache.transactions.TxDeadlock;
import org.apache.ignite.internal.transactions.IgniteTxRollbackCheckedException;
import org.apache.ignite.internal.transactions.IgniteTxTimeoutCheckedException;
import org.apache.ignite.internal.util.future.GridEmbeddedFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.C1;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.P1;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiClosure;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.transactions.TransactionDeadlockException;
import org.apache.ignite.transactions.TransactionTimeoutException;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.GridCacheOperation.TRANSFORM;
import static org.apache.ignite.transactions.TransactionState.PREPARED;
import static org.apache.ignite.transactions.TransactionState.PREPARING;

/**
 *
 */
public class GridNearOptimisticTxPrepareFuture extends GridNearOptimisticTxPrepareFutureAdapter {
    /** */
    @GridToStringExclude
    private KeyLockFuture keyLockFut;

    /**
     * @param cctx Context.
     * @param tx Transaction.
     */
    public GridNearOptimisticTxPrepareFuture(GridCacheSharedContext cctx, GridNearTxLocal tx) {
        super(cctx, tx);

        assert tx.optimistic() && !tx.serializable() : tx;
    }

    /** {@inheritDoc} */
    @Override public boolean onOwnerChanged(GridCacheEntryEx entry, GridCacheMvccCandidate owner) {
        if (log.isDebugEnabled())
            log.debug("Transaction future received owner changed callback: " + entry);

        if (tx.remainingTime() == -1)
            return false;

        if ((entry.context().isNear() || entry.context().isLocal()) &&
            owner != null && tx.hasWriteKey(entry.txKey())) {
            if (keyLockFut != null)
                keyLockFut.onKeyLocked(entry.txKey());

            return true;
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean onNodeLeft(UUID nodeId) {
        boolean found = false;

        for (IgniteInternalFuture<?> fut : futures()) {
            if (isMini(fut)) {
                MiniFuture f = (MiniFuture) fut;

                if (f.node().id().equals(nodeId)) {
                    ClusterTopologyCheckedException e = new ClusterTopologyCheckedException("Remote node left grid: " +
                        nodeId);

                    e.retryReadyFuture(cctx.nextAffinityReadyFuture(tx.topologyVersion()));

                    f.onNodeLeft(e, true);

                    found = true;
                }
            }
        }

        return found;
    }

    /**
     * @param e Error.
     * @param discoThread {@code True} if executed from discovery thread.
     */
    void onError(Throwable e, boolean discoThread) {
        if (e instanceof IgniteTxTimeoutCheckedException) {
            onTimeout();

            return;
        }

        if (X.hasCause(e, ClusterTopologyCheckedException.class) || X.hasCause(e, ClusterTopologyException.class)) {
            if (tx.onePhaseCommit()) {
                tx.markForBackupCheck();

                onComplete();

                return;
            }
        }

        if (ERR_UPD.compareAndSet(this, null, e)) {
            boolean marked = tx.setRollbackOnly();

            if (e instanceof IgniteTxRollbackCheckedException) {
                if (marked) {
                    try {
                        tx.rollback();
                    }
                    catch (IgniteCheckedException ex) {
                        U.error(log, "Failed to automatically rollback transaction: " + tx, ex);
                    }
                }
            }

            onComplete();
        }
    }

    /** {@inheritDoc} */
    @Override public void onResult(UUID nodeId, GridNearTxPrepareResponse res) {
        if (!isDone()) {
            MiniFuture mini = miniFuture(res.miniId());

            if (mini != null) {
                assert mini.node().id().equals(nodeId);

                mini.onResult(res);
            }
            else {
                if (msgLog.isDebugEnabled()) {
                    msgLog.debug("Near optimistic prepare fut, failed to find mini future [txId=" + tx.nearXidVersion() +
                        ", node=" + nodeId +
                        ", res=" + res +
                        ", fut=" + this + ']');
                }
            }
        }
        else {
            if (msgLog.isDebugEnabled()) {
                msgLog.debug("Near optimistic prepare fut, response for finished future [txId=" + tx.nearXidVersion() +
                    ", node=" + nodeId +
                    ", res=" + res +
                    ", fut=" + this + ']');
            }
        }
    }

    /**
     * @return Keys for which MiniFuture isn't completed.
     */
    @SuppressWarnings("ForLoopReplaceableByForEach")
    public Set<IgniteTxKey> requestedKeys() {
        synchronized (sync) {
            int size = futuresCountNoLock();

            for (int i = 0; i < size; i++) {
                IgniteInternalFuture<GridNearTxPrepareResponse> fut = future(i);

                if (isMini(fut) && !fut.isDone()) {
                    MiniFuture miniFut = (MiniFuture)fut;

                    Collection<IgniteTxEntry> entries = miniFut.mapping().entries();

                    Set<IgniteTxKey> keys = U.newHashSet(entries.size());

                    for (IgniteTxEntry entry : entries)
                        keys.add(entry.txKey());

                    return keys;
                }
            }
        }

        return null;
    }

    /**
     * Finds pending mini future by the given mini ID.
     *
     * @param miniId Mini ID to find.
     * @return Mini future.
     */
    @SuppressWarnings("ForLoopReplaceableByForEach")
    private MiniFuture miniFuture(IgniteUuid miniId) {
        // We iterate directly over the futs collection here to avoid copy.
        synchronized (sync) {
            int size = futuresCountNoLock();

            // Avoid iterator creation.
            for (int i = size - 1; i >= 0; i--) {
                IgniteInternalFuture<GridNearTxPrepareResponse> fut = future(i);

                if (!isMini(fut))
                    continue;

                MiniFuture mini = (MiniFuture)fut;

                if (mini.futureId().equals(miniId)) {
                    if (!mini.isDone())
                        return mini;
                    else
                        return null;
                }
            }
        }

        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean onDone(IgniteInternalTx t, Throwable err) {
        if (isDone())
            return false;

        ERR_UPD.compareAndSet(this, null, err);

        return onComplete();
    }

    /**
     * @param f Future.
     * @return {@code True} if mini-future.
     */
    private boolean isMini(IgniteInternalFuture<?> f) {
        return f.getClass().equals(MiniFuture.class);
    }

    /**
     * Completeness callback.
     *
     * @return {@code True} if future was finished by this call.
     */
    private boolean onComplete() {
        Throwable err0 = err;

        if (err0 == null || tx.needCheckBackup())
            tx.state(PREPARED);

        if (super.onDone(tx, err0)) {
            // Don't forget to clean up.
            cctx.mvcc().removeMvccFuture(this);

            return true;
        }

        return false;
    }

    /**
     * Initializes future.
     *
     * @param remap Remap flag.
     * @param topLocked {@code True} if thread already acquired lock preventing topology change.
     */
    @Override protected void prepare0(boolean remap, boolean topLocked) {
        try {
            boolean txStateCheck = remap ? tx.state() == PREPARING : tx.state(PREPARING);

            if (!txStateCheck) {
                if (tx.setRollbackOnly()) {
                    if (tx.remainingTime() == -1)
                        onError(new IgniteTxTimeoutCheckedException("Transaction timed out and " +
                            "was rolled back: " + this), false);
                    else
                        onError(new IgniteCheckedException("Invalid transaction state for prepare " +
                            "[state=" + tx.state() + ", tx=" + this + ']'), false);
                }
                else
                    onError(new IgniteTxRollbackCheckedException("Invalid transaction state for " +
                        "prepare [state=" + tx.state() + ", tx=" + this + ']'), false);

                return;
            }

            IgniteTxEntry singleWrite = tx.singleWrite();

            if (singleWrite != null)
                prepareSingle(singleWrite, topLocked, remap);
            else
                prepare(tx.writeEntries(), topLocked, remap);

            markInitialized();
        }
        catch (TransactionTimeoutException e) {
            onError(e, false);
        }
    }

    /**
     * @param write Write.
     * @param topLocked {@code True} if thread already acquired lock preventing topology change.
     */
    private void prepareSingle(IgniteTxEntry write, boolean topLocked, boolean remap) {
        write.clearEntryReadVersion();

        AffinityTopologyVersion topVer = tx.topologyVersion();

        assert topVer.topologyVersion() > 0;

        txMapping = new GridDhtTxMapping();

        GridDistributedTxMapping mapping = map(write, topVer, null, topLocked, remap);

        if (mapping.node().isLocal()) {
            if (write.context().isNear())
                tx.nearLocallyMapped(true);
            else if (write.context().isColocated())
                tx.colocatedLocallyMapped(true);
        }

        if (isDone()) {
            if (log.isDebugEnabled())
                log.debug("Abandoning (re)map because future is done: " + this);

            return;
        }

        if (keyLockFut != null)
            keyLockFut.onAllKeysAdded();

        tx.addSingleEntryMapping(mapping, write);

        cctx.mvcc().recheckPendingLocks();

        mapping.last(true);

        tx.transactionNodes(txMapping.transactionNodes());

        checkOnePhase();

        proceedPrepare(mapping, null);
    }

    /**
     * @param writes Write entries.
     * @param topLocked {@code True} if thread already acquired lock preventing topology change.
     */
    private void prepare(
        Iterable<IgniteTxEntry> writes,
        boolean topLocked,
        boolean remap
    ) {
        AffinityTopologyVersion topVer = tx.topologyVersion();

        assert topVer.topologyVersion() > 0;

        txMapping = new GridDhtTxMapping();

        Map<UUID, GridDistributedTxMapping> map = new HashMap<>();

        // Assign keys to primary nodes.
        GridDistributedTxMapping cur = null;

        Queue<GridDistributedTxMapping> mappings = new ArrayDeque<>();

        for (IgniteTxEntry write : writes) {
            write.clearEntryReadVersion();

            GridDistributedTxMapping updated = map(write, topVer, cur, topLocked, remap);

            if (cur != updated) {
                mappings.offer(updated);

                updated.last(true);

                GridDistributedTxMapping prev = map.put(updated.node().id(), updated);

                if (prev != null)
                    prev.last(false);

                if (updated.node().isLocal()) {
                    if (write.context().isNear())
                        tx.nearLocallyMapped(true);
                    else if (write.context().isColocated())
                        tx.colocatedLocallyMapped(true);
                }

                cur = updated;
            }
        }

        if (isDone()) {
            if (log.isDebugEnabled())
                log.debug("Abandoning (re)map because future is done: " + this);

            return;
        }

        if (keyLockFut != null)
            keyLockFut.onAllKeysAdded();

        tx.addEntryMapping(mappings);

        cctx.mvcc().recheckPendingLocks();

        tx.transactionNodes(txMapping.transactionNodes());

        checkOnePhase();

        proceedPrepare(mappings);
    }

    /**
     * Continues prepare after previous mapping successfully finished.
     *
     * @param mappings Queue of mappings.
     */
    private void proceedPrepare(final Queue<GridDistributedTxMapping> mappings) {
        final GridDistributedTxMapping m = mappings.poll();

        if (m == null)
            return;

        proceedPrepare(m, mappings);
    }

    /**
     * Continues prepare after previous mapping successfully finished.
     *
     * @param m Mapping.
     * @param mappings Queue of mappings.
     */
    private void proceedPrepare(GridDistributedTxMapping m, @Nullable final Queue<GridDistributedTxMapping> mappings) {
        if (isDone())
            return;

        boolean set = cctx.tm().setTxTopologyHint(tx.topologyVersionSnapshot());

        try {
            assert !m.empty();

            final ClusterNode n = m.node();

            long timeout = tx.remainingTime();

            if (timeout != -1) {
                GridNearTxPrepareRequest req = new GridNearTxPrepareRequest(
                    futId,
                    tx.topologyVersion(),
                    tx,
                    timeout,
                    null,
                    m.writes(),
                    m.near(),
                    txMapping.transactionNodes(),
                    m.last(),
                    tx.onePhaseCommit(),
                    tx.needReturnValue() && tx.implicit(),
                    tx.implicitSingle(),
                    m.explicitLock(),
                    tx.subjectId(),
                    tx.taskNameHash(),
                    m.clientFirst(),
                    tx.activeCachesDeploymentEnabled());

                for (IgniteTxEntry txEntry : m.entries()) {
                    if (txEntry.op() == TRANSFORM)
                        req.addDhtVersion(txEntry.txKey(), null);
                }

                // Must lock near entries separately.
                if (m.near()) {
                    try {
                        tx.optimisticLockEntries(req.writes());

                        tx.userPrepare();
                    }
                    catch (IgniteCheckedException e) {
                        onError(e, false);
                    }
                }

                final MiniFuture fut = new MiniFuture(this, m, mappings);

                req.miniId(fut.futureId());

                add(fut); // Append new future.

                // If this is the primary node for the keys.
                if (n.isLocal()) {
                    // At this point, if any new node joined, then it is
                    // waiting for this transaction to complete, so
                    // partition reassignments are not possible here.
                    IgniteInternalFuture<GridNearTxPrepareResponse> prepFut =
                        cctx.tm().txHandler().prepareTx(n.id(), tx, req);

                    prepFut.listen(new CI1<IgniteInternalFuture<GridNearTxPrepareResponse>>() {
                        @Override public void apply(IgniteInternalFuture<GridNearTxPrepareResponse> prepFut) {
                            try {
                                fut.onResult(prepFut.get());
                            }
                            catch (IgniteCheckedException e) {
                                fut.onResult(e);
                            }
                        }
                    });
                }
                else {
                    try {
                        cctx.io().send(n, req, tx.ioPolicy());

                        if (msgLog.isDebugEnabled()) {
                            msgLog.debug("Near optimistic prepare fut, sent request [txId=" + tx.nearXidVersion() +
                                ", node=" + n.id() + ']');
                        }
                    }
                    catch (ClusterTopologyCheckedException e) {
                        e.retryReadyFuture(cctx.nextAffinityReadyFuture(tx.topologyVersion()));

                        fut.onNodeLeft(e, false);
                    }
                    catch (IgniteCheckedException e) {
                        if (msgLog.isDebugEnabled()) {
                            msgLog.debug("Near optimistic prepare fut, failed to sent request [txId=" + tx.nearXidVersion() +
                                ", node=" + n.id() +
                                ", err=" + e + ']');
                        }

                        fut.onResult(e);
                    }
                }
            }
            else
                onTimeout();
        }
        finally {
            if (set)
                cctx.tm().setTxTopologyHint(null);
        }
    }

    /**
     * @param entry Transaction entry.
     * @param topVer Topology version.
     * @param cur Current mapping.
     * @param topLocked {@code True} if thread already acquired lock preventing topology change.
     * @return Mapping.
     */
    private GridDistributedTxMapping map(
        IgniteTxEntry entry,
        AffinityTopologyVersion topVer,
        @Nullable GridDistributedTxMapping cur,
        boolean topLocked,
        boolean remap
    ) {
        GridCacheContext cacheCtx = entry.context();

        List<ClusterNode> nodes;

        GridCacheEntryEx cached0 = entry.cached();

        if (cached0.isDht())
            nodes = cacheCtx.topology().nodes(cached0.partition(), topVer);
        else
            nodes = cacheCtx.isLocal() ?
                cacheCtx.affinity().nodesByKey(entry.key(), topVer) :
                cacheCtx.topology().nodes(cacheCtx.affinity().partition(entry.key()), topVer);

        txMapping.addMapping(nodes);

        ClusterNode primary = F.first(nodes);

        assert primary != null;

        if (log.isDebugEnabled()) {
            log.debug("Mapped key to primary node [key=" + entry.key() +
                ", part=" + cacheCtx.affinity().partition(entry.key()) +
                ", primary=" + U.toShortString(primary) + ", topVer=" + topVer + ']');
        }

        // Must re-initialize cached entry while holding topology lock.
        if (cacheCtx.isNear())
            entry.cached(cacheCtx.nearTx().entryExx(entry.key(), topVer));
        else if (!cacheCtx.isLocal())
            entry.cached(cacheCtx.colocated().entryExx(entry.key(), topVer, true));
        else
            entry.cached(cacheCtx.local().entryEx(entry.key(), topVer));

        if (cacheCtx.isNear() || cacheCtx.isLocal()) {
            if (entry.explicitVersion() == null && !remap) {
                if (keyLockFut == null) {
                    keyLockFut = new KeyLockFuture();

                    add(keyLockFut);
                }

                keyLockFut.addLockKey(entry.txKey());
            }
        }

        if (cur == null || !cur.node().id().equals(primary.id()) || cur.near() != cacheCtx.isNear()) {
            boolean clientFirst = cur == null && !topLocked && cctx.kernalContext().clientNode();

            cur = new GridDistributedTxMapping(primary);

            // Initialize near flag right away.
            cur.near(cacheCtx.isNear());

            cur.clientFirst(clientFirst);
        }

        cur.add(entry);

        if (entry.explicitVersion() != null) {
            tx.markExplicit(primary.id());

            cur.markExplicitLock();
        }

        entry.nodeId(primary.id());

        if (cacheCtx.isNear()) {
            while (true) {
                try {
                    GridNearCacheEntry cached = (GridNearCacheEntry)entry.cached();

                    cached.dhtNodeId(tx.xidVersion(), primary.id());

                    break;
                }
                catch (GridCacheEntryRemovedException ignore) {
                    entry.cached(cacheCtx.near().entryEx(entry.key(), topVer));
                }
            }
        }

        return cur;
    }

    /**
     *
     */
    @SuppressWarnings("ForLoopReplaceableByForEach")
    private void onTimeout() {
        if (cctx.tm().deadlockDetectionEnabled()) {
            Set<IgniteTxKey> keys = null;

            if (keyLockFut != null)
                keys = new HashSet<>(keyLockFut.lockKeys);
            else {
                synchronized (sync) {
                    int size = futuresCountNoLock();

                    for (int i = 0; i < size; i++) {
                        IgniteInternalFuture<GridNearTxPrepareResponse> fut = future(i);

                        if (isMini(fut) && !fut.isDone()) {
                            MiniFuture miniFut = (MiniFuture)fut;

                            Collection<IgniteTxEntry> entries = miniFut.mapping().entries();

                            keys = U.newHashSet(entries.size());

                            for (IgniteTxEntry entry : entries)
                                keys.add(entry.txKey());

                            break;
                        }
                    }
                }
            }

            add(new GridEmbeddedFuture<>(new IgniteBiClosure<TxDeadlock, Exception, GridNearTxPrepareResponse>() {
                @Override public GridNearTxPrepareResponse apply(TxDeadlock deadlock, Exception e) {
                    if (e != null)
                        U.warn(log, "Failed to detect deadlock.", e);
                    else {
                        e = new IgniteTxTimeoutCheckedException("Failed to acquire lock within provided timeout for " +
                            "transaction [timeout=" + tx.timeout() + ", tx=" + tx + ']',
                            deadlock != null ? new TransactionDeadlockException(deadlock.toString(cctx)) : null);
                    }

                    onDone(null, e);

                    return null;
                }
            }, cctx.tm().detectDeadlock(tx, keys)));
        }
        else {
            ERR_UPD.compareAndSet(this, null, new IgniteTxTimeoutCheckedException("Failed to acquire lock " +
                "within provided timeout for transaction [timeout=" + tx.timeout() + ", tx=" + tx + ']'));

            onComplete();
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        Collection<String> futs = F.viewReadOnly(futures(), new C1<IgniteInternalFuture<?>, String>() {
            @Override public String apply(IgniteInternalFuture<?> f) {
                return "[node=" + ((MiniFuture)f).node().id() +
                    ", loc=" + ((MiniFuture)f).node().isLocal() +
                    ", done=" + f.isDone() + "]";
            }
        }, new P1<IgniteInternalFuture<GridNearTxPrepareResponse>>() {
            @Override public boolean apply(IgniteInternalFuture<GridNearTxPrepareResponse> fut) {
                return isMini(fut);
            }
        });

        return S.toString(GridNearOptimisticTxPrepareFuture.class, this,
            "innerFuts", futs,
            "keyLockFut", keyLockFut,
            "tx", tx,
            "super", super.toString());
    }

    /**
     *
     */
    private static class MiniFuture extends GridFutureAdapter<GridNearTxPrepareResponse> {
        /** */
        private static final long serialVersionUID = 0L;

        /** Receive result flag updater. */
        private static final AtomicIntegerFieldUpdater<MiniFuture> RCV_RES_UPD =
            AtomicIntegerFieldUpdater.newUpdater(MiniFuture.class, "rcvRes");

        /** Parent future. */
        private final GridNearOptimisticTxPrepareFuture parent;

        /** */
        private final IgniteUuid futId = IgniteUuid.randomUuid();

        /** Keys. */
        @GridToStringInclude
        private GridDistributedTxMapping m;

        /** Flag to signal some result being processed. */
        @SuppressWarnings("UnusedDeclaration")
        private volatile int rcvRes;

        /** Mappings to proceed prepare. */
        private Queue<GridDistributedTxMapping> mappings;

        /**
         * @param parent Parent.
         * @param m Mapping.
         * @param mappings Queue of mappings to proceed with.
         */
        MiniFuture(GridNearOptimisticTxPrepareFuture parent, GridDistributedTxMapping m,
            Queue<GridDistributedTxMapping> mappings) {
            this.parent = parent;
            this.m = m;
            this.mappings = mappings;
        }

        /**
         * @return Future ID.
         */
        IgniteUuid futureId() {
            return futId;
        }

        /**
         * @return Node ID.
         */
        public ClusterNode node() {
            return m.node();
        }

        /**
         * @return Keys.
         */
        public GridDistributedTxMapping mapping() {
            return m;
        }

        /**
         * @param e Error.
         */
        void onResult(Throwable e) {
            if (RCV_RES_UPD.compareAndSet(this, 0, 1)) {
                if (log.isDebugEnabled())
                    log.debug("Failed to get future result [fut=" + this + ", err=" + e + ']');

                // Fail.
                onDone(e);
            }
            else
                U.warn(log, "Received error after another result has been processed [fut=" +
                    parent + ", mini=" + this + ']', e);
        }

        /**
         * @param e Node failure.
         * @param discoThread {@code True} if executed from discovery thread.
         */
        void onNodeLeft(ClusterTopologyCheckedException e, boolean discoThread) {
            if (msgLog.isDebugEnabled()) {
                msgLog.debug("Near optimistic prepare fut, mini future node left [txId=" + parent.tx.nearXidVersion() +
                    ", node=" + m.node().id() + ']');
            }

            if (isDone())
                return;

            if (RCV_RES_UPD.compareAndSet(this, 0, 1)) {
                if (log.isDebugEnabled())
                    log.debug("Remote node left grid while sending or waiting for reply (will not retry): " + this);

                // Fail the whole future (make sure not to remap on different primary node
                // to prevent multiple lock coordinators).
                parent.onError(e, discoThread);
            }
        }

        /**
         * @param res Result callback.
         */
        @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
        void onResult(final GridNearTxPrepareResponse res) {
            if (isDone())
                return;

            if (RCV_RES_UPD.compareAndSet(this, 0, 1)) {
                if (parent.cctx.tm().deadlockDetectionEnabled() &&
                    (parent.tx.remainingTime() == -1 || res.error() instanceof IgniteTxTimeoutCheckedException)) {
                    parent.onTimeout();

                    return;
                }

                if (res.error() != null) {
                    // Fail the whole compound future.
                    parent.onError(res.error(), false);
                }
                else {
                    if (res.clientRemapVersion() != null) {
                        assert parent.cctx.kernalContext().clientNode();
                        assert m.clientFirst();

                        IgniteInternalFuture<?> affFut =
                            parent.cctx.exchange().affinityReadyFuture(res.clientRemapVersion());

                        if (affFut != null && !affFut.isDone()) {
                            affFut.listen(new CI1<IgniteInternalFuture<?>>() {
                                @Override public void apply(IgniteInternalFuture<?> fut) {
                                    try {
                                        fut.get();

                                        remap();
                                    }
                                    catch (IgniteCheckedException e) {
                                        onDone(e);
                                    }
                                }
                            });
                        }
                        else
                            remap();
                    }
                    else {
                        parent.onPrepareResponse(m, res);

                        // Proceed prepare before finishing mini future.
                        if (mappings != null)
                            parent.proceedPrepare(mappings);

                        // Finish this mini future.
                        onDone((GridNearTxPrepareResponse)null);
                    }
                }
            }
        }

        /**
         *
         */
        private void remap() {
            parent.prepareOnTopology(true, new Runnable() {
                @Override public void run() {
                    onDone((GridNearTxPrepareResponse) null);
                }
            });
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(MiniFuture.class, this, "done", isDone(), "cancelled", isCancelled(), "err", error());
        }
    }
}

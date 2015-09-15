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

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterTopologyException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.cluster.ClusterTopologyServerNotFoundException;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.GridCacheEntryRemovedException;
import org.apache.ignite.internal.processors.cache.GridCacheMvccCandidate;
import org.apache.ignite.internal.processors.cache.GridCacheMvccFuture;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.distributed.GridDistributedTxMapping;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTopologyFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxMapping;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxEntry;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxKey;
import org.apache.ignite.internal.transactions.IgniteTxOptimisticCheckedException;
import org.apache.ignite.internal.transactions.IgniteTxRollbackCheckedException;
import org.apache.ignite.internal.transactions.IgniteTxTimeoutCheckedException;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.lang.GridPlainRunnable;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.C1;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.transactions.TransactionOptimisticException;
import org.apache.ignite.transactions.TransactionTimeoutException;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentLinkedDeque8;

import static org.apache.ignite.internal.processors.cache.GridCacheOperation.TRANSFORM;
import static org.apache.ignite.transactions.TransactionState.PREPARED;
import static org.apache.ignite.transactions.TransactionState.PREPARING;

/**
 *
 */
public class GridNearOptimisticTxPrepareFuture extends GridNearTxPrepareFutureAdapter
    implements GridCacheMvccFuture<IgniteInternalTx> {
    /** */
    @GridToStringInclude
    private Collection<IgniteTxKey> lockKeys = new GridConcurrentHashSet<>();

    /**
     * @param cctx Context.
     * @param tx Transaction.
     */
    public GridNearOptimisticTxPrepareFuture(GridCacheSharedContext cctx, GridNearTxLocal tx) {
        super(cctx, tx);

        assert tx.optimistic() : tx;
    }

    /** {@inheritDoc} */
    @Override public boolean onOwnerChanged(GridCacheEntryEx entry, GridCacheMvccCandidate owner) {
        if (log.isDebugEnabled())
            log.debug("Transaction future received owner changed callback: " + entry);

        if ((entry.context().isNear() || entry.context().isLocal()) && owner != null && tx.hasWriteKey(entry.txKey())) {
            lockKeys.remove(entry.txKey());

            // This will check for locks.
            onDone();

            return true;
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override public Collection<? extends ClusterNode> nodes() {
        return F.viewReadOnly(futures(), new IgniteClosure<IgniteInternalFuture<?>, ClusterNode>() {
            @Nullable @Override public ClusterNode apply(IgniteInternalFuture<?> f) {
                if (isMini(f))
                    return ((MiniFuture)f).node();

                return cctx.discovery().localNode();
            }
        });
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

                    f.onResult(e);

                    found = true;
                }
            }
        }

        return found;
    }

    /**
     * @param nodeId Failed node ID.
     * @param mappings Remaining mappings.
     * @param e Error.
     */
    void onError(@Nullable UUID nodeId, @Nullable Iterable<GridDistributedTxMapping> mappings, Throwable e) {
        if (X.hasCause(e, ClusterTopologyCheckedException.class) || X.hasCause(e, ClusterTopologyException.class)) {
            if (tx.onePhaseCommit()) {
                tx.markForBackupCheck();

                onComplete();

                return;
            }
        }

        if (err.compareAndSet(null, e)) {
            boolean marked = tx.setRollbackOnly();

            if (e instanceof IgniteTxOptimisticCheckedException) {
                assert nodeId != null : "Missing node ID for optimistic failure exception: " + e;

                tx.removeKeysMapping(nodeId, mappings);
            }

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

    /**
     * @return {@code True} if all locks are owned.
     */
    private boolean checkLocks() {
        boolean locked = lockKeys.isEmpty();

        if (locked) {
            if (log.isDebugEnabled())
                log.debug("All locks are acquired for near prepare future: " + this);
        }
        else {
            if (log.isDebugEnabled())
                log.debug("Still waiting for locks [fut=" + this + ", keys=" + lockKeys + ']');
        }

        return locked;
    }

    /** {@inheritDoc} */
    @Override public void onResult(UUID nodeId, GridNearTxPrepareResponse res) {
        if (!isDone()) {
            for (IgniteInternalFuture<IgniteInternalTx> fut : pending()) {
                if (isMini(fut)) {
                    MiniFuture f = (MiniFuture)fut;

                    if (f.futureId().equals(res.miniId())) {
                        assert f.node().id().equals(nodeId);

                        f.onResult(nodeId, res);
                    }
                }
            }
        }
    }

    /** {@inheritDoc} */
    @Override public boolean onDone(IgniteInternalTx t, Throwable err) {
        // If locks were not acquired yet, delay completion.
        if (isDone() || (err == null && !checkLocks()))
            return false;

        this.err.compareAndSet(null, err);

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
     */
    private boolean onComplete() {
        Throwable err0 = err.get();

        if (err0 == null || tx.needCheckBackup())
            tx.state(PREPARED);

        if (super.onDone(tx, err0)) {
            // Don't forget to clean up.
            cctx.mvcc().removeFuture(this);

            return true;
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override public void prepare() {
        // Obtain the topology version to use.
        AffinityTopologyVersion topVer = cctx.mvcc().lastExplicitLockTopologyVersion(Thread.currentThread().getId());

        // If there is another system transaction in progress, use it's topology version to prevent deadlock.
        if (topVer == null && tx != null && tx.system()) {
            IgniteInternalTx tx0 = cctx.tm().anyActiveThreadTx(tx);

            if (tx0 != null)
                topVer = tx0.topologyVersionSnapshot();
        }

        if (topVer != null) {
            tx.topologyVersion(topVer);

            cctx.mvcc().addFuture(this);

            prepare0(false);

            return;
        }

        prepareOnTopology(false, null);
    }

    /**
     * @param remap Remap flag.
     * @param c Optional closure to run after map.
     */
    private void prepareOnTopology(final boolean remap, @Nullable final Runnable c) {
        GridDhtTopologyFuture topFut = topologyReadLock();

        AffinityTopologyVersion topVer = null;

        try {
            if (topFut == null) {
                assert isDone();

                return;
            }

            if (topFut.isDone()) {
                topVer = topFut.topologyVersion();

                if (remap)
                    tx.onRemap(topVer);
                else
                    tx.topologyVersion(topVer);

                if (!remap)
                    cctx.mvcc().addFuture(this);
            }
        }
        finally {
            topologyReadUnlock();
        }

        if (topVer != null) {
            StringBuilder invalidCaches = null;

            for (Integer cacheId : tx.activeCacheIds()) {
                GridCacheContext ctx = cctx.cacheContext(cacheId);

                assert ctx != null : cacheId;

                if (!topFut.isCacheTopologyValid(ctx)) {
                    if (invalidCaches != null)
                        invalidCaches.append(", ");
                    else
                        invalidCaches = new StringBuilder();

                    invalidCaches.append(U.maskName(ctx.name()));
                }
            }

            if (invalidCaches != null) {
                onDone(new IgniteCheckedException("Failed to perform cache operation (cache topology is not valid): " +
                    invalidCaches.toString()));

                return;
            }

            prepare0(remap);

            if (c != null)
                c.run();
        }
        else {
            topFut.listen(new CI1<IgniteInternalFuture<AffinityTopologyVersion>>() {
                @Override public void apply(IgniteInternalFuture<AffinityTopologyVersion> t) {
                    cctx.kernalContext().closure().runLocalSafe(new GridPlainRunnable() {
                        @Override public void run() {
                            try {
                                prepareOnTopology(remap, c);
                            }
                            finally {
                                cctx.txContextReset();
                            }
                        }
                    });
                }
            });
        }
    }

    /**
     * Acquires topology read lock.
     *
     * @return Topology ready future.
     */
    private GridDhtTopologyFuture topologyReadLock() {
        if (tx.activeCacheIds().isEmpty())
            return cctx.exchange().lastTopologyFuture();

        GridCacheContext<?, ?> nonLocCtx = null;

        for (int cacheId : tx.activeCacheIds()) {
            GridCacheContext<?, ?> cacheCtx = cctx.cacheContext(cacheId);

            if (!cacheCtx.isLocal()) {
                nonLocCtx = cacheCtx;

                break;
            }
        }

        if (nonLocCtx == null)
            return cctx.exchange().lastTopologyFuture();

        nonLocCtx.topology().readLock();

        if (nonLocCtx.topology().stopping()) {
            onDone(new IgniteCheckedException("Failed to perform cache operation (cache is stopped): " +
                nonLocCtx.name()));

            return null;
        }

        return nonLocCtx.topology().topologyVersionFuture();
    }

    /**
     * Releases topology read lock.
     */
    private void topologyReadUnlock() {
        if (!tx.activeCacheIds().isEmpty()) {
            GridCacheContext<?, ?> nonLocCtx = null;

            for (int cacheId : tx.activeCacheIds()) {
                GridCacheContext<?, ?> cacheCtx = cctx.cacheContext(cacheId);

                if (!cacheCtx.isLocal()) {
                    nonLocCtx = cacheCtx;

                    break;
                }
            }

            if (nonLocCtx != null)
                nonLocCtx.topology().readUnlock();
        }
    }

    /**
     * Initializes future.
     *
     * @param remap Remap flag.
     */
    private void prepare0(boolean remap) {
        try {
            boolean txStateCheck = remap ? tx.state() == PREPARING : tx.state(PREPARING);

            if (!txStateCheck) {
                if (tx.setRollbackOnly()) {
                    if (tx.timedOut())
                        onError(null, null, new IgniteTxTimeoutCheckedException("Transaction timed out and " +
                            "was rolled back: " + this));
                    else
                        onError(null, null, new IgniteCheckedException("Invalid transaction state for prepare " +
                            "[state=" + tx.state() + ", tx=" + this + ']'));
                }
                else
                    onError(null, null, new IgniteTxRollbackCheckedException("Invalid transaction state for " +
                        "prepare [state=" + tx.state() + ", tx=" + this + ']'));

                return;
            }

            prepare(
                tx.optimistic() && tx.serializable() ? tx.readEntries() : Collections.<IgniteTxEntry>emptyList(),
                tx.writeEntries());

            markInitialized();
        }
        catch (TransactionTimeoutException | TransactionOptimisticException e) {
            onError(cctx.localNodeId(), null, e);
        }
        catch (IgniteCheckedException e) {
            onDone(e);
        }
    }

    /**
     * @param reads Read entries.
     * @param writes Write entries.
     */
    private void prepare(
        Iterable<IgniteTxEntry> reads,
        Iterable<IgniteTxEntry> writes
    ) throws IgniteCheckedException {
        AffinityTopologyVersion topVer = tx.topologyVersion();

        assert topVer.topologyVersion() > 0;

        txMapping = new GridDhtTxMapping();

        ConcurrentLinkedDeque8<GridDistributedTxMapping> mappings = new ConcurrentLinkedDeque8<>();

        if (!F.isEmpty(reads) || !F.isEmpty(writes)) {
            for (int cacheId : tx.activeCacheIds()) {
                GridCacheContext<?, ?> cacheCtx = cctx.cacheContext(cacheId);

                if (CU.affinityNodes(cacheCtx, topVer).isEmpty()) {
                    onDone(new ClusterTopologyServerNotFoundException("Failed to map keys for cache (all " +
                        "partition nodes left the grid): " + cacheCtx.name()));

                    return;
                }
            }
        }

        // Assign keys to primary nodes.
        GridDistributedTxMapping cur = null;

        for (IgniteTxEntry read : reads) {
            GridDistributedTxMapping updated = map(read, topVer, cur, false);

            if (cur != updated) {
                mappings.offer(updated);

                if (updated.node().isLocal()) {
                    if (read.context().isNear())
                        tx.nearLocallyMapped(true);
                    else if (read.context().isColocated())
                        tx.colocatedLocallyMapped(true);
                }

                cur = updated;
            }
        }

        for (IgniteTxEntry write : writes) {
            GridDistributedTxMapping updated = map(write, topVer, cur, true);

            if (cur != updated) {
                mappings.offer(updated);

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

        tx.addEntryMapping(mappings);

        cctx.mvcc().recheckPendingLocks();

        txMapping.initLast(mappings);

        tx.transactionNodes(txMapping.transactionNodes());

        checkOnePhase();

        proceedPrepare(mappings);
    }

    /**
     * Continues prepare after previous mapping successfully finished.
     *
     * @param mappings Queue of mappings.
     */
    private void proceedPrepare(final ConcurrentLinkedDeque8<GridDistributedTxMapping> mappings) {
        if (isDone())
            return;

        final GridDistributedTxMapping m = mappings.poll();

        if (m == null)
            return;

        assert !m.empty();

        final ClusterNode n = m.node();

        GridNearTxPrepareRequest req = new GridNearTxPrepareRequest(
            futId,
            tx.topologyVersion(),
            tx,
            tx.optimistic() && tx.serializable() ? m.reads() : null,
            m.writes(),
            m.near(),
            txMapping.transactionNodes(),
            m.last(),
            m.lastBackups(),
            tx.onePhaseCommit(),
            tx.needReturnValue() && tx.implicit(),
            tx.implicitSingle(),
            m.explicitLock(),
            tx.subjectId(),
            tx.taskNameHash(),
            m.clientFirst());

        for (IgniteTxEntry txEntry : m.writes()) {
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
                onError(null, null, e);
            }
        }

        final MiniFuture fut = new MiniFuture(m, mappings);

        req.miniId(fut.futureId());

        add(fut); // Append new future.

        // If this is the primary node for the keys.
        if (n.isLocal()) {
            // At this point, if any new node joined, then it is
            // waiting for this transaction to complete, so
            // partition reassignments are not possible here.
            IgniteInternalFuture<GridNearTxPrepareResponse> prepFut = cctx.tm().txHandler().prepareTx(n.id(), tx, req);

            prepFut.listen(new CI1<IgniteInternalFuture<GridNearTxPrepareResponse>>() {
                @Override public void apply(IgniteInternalFuture<GridNearTxPrepareResponse> prepFut) {
                    try {
                        fut.onResult(n.id(), prepFut.get());
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
            }
            catch (ClusterTopologyCheckedException e) {
                e.retryReadyFuture(cctx.nextAffinityReadyFuture(tx.topologyVersion()));

                fut.onResult(e);
            }
            catch (IgniteCheckedException e) {
                fut.onResult(e);
            }
        }
    }

    /**
     * @param entry Transaction entry.
     * @param topVer Topology version.
     * @param cur Current mapping.
     * @param waitLock Wait lock flag.
     * @return Mapping.
     */
    private GridDistributedTxMapping map(
        IgniteTxEntry entry,
        AffinityTopologyVersion topVer,
        @Nullable GridDistributedTxMapping cur,
        boolean waitLock
    ) {
        GridCacheContext cacheCtx = entry.context();

        List<ClusterNode> nodes = cacheCtx.affinity().nodes(entry.key(), topVer);

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
            if (waitLock && entry.explicitVersion() == null)
                lockKeys.add(entry.txKey());
        }

        if (cur == null || !cur.node().id().equals(primary.id()) || cur.near() != cacheCtx.isNear()) {
            boolean clientFirst = cur == null && cctx.kernalContext().clientNode();

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
                    entry.cached(cacheCtx.near().entryEx(entry.key()));
                }
            }
        }

        return cur;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        Collection<String> futs = F.viewReadOnly(futures(), new C1<IgniteInternalFuture<?>, String>() {
            @Override public String apply(IgniteInternalFuture<?> f) {
                return "[node=" + ((MiniFuture)f).node().id() +
                    ", loc=" + ((MiniFuture)f).node().isLocal() +
                    ", done=" + f.isDone() + "]";
            }
        });

        return S.toString(GridNearOptimisticTxPrepareFuture.class, this,
            "innerFuts", futs,
            "tx", tx,
            "super", super.toString());
    }

    /**
     *
     */
    private class MiniFuture extends GridFutureAdapter<IgniteInternalTx> {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private final IgniteUuid futId = IgniteUuid.randomUuid();

        /** Keys. */
        @GridToStringInclude
        private GridDistributedTxMapping m;

        /** Flag to signal some result being processed. */
        private AtomicBoolean rcvRes = new AtomicBoolean(false);

        /** Mappings to proceed prepare. */
        private ConcurrentLinkedDeque8<GridDistributedTxMapping> mappings;

        /**
         * @param m Mapping.
         * @param mappings Queue of mappings to proceed with.
         */
        MiniFuture(
            GridDistributedTxMapping m,
            ConcurrentLinkedDeque8<GridDistributedTxMapping> mappings
        ) {
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
            if (rcvRes.compareAndSet(false, true)) {
                if (log.isDebugEnabled())
                    log.debug("Failed to get future result [fut=" + this + ", err=" + e + ']');

                // Fail.
                onDone(e);
            }
            else
                U.warn(log, "Received error after another result has been processed [fut=" +
                    GridNearOptimisticTxPrepareFuture.this + ", mini=" + this + ']', e);
        }

        /**
         * @param e Node failure.
         */
        void onResult(ClusterTopologyCheckedException e) {
            if (isDone())
                return;

            if (rcvRes.compareAndSet(false, true)) {
                if (log.isDebugEnabled())
                    log.debug("Remote node left grid while sending or waiting for reply (will not retry): " + this);

                // Fail the whole future (make sure not to remap on different primary node
                // to prevent multiple lock coordinators).
                onError(null, null, e);
            }
        }

        /**
         * @param nodeId Failed node ID.
         * @param res Result callback.
         */
        void onResult(UUID nodeId, GridNearTxPrepareResponse res) {
            if (isDone())
                return;

            if (rcvRes.compareAndSet(false, true)) {
                if (res.error() != null) {
                    // Fail the whole compound future.
                    onError(nodeId, mappings, res.error());
                }
                else {
                    if (res.clientRemapVersion() != null) {
                        assert cctx.kernalContext().clientNode();
                        assert m.clientFirst();

                        IgniteInternalFuture<?> affFut = cctx.exchange().affinityReadyFuture(res.clientRemapVersion());

                        if (affFut != null && !affFut.isDone()) {
                            affFut.listen(new CI1<IgniteInternalFuture<?>>() {
                                @Override public void apply(IgniteInternalFuture<?> fut) {
                                    remap();
                                }
                            });
                        }
                        else
                            remap();
                    }
                    else {
                        onPrepareResponse(m, res);

                        // Proceed prepare before finishing mini future.
                        if (mappings != null)
                            proceedPrepare(mappings);

                        // Finish this mini future.
                        onDone(tx);
                    }
                }
            }
        }

        /**
         *
         */
        private void remap() {
            prepareOnTopology(true, new Runnable() {
                @Override public void run() {
                    onDone(tx);
                }
            });
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(MiniFuture.class, this, "done", isDone(), "cancelled", isCancelled(), "err", error());
        }
    }
}

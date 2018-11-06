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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
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
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.distributed.GridDistributedTxMapping;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxMapping;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxEntry;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.transactions.IgniteTxOptimisticCheckedException;
import org.apache.ignite.internal.transactions.IgniteTxRollbackCheckedException;
import org.apache.ignite.internal.transactions.IgniteTxTimeoutCheckedException;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
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
import org.apache.ignite.lang.IgniteReducer;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.GridCacheOperation.TRANSFORM;
import static org.apache.ignite.transactions.TransactionState.PREPARED;
import static org.apache.ignite.transactions.TransactionState.PREPARING;

/**
 *
 */
public class GridNearOptimisticSerializableTxPrepareFuture extends GridNearOptimisticTxPrepareFutureAdapter {
    /** */
    @GridToStringExclude
    private ClientRemapFuture remapFut;

    /** */
    private int miniId;

    /**
     * @param cctx Context.
     * @param tx Transaction.
     */
    public GridNearOptimisticSerializableTxPrepareFuture(GridCacheSharedContext cctx,
        GridNearTxLocal tx) {
        super(cctx, tx);

        assert tx.optimistic() && tx.serializable() : tx;
    }

    /** {@inheritDoc} */
    @Override protected boolean ignoreFailure(Throwable err) {
        return IgniteCheckedException.class.isAssignableFrom(err.getClass());
    }

    /** {@inheritDoc} */
    @Override public boolean onOwnerChanged(GridCacheEntryEx entry, GridCacheMvccCandidate owner) {
        if (log.isDebugEnabled())
            log.debug("Transaction future received owner changed callback: " + entry);

        if ((entry.context().isNear() || entry.context().isLocal()) && owner != null) {
            IgniteTxEntry txEntry = tx.entry(entry.txKey());

            if (txEntry != null) {
                if (entry.context().isLocal()) {
                    GridCacheVersion serReadVer = txEntry.entryReadVersion();

                    if (serReadVer != null) {
                        GridCacheContext ctx = entry.context();

                        while (true) {
                            try {
                                if (!entry.checkSerializableReadVersion(serReadVer)) {
                                    Object key = entry.key().value(ctx.cacheObjectContext(), false);

                                    IgniteTxOptimisticCheckedException err0 =
                                        new IgniteTxOptimisticCheckedException(S.toString(
                                            "Failed to prepare transaction, read/write conflict",
                                            "key", key, true,
                                            "cache", ctx.name(), false));

                                    ERR_UPD.compareAndSet(this, null, err0);
                                }

                                break;
                            }
                            catch (GridCacheEntryRemovedException ignored) {
                                entry = ctx.cache().entryEx(entry.key(), tx.topologyVersion());

                                txEntry.cached(entry);
                            }
                        }

                    }
                }

                if (keyLockFut != null)
                    keyLockFut.onKeyLocked(entry.txKey());

                return true;
            }
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean onNodeLeft(UUID nodeId) {
        boolean found = false;

        for (IgniteInternalFuture<?> fut : futures()) {
            if (isMini(fut)) {
                MiniFuture f = (MiniFuture) fut;

                if (f.primary().id().equals(nodeId)) {
                    ClusterTopologyCheckedException e = new ClusterTopologyCheckedException("Remote node left grid: " +
                        nodeId);

                    e.retryReadyFuture(cctx.nextAffinityReadyFuture(tx.topologyVersion()));

                    f.onNodeLeft(e);

                    found = true;
                }
            }
        }

        return found;
    }

    /**
     * @param m Failed mapping.
     * @param e Error.
     */
    private void onError(@Nullable GridDistributedTxMapping m, Throwable e) {
        if (X.hasCause(e, ClusterTopologyCheckedException.class) || X.hasCause(e, ClusterTopologyException.class)) {
            if (tx.onePhaseCommit()) {
                tx.markForBackupCheck();

                onComplete();

                return;
            }
        }

        if (e instanceof IgniteTxOptimisticCheckedException) {
            if (m != null)
                tx.removeMapping(m.primary().id());
        }

        ERR_UPD.compareAndSet(this, null, e);

        if (keyLockFut != null)
            keyLockFut.onDone(e);
    }

    /** {@inheritDoc} */
    @Override public void onResult(UUID nodeId, GridNearTxPrepareResponse res) {
        if (!isDone()) {
            MiniFuture mini = miniFuture(res.miniId());

            if (mini != null)
                mini.onResult(res, true);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean onDone(IgniteInternalTx t, Throwable err) {
        if (isDone())
            return false;

        if (err != null) {
            ERR_UPD.compareAndSet(this, null, err);

            if (keyLockFut != null)
                keyLockFut.onDone(err);
        }

        return onComplete();
    }

    /**
     * Finds pending mini future by the given mini ID.
     *
     * @param miniId Mini ID to find.
     * @return Mini future.
     */
    @SuppressWarnings("ForLoopReplaceableByForEach")
    private MiniFuture miniFuture(int miniId) {
        // We iterate directly over the futs collection here to avoid copy.
        synchronized (GridNearOptimisticSerializableTxPrepareFuture.this) {
            int size = futuresCountNoLock();

            // Avoid iterator creation.
            for (int i = 0; i < size; i++) {
                IgniteInternalFuture<GridNearTxPrepareResponse> fut = future(i);

                if (!isMini(fut))
                    continue;

                MiniFuture mini = (MiniFuture)fut;

                if (mini.futureId() == miniId) {
                    if (!mini.isDone())
                        return mini;
                    else
                        return null;
                }
            }
        }

        return null;
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
            if (err0 != null)
                tx.setRollbackOnly();

            // Don't forget to clean up.
            cctx.mvcc().removeVersionedFuture(this);

            return true;
        }

        return false;
    }

    /**
     * Initializes future.
     *
     * @param remap Remap flag.
     */
    @Override protected void prepare0(boolean remap, boolean topLocked) {
        boolean txStateCheck = remap ? tx.state() == PREPARING : tx.state(PREPARING);

        if (!txStateCheck) {
            if (tx.isRollbackOnly() || tx.setRollbackOnly()) {
                if (tx.timedOut())
                    onDone(null, tx.timeoutException());
                else
                    onDone(null, tx.rollbackException());
            }
            else
                onDone(null, new IgniteCheckedException("Invalid transaction state for " +
                    "prepare [state=" + tx.state() + ", tx=" + this + ']'));

            return;
        }

        boolean set = cctx.tm().setTxTopologyHint(tx.topologyVersionSnapshot());

        try {
            prepare(tx.readEntries(), tx.writeEntries(), remap, topLocked);

            markInitialized();
        }
        finally {
            if (set)
                cctx.tm().setTxTopologyHint(null);
        }
    }

    /**
     * @param reads Read entries.
     * @param writes Write entries.
     * @param remap Remap flag.
     * @param topLocked Topology locked flag.
     */
    @SuppressWarnings("unchecked")
    private void prepare(
        Iterable<IgniteTxEntry> reads,
        Iterable<IgniteTxEntry> writes,
        boolean remap,
        boolean topLocked
    ) {
        AffinityTopologyVersion topVer = tx.topologyVersion();

        assert topVer.topologyVersion() > 0;

        GridDhtTxMapping txMapping = new GridDhtTxMapping();

        Map<UUID, GridDistributedTxMapping> mappings = new HashMap<>();

        boolean hasNearCache = false;

        for (IgniteTxEntry write : writes) {
            map(write, topVer, mappings, txMapping, remap, topLocked);

            if (write.context().isNear())
                hasNearCache = true;
        }

        for (IgniteTxEntry read : reads)
            map(read, topVer, mappings, txMapping, remap, topLocked);

        if (keyLockFut != null)
            keyLockFut.onAllKeysAdded();

        if (isDone()) {
            if (log.isDebugEnabled())
                log.debug("Abandoning (re)map because future is done: " + this);

            return;
        }

        tx.addEntryMapping(mappings.values());

        cctx.mvcc().recheckPendingLocks();

        tx.transactionNodes(txMapping.transactionNodes());

        if (!hasNearCache)
            checkOnePhase(txMapping);

        MiniFuture locNearEntriesFut = null;

        // Create futures in advance to have all futures when process {@link GridNearTxPrepareResponse#clientRemapVersion}.
        for (GridDistributedTxMapping m : mappings.values()) {
            assert !m.empty();

            MiniFuture fut = new MiniFuture(this, m, ++miniId);

            add(fut);

            if (m.primary().isLocal() && m.hasNearCacheEntries() && m.hasColocatedCacheEntries()) {
                assert locNearEntriesFut == null;

                locNearEntriesFut = fut;

                add(new MiniFuture(this, m, ++miniId));
            }
        }

        Collection<IgniteInternalFuture<?>> futs = (Collection)futures();

        Iterator<IgniteInternalFuture<?>> it = futs.iterator();

        while (it.hasNext()) {
            IgniteInternalFuture<?> fut0 = it.next();

            if (skipFuture(remap, fut0))
                continue;

            MiniFuture fut = (MiniFuture)fut0;

            IgniteCheckedException err = prepare(fut, txMapping.transactionNodes(), locNearEntriesFut);

            if (err != null) {
                while (it.hasNext()) {
                    fut0 = it.next();

                    if (skipFuture(remap, fut0))
                        continue;

                    fut = (MiniFuture)fut0;

                    tx.removeMapping(fut.mapping().primary().id());

                    fut.onResult(new IgniteCheckedException("Failed to prepare transaction.", err));
                }

                break;
            }
        }

        markInitialized();
    }

    /**
     * @param remap Remap flag.
     * @param fut Future.
     * @return {@code True} if skip future during remap.
     */
    private boolean skipFuture(boolean remap, IgniteInternalFuture<?> fut) {
        return !(isMini(fut)) || (remap && (((MiniFuture)fut).rcvRes == 1));
    }

    /**
     * @param fut Mini future.
     * @param txNodes Tx nodes.
     * @param locNearEntriesFut Local future for near cache entries prepare.
     * @return Prepare error if any.
     */
    @Nullable private IgniteCheckedException prepare(final MiniFuture fut,
        Map<UUID, Collection<UUID>> txNodes,
        @Nullable MiniFuture locNearEntriesFut) {
        GridDistributedTxMapping m = fut.mapping();

        final ClusterNode primary = m.primary();

        long timeout = tx.remainingTime();

        if (timeout == -1) {
            IgniteCheckedException err = tx.timeoutException();

            fut.onResult(err);

            return err;
        }

        // Must lock near entries separately.
        if (m.hasNearCacheEntries()) {
            try {
                cctx.tm().prepareTx(tx, m.nearCacheEntries());
            }
            catch (IgniteCheckedException e) {
                fut.onResult(e);

                return e;
            }
        }

        if (primary.isLocal()) {
            if (locNearEntriesFut != null) {
                boolean nearEntries = fut == locNearEntriesFut;

                GridNearTxPrepareRequest req = createRequest(txNodes,
                    fut,
                    timeout,
                    nearEntries ? m.nearEntriesReads() : m.colocatedEntriesReads(),
                    nearEntries ? m.nearEntriesWrites() : m.colocatedEntriesWrites());

                prepareLocal(req, fut, nearEntries);
            }
            else {
                GridNearTxPrepareRequest req = createRequest(txNodes,
                    fut,
                    timeout,
                    m.reads(),
                    m.writes());

                prepareLocal(req, fut, m.hasNearCacheEntries());
            }
        }
        else {
            try {
                GridNearTxPrepareRequest req = createRequest(txNodes,
                    fut,
                    timeout,
                    m.reads(),
                    m.writes());

                cctx.io().send(primary, req, tx.ioPolicy());
            }
            catch (ClusterTopologyCheckedException e) {
                e.retryReadyFuture(cctx.nextAffinityReadyFuture(tx.topologyVersion()));

                fut.onNodeLeft(e);

                return e;
            }
            catch (IgniteCheckedException e) {
                fut.onResult(e);

                return e;
            }
        }

        return null;
    }

    /**
     * @param txNodes Tx nodes.
     * @param fut Future.
     * @param timeout Timeout.
     * @param reads Read entries.
     * @param writes Write entries.
     * @return Request.
     */
    private GridNearTxPrepareRequest createRequest(
        Map<UUID, Collection<UUID>> txNodes,
        MiniFuture fut,
        long timeout,
        Collection<IgniteTxEntry> reads,
        Collection<IgniteTxEntry> writes) {
        GridDistributedTxMapping m = fut.mapping();

        GridNearTxPrepareRequest req = new GridNearTxPrepareRequest(
            futId,
            tx.topologyVersion(),
            tx,
            timeout,
            reads,
            writes,
            m.hasNearCacheEntries(),
            txNodes,
            m.last(),
            tx.onePhaseCommit(),
            tx.needReturnValue() && tx.implicit(),
            tx.implicitSingle(),
            m.explicitLock(),
            tx.subjectId(),
            tx.taskNameHash(),
            m.clientFirst(),
            txNodes.size() == 1,
            tx.activeCachesDeploymentEnabled());

        for (IgniteTxEntry txEntry : writes) {
            if (txEntry.op() == TRANSFORM)
                req.addDhtVersion(txEntry.txKey(), null);
        }

        req.miniId(fut.futureId());

        return req;
    }

    /**
     * @param req Request.
     * @param fut Future.
     * @param nearEntries {@code True} if prepare near cache entries.
     */
    private void prepareLocal(GridNearTxPrepareRequest req,
        final MiniFuture fut,
        final boolean nearEntries) {
        IgniteInternalFuture<GridNearTxPrepareResponse> prepFut = nearEntries ?
            cctx.tm().txHandler().prepareNearTxLocal(req) :
            cctx.tm().txHandler().prepareColocatedTx(tx, req);

        prepFut.listen(new CI1<IgniteInternalFuture<GridNearTxPrepareResponse>>() {
            @Override public void apply(IgniteInternalFuture<GridNearTxPrepareResponse> prepFut) {
                try {
                    fut.onResult(prepFut.get(), nearEntries);
                }
                catch (IgniteCheckedException e) {
                    fut.onResult(e);
                }
            }
        });
    }

    /**
     * @param entry Transaction entry.
     * @param topVer Topology version.
     * @param curMapping Current mapping.
     * @param txMapping Mapping.
     * @param remap Remap flag.
     * @param topLocked Topology locked flag.
     */
    private void map(
        IgniteTxEntry entry,
        AffinityTopologyVersion topVer,
        Map<UUID, GridDistributedTxMapping> curMapping,
        GridDhtTxMapping txMapping,
        boolean remap,
        boolean topLocked
    ) {
        GridCacheContext cacheCtx = entry.context();

        List<ClusterNode> nodes = cacheCtx.isLocal() ?
            cacheCtx.affinity().nodesByKey(entry.key(), topVer) :
            cacheCtx.topology().nodes(cacheCtx.affinity().partition(entry.key()), topVer);

        if (F.isEmpty(nodes)) {
            onDone(new ClusterTopologyServerNotFoundException("Failed to map keys to nodes " +
                "(partition is not mapped to any node) [key=" + entry.key() +
                ", partition=" + cacheCtx.affinity().partition(entry.key()) + ", topVer=" + topVer + ']'));

            return;
        }

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

        if (!remap && (cacheCtx.isNear() || cacheCtx.isLocal())) {
            if (entry.explicitVersion() == null) {
                if (keyLockFut == null) {
                    keyLockFut = new KeyLockFuture();

                    add(keyLockFut);
                }

                keyLockFut.addLockKey(entry.txKey());
            }
        }

        GridDistributedTxMapping cur = curMapping.get(primary.id());

        if (cur == null) {
            cur = new GridDistributedTxMapping(primary);

            curMapping.put(primary.id(), cur);

            cur.clientFirst(!topLocked && cctx.kernalContext().clientNode());

            cur.last(true);
        }

        if (primary.isLocal()) {
            if (cacheCtx.isNear())
                tx.nearLocallyMapped(true);
            else if (cacheCtx.isColocated())
                tx.colocatedLocallyMapped(true);
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
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        Collection<String> futs = F.viewReadOnly(futures(),
            new C1<IgniteInternalFuture<?>, String>() {
                @Override public String apply(IgniteInternalFuture<?> f) {
                    return "[node=" + ((MiniFuture)f).primary().id() +
                        ", loc=" + ((MiniFuture)f).primary().isLocal() +
                        ", done=" + f.isDone() + "]";
                }
            },
            new P1<IgniteInternalFuture<?>>() {
                @Override public boolean apply(IgniteInternalFuture<?> f) {
                    return isMini(f);
                }
            });

        return S.toString(GridNearOptimisticSerializableTxPrepareFuture.class, this,
            "innerFuts", futs,
            "keyLockFut", keyLockFut,
            "tx", tx,
            "super", super.toString());
    }

    /**
     * Client remap future.
     */
    private static class ClientRemapFuture extends GridCompoundFuture<GridNearTxPrepareResponse, Boolean> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * Constructor.
         */
        ClientRemapFuture() {
            super(new ClientRemapFutureReducer());
        }
    }

    /**
     * Client remap future reducer.
     */
    private static class ClientRemapFutureReducer implements IgniteReducer<GridNearTxPrepareResponse, Boolean> {
        /** */
        private static final long serialVersionUID = 0L;

        /** Remap flag. */
        private boolean remap = true;

        /** {@inheritDoc} */
        @Override public boolean collect(@Nullable GridNearTxPrepareResponse res) {
            assert res != null;

            if (res.clientRemapVersion() == null)
                remap = false;

            return true;
        }

        /** {@inheritDoc} */
        @Override public Boolean reduce() {
            return remap;
        }
    }

    /**
     *
     */
    private static class MiniFuture extends GridFutureAdapter<GridNearTxPrepareResponse> {
        /** Receive result flag updater. */
        private static AtomicIntegerFieldUpdater<MiniFuture> RCV_RES_UPD =
            AtomicIntegerFieldUpdater.newUpdater(MiniFuture.class, "rcvRes");

        /** */
        private final int futId;

        /** Parent future. */
        private final GridNearOptimisticSerializableTxPrepareFuture parent;

        /** Keys. */
        @GridToStringInclude
        private GridDistributedTxMapping m;

        /** Flag to signal some result being processed. */
        @SuppressWarnings("UnusedDeclaration")
        private volatile int rcvRes;

        /**
         * @param parent Parent future.
         * @param m Mapping.
         * @param futId Mini future ID.
         */
        MiniFuture(GridNearOptimisticSerializableTxPrepareFuture parent, GridDistributedTxMapping m, int futId) {
            this.parent = parent;
            this.m = m;
            this.futId = futId;
        }

        /**
         * @return Future ID.
         */
        int futureId() {
            return futId;
        }

        /**
         * @return Primary node.
         */
        public ClusterNode primary() {
            return m.primary();
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
                parent.onError(m, e);

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
         */
        void onNodeLeft(ClusterTopologyCheckedException e) {
            if (isDone())
                return;

            if (RCV_RES_UPD.compareAndSet(this, 0, 1)) {
                if (log.isDebugEnabled())
                    log.debug("Remote node left grid while sending or waiting for reply (will not retry): " + this);

                parent.onError(null, e);

                onDone(e);
            }
        }

        /**
         * @param res Result callback.
         * @param updateMapping Update mapping flag.
         */
        @SuppressWarnings({"unchecked", "ThrowableResultOfMethodCallIgnored"})
        void onResult(final GridNearTxPrepareResponse res, boolean updateMapping) {
            if (isDone())
                return;

            if (RCV_RES_UPD.compareAndSet(this, 0, 1)) {
                if (res.error() != null) {
                    // Fail the whole compound future.
                    parent.onError(m, res.error());

                    onDone(res.error());
                }
                else {
                    if (res.clientRemapVersion() != null) {
                        assert parent.cctx.kernalContext().clientNode();
                        assert m.clientFirst();

                        parent.tx.removeMapping(m.primary().id());

                        ClientRemapFuture remapFut0 = null;

                        synchronized (parent) {
                            if (parent.remapFut == null) {
                                parent.remapFut = new ClientRemapFuture();

                                remapFut0 = parent.remapFut;
                            }
                        }

                        if (remapFut0 != null) {
                            Collection<IgniteInternalFuture<?>> futs = (Collection)parent.futures();

                            for (IgniteInternalFuture<?> fut : futs) {
                                if (parent.isMini(fut) && fut != this)
                                    remapFut0.add((MiniFuture)fut);
                            }

                            remapFut0.markInitialized();

                            remapFut0.listen(new CI1<IgniteInternalFuture<Boolean>>() {
                                @Override public void apply(IgniteInternalFuture<Boolean> remapFut0) {
                                    try {
                                        IgniteInternalFuture<?> affFut =
                                            parent.cctx.exchange().affinityReadyFuture(res.clientRemapVersion());

                                        if (affFut == null)
                                            affFut = new GridFinishedFuture<Object>();

                                        if (parent.remapFut.get()) {
                                            if (log.isDebugEnabled()) {
                                                log.debug("Will remap client tx [" +
                                                    "fut=" + parent +
                                                    ", topVer=" + res.topologyVersion() + ']');
                                            }

                                            synchronized (parent) {
                                                assert remapFut0 == parent.remapFut;

                                                parent.remapFut = null;
                                            }

                                            affFut.listen(new CI1<IgniteInternalFuture<?>>() {
                                                @Override public void apply(IgniteInternalFuture<?> affFut) {
                                                    try {
                                                        affFut.get();

                                                        remap(res);
                                                    }
                                                    catch (IgniteCheckedException e) {
                                                        ERR_UPD.compareAndSet(parent, null, e);

                                                        onDone(e);
                                                    }
                                                }
                                            });
                                        }
                                        else {
                                            ClusterTopologyCheckedException err0 = new ClusterTopologyCheckedException(
                                                "Cluster topology changed while client transaction is preparing.");

                                            err0.retryReadyFuture(affFut);

                                            ERR_UPD.compareAndSet(parent, null, err0);

                                            onDone(err0);
                                        }
                                    }
                                    catch (IgniteCheckedException e) {
                                        if (log.isDebugEnabled()) {
                                            log.debug("Prepare failed, will not remap tx: " +
                                                parent);
                                        }

                                        ERR_UPD.compareAndSet(parent, null, e);

                                        onDone(e);
                                    }
                                }
                            });
                        }
                        else
                            onDone(res);
                    }
                    else {
                        parent.onPrepareResponse(m, res, updateMapping);

                        // Finish this mini future (need result only on client node).
                        onDone(parent.cctx.kernalContext().clientNode() ? res : null);
                    }
                }
            }
        }

        /**
         * @param res Response.
         */
        private void remap(final GridNearTxPrepareResponse res) {
            parent.prepareOnTopology(true, new Runnable() {
                @Override public void run() {
                    onDone(res);
                }
            });
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(MiniFuture.class, this, "done", isDone(), "cancelled", isCancelled(), "err", error());
        }
    }
}

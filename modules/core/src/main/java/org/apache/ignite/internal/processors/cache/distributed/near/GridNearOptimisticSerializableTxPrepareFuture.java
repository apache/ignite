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
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.lang.IgniteReducer;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.GridCacheOperation.TRANSFORM;
import static org.apache.ignite.transactions.TransactionState.PREPARED;
import static org.apache.ignite.transactions.TransactionState.PREPARING;

/**
 *
 */
public class GridNearOptimisticSerializableTxPrepareFuture extends GridNearOptimisticTxPrepareFutureAdapter {
    /** */
    public static final IgniteProductVersion SER_TX_SINCE = IgniteProductVersion.fromString("1.5.0");

    /** */
    @GridToStringExclude
    private KeyLockFuture keyLockFut;

    /** */
    @GridToStringExclude
    private ClientRemapFuture remapFut;

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
                                        new IgniteTxOptimisticCheckedException("Failed to prepare transaction, " +
                                            "read/write conflict [key=" + key + ", cache=" + ctx.name() + ']');

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

                if (f.node().id().equals(nodeId)) {
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

        if (e instanceof IgniteTxOptimisticCheckedException || e instanceof IgniteTxTimeoutCheckedException) {
            if (m != null)
                tx.removeMapping(m.node().id());
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
                mini.onResult(res);
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
    private MiniFuture miniFuture(IgniteUuid miniId) {
        // We iterate directly over the futs collection here to avoid copy.
        synchronized (sync) {
            int size = futuresCountNoLock();

            // Avoid iterator creation.
            for (int i = 0; i < size; i++) {
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
            cctx.mvcc().removeMvccFuture(this);

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
            if (tx.setRollbackOnly()) {
                if (tx.timedOut())
                    onError(null, new IgniteTxTimeoutCheckedException("Transaction timed out and " +
                        "was rolled back: " + this));
                else
                    onError(null, new IgniteCheckedException("Invalid transaction state for prepare " +
                        "[state=" + tx.state() + ", tx=" + this + ']'));
            }
            else
                onError(null, new IgniteTxRollbackCheckedException("Invalid transaction state for " +
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

        txMapping = new GridDhtTxMapping();

        Map<IgniteBiTuple<ClusterNode, Boolean>, GridDistributedTxMapping> mappings = new HashMap<>();

        for (IgniteTxEntry write : writes)
            map(write, topVer, mappings, remap, topLocked);

        for (IgniteTxEntry read : reads)
            map(read, topVer, mappings, remap, topLocked);

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

        checkOnePhase();

        for (GridDistributedTxMapping m : mappings.values()) {
            assert !m.empty();

            add(new MiniFuture(this, m));
        }

        Collection<IgniteInternalFuture<?>> futs = (Collection)futures();

        Iterator<IgniteInternalFuture<?>> it = futs.iterator();

        while (it.hasNext()) {
            IgniteInternalFuture<?> fut0 = it.next();

            if (skipFuture(remap, fut0))
                continue;

            MiniFuture fut = (MiniFuture)fut0;

            IgniteCheckedException err = prepare(fut);

            if (err != null) {
                while (it.hasNext()) {
                    fut0 = it.next();

                    if (skipFuture(remap, fut0))
                        continue;

                    fut = (MiniFuture)fut0;

                    tx.removeMapping(fut.mapping().node().id());

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
     * @return Prepare error if any.
     */
    @Nullable private IgniteCheckedException prepare(final MiniFuture fut) {
        GridDistributedTxMapping m = fut.mapping();

        final ClusterNode n = m.node();

        long timeout = tx.remainingTime();

        if (timeout == -1) {
            IgniteCheckedException err = tx.timeoutException();

            fut.onResult(err);

            return err;
        }

        GridNearTxPrepareRequest req = new GridNearTxPrepareRequest(
            futId,
            tx.topologyVersion(),
            tx,
            timeout,
            m.reads(),
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
                tx.optimisticLockEntries(m.entries());

                tx.userPrepare();
            }
            catch (IgniteCheckedException e) {
                fut.onResult(e);

                return e;
            }
        }

        req.miniId(fut.futureId());

        // If this is the primary node for the keys.
        if (n.isLocal()) {
            IgniteInternalFuture<GridNearTxPrepareResponse> prepFut = cctx.tm().txHandler().prepareTx(n.id(), tx, req);

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
     * @param entry Transaction entry.
     * @param topVer Topology version.
     * @param curMapping Current mapping.
     * @param remap Remap flag.
     * @param topLocked Topology locked flag.
     */
    private void map(
        IgniteTxEntry entry,
        AffinityTopologyVersion topVer,
        Map<IgniteBiTuple<ClusterNode, Boolean>, GridDistributedTxMapping> curMapping,
        boolean remap,
        boolean topLocked
    ) {
        GridCacheContext cacheCtx = entry.context();

        List<ClusterNode> nodes = cacheCtx.isLocal() ?
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

        if (primary.version().compareTo(SER_TX_SINCE) < 0) {
            onDone(new IgniteCheckedException("Optimistic serializable transactions can be used only with node " +
                "version starting from " + SER_TX_SINCE));

            return;
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

        IgniteBiTuple<ClusterNode, Boolean> key = F.t(primary, cacheCtx.isNear());

        GridDistributedTxMapping cur = curMapping.get(key);

        if (cur == null) {
            cur = new GridDistributedTxMapping(primary);

            curMapping.put(key, cur);

            if (primary.isLocal()) {
                if (entry.context().isNear())
                    tx.nearLocallyMapped(true);
                else if (entry.context().isColocated())
                    tx.colocatedLocallyMapped(true);
            }

            // Initialize near flag right away.
            cur.near(cacheCtx.isNear());

            cur.clientFirst(!topLocked && cctx.kernalContext().clientNode());

            cur.last(true);
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
                    return "[node=" + ((MiniFuture)f).node().id() +
                        ", loc=" + ((MiniFuture)f).node().isLocal() +
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
        public ClientRemapFuture() {
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
        /** */
        private static final long serialVersionUID = 0L;

        /** Receive result flag updater. */
        private static AtomicIntegerFieldUpdater<MiniFuture> RCV_RES_UPD =
            AtomicIntegerFieldUpdater.newUpdater(MiniFuture.class, "rcvRes");

        /** */
        private final IgniteUuid futId = IgniteUuid.randomUuid();

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
         */
        MiniFuture(GridNearOptimisticSerializableTxPrepareFuture parent, GridDistributedTxMapping m) {
            this.parent = parent;
            this.m = m;
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
         */
        @SuppressWarnings({"unchecked", "ThrowableResultOfMethodCallIgnored"})
        void onResult(final GridNearTxPrepareResponse res) {
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

                        parent.tx.removeMapping(m.node().id());

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
                        parent.onPrepareResponse(m, res);

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
                @Override
                public void run() {
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

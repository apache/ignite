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

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.cluster.*;
import org.apache.ignite.internal.processors.affinity.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.cache.distributed.*;
import org.apache.ignite.internal.processors.cache.distributed.dht.*;
import org.apache.ignite.internal.processors.cache.distributed.dht.colocated.*;
import org.apache.ignite.internal.processors.cache.transactions.*;
import org.apache.ignite.internal.processors.cache.version.*;
import org.apache.ignite.internal.transactions.*;
import org.apache.ignite.internal.util.*;
import org.apache.ignite.internal.util.future.*;
import org.apache.ignite.internal.util.lang.*;
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.transactions.*;
import org.jetbrains.annotations.*;
import org.jsr166.*;

import javax.cache.expiry.*;
import java.util.*;
import java.util.concurrent.atomic.*;

import static org.apache.ignite.internal.processors.cache.GridCacheOperation.*;
import static org.apache.ignite.transactions.TransactionState.*;

/**
 *
 */
public final class GridNearTxPrepareFuture<K, V> extends GridCompoundIdentityFuture<IgniteInternalTx>
    implements GridCacheMvccFuture<IgniteInternalTx> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Logger reference. */
    private static final AtomicReference<IgniteLogger> logRef = new AtomicReference<>();

    /** Logger. */
    private static IgniteLogger log;

    /** Context. */
    private GridCacheSharedContext<K, V> cctx;

    /** Future ID. */
    private IgniteUuid futId;

    /** Transaction. */
    @GridToStringExclude
    private GridNearTxLocal tx;

    /** Error. */
    @GridToStringExclude
    private AtomicReference<Throwable> err = new AtomicReference<>(null);

    /** Trackable flag. */
    private boolean trackable = true;

    /** Full information about transaction nodes mapping. */
    private GridDhtTxMapping<K, V> txMapping;

    /** */
    private Collection<IgniteTxKey> lockKeys = new GridConcurrentHashSet<>();

    /**
     * @param cctx Context.
     * @param tx Transaction.
     */
    public GridNearTxPrepareFuture(GridCacheSharedContext<K, V> cctx, final GridNearTxLocal tx) {
        super(cctx.kernalContext(), new IgniteReducer<IgniteInternalTx, IgniteInternalTx>() {
            @Override public boolean collect(IgniteInternalTx e) {
                return true;
            }

            @Override public IgniteInternalTx reduce() {
                // Nothing to aggregate.
                return tx;
            }
        });

        assert cctx != null;
        assert tx != null;

        this.cctx = cctx;
        this.tx = tx;

        futId = IgniteUuid.randomUuid();

        if (log == null)
            log = U.logger(cctx.kernalContext(), logRef, GridNearTxPrepareFuture.class);
    }

    /** {@inheritDoc} */
    @Override public IgniteUuid futureId() {
        return futId;
    }

    /** {@inheritDoc} */
    @Override public GridCacheVersion version() {
        return tx.xidVersion();
    }

    /** {@inheritDoc} */
    @Override public boolean onOwnerChanged(GridCacheEntryEx entry, GridCacheMvccCandidate owner) {
        if (log.isDebugEnabled())
            log.debug("Transaction future received owner changed callback: " + entry);

        if (tx.optimistic()) {
            if ((entry.context().isNear() || entry.context().isLocal()) && owner != null && tx.hasWriteKey(entry.txKey())) {
                lockKeys.remove(entry.txKey());

                // This will check for locks.
                onDone();

                return true;
            }
        }

        return false;
    }

    /**
     * @return Involved nodes.
     */
    @Override public Collection<? extends ClusterNode> nodes() {
        return
            F.viewReadOnly(futures(), new IgniteClosure<IgniteInternalFuture<?>, ClusterNode>() {
                @Nullable @Override public ClusterNode apply(IgniteInternalFuture<?> f) {
                    if (isMini(f))
                        return ((MiniFuture)f).node();

                    return cctx.discovery().localNode();
                }
            });
    }

    /** {@inheritDoc} */
    @Override public boolean trackable() {
        return trackable;
    }

    /** {@inheritDoc} */
    @Override public void markNotTrackable() {
        trackable = false;
    }

    /** {@inheritDoc} */
    @Override public boolean onNodeLeft(UUID nodeId) {
        boolean found = false;

        for (IgniteInternalFuture<?> fut : futures())
            if (isMini(fut)) {
                MiniFuture f = (MiniFuture)fut;

                if (f.node().id().equals(nodeId)) {
                    f.onResult(new ClusterTopologyCheckedException("Remote node left grid (will retry): " + nodeId));

                    found = true;
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

    /**
     * @param e Error.
     */
    void onError(Throwable e) {
        onError(null, null, e);
    }

    /**
     * @param nodeId Sender.
     * @param res Result.
     */
    public void onResult(UUID nodeId, GridNearTxPrepareResponse res) {
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

        if (err == null)
            tx.state(PREPARED);

        if (super.onDone(tx, err)) {
            // Don't forget to clean up.
            cctx.mvcc().removeFuture(this);

            return true;
        }

        return false;
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
    private void onComplete() {
        if (super.onDone(tx, err.get()))
            // Don't forget to clean up.
            cctx.mvcc().removeFuture(this);
    }

    /**
     * Completes this future.
     */
    void complete() {
        onComplete();
    }

    /**
     * Waits for topology exchange future to be ready and then prepares user transaction.
     */
    public void prepare() {
        if (tx.optimistic()) {
            GridDhtTopologyFuture topFut = topologyReadLock();

            try {
                if (topFut == null) {
                    assert isDone();

                    return;
                }

                if (topFut.isDone()) {
                    try {
                        if (!tx.state(PREPARING)) {
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

                        tx.topologyVersion(topFut.topologyVersion());

                        // Make sure to add future before calling prepare.
                        cctx.mvcc().addFuture(this);

                        prepare0();
                    }
                    catch (TransactionTimeoutException | TransactionOptimisticException e) {
                        onError(cctx.localNodeId(), null, e);
                    }
                }
                else {
                    topFut.listen(new CI1<IgniteInternalFuture<AffinityTopologyVersion>>() {
                        @Override public void apply(IgniteInternalFuture<AffinityTopologyVersion> t) {
                            cctx.kernalContext().closure().runLocalSafe(new GridPlainRunnable() {
                                @Override public void run() {
                                    prepare();
                                }
                            });
                        }
                    });
                }
            }
            finally {
                topologyReadUnlock();
            }
        }
        else
            preparePessimistic();
    }

    /**
     * Acquires topology read lock.
     *
     * @return Topology ready future.
     */
    private GridDhtTopologyFuture topologyReadLock() {
        if (tx.activeCacheIds().isEmpty())
            return cctx.exchange().lastTopologyFuture();

        GridCacheContext<K, V> nonLocCtx = null;

        for (int cacheId : tx.activeCacheIds()) {
            GridCacheContext<K, V> cacheCtx = cctx.cacheContext(cacheId);

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
            GridCacheContext<K, V> nonLocalCtx = null;

            for (int cacheId : tx.activeCacheIds()) {
                GridCacheContext<K, V> cacheCtx = cctx.cacheContext(cacheId);

                if (!cacheCtx.isLocal()) {
                    nonLocalCtx = cacheCtx;

                    break;
                }
            }

            if (nonLocalCtx != null)
                nonLocalCtx.topology().readUnlock();
        }
    }

    /**
     * Initializes future.
     */
    private void prepare0() {
        assert tx.optimistic();

        try {
            prepare(
                tx.optimistic() && tx.serializable() ? tx.readEntries() : Collections.<IgniteTxEntry>emptyList(),
                tx.writeEntries());

            markInitialized();
        }
        catch (IgniteCheckedException e) {
            onDone(e);
        }
    }

    /**
     * @param reads Read entries.
     * @param writes Write entries.
     * @throws IgniteCheckedException If transaction is group-lock and some key was mapped to to the local node.
     */
    private void prepare(
        Iterable<IgniteTxEntry> reads,
        Iterable<IgniteTxEntry> writes
    ) throws IgniteCheckedException {
        assert tx.optimistic();

        AffinityTopologyVersion topVer = tx.topologyVersion();

        assert topVer.topologyVersion() > 0;

        txMapping = new GridDhtTxMapping<>();

        ConcurrentLinkedDeque8<GridDistributedTxMapping> mappings =
            new ConcurrentLinkedDeque8<>();

        if (!F.isEmpty(reads) || !F.isEmpty(writes)) {
            for (int cacheId : tx.activeCacheIds()) {
                GridCacheContext<K, V> cacheCtx = cctx.cacheContext(cacheId);

                if (CU.affinityNodes(cacheCtx, topVer).isEmpty()) {
                    onDone(new ClusterTopologyCheckedException("Failed to map keys for cache (all " +
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
     *
     */
    private void preparePessimistic() {
        Map<IgniteBiTuple<ClusterNode, Boolean>, GridDistributedTxMapping> mappings = new HashMap<>();

        AffinityTopologyVersion topVer = tx.topologyVersion();

        txMapping = new GridDhtTxMapping<>();

        for (IgniteTxEntry txEntry : tx.allEntries()) {
            GridCacheContext cacheCtx = txEntry.context();

            List<ClusterNode> nodes = cacheCtx.affinity().nodes(txEntry.key(), topVer);

            ClusterNode primary = F.first(nodes);

            boolean near = cacheCtx.isNear();

            IgniteBiTuple<ClusterNode, Boolean> key = F.t(primary, near);

            GridDistributedTxMapping nodeMapping = mappings.get(key);

            if (nodeMapping == null) {
                nodeMapping = new GridDistributedTxMapping(primary);

                nodeMapping.near(cacheCtx.isNear());

                mappings.put(key, nodeMapping);
            }

            txEntry.nodeId(primary.id());

            nodeMapping.add(txEntry);

            txMapping.addMapping(nodes);
        }

        tx.transactionNodes(txMapping.transactionNodes());

        checkOnePhase();

        for (final GridDistributedTxMapping m : mappings.values()) {
            final ClusterNode node = m.node();

            GridNearTxPrepareRequest req = new GridNearTxPrepareRequest(
                futId,
                tx.topologyVersion(),
                tx,
                m.reads(),
                m.writes(),
                /*grp lock key*/null,
                /*part lock*/false,
                m.near(),
                txMapping.transactionNodes(),
                true,
                txMapping.transactionNodes().get(node.id()),
                tx.onePhaseCommit(),
                tx.needReturnValue() && tx.implicit(),
                tx.implicitSingle(),
                tx.subjectId(),
                tx.taskNameHash());

            for (IgniteTxEntry txEntry : m.writes()) {
                if (txEntry.op() == TRANSFORM)
                    req.addDhtVersion(txEntry.txKey(), null);
            }

            final MiniFuture fut = new MiniFuture(m, null);

            req.miniId(fut.futureId());

            add(fut);

            if (node.isLocal()) {
                cctx.tm().txHandler().prepareTx(node.id(), tx, req, new CI1<GridNearTxPrepareResponse>() {
                    @Override public void apply(GridNearTxPrepareResponse res) {
                        fut.onResult(node.id(), res);
                    }
                });
            }
            else {
                try {
                    cctx.io().send(node, req, tx.ioPolicy());
                }
                catch (IgniteCheckedException e) {
                    // Fail the whole thing.
                    fut.onResult(e);
                }
            }
        }

        markInitialized();
    }

    /**
     * Checks if mapped transaction can be committed on one phase.
     * One-phase commit can be done if transaction maps to one primary node and not more than one backup.
     */
    private void checkOnePhase() {
        if (tx.storeUsed())
            return;

        Map<UUID, Collection<UUID>> map = txMapping.transactionNodes();

        if (map.size() == 1) {
            Map.Entry<UUID, Collection<UUID>> entry = F.firstEntry(map);

            assert entry != null;

            Collection<UUID> backups = entry.getValue();

            if (backups.size() <= 1)
                tx.onePhaseCommit(true);
        }
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
            tx.groupLockKey(),
            tx.partitionLock(),
            m.near(),
            txMapping.transactionNodes(),
            m.last(),
            m.lastBackups(),
            tx.onePhaseCommit(),
            tx.needReturnValue() && tx.implicit(),
            tx.implicitSingle(),
            tx.subjectId(),
            tx.taskNameHash());

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
            cctx.tm().txHandler().prepareTx(n.id(), tx, req, new CI1<GridNearTxPrepareResponse>() {
                @Override public void apply(GridNearTxPrepareResponse res) {
                    fut.onResult(n.id(), res);
                }
            });
        }
        else {
            assert !tx.groupLock() : "Got group lock transaction that is mapped on remote node [tx=" + tx +
                ", nodeId=" + n.id() + ']';

            try {
                cctx.io().send(n, req, tx.ioPolicy());
            }
            catch (IgniteCheckedException e) {
                // Fail the whole thing.
                fut.onResult(e);
            }
        }
    }

    /**
     * @param entry Transaction entry.
     * @param topVer Topology version.
     * @param cur Current mapping.
     * @throws IgniteCheckedException If transaction is group-lock and local node is not primary for key.
     * @return Mapping.
     */
    private GridDistributedTxMapping map(
        IgniteTxEntry entry,
        AffinityTopologyVersion topVer,
        GridDistributedTxMapping cur,
        boolean waitLock
    ) throws IgniteCheckedException {
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

        if (tx.groupLock() && !primary.isLocal())
            throw new IgniteCheckedException("Failed to prepare group lock transaction (local node is not primary for " +
                " key)[key=" + entry.key() + ", primaryNodeId=" + primary.id() + ']');

        // Must re-initialize cached entry while holding topology lock.
        if (cacheCtx.isNear())
            entry.cached(cacheCtx.nearTx().entryExx(entry.key(), topVer));
        else if (!cacheCtx.isLocal())
            entry.cached(cacheCtx.colocated().entryExx(entry.key(), topVer, true));
        else
            entry.cached(cacheCtx.local().entryEx(entry.key(), topVer));

        if (cacheCtx.isNear() || cacheCtx.isLocal()) {
            if (waitLock && entry.explicitVersion() == null) {
                if (!tx.groupLock() || tx.groupLockKey().equals(entry.txKey()))
                    lockKeys.add(entry.txKey());
            }
        }

        if (cur == null || !cur.node().id().equals(primary.id()) || cur.near() != cacheCtx.isNear()) {
            cur = new GridDistributedTxMapping(primary);

            // Initialize near flag right away.
            cur.near(cacheCtx.isNear());
        }

        cur.add(entry);

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
        return S.toString(GridNearTxPrepareFuture.class, this, super.toString());
    }

    /**
     * Mini-future for get operations. Mini-futures are only waiting on a single
     * node as opposed to multiple nodes.
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
                    GridNearTxPrepareFuture.this + ", mini=" + this + ']', e);
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
                    assert F.isEmpty(res.invalidPartitions());

                    for (Map.Entry<IgniteTxKey, CacheVersionedValue> entry : res.ownedValues().entrySet()) {
                        IgniteTxEntry txEntry = tx.entry(entry.getKey());

                        assert txEntry != null;

                        GridCacheContext cacheCtx = txEntry.context();

                        while (true) {
                            try {
                                if (cacheCtx.isNear()) {
                                    GridNearCacheEntry nearEntry = (GridNearCacheEntry)txEntry.cached();

                                    CacheVersionedValue tup = entry.getValue();

                                    nearEntry.resetFromPrimary(tup.value(), tx.xidVersion(),
                                        tup.version(), m.node().id(), tx.topologyVersion());
                                }
                                else if (txEntry.cached().detached()) {
                                    GridDhtDetachedCacheEntry detachedEntry = (GridDhtDetachedCacheEntry)txEntry.cached();

                                    CacheVersionedValue tup = entry.getValue();

                                    detachedEntry.resetFromPrimary(tup.value(), tx.xidVersion());
                                }

                                break;
                            }
                            catch (GridCacheEntryRemovedException ignored) {
                                // Retry.
                            }
                            catch (IgniteCheckedException e) {
                                // Fail the whole compound future.
                                onError(nodeId, mappings, e);

                                return;
                            }
                        }
                    }

                    tx.implicitSingleResult(res.returnValue());

                    for (IgniteTxKey key : res.filterFailedKeys()) {
                        IgniteTxEntry txEntry = tx.entry(key);

                        assert txEntry != null : "Missing tx entry for write key: " + key;

                        txEntry.op(NOOP);

                        assert txEntry.context() != null;

                        ExpiryPolicy expiry = txEntry.context().expiryForTxEntry(txEntry);

                        if (expiry != null)
                            txEntry.ttl(CU.toTtl(expiry.getExpiryForAccess()));
                    }

                    if (!m.empty()) {
                        // Register DHT version.
                        tx.addDhtVersion(m.node().id(), res.dhtVersion());

                        m.dhtVersion(res.dhtVersion());

                        if (m.near())
                            tx.readyNearLocks(m, res.pending(), res.committedVersions(), res.rolledbackVersions());
                    }

                    // Proceed prepare before finishing mini future.
                    if (mappings != null)
                        proceedPrepare(mappings);

                    // Finish this mini future.
                    onDone(tx);
                }
            }
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(MiniFuture.class, this, "done", isDone(), "cancelled", isCancelled(), "err", error());
        }
    }
}

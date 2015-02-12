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
import org.apache.ignite.internal.managers.discovery.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.cache.distributed.*;
import org.apache.ignite.internal.processors.cache.distributed.dht.*;
import org.apache.ignite.internal.processors.cache.transactions.*;
import org.apache.ignite.internal.processors.cache.version.*;
import org.apache.ignite.internal.transactions.*;
import org.apache.ignite.internal.util.future.*;
import org.apache.ignite.internal.util.lang.*;
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.jdk8.backport.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.*;

import static org.apache.ignite.internal.managers.communication.GridIoPolicy.*;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.*;
import static org.apache.ignite.transactions.IgniteTxState.*;

/**
 *
 */
public final class GridNearTxPrepareFuture<K, V> extends GridCompoundIdentityFuture<IgniteInternalTx<K, V>>
    implements GridCacheMvccFuture<K, V, IgniteInternalTx<K, V>> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Logger reference. */
    private static final AtomicReference<IgniteLogger> logRef = new AtomicReference<>();

    /** Context. */
    private GridCacheSharedContext<K, V> cctx;

    /** Future ID. */
    private IgniteUuid futId;

    /** Transaction. */
    @GridToStringExclude
    private GridNearTxLocal<K, V> tx;

    /** Logger. */
    private IgniteLogger log;

    /** Error. */
    @GridToStringExclude
    private AtomicReference<Throwable> err = new AtomicReference<>(null);

    /** Trackable flag. */
    private boolean trackable = true;

    /** Full information about transaction nodes mapping. */
    private GridDhtTxMapping<K, V> txMapping;

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public GridNearTxPrepareFuture() {
        // No-op.
    }

    /**
     * @param cctx Context.
     * @param tx Transaction.
     */
    public GridNearTxPrepareFuture(GridCacheSharedContext<K, V> cctx, final GridNearTxLocal<K, V> tx) {
        super(cctx.kernalContext(), new IgniteReducer<IgniteInternalTx<K, V>, IgniteInternalTx<K, V>>() {
            @Override public boolean collect(IgniteInternalTx<K, V> e) {
                return true;
            }

            @Override public IgniteInternalTx<K, V> reduce() {
                // Nothing to aggregate.
                return tx;
            }
        });

        assert cctx != null;
        assert tx != null;

        this.cctx = cctx;
        this.tx = tx;

        futId = IgniteUuid.randomUuid();

        log = U.logger(ctx, logRef, GridNearTxPrepareFuture.class);
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
    @Override public boolean onOwnerChanged(GridCacheEntryEx<K, V> entry, GridCacheMvccCandidate<K> owner) {
        if (log.isDebugEnabled())
            log.debug("Transaction future received owner changed callback: " + entry);

        if (entry.context().isNear() && owner != null && tx.hasWriteKey(entry.txKey())) {
            // This will check for locks.
            onDone();

            return true;
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
    void onError(@Nullable UUID nodeId, @Nullable Iterable<GridDistributedTxMapping<K, V>> mappings, Throwable e) {
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
        Collection<IgniteTxEntry<K, V>> checkEntries = tx.groupLock() ?
            Collections.singletonList(tx.groupLockEntry()) :
            tx.writeEntries();

        for (IgniteTxEntry<K, V> txEntry : checkEntries) {
            // Wait for near locks only.
            if (!txEntry.context().isNear())
                continue;

            while (true) {
                GridCacheEntryEx<K, V> cached = txEntry.cached();

                try {
                    GridCacheVersion ver = txEntry.explicitVersion() != null ?
                        txEntry.explicitVersion() : tx.xidVersion();

                    // If locks haven't been acquired yet, keep waiting.
                    if (!cached.lockedBy(ver)) {
                        if (log.isDebugEnabled())
                            log.debug("Transaction entry is not locked by transaction (will wait) [entry=" + cached +
                                ", tx=" + tx + ']');

                        return false;
                    }

                    break; // While.
                }
                // Possible if entry cached within transaction is obsolete.
                catch (GridCacheEntryRemovedException ignored) {
                    if (log.isDebugEnabled())
                        log.debug("Got removed entry in future onAllReplies method (will retry): " + txEntry);

                    txEntry.cached(txEntry.context().cache().entryEx(txEntry.key()), txEntry.keyBytes());
                }
            }
        }

        if (log.isDebugEnabled())
            log.debug("All locks are acquired for near prepare future: " + this);

        return true;
    }

    /**
     * @param nodeId Sender.
     * @param res Result.
     */
    public void onResult(UUID nodeId, GridNearTxPrepareResponse<K, V> res) {
        if (!isDone()) {
            for (IgniteInternalFuture<IgniteInternalTx<K, V>> fut : pending()) {
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
    @Override public boolean onDone(IgniteInternalTx<K, V> t, Throwable err) {
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
        GridDhtTopologyFuture topFut = topologyReadLock();

        try {
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

                    GridDiscoveryTopologySnapshot snapshot = topFut.topologySnapshot();

                    tx.topologyVersion(snapshot.topologyVersion());
                    tx.topologySnapshot(snapshot);

                    // Make sure to add future before calling prepare.
                    cctx.mvcc().addFuture(this);

                    prepare0();
                }
                catch (IgniteTxTimeoutCheckedException | IgniteTxOptimisticCheckedException e) {
                    onError(cctx.localNodeId(), null, e);
                }
                catch (IgniteCheckedException e) {
                    tx.setRollbackOnly();

                    String msg = "Failed to prepare transaction (will attempt rollback): " + this;

                    U.error(log, msg, e);

                    tx.rollbackAsync();

                    onError(null, null, new IgniteTxRollbackCheckedException(msg, e));
                }
            }
            else {
                topFut.syncNotify(false);

                topFut.listenAsync(new CI1<IgniteInternalFuture<Long>>() {
                    @Override public void apply(IgniteInternalFuture<Long> t) {
                        prepare();
                    }
                });
            }
        }
        finally {
            topologyReadUnlock();
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

        GridCacheContext<K, V> nonLocalCtx = null;

        for (int cacheId : tx.activeCacheIds()) {
            GridCacheContext<K, V> cacheCtx = cctx.cacheContext(cacheId);

            if (!cacheCtx.isLocal()) {
                nonLocalCtx = cacheCtx;

                break;
            }
        }

        if (nonLocalCtx == null)
            return cctx.exchange().lastTopologyFuture();

        nonLocalCtx.topology().readLock();

        return nonLocalCtx.topology().topologyVersionFuture();
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
                tx.optimistic() && tx.serializable() ? tx.readEntries() : Collections.<IgniteTxEntry<K, V>>emptyList(),
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
        Iterable<IgniteTxEntry<K, V>> reads,
        Iterable<IgniteTxEntry<K, V>> writes
    ) throws IgniteCheckedException {
        assert tx.optimistic();

        GridDiscoveryTopologySnapshot snapshot = tx.topologySnapshot();

        assert snapshot != null;

        long topVer = snapshot.topologyVersion();

        assert topVer > 0;

        for (int cacheId : tx.activeCacheIds()) {
            GridCacheContext<K, V> cacheCtx = cctx.cacheContext(cacheId);

            if (CU.affinityNodes(cacheCtx, topVer).isEmpty()) {
                onDone(new ClusterTopologyCheckedException("Failed to map keys for cache (all " +
                    "partition nodes left the grid): " + cacheCtx.name()));

                return;
            }
        }

        txMapping = new GridDhtTxMapping<>();

        ConcurrentLinkedDeque8<GridDistributedTxMapping<K, V>> mappings =
            new ConcurrentLinkedDeque8<>();

        // Assign keys to primary nodes.
        GridDistributedTxMapping<K, V> cur = null;

        for (IgniteTxEntry<K, V> read : reads) {
            GridDistributedTxMapping<K, V> updated = map(read, topVer, cur);

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

        for (IgniteTxEntry<K, V> write : writes) {
            GridDistributedTxMapping<K, V> updated = map(write, topVer, cur);

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

        proceedPrepare(mappings);
    }

    /**
     * Continues prepare after previous mapping successfully finished.
     *
     * @param mappings Queue of mappings.
     */
    private void proceedPrepare(final ConcurrentLinkedDeque8<GridDistributedTxMapping<K, V>> mappings) {
        if (isDone())
            return;

        final GridDistributedTxMapping<K, V> m = mappings.poll();

        if (m == null)
            return;

        assert !m.empty();

        final ClusterNode n = m.node();

        GridNearTxPrepareRequest<K, V> req = new GridNearTxPrepareRequest<>(
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
            tx.subjectId(),
            tx.taskNameHash());

        for (IgniteTxEntry<K, V> txEntry : m.writes()) {
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

        // If this is the primary node for the keys.
        if (n.isLocal()) {
            req.miniId(IgniteUuid.randomUuid());

            // At this point, if any new node joined, then it is
            // waiting for this transaction to complete, so
            // partition reassignments are not possible here.
            IgniteInternalFuture<IgniteInternalTx<K, V>> fut = cctx.tm().txHandler().prepareTx(n.id(), tx, req);

            // Add new future.
            add(new GridEmbeddedFuture<>(
                cctx.kernalContext(),
                fut,
                new C2<IgniteInternalTx<K, V>, Exception, IgniteInternalTx<K, V>>() {
                    @Override public IgniteInternalTx<K, V> apply(IgniteInternalTx<K, V> t, Exception ex) {
                        if (ex != null) {
                            onError(n.id(), mappings, ex);

                            return t;
                        }

                        IgniteTxLocalEx<K, V> dhtTx = (IgniteTxLocalEx<K, V>)t;

                        Collection<Integer> invalidParts = dhtTx.invalidPartitions();

                        assert F.isEmpty(invalidParts);

                        if (!m.empty()) {
                            tx.addDhtVersion(m.node().id(), dhtTx.xidVersion());

                            m.dhtVersion(dhtTx.xidVersion());

                            GridCacheVersion min = dhtTx.minVersion();

                            IgniteTxManager<K, V> tm = cctx.tm();

                            tx.readyNearLocks(m, Collections.<GridCacheVersion>emptyList(),
                                tm.committedVersions(min), tm.rolledbackVersions(min));
                        }

                        // Continue prepare before completing the future.
                        proceedPrepare(mappings);

                        return tx;
                    }
                }
            ));
        }
        else {
            assert !tx.groupLock() : "Got group lock transaction that is mapped on remote node [tx=" + tx +
                ", nodeId=" + n.id() + ']';

            MiniFuture fut = new MiniFuture(m, mappings);

            req.miniId(fut.futureId());

            add(fut); // Append new future.

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
    private GridDistributedTxMapping<K, V> map(IgniteTxEntry<K, V> entry, long topVer,
        GridDistributedTxMapping<K, V> cur) throws IgniteCheckedException {
        GridCacheContext<K, V> cacheCtx = entry.context();

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
            entry.cached(cacheCtx.nearTx().entryExx(entry.key(), topVer), entry.keyBytes());
        else if (!cacheCtx.isLocal())
            entry.cached(cacheCtx.colocated().entryExx(entry.key(), topVer, true), entry.keyBytes());
        else
            entry.cached(cacheCtx.local().entryEx(entry.key(), topVer), entry.keyBytes());

        if (cur == null || !cur.node().id().equals(primary.id()) || cur.near() != cacheCtx.isNear()) {
            cur = new GridDistributedTxMapping<>(primary);

            // Initialize near flag right away.
            cur.near(cacheCtx.isNear());
        }

        cur.add(entry);

        entry.nodeId(primary.id());

        if (cacheCtx.isNear()) {
            while (true) {
                try {
                    GridNearCacheEntry<K, V> cached = (GridNearCacheEntry<K, V>)entry.cached();

                    cached.dhtNodeId(tx.xidVersion(), primary.id());

                    break;
                }
                catch (GridCacheEntryRemovedException ignore) {
                    entry.cached(cacheCtx.near().entryEx(entry.key()), entry.keyBytes());
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
    private class MiniFuture extends GridFutureAdapter<IgniteInternalTx<K, V>> {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private final IgniteUuid futId = IgniteUuid.randomUuid();

        /** Keys. */
        @GridToStringInclude
        private GridDistributedTxMapping<K, V> m;

        /** Flag to signal some result being processed. */
        private AtomicBoolean rcvRes = new AtomicBoolean(false);

        /** Mappings to proceed prepare. */
        private ConcurrentLinkedDeque8<GridDistributedTxMapping<K, V>> mappings;

        /**
         * Empty constructor required for {@link Externalizable}.
         */
        public MiniFuture() {
            // No-op.
        }

        /**
         * @param m Mapping.
         * @param mappings Queue of mappings to proceed with.
         */
        MiniFuture(GridDistributedTxMapping<K, V> m,
            ConcurrentLinkedDeque8<GridDistributedTxMapping<K, V>> mappings) {
            super(cctx.kernalContext());

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
        public GridDistributedTxMapping<K, V> mapping() {
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
        void onResult(UUID nodeId, GridNearTxPrepareResponse<K, V> res) {
            if (isDone())
                return;

            if (rcvRes.compareAndSet(false, true)) {
                if (res.error() != null) {
                    // Fail the whole compound future.
                    onError(nodeId, mappings, res.error());
                }
                else {
                    assert F.isEmpty(res.invalidPartitions());

                    for (Map.Entry<IgniteTxKey<K>, GridTuple3<GridCacheVersion, V, byte[]>> entry : res.ownedValues().entrySet()) {
                        IgniteTxEntry<K, V> txEntry = tx.entry(entry.getKey());

                        assert txEntry != null;

                        GridCacheContext<K, V> cacheCtx = txEntry.context();

                        while (true) {
                            try {
                                if (cacheCtx.isNear()) {
                                    GridNearCacheEntry<K, V> nearEntry = (GridNearCacheEntry<K, V>)txEntry.cached();

                                    GridTuple3<GridCacheVersion, V, byte[]> tup = entry.getValue();

                                    nearEntry.resetFromPrimary(tup.get2(), tup.get3(), tx.xidVersion(),
                                        tup.get1(), m.node().id());
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

                    if (!m.empty()) {
                        // Register DHT version.
                        tx.addDhtVersion(m.node().id(), res.dhtVersion());

                        m.dhtVersion(res.dhtVersion());

                        tx.readyNearLocks(m, res.pending(), res.committedVersions(), res.rolledbackVersions());
                    }

                    // Proceed prepare before finishing mini future.
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

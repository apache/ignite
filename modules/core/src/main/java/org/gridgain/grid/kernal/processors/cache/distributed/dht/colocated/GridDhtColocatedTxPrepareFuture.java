/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed.dht.colocated;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.kernal.managers.discovery.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.kernal.processors.cache.distributed.*;
import org.gridgain.grid.kernal.processors.cache.distributed.dht.*;
import org.gridgain.grid.kernal.processors.cache.distributed.near.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.util.future.*;
import org.gridgain.grid.util.lang.*;
import org.gridgain.grid.util.tostring.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jdk8.backport.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.*;

import static org.gridgain.grid.cache.GridCacheTxState.*;
import static org.gridgain.grid.kernal.processors.cache.GridCacheOperation.*;

/**
 * Prepare future for colocated cache. For pessimistic transactions this future
 * never gets to real work as prepare step for pessimistic transactions is no-op
 * (locks are acquired on write or read).
 */
public class GridDhtColocatedTxPrepareFuture<K, V> extends GridCompoundIdentityFuture<GridCacheTxEx<K, V>>
    implements GridCacheFuture<GridCacheTxEx<K, V>> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Logger reference. */
    private static final AtomicReference<GridLogger> logRef = new AtomicReference<>();

    /** Context. */
    private GridCacheContext<K, V> cctx;

    /** Future ID. */
    private GridUuid futId;

    /** Transaction. */
    @GridToStringExclude
    private GridDhtColocatedTxLocal<K, V> tx;

    /** Logger. */
    private GridLogger log;

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
    public GridDhtColocatedTxPrepareFuture() {
        // No-op.
    }

    /**
     * @param cctx Context.
     * @param tx Transaction.
     */
    public GridDhtColocatedTxPrepareFuture(GridCacheContext<K, V> cctx, final GridDhtColocatedTxLocal<K, V> tx) {
        super(cctx.kernalContext(), new GridReducer<GridCacheTxEx<K, V>, GridCacheTxEx<K, V>>() {
            @Override public boolean collect(GridCacheTxEx<K, V> e) {
                return true;
            }

            @Override public GridCacheTxEx<K, V> reduce() {
                // Nothing to aggregate.
                return tx;
            }
        });

        assert cctx != null;
        assert tx != null;

        this.cctx = cctx;
        this.tx = tx;

        futId = GridUuid.randomUuid();

        log = U.logger(ctx, logRef, GridDhtColocatedTxPrepareFuture.class);
    }

    /** {@inheritDoc} */
    @Override public GridUuid futureId() {
        return futId;
    }

    /** {@inheritDoc} */
    @Override public GridCacheVersion version() {
        return tx.xidVersion();
    }

    /**
     * @return Involved nodes.
     */
    @Override public Collection<? extends GridNode> nodes() {
        return F.viewReadOnly(futures(), new GridClosure<GridFuture<?>, GridNode>() {
            @Nullable @Override public GridNode apply(GridFuture<?> f) {
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

        for (GridFuture<?> fut : futures())
            if (isMini(fut)) {
                MiniFuture f = (MiniFuture)fut;

                if (f.node().id().equals(nodeId)) {
                    f.onResult(new GridTopologyException("Remote node left grid (will retry): " + nodeId));

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
    void onError(@Nullable UUID nodeId, @Nullable Iterable<GridDistributedTxMapping<K, V>> mappings,
        Throwable e) {
        if (err.compareAndSet(null, e)) {
            boolean marked = tx.setRollbackOnly();

            if (e instanceof GridCacheTxOptimisticException) {
                assert nodeId != null : "Missing node ID for optimistic failure exception: " + e;

                tx.removeKeysMapping(nodeId, mappings);
            }
            else if (e instanceof GridCacheTxRollbackException) {
                if (marked) {
                    try {
                        tx.rollback();
                    }
                    catch (GridException ex) {
                        U.error(log, "Failed to automatically rollback transaction: " + tx, ex);
                    }
                }
            }

            onComplete();
        }
    }

    /**
     * @param nodeId Sender.
     * @param res Result.
     */
    void onResult(UUID nodeId, GridNearTxPrepareResponse<K, V> res) {
        if (!isDone()) {
            for (GridFuture<GridCacheTxEx<K, V>> fut : pending()) {
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
    @Override public boolean onDone(GridCacheTxEx<K, V> t, Throwable err) {
        if (isDone())
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
    private boolean isMini(GridFuture<?> f) {
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
     * Initializes future.
     */
    void prepare() {
        assert tx.optimistic();

        try {
            prepare(
                tx.optimistic() && tx.serializable() ? tx.readEntries() : Collections.<GridCacheTxEntry<K, V>>emptyList(),
                tx.writeEntries());

            markInitialized();
        }
        catch (GridException e) {
            onDone(e);
        }
    }

    /**
     * @param reads Read entries.
     * @param writes Write entries.
     * @throws GridException If transaction is group-lock and local node is not primary for key.
     */
    private void prepare(Iterable<GridCacheTxEntry<K, V>> reads, Iterable<GridCacheTxEntry<K, V>> writes)
        throws GridException {
        assert tx.optimistic();

        GridDiscoveryTopologySnapshot snapshot = tx.topologySnapshot();

        assert snapshot != null;

        long topVer = snapshot.topologyVersion();

        assert topVer > 0;

        if (CU.affinityNodes(cctx, topVer).isEmpty()) {
            onDone(new GridTopologyException("Failed to map keys for cache (all partition nodes left the grid)."));

            return;
        }

        txMapping = new GridDhtTxMapping<>();

        ConcurrentLinkedDeque8<GridDistributedTxMapping<K, V>> mappings =
            new ConcurrentLinkedDeque8<>();

        // Assign keys to primary nodes.
        GridDistributedTxMapping<K, V> cur = null;

        for (GridCacheTxEntry<K, V> read : reads) {
            GridDistributedTxMapping<K, V> updated = map(read, topVer, cur);

            if (cur != updated) {
                mappings.offer(updated);

                cur = updated;
            }
        }

        for (GridCacheTxEntry<K, V> write : writes) {
            GridDistributedTxMapping<K, V> updated = map(write, topVer, cur);

            if (cur != updated) {
                mappings.offer(updated);

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

        final GridNode n = m.node();

        Collection<GridCacheTxEntry<K, V>> reads = tx.serializable() ? m.reads() :
            Collections.<GridCacheTxEntry<K,V>>emptyList();

        Collection<GridCacheTxEntry<K, V>> writes = m.writes();

        // If this is the primary node for the keys.
        if (n.isLocal()) {
            // At this point, if any new node joined, then it is
            // waiting for this transaction to complete, so
            // partition reassignments are not possible here.
            GridFuture<GridCacheTxEx<K, V>> fut = cctx.colocated().prepareTxLocally(tx, reads, writes,
                txMapping.transactionNodes(), m.last(), m.lastBackups());

            // Add new future.
            add(fut.chain(new C1<GridFuture<GridCacheTxEx<K, V>>, GridCacheTxEx<K, V>>() {
                @Override public GridCacheTxEx<K, V> apply(GridFuture<GridCacheTxEx<K, V>> fut) {
                    try {
                        GridCacheTxEx<K, V> t = fut.get();

                        assert F.isEmpty(t.invalidPartitions());

                        // Continue prepare before completing the future.
                        proceedPrepare(mappings);

                        return tx;
                    }
                    catch (Exception e) {
                        onError(n.id(), mappings, e);

                        return null;
                    }
                }
            }));
        }
        else {
            assert !tx.groupLock() : "Got group lock transaction that is mapped on remote node [tx=" + tx +
                ", nodeId=" + n.id() + ']';

            GridNearTxPrepareRequest<K, V> req = new GridNearTxPrepareRequest<>(
                futId,
                tx.topologyVersion(),
                tx,
                tx.optimistic() && tx.serializable() ? m.reads() : null,
                m.writes(),
                /*grp lock key*/null,
                /*part lock*/false,
                tx.syncCommit(),
                tx.syncRollback(),
                txMapping.transactionNodes(),
                m.last(),
                m.lastBackups(),
                tx.subjectId(),
                tx.taskNameHash());

            for (GridCacheTxEntry<K, V> txEntry : m.writes()) {
                assert txEntry.cached().detached() : "Expected detached entry while preparign transaction " +
                    "[locNodeId=" + cctx.localNodeId() +
                    ", txEntry=" + txEntry + ']';

                if (txEntry.op() == TRANSFORM)
                    req.addDhtVersion(txEntry.key(), null);
            }

            MiniFuture fut = new MiniFuture(m, mappings);

            req.miniId(fut.futureId());

            add(fut); // Append new future.

            try {
                cctx.io().send(n, req);
            }
            catch (GridException e) {
                // Fail the whole thing.
                fut.onResult(e);
            }
        }
    }

    /**
     * @param entry Transaction entry.
     * @param topVer Topology version.
     * @param cur Current mapping.
     * @throws GridException If transaction is group-lock and local node is not primary for key.
     * @return Mapping.
     */
    private GridDistributedTxMapping<K, V> map(GridCacheTxEntry<K, V> entry, long topVer,
        GridDistributedTxMapping<K, V> cur) throws GridException {
        Collection<GridNode> nodes = cctx.affinity().nodes(entry.key(), topVer);

        txMapping.addMapping(nodes);

        GridNode primary = F.first(nodes);

        assert primary != null;

        if (log.isDebugEnabled())
            log.debug("Mapped key to primary node [key=" + entry.key() +
                ", part=" + cctx.affinity().partition(entry.key()) +
                ", primary=" + U.toShortString(primary) + ", topVer=" + topVer + ']');

        if (tx.groupLock() && !primary.isLocal())
            throw new GridException("Failed to prepare group lock transaction (local node is not primary for " +
                " key) [key=" + entry.key() + ", primaryNodeId=" + primary.id() + ']');

        // Must re-initialize cached entry while holding topology lock.
        entry.cached(cctx.colocated().entryExx(entry.key(), topVer, true), entry.keyBytes());

        if (cur == null || !cur.node().id().equals(primary.id()))
            cur = new GridDistributedTxMapping<>(primary);

        cur.add(entry);

        entry.nodeId(primary.id());

        return cur;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtColocatedTxPrepareFuture.class, this, super.toString());
    }

    /**
     * Mini-future for get operations. Mini-futures are only waiting on a single
     * node as opposed to multiple nodes.
     */
    private class MiniFuture extends GridFutureAdapter<GridCacheTxEx<K, V>> {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private final GridUuid futId = GridUuid.randomUuid();

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
        GridUuid futureId() {
            return futId;
        }

        /**
         * @return Node ID.
         */
        public GridNode node() {
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
                    GridDhtColocatedTxPrepareFuture.this + ", mini=" + this + ']', e);
        }

        /**
         * @param e Node failure.
         */
        void onResult(GridTopologyException e) {
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
         * @param nodeId Node ID.
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

                    try {
                        for (Map.Entry<K, GridTuple3<GridCacheVersion, V, byte[]>> entry : res.ownedValues().entrySet()) {
                            GridCacheTxEntry<K, V> txEntry = tx.entry(entry.getKey());

                            assert txEntry != null;

                            assert txEntry.cached().detached();

                            GridDhtDetachedCacheEntry<K, V> nearEntry =
                                (GridDhtDetachedCacheEntry<K, V>)txEntry.cached();

                            GridTuple3<GridCacheVersion, V, byte[]> tup = entry.getValue();

                            nearEntry.resetFromPrimary(tup.get2(), tup.get3(), tup.get1());
                        }
                    }
                    catch (GridException e) {
                        // Fail the whole compound future.
                        onError(nodeId, mappings, e);

                        return;
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

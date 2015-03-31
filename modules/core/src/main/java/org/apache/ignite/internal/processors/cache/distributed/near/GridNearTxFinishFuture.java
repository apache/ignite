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
import org.apache.ignite.internal.processors.cache.transactions.*;
import org.apache.ignite.internal.processors.cache.version.*;
import org.apache.ignite.internal.transactions.*;
import org.apache.ignite.internal.util.future.*;
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.apache.ignite.internal.processors.cache.GridCacheOperation.*;
import static org.apache.ignite.transactions.TransactionState.*;

/**
 *
 */
public final class GridNearTxFinishFuture<K, V> extends GridCompoundIdentityFuture<IgniteInternalTx>
    implements GridCacheFuture<IgniteInternalTx> {
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

    /** Commit flag. */
    private boolean commit;

    /** Error. */
    private AtomicReference<Throwable> err = new AtomicReference<>(null);

    /** Node mappings. */
    private ConcurrentMap<UUID, GridDistributedTxMapping> mappings;

    /** Trackable flag. */
    private boolean trackable = true;

    /**
     * @param cctx Context.
     * @param tx Transaction.
     * @param commit Commit flag.
     */
    public GridNearTxFinishFuture(GridCacheSharedContext<K, V> cctx, GridNearTxLocal tx, boolean commit) {
        super(cctx.kernalContext(), F.<IgniteInternalTx>identityReducer(tx));

        assert cctx != null;

        this.cctx = cctx;
        this.tx = tx;
        this.commit = commit;

        mappings = tx.mappings();

        futId = IgniteUuid.randomUuid();

        if (log == null)
            log = U.logger(cctx.kernalContext(), logRef, GridNearTxFinishFuture.class);
    }

    /** {@inheritDoc} */
    @Override public IgniteUuid futureId() {
        return futId;
    }

    /** {@inheritDoc} */
    @Override public GridCacheVersion version() {
        return tx.xidVersion();
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
    @Override public boolean onNodeLeft(UUID nodeId) {
        for (IgniteInternalFuture<?> fut : futures())
            if (isMini(fut)) {
                MiniFuture f = (MiniFuture)fut;

                if (f.node().id().equals(nodeId)) {
                    // Remove previous mapping.
                    mappings.remove(nodeId);

                    f.onResult(new ClusterTopologyCheckedException("Remote node left grid (will fail): " + nodeId));

                    return true;
                }
            }

        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean trackable() {
        return trackable;
    }

    /**
     * Marks this future as not trackable.
     */
    @Override public void markNotTrackable() {
        trackable = false;
    }

    /**
     * @param e Error.
     */
    void onError(Throwable e) {
        tx.commitError(e);

        if (err.compareAndSet(null, e)) {
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
            else if (tx.implicit() && tx.isSystemInvalidate()) { // Finish implicit transaction on heuristic error.
                try {
                    tx.close();
                }
                catch (IgniteCheckedException ex) {
                    U.error(log, "Failed to invalidate transaction: " + tx, ex);
                }
            }

            onComplete();
        }
    }

    /**
     * @param nodeId Sender.
     * @param res Result.
     */
    public void onResult(UUID nodeId, GridNearTxFinishResponse res) {
        if (!isDone())
            for (IgniteInternalFuture<IgniteInternalTx> fut : futures()) {
                if (isMini(fut)) {
                    MiniFuture f = (MiniFuture)fut;

                    if (f.futureId().equals(res.miniId())) {
                        assert f.node().id().equals(nodeId);

                        f.onResult(res);
                    }
                }
            }
    }

    /** {@inheritDoc} */
    @Override public boolean onDone(IgniteInternalTx tx, Throwable err) {
        if ((initialized() || err != null)) {
            if (this.tx.onePhaseCommit() && (this.tx.state() == COMMITTING))
                this.tx.tmCommit();

            Throwable th = this.err.get();

            if (super.onDone(tx, th != null ? th : err)) {
                if (error() instanceof IgniteTxHeuristicCheckedException) {
                    AffinityTopologyVersion topVer = this.tx.topologyVersion();

                    for (IgniteTxEntry e : this.tx.writeMap().values()) {
                        GridCacheContext cacheCtx = e.context();

                        try {
                            if (e.op() != NOOP && !cacheCtx.affinity().localNode(e.key(), topVer)) {
                                GridCacheEntryEx Entry = cacheCtx.cache().peekEx(e.key());

                                if (Entry != null)
                                    Entry.invalidate(null, this.tx.xidVersion());
                            }
                        }
                        catch (Throwable t) {
                            U.error(log, "Failed to invalidate entry.", t);
                        }
                    }
                }

                // Don't forget to clean up.
                cctx.mvcc().removeFuture(this);

                return true;
            }
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
        onDone(tx, err.get());
    }

    /**
     * @return Synchronous flag.
     */
    private boolean isSync() {
        return commit ? tx.syncCommit() : tx.syncRollback();
    }

    /**
     * Initializes future.
     */
    void finish() {
        if (tx.onePhaseCommit()) {
            // No need to send messages as transaction was already committed on remote node.
            // Finish local mapping only as we need send commit message to backups.
            for (GridDistributedTxMapping m : mappings.values()) {
                if (m.node().isLocal()) {
                    IgniteInternalFuture<IgniteInternalTx> fut = cctx.tm().txHandler().finishColocatedLocal(commit, tx);

                    // Add new future.
                    if (fut != null)
                        add(fut);
                }
            }

            markInitialized();

            return;
        }

        if (mappings != null) {
            finish(mappings.values());

            markInitialized();

            if (!isSync()) {
                boolean complete = true;

                for (IgniteInternalFuture<?> f : pending())
                    // Mini-future in non-sync mode gets done when message gets sent.
                    if (isMini(f) && !f.isDone())
                        complete = false;

                if (complete)
                    onComplete();
            }
        }
        else {
            assert !commit;

            try {
                tx.rollback();
            }
            catch (IgniteCheckedException e) {
                U.error(log, "Failed to rollback empty transaction: " + tx, e);
            }

            markInitialized();
        }
    }

    /**
     * @param mappings Mappings.
     */
    private void finish(Iterable<GridDistributedTxMapping> mappings) {
        // Create mini futures.
        for (GridDistributedTxMapping m : mappings)
            finish(m);
    }

    /**
     * @param m Mapping.
     */
    private void finish(GridDistributedTxMapping m) {
        ClusterNode n = m.node();

        assert !m.empty();

        GridNearTxFinishRequest req = new GridNearTxFinishRequest(
            futId,
            tx.xidVersion(),
            tx.threadId(),
            commit,
            tx.isInvalidate(),
            tx.system(),
            tx.ioPolicy(),
            tx.syncCommit(),
            tx.syncRollback(),
            m.explicitLock(),
            tx.storeEnabled(),
            tx.topologyVersion(),
            null,
            null,
            null,
            tx.size(),
            tx.subjectId(),
            tx.taskNameHash()
        );

        // If this is the primary node for the keys.
        if (n.isLocal()) {
            req.miniId(IgniteUuid.randomUuid());

            IgniteInternalFuture<IgniteInternalTx> fut = cctx.tm().txHandler().finish(n.id(), tx, req);

            // Add new future.
            if (fut != null)
                add(fut);
        }
        else {
            MiniFuture fut = new MiniFuture(m);

            req.miniId(fut.futureId());

            add(fut); // Append new future.

            if (tx.pessimistic())
                cctx.tm().beforeFinishRemote(n.id(), tx.threadId());

            try {
                cctx.io().send(n, req, tx.ioPolicy());

                // If we don't wait for result, then mark future as done.
                if (!isSync() && !m.explicitLock())
                    fut.onDone();
            }
            catch (ClusterTopologyCheckedException e) {
                // Remove previous mapping.
                mappings.remove(m.node().id());

                fut.onResult(e);
            }
            catch (IgniteCheckedException e) {
                // Fail the whole thing.
                fut.onResult(e);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridNearTxFinishFuture.class, this, super.toString());
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

        /**
         * @param m Mapping.
         */
        MiniFuture(GridDistributedTxMapping m) {
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
            if (log.isDebugEnabled())
                log.debug("Failed to get future result [fut=" + this + ", err=" + e + ']');

            // Fail.
            onDone(e);
        }

        /**
         * @param e Node failure.
         */
        void onResult(ClusterTopologyCheckedException e) {
            if (log.isDebugEnabled())
                log.debug("Remote node left grid while sending or waiting for reply (will fail): " + this);

            // Complete future with tx.
            onDone(tx);
        }

        /**
         * @param res Result callback.
         */
        void onResult(GridNearTxFinishResponse res) {
            if (res.error() != null)
                onDone(res.error());
            else
                onDone(tx);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(MiniFuture.class, this, "done", isDone(), "cancelled", isCancelled(), "err", error());
        }
    }
}

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
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.GridCacheFuture;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.distributed.GridDistributedTxMapping;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxFinishRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxFinishResponse;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxEntry;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.transactions.IgniteTxHeuristicCheckedException;
import org.apache.ignite.internal.transactions.IgniteTxRollbackCheckedException;
import org.apache.ignite.internal.util.future.GridCompoundIdentityFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.C1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.transactions.TransactionRollbackException;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.GridCacheOperation.NOOP;
import static org.apache.ignite.transactions.TransactionState.UNKNOWN;

/**
 *
 */
public final class GridNearTxFinishFuture<K, V> extends GridCompoundIdentityFuture<IgniteInternalTx>
    implements GridCacheFuture<IgniteInternalTx> {
    /** */
    public static final IgniteProductVersion FINISH_NEAR_ONE_PHASE_SINCE = IgniteProductVersion.fromString("1.4.0");

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
    @GridToStringInclude
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

        this.cctx = cctx;
        this.tx = tx;
        this.commit = commit;

        ignoreInterrupts(true);

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

    /**
     * @param nodeId Sender.
     * @param res Result.
     */
    public void onResult(UUID nodeId, GridDhtTxFinishResponse res) {
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
    @Override public boolean onDone(IgniteInternalTx tx0, Throwable err) {
        if ((initialized() || err != null)) {
            if (tx.needCheckBackup()) {
                assert tx.onePhaseCommit();

                if (err != null)
                    err = new TransactionRollbackException("Failed to commit transaction.", err);

                try {
                    tx.finish(err == null);
                }
                catch (IgniteCheckedException e) {
                    if (err != null)
                        err.addSuppressed(e);
                    else
                        err = e;
                }
            }

            if (tx.onePhaseCommit()) {
                finishOnePhase();

                tx.tmFinish(err == null);
            }

            Throwable th = this.err.get();

            if (super.onDone(tx0, th != null ? th : err)) {
                if (error() instanceof IgniteTxHeuristicCheckedException) {
                    AffinityTopologyVersion topVer = tx.topologyVersion();

                    for (IgniteTxEntry e : tx.writeMap().values()) {
                        GridCacheContext cacheCtx = e.context();

                        try {
                            if (e.op() != NOOP && !cacheCtx.affinity().localNode(e.key(), topVer)) {
                                GridCacheEntryEx entry = cacheCtx.cache().peekEx(e.key());

                                if (entry != null)
                                    entry.invalidate(null, tx.xidVersion());
                            }
                        }
                        catch (Throwable t) {
                            U.error(log, "Failed to invalidate entry.", t);

                            if (t instanceof Error)
                                throw (Error)t;
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
        return tx.explicitLock() || (commit ? tx.syncCommit() : tx.syncRollback());
    }

    /**
     * Initializes future.
     */
    void finish() {
        if (tx.needCheckBackup()) {
            assert tx.onePhaseCommit();

            checkBackup();

            // If checkBackup is set, it means that primary node has crashed and we will not need to send
            // finish request to it, so we can mark future as initialized.
            markInitialized();

            return;
        }

        try {
            if (tx.finish(commit) || (!commit && tx.state() == UNKNOWN)) {
                if ((tx.onePhaseCommit() && needFinishOnePhase()) || (!tx.onePhaseCommit() && mappings != null))
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
            else
                onError(new IgniteCheckedException("Failed to commit transaction: " + CU.txString(tx)));
        }
        catch (Error | RuntimeException e) {
            onError(e);

            throw e;
        }
        catch (IgniteCheckedException e) {
            onError(e);
        }
    }

    /**
     *
     */
    private void checkBackup() {
        assert mappings.size() <= 1;

        for (Map.Entry<UUID, GridDistributedTxMapping> entry : mappings.entrySet()) {
            UUID nodeId = entry.getKey();
            GridDistributedTxMapping mapping = entry.getValue();

            Collection<UUID> backups = tx.transactionNodes().get(nodeId);

            if (!F.isEmpty(backups)) {
                assert backups.size() == 1;

                UUID backupId = F.first(backups);

                ClusterNode backup = cctx.discovery().node(backupId);

                MiniFuture mini = new MiniFuture(backup, mapping);

                add(mini);

                // Nothing to do if backup has left the grid.
                if (backup == null) {
                    readyNearMappingFromBackup(mapping);

                    mini.onDone(new IgniteTxRollbackCheckedException("Failed to commit transaction " +
                        "(backup has left grid): " + tx.xidVersion()));
                }
                else if (backup.isLocal()) {
                    boolean committed = cctx.tm().txHandler().checkDhtRemoteTxCommitted(tx.xidVersion());

                    readyNearMappingFromBackup(mapping);

                    if (committed)
                        mini.onDone(tx);
                    else
                        mini.onDone(new IgniteTxRollbackCheckedException("Failed to commit transaction " +
                            "(transaction has been rolled back on backup node): " + tx.xidVersion()));
                }
                else {
                    GridDhtTxFinishRequest finishReq = new GridDhtTxFinishRequest(
                        cctx.localNodeId(),
                        futureId(),
                        mini.futureId(),
                        tx.topologyVersion(),
                        tx.xidVersion(),
                        tx.commitVersion(),
                        tx.threadId(),
                        tx.isolation(),
                        true,
                        false,
                        tx.system(),
                        tx.ioPolicy(),
                        false,
                        true,
                        true,
                        null,
                        null,
                        null,
                        null,
                        0,
                        null,
                        0);

                    finishReq.checkCommitted(true);

                    try {
                        if (FINISH_NEAR_ONE_PHASE_SINCE.compareTo(backup.version()) <= 0)
                            cctx.io().send(backup, finishReq, tx.ioPolicy());
                        else
                            mini.onDone(new IgniteTxHeuristicCheckedException("Failed to check for tx commit on " +
                                "the backup node (node has an old Ignite version) [rmtNodeId=" + backup.id() +
                                ", ver=" + backup.version() + ']'));
                    }
                    catch (ClusterTopologyCheckedException e) {
                        mini.onResult(e);
                    }
                    catch (IgniteCheckedException e) {
                        mini.onResult(e);
                    }
                }
            }
            else
                readyNearMappingFromBackup(mapping);
        }
    }

    /**
     *
     */
    private boolean needFinishOnePhase() {
        if (F.isEmpty(tx.mappings()))
            return false;

        assert tx.mappings().size() == 1;

        boolean finish = false;

        for (Integer cacheId : tx.activeCacheIds()) {
            GridCacheContext<K, V> cacheCtx = cctx.cacheContext(cacheId);

            if (cacheCtx.isNear()) {
                finish = true;

                break;
            }
        }

        if (finish) {
            GridDistributedTxMapping mapping = F.first(tx.mappings().values());

            if (FINISH_NEAR_ONE_PHASE_SINCE.compareTo(mapping.node().version()) > 0)
                finish = false;
        }

        return finish;
    }

    /**
     *
     */
    private void finishOnePhase() {
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
    }

    /**
     * @param mapping Mapping to finish.
     */
    private void readyNearMappingFromBackup(GridDistributedTxMapping mapping) {
        if (mapping.near()) {
            GridCacheVersion xidVer = tx.xidVersion();

            mapping.dhtVersion(xidVer, xidVer);

            tx.readyNearLocks(mapping, Collections.<GridCacheVersion>emptyList(), Collections.<GridCacheVersion>emptyList(),
                Collections.<GridCacheVersion>emptyList());
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
        Collection<String> futs = F.viewReadOnly(futures(), new C1<IgniteInternalFuture<?>, String>() {
            @SuppressWarnings("unchecked")
            @Override public String apply(IgniteInternalFuture<?> f) {
                if (isMini(f)) {
                    MiniFuture m = (MiniFuture)f;

                    return "[node=" + m.node().id() + ", loc=" + m.node().isLocal() + ", done=" + f.isDone() + "]";
                }
                else
                    return "[loc=true, done=" + f.isDone() + "]";
            }
        });

        return S.toString(GridNearTxFinishFuture.class, this,
            "innerFuts", futs,
            "super", super.toString());
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

        /** Backup check flag. */
        private ClusterNode backup;

        /**
         * @param m Mapping.
         */
        MiniFuture(GridDistributedTxMapping m) {
            this.m = m;
        }

        /**
         * @param backup Backup to check.
         * @param m Mapping associated with the backup.
         */
        MiniFuture(ClusterNode backup, GridDistributedTxMapping m) {
            this.backup = backup;
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
            assert m != null || backup != null;

            return backup != null ? backup : m.node();
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

            if (backup != null) {
                readyNearMappingFromBackup(m);

                onDone(e);
            }
            else
                // Complete future with tx.
                onDone(tx);
        }

        /**
         * @param res Result callback.
         */
        void onResult(GridNearTxFinishResponse res) {
            assert backup == null;

            if (res.error() != null)
                onDone(res.error());
            else
                onDone(tx);
        }

        /**
         * @param res Response.
         */
        void onResult(GridDhtTxFinishResponse res) {
            assert backup != null;

            readyNearMappingFromBackup(m);

            if (res.checkCommittedError() != null)
                onDone(res.checkCommittedError());
            else
                onDone(tx);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(MiniFuture.class, this, "done", isDone(), "cancelled", isCancelled(), "err", error());
        }
    }
}
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
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.NodeStoppingException;
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
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.transactions.TransactionRollbackException;

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

    /** Logger. */
    protected static IgniteLogger msgLog;

    /** Context. */
    private GridCacheSharedContext<K, V> cctx;

    /** Future ID. */
    private final IgniteUuid futId;

    /** Transaction. */
    @GridToStringInclude
    private GridNearTxLocal tx;

    /** Commit flag. */
    private boolean commit;

    /** Node mappings. */
    private IgniteTxMappings mappings;

    /** Trackable flag. */
    private boolean trackable = true;

    /** */
    private boolean finishOnePhaseCalled;

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

        if (log == null) {
            msgLog = cctx.txFinishMessageLogger();
            log = U.logger(cctx.kernalContext(), logRef, GridNearTxFinishFuture.class);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteUuid futureId() {
        return futId;
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

    /**
     * @return Transaction.
     */
    public GridNearTxLocal tx() {
        return tx;
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
     * @param nodeId Sender.
     * @param res Result.
     */
    public void onResult(UUID nodeId, GridNearTxFinishResponse res) {
        if (!isDone()) {
            boolean found = false;

            for (IgniteInternalFuture<IgniteInternalTx> fut : futures()) {
                if (isMini(fut)) {
                    MiniFuture f = (MiniFuture) fut;

                    if (f.futureId().equals(res.miniId())) {
                        found = true;

                        assert f.node().id().equals(nodeId);

                        f.onResult(res);
                    }
                }
            }

            if (!found && msgLog.isDebugEnabled()) {
                msgLog.debug("Near finish fut, failed to find mini future [txId=" + tx.nearXidVersion() +
                    ", node=" + nodeId +
                    ", res=" + res +
                    ", fut=" + this + ']');
            }
        }
        else {
            if (msgLog.isDebugEnabled()) {
                msgLog.debug("Near finish fut, response for finished future [txId=" + tx.nearXidVersion() +
                    ", node=" + nodeId +
                    ", res=" + res +
                    ", fut=" + this + ']');
            }
        }
    }

    /**
     * @param nodeId Sender.
     * @param res Result.
     */
    public void onResult(UUID nodeId, GridDhtTxFinishResponse res) {
        if (!isDone()) {
            boolean found = false;

            for (IgniteInternalFuture<IgniteInternalTx> fut : futures()) {
                if (isMini(fut)) {
                    MiniFuture f = (MiniFuture) fut;

                    if (f.futureId().equals(res.miniId())) {
                        found = true;

                        assert f.node().id().equals(nodeId);

                        f.onResult(res);
                    }
                }
            }

            if (!found && msgLog.isDebugEnabled()) {
                msgLog.debug("Near finish fut, failed to find mini future [txId=" + tx.nearXidVersion() +
                    ", node=" + nodeId +
                    ", res=" + res +
                    ", fut=" + this + ']');
            }
        }
        else {
            if (msgLog.isDebugEnabled()) {
                msgLog.debug("Near finish fut, response for finished future [txId=" + tx.nearXidVersion() +
                    ", node=" + nodeId +
                    ", res=" + res +
                    ", fut=" + this + ']');
            }
        }
    }

    /** {@inheritDoc} */
    @Override public boolean onDone(IgniteInternalTx tx0, Throwable err) {
        if (isDone())
            return false;

        synchronized (this) {
            if (isDone())
                return false;

            if (err != null) {
                tx.commitError(err);

                boolean marked = tx.setRollbackOnly();

                if (err instanceof NodeStoppingException)
                    return super.onDone(null, err);

                if (err instanceof IgniteTxRollbackCheckedException) {
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
            }

            if (initialized() || err != null) {
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
                    boolean commit = this.commit && err == null;

                    finishOnePhase(commit);

                    tx.tmFinish(commit);
                }

                if (super.onDone(tx0, err)) {
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
                    cctx.mvcc().removeFuture(futId);

                    return true;
                }
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
        onDone(tx);
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
    @SuppressWarnings("ForLoopReplaceableByForEach")
    void finish() {
        if (tx.onNeedCheckBackup()) {
            assert tx.onePhaseCommit();

            checkBackup();

            // If checkBackup is set, it means that primary node has crashed and we will not need to send
            // finish request to it, so we can mark future as initialized.
            markInitialized();

            return;
        }

        try {
            if (tx.finish(commit) || (!commit && tx.state() == UNKNOWN)) {
                if ((tx.onePhaseCommit() && needFinishOnePhase()) || (!tx.onePhaseCommit() && mappings != null)) {
                    if (mappings.single()) {
                        GridDistributedTxMapping mapping = mappings.singleMapping();

                        if (mapping != null)
                            finish(mapping);
                    }
                    else
                        finish(mappings.mappings());
                }

                markInitialized();

                if (!isSync() && !isDone()) {
                    boolean complete = true;

                    synchronized (futs) {
                        // Avoid collection copy and iterator creation.
                        for (int i = 0; i < futs.size(); i++) {
                            IgniteInternalFuture<IgniteInternalTx> f = futs.get(i);

                            if (isMini(f) && !f.isDone()) {
                                complete = false;

                                break;
                            }
                        }
                    }

                    if (complete)
                        onComplete();
                }
            }
            else
                onDone(new IgniteCheckedException("Failed to commit transaction: " + CU.txString(tx)));
        }
        catch (Error | RuntimeException e) {
            onDone(e);

            throw e;
        }
        catch (IgniteCheckedException e) {
            onDone(e);
        }
    }

    /**
     *
     */
    private void checkBackup() {
        GridDistributedTxMapping mapping = mappings.singleMapping();

        if (mapping != null) {
            UUID nodeId = mapping.node().id();

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

                    ClusterTopologyCheckedException cause =
                        new ClusterTopologyCheckedException("Backup node left grid: " + backupId);

                    cause.retryReadyFuture(cctx.nextAffinityReadyFuture(tx.topologyVersion()));

                    mini.onDone(new IgniteTxRollbackCheckedException("Failed to commit transaction " +
                        "(backup has left grid): " + tx.xidVersion(), cause));
                }
                else if (backup.isLocal()) {
                    boolean committed = !cctx.tm().addRolledbackTx(tx);

                    readyNearMappingFromBackup(mapping);

                    if (committed)
                        mini.onDone(tx);
                    else {
                        ClusterTopologyCheckedException cause =
                            new ClusterTopologyCheckedException("Primary node left grid: " + nodeId);

                        cause.retryReadyFuture(cctx.nextAffinityReadyFuture(tx.topologyVersion()));

                        mini.onDone(new IgniteTxRollbackCheckedException("Failed to commit transaction " +
                            "(transaction has been rolled back on backup node): " + tx.xidVersion(), cause));
                    }
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
                        0,
                        tx.activeCachesDeploymentEnabled());

                    finishReq.checkCommitted(true);

                    try {
                        if (FINISH_NEAR_ONE_PHASE_SINCE.compareTo(backup.version()) <= 0) {
                            cctx.io().send(backup, finishReq, tx.ioPolicy());

                            if (msgLog.isDebugEnabled()) {
                                msgLog.debug("Near finish fut, sent check committed request [" +
                                    "txId=" + tx.nearXidVersion() +
                                    ", node=" + backup.id() + ']');
                            }
                        }
                        else
                            mini.onDone(new IgniteTxHeuristicCheckedException("Failed to check for tx commit on " +
                                "the backup node (node has an old Ignite version) [rmtNodeId=" + backup.id() +
                                ", ver=" + backup.version() + ']'));
                    }
                    catch (ClusterTopologyCheckedException e) {
                        mini.onResult(e);
                    }
                    catch (IgniteCheckedException e) {
                        if (msgLog.isDebugEnabled()) {
                            msgLog.debug("Near finish fut, failed to send check committed request [" +
                                "txId=" + tx.nearXidVersion() +
                                ", node=" + backup.id() +
                                ", err=" + e + ']');
                        }

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
        if (tx.mappings().empty())
            return false;

        boolean finish = tx.txState().hasNearCache(cctx);

        if (finish) {
            GridDistributedTxMapping mapping = tx.mappings().singleMapping();

            if (FINISH_NEAR_ONE_PHASE_SINCE.compareTo(mapping.node().version()) > 0)
                finish = false;
        }

        return finish;
    }

    /**
     * @param commit Commit flag.
     */
    private void finishOnePhase(boolean commit) {
        assert Thread.holdsLock(this);

        if (finishOnePhaseCalled)
            return;

        finishOnePhaseCalled = true;

        GridDistributedTxMapping locMapping = mappings.localMapping();

        if (locMapping != null) {
            // No need to send messages as transaction was already committed on remote node.
            // Finish local mapping only as we need send commit message to backups.
            IgniteInternalFuture<IgniteInternalTx> fut = cctx.tm().txHandler().finishColocatedLocal(commit, tx);

            // Add new future.
            if (fut != null)
                add(fut);
        }
    }

    /**
     * @param mapping Mapping to finish.
     */
    private void readyNearMappingFromBackup(GridDistributedTxMapping mapping) {
        if (mapping.near()) {
            GridCacheVersion xidVer = tx.xidVersion();

            mapping.dhtVersion(xidVer, xidVer);

            tx.readyNearLocks(mapping,
                Collections.<GridCacheVersion>emptyList(),
                Collections.<GridCacheVersion>emptyList(),
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
            tx.taskNameHash(),
            tx.activeCachesDeploymentEnabled()
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

                if (msgLog.isDebugEnabled()) {
                    msgLog.debug("Near finish fut, sent request [" +
                        "txId=" + tx.nearXidVersion() +
                        ", node=" + n.id() + ']');
                }

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
                if (msgLog.isDebugEnabled()) {
                    msgLog.debug("Near finish fut, failed to send request [" +
                        "txId=" + tx.nearXidVersion() +
                        ", node=" + n.id() +
                        ", err=" + e + ']');
                }

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
            if (msgLog.isDebugEnabled()) {
                msgLog.debug("Near finish fut, mini future node left [txId=" + tx.nearXidVersion() +
                    ", node=" + m.node().id() + ']');
            }

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

            Throwable err = res.checkCommittedError();

            if (err != null) {
                if (err instanceof IgniteCheckedException) {
                    ClusterTopologyCheckedException cause =
                        ((IgniteCheckedException)err).getCause(ClusterTopologyCheckedException.class);

                    if (cause != null)
                        cause.retryReadyFuture(cctx.nextAffinityReadyFuture(tx.topologyVersion()));
                }

                onDone(err);
            }
            else
                onDone(tx);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(MiniFuture.class, this, "done", isDone(), "cancelled", isCancelled(), "err", error());
        }
    }
}

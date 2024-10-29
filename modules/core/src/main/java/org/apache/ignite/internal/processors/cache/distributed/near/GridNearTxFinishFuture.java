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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.NodeStoppingException;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.processors.cache.CacheInvalidStateException;
import org.apache.ignite.internal.processors.cache.GridCacheCompoundIdentityFuture;
import org.apache.ignite.internal.processors.cache.GridCacheReturn;
import org.apache.ignite.internal.processors.cache.GridCacheReturnCompletableWrapper;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.GridCacheVersionedFuture;
import org.apache.ignite.internal.processors.cache.distributed.GridDistributedTxMapping;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxFinishRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxFinishResponse;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.tracing.MTC;
import org.apache.ignite.internal.processors.tracing.Span;
import org.apache.ignite.internal.transactions.IgniteTxRollbackCheckedException;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.transactions.TransactionRollbackException;

import static java.util.Collections.emptySet;
import static java.util.stream.Stream.concat;
import static java.util.stream.Stream.of;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_ASYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.internal.processors.tracing.MTC.TraceSurroundings;
import static org.apache.ignite.internal.processors.tracing.MTC.support;
import static org.apache.ignite.internal.processors.tracing.SpanType.TX_NEAR_FINISH;
import static org.apache.ignite.transactions.TransactionState.COMMITTED;
import static org.apache.ignite.transactions.TransactionState.COMMITTING;
import static org.apache.ignite.transactions.TransactionState.UNKNOWN;

/**
 *
 */
public final class GridNearTxFinishFuture<K, V> extends GridCacheCompoundIdentityFuture<IgniteInternalTx> implements NearTxFinishFuture {
    /** */
    private static final long serialVersionUID = 0L;

    /** All owners left grid message. */
    public static final String ALL_PARTITION_OWNERS_LEFT_GRID_MSG =
        "Failed to commit a transaction (all partition owners have left the grid, partition data has been lost)";

    /** Tracing span. */
    private Span span;

    /** Logger reference. */
    private static final AtomicReference<IgniteLogger> logRef = new AtomicReference<>();

    /** Logger. */
    private static IgniteLogger log;

    /** Logger. */
    private static IgniteLogger msgLog;

    /** Context. */
    private final GridCacheSharedContext<K, V> cctx;

    /** Future ID. */
    private final IgniteUuid futId;

    /** Transaction. */
    @GridToStringInclude
    private final GridNearTxLocal tx;

    /** Commit flag. This flag used only for one-phase commit transaction. */
    private final boolean commit;

    /** Node mappings. */
    private final IgniteTxMappings mappings;

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
        super(F.identityReducer(tx));

        this.cctx = cctx;
        this.tx = tx;
        this.commit = commit;

        ignoreInterrupts();

        mappings = tx.mappings();

        futId = IgniteUuid.randomUuid();

        if (tx.explicitLock())
            tx.syncMode(FULL_SYNC);

        if (log == null) {
            msgLog = cctx.txFinishMessageLogger();
            log = U.logger(cctx.kernalContext(), logRef, GridNearTxFinishFuture.class);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean commit() {
        return commit;
    }

    /** {@inheritDoc} */
    @Override public IgniteUuid futureId() {
        return futId;
    }

    /** {@inheritDoc} */
    @Override public boolean onNodeLeft(UUID nodeId) {
        boolean found = false;

        for (IgniteInternalFuture<?> fut : futures()) {
            if (isMini(fut)) {
                MinFuture f = (MinFuture)fut;

                if (f.onNodeLeft(nodeId, true)) {
                    // Remove previous mapping.
                    mappings.remove(nodeId);

                    found = true;
                }
            }
        }

        return found;
    }

    /**
     * @return Transaction.
     */
    @Override public GridNearTxLocal tx() {
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
            FinishMiniFuture finishFut = null;

            compoundsReadLock();

            try {
                int size = futuresCountNoLock();

                for (int i = 0; i < size; i++) {
                    IgniteInternalFuture<IgniteInternalTx> fut = future(i);

                    if (fut.getClass() == FinishMiniFuture.class) {
                        FinishMiniFuture f = (FinishMiniFuture)fut;

                        if (f.futureId() == res.miniId()) {
                            assert f.primary().id().equals(nodeId);

                            finishFut = f;

                            break;
                        }
                    }
                }
            }
            finally {
                compoundsReadUnlock();
            }

            if (finishFut != null)
                finishFut.onNearFinishResponse(res);
            else {
                if (msgLog.isDebugEnabled()) {
                    msgLog.debug("Near finish fut, failed to find mini future [txId=" + tx.nearXidVersion() +
                        ", node=" + nodeId +
                        ", res=" + res +
                        ", fut=" + this + ']');
                }
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
                if (fut.getClass() == CheckBackupMiniFuture.class) {
                    CheckBackupMiniFuture f = (CheckBackupMiniFuture)fut;

                    if (f.futureId() == res.miniId()) {
                        found = true;

                        assert f.node().id().equals(nodeId);

                        if (res.returnValue() != null)
                            tx.implicitSingleResult(res.returnValue());

                        f.onDhtFinishResponse(res);
                    }
                }
                else if (fut.getClass() == CheckRemoteTxMiniFuture.class) {
                    CheckRemoteTxMiniFuture f = (CheckRemoteTxMiniFuture)fut;

                    if (f.futureId() == res.miniId())
                        f.onDhtFinishResponse(nodeId);
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
     *
     */
    void forceFinish() {
        onDone(tx, null, false);
    }

    /** {@inheritDoc} */
    @Override public boolean onDone(IgniteInternalTx tx0, Throwable err) {
        try (MTC.TraceSurroundings ignored = support(span)) {
            if (isDone())
                return false;

            synchronized (this) {
                if (isDone())
                    return false;

                boolean nodeStop = false;

                if (err != null) {
                    tx.setRollbackOnly();

                    nodeStop = err instanceof NodeStoppingException || cctx.kernalContext().failure().nodeStopping();
                }

                if (commit) {
                    if (tx.commitError() != null)
                        err = tx.commitError();
                    else if (err != null)
                        tx.commitError(err);
                }

                if (initialized() || err != null) {
                    if (tx.needCheckBackup()) {
                        assert tx.onePhaseCommit();

                        if (err != null)
                            err = new TransactionRollbackException("Failed to commit transaction.", err);

                        try {
                            tx.localFinish(err == null, true);
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

                        if (!nodeStop)
                            finishOnePhase(commit);

                        try {
                            tx.tmFinish(commit, nodeStop, true);
                        }
                        catch (IgniteCheckedException e) {
                            U.error(log, "Failed to finish tx: " + tx, e);

                            if (err == null)
                                err = e;
                        }
                    }

                    if (super.onDone(tx0, err)) {
                        // Don't forget to clean up.
                        cctx.mvcc().removeFuture(futId);

                        return true;
                    }
                }
            }

            return false;
        }
    }

    /**
     * @param fut Future.
     * @return {@code True} if mini-future.
     */
    private boolean isMini(IgniteInternalFuture<?> fut) {
        return fut.getClass() == FinishMiniFuture.class ||
            fut.getClass() == CheckBackupMiniFuture.class ||
            fut.getClass() == CheckRemoteTxMiniFuture.class;
    }

    /** {@inheritDoc} */
    @Override public void finish(final boolean commit, final boolean clearThreadMap, final boolean onTimeout) {
        try (TraceSurroundings ignored =
                 MTC.supportContinual(span = cctx.kernalContext().tracing().create(TX_NEAR_FINISH, MTC.span()))) {
            if (!cctx.mvcc().addFuture(this, futureId()))
                return;

            if (tx.onNeedCheckBackup()) {
                assert tx.onePhaseCommit();

                checkBackup();

                // If checkBackup is set, it means that primary node has crashed and we will not need to send
                // finish request to it, so we can mark future as initialized.
                markInitialized();

                return;
            }

            if (!commit && !clearThreadMap)
                rollbackAsyncSafe(onTimeout);
            else
                doFinish(commit, clearThreadMap);
        }
    }

    /**
     * Rollback tx when it's safe.
     * If current future is not lock future (enlist future) wait until completion and tries again.
     * Else cancel lock future (onTimeout=false) or wait for completion due to deadlock detection (onTimeout=true).
     *
     * @param onTimeout If {@code true} called from timeout handler.
     */
    private void rollbackAsyncSafe(boolean onTimeout) {
        IgniteInternalFuture<?> curFut = tx.tryRollbackAsync();

        if (curFut == null) { // Safe to rollback.
            doFinish(false, false);

            return;
        }

        if (curFut instanceof GridCacheVersionedFuture && !onTimeout) {
            try {
                curFut.cancel(); // Force cancellation.
            }
            catch (IgniteCheckedException e) {
                log.error("Failed to cancel lock for the transaction: " + CU.txString(tx), e);
            }
        }

        curFut.listen(() -> {
            try {
                curFut.get();

                rollbackAsyncSafe(onTimeout);
            }
            catch (IgniteCheckedException e) {
                doFinish(false, false);
            }
        });
    }

    /**
     * Finishes a transaction.
     *
     * @param commit Commit.
     * @param clearThreadMap Clear thread map.
     */
    private void doFinish(boolean commit, boolean clearThreadMap) {
        try {
            if (tx.localFinish(commit, clearThreadMap) || (!commit && tx.state() == UNKNOWN)) {
                // Cleanup transaction if heuristic failure.
                if (tx.state() == UNKNOWN)
                    cctx.tm().rollbackTx(tx, clearThreadMap, false);

                if ((tx.onePhaseCommit() && needFinishOnePhase(commit)) || (!tx.onePhaseCommit() && mappings != null)) {
                    if (mappings.single()) {
                        GridDistributedTxMapping mapping = mappings.singleMapping();

                        if (mapping != null) {
                            assert !hasFutures() || isDone() : futures();

                            finish(1, mapping, commit);
                        }
                    }
                    else {
                        assert !hasFutures() || isDone() : futures();

                        finish(mappings.mappings(), commit);
                    }
                }

                markInitialized();
            }
            else
                onDone(new IgniteCheckedException("Failed to " + (commit ? "commit" : "rollback") +
                    " transaction: " + CU.txString(tx)));
        }
        catch (Error | RuntimeException e) {
            onDone(e);

            throw e;
        }
        catch (IgniteCheckedException e) {
            onDone(e);
        }
        finally {
            if (commit &&
                tx.onePhaseCommit() &&
                !tx.writeMap().isEmpty()) // Readonly operations require no ack.
                ackBackup();
        }
    }

    /** {@inheritDoc} */
    @Override public void onNodeStop(IgniteCheckedException e) {
        super.onDone(tx, e);
    }

    /**
     *
     */
    private void ackBackup() {
        if (mappings.empty())
            return;

        if (!tx.needReturnValue() || !tx.implicit())
            return; // GridCacheReturn was not saved at backup.

        GridDistributedTxMapping mapping = mappings.singleMapping();

        if (mapping != null) {
            UUID nodeId = mapping.primary().id();

            Collection<UUID> backups = tx.transactionNodes().get(nodeId);

            if (!F.isEmpty(backups)) {
                assert backups.size() == 1 : backups;

                UUID backupId = F.first(backups);

                ClusterNode backup = cctx.discovery().node(backupId);

                // Nothing to do if backup has left the grid.
                if (backup != null) {
                    if (backup.isLocal())
                        cctx.tm().removeTxReturn(tx.xidVersion());
                    else
                        cctx.tm().sendDeferredAckResponse(backupId, tx.xidVersion());
                }
            }
        }
    }

    /**
     *
     */
    private void checkBackup() {
        assert !hasFutures() : futures();

        GridDistributedTxMapping mapping = mappings.singleMapping();

        if (mapping != null) {
            UUID nodeId = mapping.primary().id();

            Collection<UUID> backups = tx.transactionNodes().get(nodeId);

            if (!F.isEmpty(backups)) {
                assert backups.size() == 1;

                UUID backupId = F.first(backups);

                ClusterNode backup = cctx.discovery().node(backupId);

                // Nothing to do if backup has left the grid.
                if (backup == null) {
                    readyNearMappingFromBackup(mapping);

                    ClusterTopologyCheckedException cause =
                        new ClusterTopologyCheckedException("Backup node left grid: " + backupId);

                    cause.retryReadyFuture(cctx.nextAffinityReadyFuture(tx.topologyVersion()));

                    onDone(new IgniteTxRollbackCheckedException("Failed to commit transaction " +
                        "(backup has left grid): " + tx.xidVersion(), cause));
                }
                else {
                    final CheckBackupMiniFuture mini = new CheckBackupMiniFuture(1, backup, mapping);

                    add(mini);

                    if (backup.isLocal()) {
                        boolean committed = !cctx.tm().addRolledbackTx(tx);

                        readyNearMappingFromBackup(mapping);

                        if (committed) {
                            try {
                                if (tx.needReturnValue() && tx.implicit()) {
                                    GridCacheReturnCompletableWrapper wrapper =
                                        cctx.tm().getCommittedTxReturn(tx.xidVersion());

                                    assert wrapper != null : tx.xidVersion();

                                    GridCacheReturn retVal = wrapper.fut().get();

                                    assert retVal != null;

                                    tx.implicitSingleResult(retVal);
                                }

                                if (tx.syncMode() == FULL_SYNC) {
                                    GridCacheVersion nearXidVer = tx.nearXidVersion();

                                    assert nearXidVer != null : tx;

                                    IgniteInternalFuture<?> fut = cctx.tm().remoteTxFinishFuture(nearXidVer);

                                    fut.listen(() -> mini.onDone(tx));

                                    return;
                                }

                                mini.onDone(tx);
                            }
                            catch (IgniteCheckedException e) {
                                if (msgLog.isDebugEnabled()) {
                                    msgLog.debug("Near finish fut, failed to finish [" +
                                        "txId=" + tx.nearXidVersion() +
                                        ", node=" + backup.id() +
                                        ", err=" + e + ']');
                                }

                                mini.onDone(e);
                            }
                        }
                        else {
                            ClusterTopologyCheckedException cause =
                                new ClusterTopologyCheckedException("Primary node left grid: " + nodeId);

                            cause.retryReadyFuture(cctx.nextAffinityReadyFuture(tx.topologyVersion()));

                            mini.onDone(new IgniteTxRollbackCheckedException("Failed to commit transaction " +
                                "(transaction has been rolled back on backup node): " + tx.xidVersion(), cause));
                        }
                    }
                    else {
                        GridDhtTxFinishRequest finishReq = checkCommittedRequest(mini.futureId(), false);

                        try {
                            cctx.io().send(backup, finishReq, tx.ioPolicy());

                            if (msgLog.isDebugEnabled()) {
                                msgLog.debug("Near finish fut, sent check committed request [" +
                                    "txId=" + tx.nearXidVersion() +
                                    ", node=" + backup.id() + ']');
                            }
                        }
                        catch (ClusterTopologyCheckedException ignored) {
                            mini.onNodeLeft(backupId, false);
                        }
                        catch (IgniteCheckedException e) {
                            if (msgLog.isDebugEnabled()) {
                                msgLog.debug("Near finish fut, failed to send check committed request [" +
                                    "txId=" + tx.nearXidVersion() +
                                    ", node=" + backup.id() +
                                    ", err=" + e + ']');
                            }

                            mini.onDone(e);
                        }
                    }
                }
            }
            else
                readyNearMappingFromBackup(mapping);
        }
    }

    /**
     * @param commit Commit flag.
     * @return {@code True} if need to send finish request for one phase commit transaction.
     */
    private boolean needFinishOnePhase(boolean commit) {
        assert tx.onePhaseCommit();

        if (tx.mappings().empty())
            return false;

        if (!commit)
            return true;

        GridDistributedTxMapping mapping = tx.mappings().singleMapping();

        assert mapping != null;

        return mapping.hasNearCacheEntries();
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
        if (mapping.hasNearCacheEntries()) {
            GridCacheVersion xidVer = tx.xidVersion();

            mapping.dhtVersion(xidVer, xidVer);

            tx.readyNearLocks(mapping,
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.emptyList());
        }
    }

    /**
     * @param mappings Mappings.
     * @param commit Commit flag.
     */
    private void finish(Iterable<GridDistributedTxMapping> mappings, boolean commit) {
        int miniId = 0;

        // Create mini futures.
        for (GridDistributedTxMapping m : mappings)
            finish(++miniId, m, commit);
    }

    /**
     * @param miniId Mini future ID.
     * @param m Mapping.
     * @param commit Commit flag.
     */
    private void finish(int miniId, GridDistributedTxMapping m, boolean commit) {
        ClusterNode n = m.primary();

        assert !m.empty() : m + " " + tx.state();

        CacheWriteSynchronizationMode syncMode = tx.syncMode();

        if (m.explicitLock())
            syncMode = FULL_SYNC;

        GridNearTxFinishRequest req = new GridNearTxFinishRequest(
            futId,
            tx.xidVersion(),
            tx.threadId(),
            commit,
            tx.isInvalidate(),
            tx.system(),
            tx.ioPolicy(),
            syncMode,
            m.explicitLock(),
            tx.storeEnabled(),
            tx.topologyVersion(),
            null,
            null,
            null,
            tx.size(),
            tx.taskNameHash(),
            tx.activeCachesDeploymentEnabled()
        );

        // If this is the primary node for the keys.
        if (n.isLocal()) {
            req.miniId(miniId);

            IgniteInternalFuture<IgniteInternalTx> fut = cctx.tm().txHandler().finish(n.id(), tx, req);

            // Add new future.
            if (fut != null && syncMode == FULL_SYNC)
                add(fut);
        }
        else {
            FinishMiniFuture fut = new FinishMiniFuture(miniId, m);

            req.miniId(fut.futureId());

            add(fut); // Append new future.

            try {
                cctx.tm().sendTransactionMessage(n, req, tx, tx.ioPolicy());

                if (msgLog.isDebugEnabled()) {
                    msgLog.debug("Near finish fut, sent request [" +
                        "txId=" + tx.nearXidVersion() +
                        ", node=" + n.id() + ']');
                }

                boolean wait = syncMode != FULL_ASYNC;

                // If we don't wait for result, then mark future as done.
                if (!wait)
                    fut.onDone();
            }
            catch (ClusterTopologyCheckedException ignored) {
                // Remove previous mapping.
                mappings.remove(m.primary().id());

                fut.onNodeLeft(n.id(), false);
            }
            catch (IgniteCheckedException e) {
                if (msgLog.isDebugEnabled()) {
                    msgLog.debug("Near finish fut, failed to send request [" +
                        "txId=" + tx.nearXidVersion() +
                        ", node=" + n.id() +
                        ", err=" + e + ']');
                }

                // Fail the whole thing.
                fut.onDone(e);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        Collection<String> futs = F.viewReadOnly(futures(), (IgniteInternalFuture<?> f) -> {
            if (f.getClass() == FinishMiniFuture.class) {
                FinishMiniFuture fut = (FinishMiniFuture)f;

                ClusterNode node = fut.primary();

                if (node != null) {
                    return "FinishFuture[node=" + node.id() +
                        ", loc=" + node.isLocal() +
                        ", done=" + fut.isDone() + ']';
                }
                else
                    return "FinishFuture[node=null, done=" + fut.isDone() + ']';
            }
            else if (f.getClass() == CheckBackupMiniFuture.class) {
                CheckBackupMiniFuture fut = (CheckBackupMiniFuture)f;

                ClusterNode node = fut.node();

                if (node != null) {
                    return "CheckBackupFuture[node=" + node.id() +
                        ", loc=" + node.isLocal() +
                        ", done=" + f.isDone() + "]";
                }
                else
                    return "CheckBackupFuture[node=null, done=" + f.isDone() + "]";
            }
            else if (f.getClass() == CheckRemoteTxMiniFuture.class) {
                CheckRemoteTxMiniFuture fut = (CheckRemoteTxMiniFuture)f;

                return "CheckRemoteTxMiniFuture[nodes=" + fut.nodes() + ", done=" + f.isDone() + "]";
            }
            else
                return "[loc=true, done=" + f.isDone() + "]";
        });

        return S.toString(GridNearTxFinishFuture.class, this,
            "innerFuts", futs,
            "super", super.toString());
    }

    /**
     * @param miniId Mini future ID.
     * @param waitRemoteTxs Wait for remote txs.
     * @return Finish request.
     */
    private GridDhtTxFinishRequest checkCommittedRequest(int miniId, boolean waitRemoteTxs) {
        GridDhtTxFinishRequest finishReq = new GridDhtTxFinishRequest(
            cctx.localNodeId(),
            futureId(),
            miniId,
            tx.topologyVersion(),
            tx.xidVersion(),
            tx.commitVersion(),
            tx.threadId(),
            true,
            false,
            tx.system(),
            tx.ioPolicy(),
            false,
            tx.syncMode(),
            null,
            null,
            null,
            0,
            0,
            tx.activeCachesDeploymentEnabled(),
            !waitRemoteTxs && (tx.needReturnValue() && tx.implicit()),
            waitRemoteTxs,
            null);

        finishReq.checkCommitted(true);

        return finishReq;
    }

    /**
     *
     */
    private abstract static class MinFuture extends GridFutureAdapter<IgniteInternalTx> {
        /** */
        private final int futId;

        /**
         * @param futId Future ID.
         */
        MinFuture(int futId) {
            this.futId = futId;
        }

        /**
         * @param nodeId Node ID.
         * @param discoThread {@code True} if executed from discovery thread.
         * @return {@code True} if future processed node failure.
         */
        abstract boolean onNodeLeft(UUID nodeId, boolean discoThread);

        /**
         * @return Future ID.
         */
        final int futureId() {
            return futId;
        }
    }

    /**
     *
     */
    private class FinishMiniFuture extends MinFuture {
        /** Keys. */
        @GridToStringInclude
        private final GridDistributedTxMapping m;

        /**
         * @param futId Future ID.
         * @param m Mapping.
         */
        FinishMiniFuture(int futId, GridDistributedTxMapping m) {
            super(futId);

            this.m = m;
        }

        /**
         * @return Node ID.
         */
        ClusterNode primary() {
            return m.primary();
        }

        /** {@inheritDoc} */
        @Override boolean onNodeLeft(UUID nodeId, boolean discoThread) {
            if (tx.state() == COMMITTING || tx.state() == COMMITTED) {
                if (concat(of(m.primary().id()), tx.transactionNodes().getOrDefault(m.primary().id(), emptySet()).stream())
                    .noneMatch(uuid -> cctx.discovery().alive(uuid))) {
                    onDone(new CacheInvalidStateException(ALL_PARTITION_OWNERS_LEFT_GRID_MSG +
                        m.entries().stream().map(e -> " [cacheName=" + e.cached().context().name() +
                            ", partition=" + e.key().partition() +
                            (S.includeSensitive() ? ", key=" + e.key() : "") +
                            "]").findFirst().orElse("")));

                    return true;
                }
            }

            if (nodeId.equals(m.primary().id())) {
                if (msgLog.isDebugEnabled()) {
                    msgLog.debug("Near finish fut, mini future node left [txId=" + tx.nearXidVersion() +
                        ", node=" + m.primary().id() + ']');
                }

                if (tx.syncMode() == FULL_SYNC) {
                    Map<UUID, Collection<UUID>> txNodes = tx.transactionNodes();

                    if (txNodes != null) {
                        Collection<UUID> backups = txNodes.get(nodeId);

                        if (!F.isEmpty(backups)) {
                            CheckRemoteTxMiniFuture mini = (CheckRemoteTxMiniFuture)compoundsLockedExclusively(() -> {
                                int futId = Integer.MIN_VALUE + futuresCountNoLock();

                                CheckRemoteTxMiniFuture miniFut = new CheckRemoteTxMiniFuture(futId, new HashSet<>(backups));

                                add(miniFut);

                                return miniFut;
                            });

                            GridDhtTxFinishRequest req = checkCommittedRequest(mini.futureId(), true);

                            for (UUID backupId : backups) {
                                ClusterNode backup = cctx.discovery().node(backupId);

                                if (backup != null) {
                                    if (backup.isLocal()) {
                                        IgniteInternalFuture<?> fut = cctx.tm().remoteTxFinishFuture(tx.nearXidVersion());

                                        fut.listen(() -> mini.onDhtFinishResponse(cctx.localNodeId()));
                                    }
                                    else {
                                        try {
                                            cctx.io().send(backup, req, tx.ioPolicy());
                                        }
                                        catch (ClusterTopologyCheckedException ignored) {
                                            mini.onNodeLeft(backupId, discoThread);
                                        }
                                        catch (IgniteCheckedException e) {
                                            mini.onDone(e);
                                        }
                                    }
                                }
                                else
                                    mini.onDhtFinishResponse(backupId);
                            }
                        }
                    }
                }

                onDone(tx);

                return true;
            }

            return false;
        }

        /**
         * @param res Result callback.
         */
        void onNearFinishResponse(GridNearTxFinishResponse res) {
            if (res.error() != null)
                if (res.error() instanceof IgniteTxRollbackCheckedException) {
                    // This exception is expected on asynchronous rollback.
                    if (log.isDebugEnabled())
                        log.debug("Transaction was rolled back: " + tx);

                    onDone(tx);
                }
                else
                    onDone(res.error());
            else
                onDone(tx);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(FinishMiniFuture.class, this, "done", isDone(), "cancelled", isCancelled(), "err", error());
        }
    }

    /**
     *
     */
    private class CheckBackupMiniFuture extends MinFuture {
        /** Keys. */
        @GridToStringInclude
        private final GridDistributedTxMapping m;

        /** Backup node to check. */
        private final ClusterNode backup;

        /**
         * @param futId Future ID.
         * @param backup Backup to check.
         * @param m Mapping associated with the backup.
         */
        CheckBackupMiniFuture(int futId, ClusterNode backup, GridDistributedTxMapping m) {
            super(futId);

            this.backup = backup;
            this.m = m;
        }

        /**
         * @return Node ID.
         */
        public ClusterNode node() {
            return backup;
        }

        /** {@inheritDoc} */
        @Override boolean onNodeLeft(UUID nodeId, boolean discoThread) {
            if (nodeId.equals(backup.id())) {
                readyNearMappingFromBackup(m);

                onDone(new ClusterTopologyCheckedException("Remote node left grid: " + nodeId));

                return true;
            }

            return false;
        }

        /**
         * @param res Response.
         */
        void onDhtFinishResponse(GridDhtTxFinishResponse res) {
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

    }

    /**
     *
     */
    private class CheckRemoteTxMiniFuture extends MinFuture {
        /** */
        private final Set<UUID> nodes;

        /**
         * @param futId Future ID.
         * @param nodes Backup nodes.
         */
        CheckRemoteTxMiniFuture(int futId, Set<UUID> nodes) {
            super(futId);

            this.nodes = nodes;
        }

        /**
         * @return Backup nodes.
         */
        Set<UUID> nodes() {
            synchronized (this) {
                return new HashSet<>(nodes);
            }
        }

        /** {@inheritDoc} */
        @Override boolean onNodeLeft(UUID nodeId, boolean discoThread) {
            return onResponse(nodeId);
        }

        /**
         * @param nodeId Node ID.
         */
        void onDhtFinishResponse(UUID nodeId) {
            onResponse(nodeId);
        }

        /**
         * @param nodeId Node ID.
         * @return {@code True} if processed node response.
         */
        private boolean onResponse(UUID nodeId) {
            boolean done;

            boolean ret;

            synchronized (this) {
                ret = nodes.remove(nodeId);

                done = nodes.isEmpty();
            }

            if (done)
                onDone(tx);

            return ret;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(CheckRemoteTxMiniFuture.class, this);
        }
    }
}

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
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheCompoundIdentityFuture;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.GridCacheFuture;
import org.apache.ignite.internal.processors.cache.GridCacheReturn;
import org.apache.ignite.internal.processors.cache.GridCacheReturnCompletableWrapper;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.distributed.GridDistributedTxMapping;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxFinishRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxFinishResponse;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxEntry;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.transactions.IgniteTxHeuristicCheckedException;
import org.apache.ignite.internal.transactions.IgniteTxRollbackCheckedException;
import org.apache.ignite.internal.transactions.IgniteTxTimeoutCheckedException;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.C1;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.transactions.TransactionRollbackException;

import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_ASYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.NOOP;
import static org.apache.ignite.transactions.TransactionState.UNKNOWN;

/**
 *
 */
public final class GridNearTxFinishFuture<K, V> extends GridCacheCompoundIdentityFuture<IgniteInternalTx>
    implements GridCacheFuture<IgniteInternalTx>, NearTxFinishFuture {
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

    /** Commit flag. This flag used only for one-phase commit transaction. */
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
        super(F.<IgniteInternalTx>identityReducer(tx));

        this.cctx = cctx;
        this.tx = tx;
        this.commit = commit;

        ignoreInterrupts();

        mappings = tx.mappings();

        futId = IgniteUuid.randomUuid();

        CacheWriteSynchronizationMode syncMode;

        if (tx.explicitLock())
            syncMode = FULL_SYNC;
        else
            syncMode = tx.syncMode();

        tx.syncMode(syncMode);

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
    @Override public void onTimeout() {
        finish(false, false);
    }

    /** {@inheritDoc} */
    @Override public IgniteUuid futureId() {
        return futId;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
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
    @SuppressWarnings("ForLoopReplaceableByForEach")
    public void onResult(UUID nodeId, GridNearTxFinishResponse res) {
        if (!isDone()) {
            FinishMiniFuture finishFut = null;

            synchronized (this) {
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
                        f.onDhtFinishResponse(nodeId, false);
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
        super.onDone(tx, null, false);
    }

    /** {@inheritDoc} */
    @Override public boolean onDone(IgniteInternalTx tx0, Throwable err) {
        if (isDone())
            return false;

        synchronized (this) {
            if (isDone())
                return false;

            boolean nodeStop = false;

            if (err != null) {
                tx.setRollbackOnly();

                nodeStop = err instanceof NodeStoppingException;
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
                    if (error() instanceof IgniteTxHeuristicCheckedException && !nodeStop) {
                        AffinityTopologyVersion topVer = tx.topologyVersion();

                        for (IgniteTxEntry e : tx.writeMap().values()) {
                            GridCacheContext cacheCtx = e.context();

                            try {
                                if (e.op() != NOOP && !cacheCtx.affinity().keyLocalNode(e.key(), topVer)) {
                                    GridCacheEntryEx entry = cacheCtx.cache().peekEx(e.key());

                                    if (entry != null)
                                        entry.invalidate(tx.xidVersion());
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
     * @param fut Future.
     * @return {@code True} if mini-future.
     */
    private boolean isMini(IgniteInternalFuture<?> fut) {
        return fut.getClass() == FinishMiniFuture.class ||
            fut.getClass() == CheckBackupMiniFuture.class ||
            fut.getClass() == CheckRemoteTxMiniFuture.class;
    }

    /**
     * Initializes future.
     *
     * @param commit Commit flag.
     * @param clearThreadMap If {@code true} removes {@link GridNearTxLocal} from thread map.
     */
    @SuppressWarnings("ForLoopReplaceableByForEach")
    public void finish(boolean commit, boolean clearThreadMap) {
        if (tx.onNeedCheckBackup()) {
            assert tx.onePhaseCommit();

            checkBackup();

            // If checkBackup is set, it means that primary node has crashed and we will not need to send
            // finish request to it, so we can mark future as initialized.
            markInitialized();

            return;
        }

        try {
            if (tx.localFinish(commit, clearThreadMap) || (!commit && tx.state() == UNKNOWN)) {
                if ((tx.onePhaseCommit() && needFinishOnePhase(commit)) || (!tx.onePhaseCommit() && mappings != null)) {
                    if (mappings.single()) {
                        GridDistributedTxMapping mapping = mappings.singleMapping();

                        if (mapping != null) {
                            assert !hasFutures() : futures();

                            finish(1, mapping, commit);
                        }
                    }
                    else
                        finish(mappings.mappings(), commit);
                }

                markInitialized();
            }
            else {
                if (commit)
                    onDone(new IgniteCheckedException("Failed to commit transaction: " + System.identityHashCode(tx)));
                else
                    onDone(new IgniteTxTimeoutCheckedException("Transaction timed out and was rolled back: " + System.identityHashCode(tx)));
            }
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
                if (backup == null) {
                    // No-op.
                }
                else if (backup.isLocal())
                    cctx.tm().removeTxReturn(tx.xidVersion());
                else
                    cctx.tm().sendDeferredAckResponse(backupId, tx.xidVersion());
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

                                    fut.listen(new CI1<IgniteInternalFuture<?>>() {
                                        @Override public void apply(IgniteInternalFuture<?> fut) {
                                            mini.onDone(tx);
                                        }
                                    });

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
                Collections.<GridCacheVersion>emptyList(),
                Collections.<GridCacheVersion>emptyList(),
                Collections.<GridCacheVersion>emptyList());
        }
    }

    /**
     * @param mappings Mappings.
     * @param commit Commit flag.
     */
    private void finish(Iterable<GridDistributedTxMapping> mappings, boolean commit) {
        assert !hasFutures() : futures();

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

        assert !m.empty() : m;

        CacheWriteSynchronizationMode syncMode = tx.syncMode();

        if (m.explicitLock())
            syncMode = FULL_SYNC;

        // Version to be added in completed versions on primary node.
        GridCacheVersion completedVer = !commit && tx.timeout() > 0 ? tx.xidVersion() : null;

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
            completedVer, // Reuse 'baseVersion' to do not add new fields in message.
            null,
            null,
            tx.size(),
            tx.subjectId(),
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

            if (tx.pessimistic())
                cctx.tm().beforeFinishRemote(n.id(), tx.threadId());

            try {
                cctx.io().send(n, req, tx.ioPolicy());

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
        Collection<String> futs = F.viewReadOnly(futures(), new C1<IgniteInternalFuture<?>, String>() {
            @SuppressWarnings("unchecked")
            @Override public String apply(IgniteInternalFuture<?> f) {
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
            }
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
            tx.isolation(),
            true,
            false,
            tx.system(),
            tx.ioPolicy(),
            false,
            tx.syncMode(),
            null,
            null,
            null,
            null,
            0,
            null,
            0,
            tx.activeCachesDeploymentEnabled(),
            !waitRemoteTxs && (tx.needReturnValue() && tx.implicit()),
            waitRemoteTxs);

        finishReq.checkCommitted(true);

        return finishReq;
    }

    /**
     *
     */
    private abstract class MinFuture extends GridFutureAdapter<IgniteInternalTx> {
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
        private GridDistributedTxMapping m;

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

        /**
         * @return Keys.
         */
        public GridDistributedTxMapping mapping() {
            return m;
        }

        /** {@inheritDoc} */
        boolean onNodeLeft(UUID nodeId, boolean discoThread) {
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
                            final CheckRemoteTxMiniFuture mini;

                            synchronized (GridNearTxFinishFuture.this) {
                                int futId = Integer.MIN_VALUE + futuresCountNoLock();

                                mini = new CheckRemoteTxMiniFuture(futId, new HashSet<>(backups));

                                add(mini);
                            }

                            GridDhtTxFinishRequest req = checkCommittedRequest(mini.futureId(), true);

                            for (UUID backupId : backups) {
                                ClusterNode backup = cctx.discovery().node(backupId);

                                if (backup != null) {
                                    if (backup.isLocal()) {
                                        IgniteInternalFuture<?> fut = cctx.tm().remoteTxFinishFuture(tx.nearXidVersion());

                                        fut.listen(new CI1<IgniteInternalFuture<?>>() {
                                            @Override public void apply(IgniteInternalFuture<?> fut) {
                                                mini.onDhtFinishResponse(cctx.localNodeId(), true);
                                            }
                                        });
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
                                    mini.onDhtFinishResponse(backupId, true);
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
        private GridDistributedTxMapping m;

        /** Backup node to check. */
        private ClusterNode backup;

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
        private Set<UUID> nodes;

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
         * @param discoThread {@code True} if executed from discovery thread.
         */
        void onDhtFinishResponse(UUID nodeId, boolean discoThread) {
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

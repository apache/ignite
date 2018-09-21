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

package org.apache.ignite.internal.processors.cache.distributed.dht;

import java.io.Externalizable;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.GridCacheEntryRemovedException;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.GridCacheMappedVersion;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxFinishResponse;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareResponse;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxEntry;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.transactions.IgniteTxOptimisticCheckedException;
import org.apache.ignite.internal.transactions.IgniteTxRollbackCheckedException;
import org.apache.ignite.internal.transactions.IgniteTxTimeoutCheckedException;
import org.apache.ignite.internal.util.tostring.GridToStringBuilder;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.cache.CacheWriteSynchronizationMode.PRIMARY_SYNC;
import static org.apache.ignite.internal.processors.cache.GridCacheUtils.isNearEnabled;
import static org.apache.ignite.transactions.TransactionState.PREPARED;
import static org.apache.ignite.transactions.TransactionState.PREPARING;

/**
 * Replicated user transaction.
 */
public class GridDhtTxLocal extends GridDhtTxLocalAdapter implements GridCacheMappedVersion {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private UUID nearNodeId;

    /** Near future ID. */
    private IgniteUuid nearFutId;

    /** Near future ID. */
    private int nearMiniId;

    /** Near future ID. */
    private IgniteUuid nearFinFutId;

    /** Near future ID. */
    private int nearFinMiniId;

    /** Near XID. */
    private GridCacheVersion nearXidVer;

    /** Future updater. */
    private static final AtomicReferenceFieldUpdater<GridDhtTxLocal, GridDhtTxPrepareFuture> PREP_FUT_UPD =
        AtomicReferenceFieldUpdater.newUpdater(GridDhtTxLocal.class, GridDhtTxPrepareFuture.class, "prepFut");

    /** Future. */
    @SuppressWarnings("UnusedDeclaration")
    @GridToStringExclude
    private volatile GridDhtTxPrepareFuture prepFut;

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public GridDhtTxLocal() {
        // No-op.
    }

    /**
     * @param nearNodeId Near node ID that initiated transaction.
     * @param nearXidVer Near transaction ID.
     * @param nearFutId Near future ID.
     * @param nearMiniId Near mini future ID.
     * @param nearThreadId Near thread ID.
     * @param implicit Implicit flag.
     * @param implicitSingle Implicit-with-single-key flag.
     * @param cctx Cache context.
     * @param concurrency Concurrency.
     * @param isolation Isolation.
     * @param timeout Timeout.
     * @param storeEnabled Store enabled flag.
     * @param txSize Expected transaction size.
     * @param txNodes Transaction nodes mapping.
     */
    public GridDhtTxLocal(
        GridCacheSharedContext cctx,
        AffinityTopologyVersion topVer,
        UUID nearNodeId,
        GridCacheVersion nearXidVer,
        IgniteUuid nearFutId,
        int nearMiniId,
        long nearThreadId,
        boolean implicit,
        boolean implicitSingle,
        boolean sys,
        boolean explicitLock,
        byte plc,
        TransactionConcurrency concurrency,
        TransactionIsolation isolation,
        long timeout,
        boolean invalidate,
        boolean storeEnabled,
        boolean onePhaseCommit,
        int txSize,
        Map<UUID, Collection<UUID>> txNodes,
        UUID subjId,
        int taskNameHash
    ) {
        super(
            cctx,
            cctx.versions().onReceivedAndNext(nearNodeId, nearXidVer),
            implicit,
            implicitSingle,
            sys,
            explicitLock,
            plc,
            concurrency,
            isolation,
            timeout,
            invalidate,
            storeEnabled,
            onePhaseCommit,
            txSize,
            subjId,
            taskNameHash);

        assert nearNodeId != null;
        assert nearFutId != null;
        assert nearXidVer != null;

        this.nearNodeId = nearNodeId;
        this.nearXidVer = nearXidVer;
        this.nearFutId = nearFutId;
        this.nearMiniId = nearMiniId;
        this.txNodes = txNodes;

        threadId = nearThreadId;

        assert !F.eq(xidVer, nearXidVer);

        initResult();

        assert topVer != null && topVer.topologyVersion() > 0 : topVer;

        topologyVersion(topVer);
    }

    /** {@inheritDoc} */
    @Override public UUID eventNodeId() {
        return nearNodeId;
    }

    /** {@inheritDoc} */
    @Override public Collection<UUID> masterNodeIds() {
        assert nearNodeId != null;

        return Collections.singleton(nearNodeId);
    }

    /** {@inheritDoc} */
    @Override public UUID otherNodeId() {
        assert nearNodeId != null;

        return nearNodeId;
    }

    /** {@inheritDoc} */
    @Override public UUID originatingNodeId() {
        return nearNodeId;
    }

    /** {@inheritDoc} */
    @Override protected UUID nearNodeId() {
        return nearNodeId;
    }

    /** {@inheritDoc} */
    @Override public GridCacheVersion nearXidVersion() {
        return nearXidVer;
    }

    /** {@inheritDoc} */
    @Override public GridCacheVersion mappedVersion() {
        return nearXidVer;
    }

    /** {@inheritDoc} */
    @Override protected IgniteUuid nearFutureId() {
        return nearFutId;
    }

    /**
     * @param nearFutId Near future ID.
     */
    public void nearFutureId(IgniteUuid nearFutId) {
        this.nearFutId = nearFutId;
    }

    /** {@inheritDoc} */
    @Override public boolean dht() {
        return true;
    }

    /** {@inheritDoc} */
    @Override protected boolean updateNearCache(GridCacheContext cacheCtx, KeyCacheObject key, AffinityTopologyVersion topVer) {
        return cacheCtx.isDht() && isNearEnabled(cacheCtx) && !cctx.localNodeId().equals(nearNodeId());
    }

    /**
     * @return Near future ID.
     */
    public IgniteUuid nearFinishFutureId() {
        return nearFinFutId;
    }

    /**
     * @param nearFinFutId Near future ID.
     */
    public void nearFinishFutureId(IgniteUuid nearFinFutId) {
        this.nearFinFutId = nearFinFutId;
    }

    /**
     * @param nearFinMiniId Near future mini ID.
     */
    public void nearFinishMiniId(int nearFinMiniId) {
        this.nearFinMiniId = nearFinMiniId;
    }

    /** {@inheritDoc} */
    @Override @Nullable protected IgniteInternalFuture<Boolean> addReader(long msgId, GridDhtCacheEntry cached,
        IgniteTxEntry entry, AffinityTopologyVersion topVer) {
        // Don't add local node as reader.
        if (entry.addReader() && !cctx.localNodeId().equals(nearNodeId)) {
            GridCacheContext cacheCtx = cached.context();

            while (true) {
                try {
                    return cached.addReader(nearNodeId, msgId, topVer);
                }
                catch (GridCacheEntryRemovedException ignore) {
                    if (log.isDebugEnabled())
                        log.debug("Got removed entry when adding to DHT local transaction: " + cached);

                    cached = cacheCtx.dht().entryExx(entry.key(), topVer);
                }
            }
        }

        return null;
    }

    /** {@inheritDoc} */
    @Override protected void updateExplicitVersion(IgniteTxEntry txEntry, GridCacheEntryEx entry)
        throws GridCacheEntryRemovedException {
        // DHT local transactions don't have explicit locks.
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> salvageTx() {
        systemInvalidate(true);

        state(PREPARED);

        if (state() == PREPARING) {
            if (log.isDebugEnabled())
                log.debug("Ignoring transaction in PREPARING state as it is currently handled " +
                    "by another thread: " + this);

            return null;
        }

        setRollbackOnly();

        return rollbackDhtLocalAsync();
    }

    /**
     * Prepares next batch of entries in dht transaction.
     *
     * @param req Prepare request.
     * @return Future that will be completed when locks are acquired.
     */
    public final IgniteInternalFuture<GridNearTxPrepareResponse> prepareAsync(GridNearTxPrepareRequest req) {
        // In optimistic mode prepare still can be called explicitly from salvageTx.
        GridDhtTxPrepareFuture fut = prepFut;

        long timeout = remainingTime();

        if (fut == null) {
            init();

            // Future must be created before any exception can be thrown.
            if (!PREP_FUT_UPD.compareAndSet(this, null, fut = new GridDhtTxPrepareFuture(
                cctx,
                this,
                timeout,
                req.miniId(),
                req.dhtVersions(),
                req.last(),
                needReturnValue()))) {
                GridDhtTxPrepareFuture f = prepFut;

                assert f.nearMiniId() == req.miniId() : "Wrong near mini id on existing future " +
                    "[futMiniId=" + f.nearMiniId() + ", miniId=" + req.miniId() + ", fut=" + f + ']';

                if (timeout == -1)
                    f.onError(timeoutException());

                return chainOnePhasePrepare(f);
            }
        }
        else {
            assert fut.nearMiniId() == req.miniId() : "Wrong near mini id on existing future " +
                "[futMiniId=" + fut.nearMiniId() + ", miniId=" + req.miniId() + ", fut=" + fut + ']';

            // Prepare was called explicitly.
            return chainOnePhasePrepare(fut);
        }

        if (state() != PREPARING) {
            if (!state(PREPARING)) {
                if (state() == PREPARED && isSystemInvalidate())
                    fut.complete();

                if (setRollbackOnly()) {
                    if (timeout == -1)
                        fut.onError(new IgniteTxTimeoutCheckedException("Transaction timed out and was rolled back: " +
                            this));
                    else
                        fut.onError(new IgniteCheckedException("Invalid transaction state for prepare [state=" + state() +
                            ", tx=" + this + ']'));
                }
                else
                    fut.onError(new IgniteTxRollbackCheckedException("Invalid transaction state for prepare [state=" +
                        state() + ", tx=" + this + ']'));

                return fut;
            }
        }

        try {
            if (req.reads() != null) {
                for (IgniteTxEntry e : req.reads())
                    addEntry(req.messageId(), e);
            }

            if (req.writes() != null) {
                for (IgniteTxEntry e : req.writes())
                    addEntry(req.messageId(), e);
            }

            userPrepare(null);

            // Make sure to add future before calling prepare on it.
            cctx.mvcc().addFuture(fut);

            if (isSystemInvalidate())
                fut.complete();
            else
                fut.prepare(req);
        }
        catch (IgniteTxTimeoutCheckedException | IgniteTxRollbackCheckedException | IgniteTxOptimisticCheckedException e) {
            fut.onError(e);
        }
        catch (IgniteCheckedException e) {
            setRollbackOnly();

            fut.onError(new IgniteTxRollbackCheckedException("Failed to prepare transaction: " + CU.txString(this), e));
        }

        return chainOnePhasePrepare(fut);
    }

    /**
     * @param commit Commit flag.
     * @param prepFut Prepare future.
     * @param fut Finish future.
     */
    private void finishTx(boolean commit, @Nullable IgniteInternalFuture prepFut, GridDhtTxFinishFuture fut) {
        assert prepFut == null || prepFut.isDone();

        boolean primarySync = syncMode() == PRIMARY_SYNC;

        IgniteCheckedException err = null;

        if (!commit) {
            final IgniteInternalFuture<?> lockFut = tryRollbackAsync();

            if (lockFut != null) {
                if (lockFut instanceof DhtLockFuture)
                    ((DhtLockFuture<?>)lockFut).onError(rollbackException());
                else if (!lockFut.isDone()) {
                    /*
                     * Prevents race with {@link GridDhtTransactionalCacheAdapter#lockAllAsync
                     * (GridCacheContext, ClusterNode, GridNearLockRequest, CacheEntryPredicate[])}
                     */
                    final IgniteInternalFuture finalPrepFut = prepFut;

                    lockFut.listen(new IgniteInClosure<IgniteInternalFuture<?>>() {
                        @Override public void apply(IgniteInternalFuture<?> ignored) {
                            finishTx(false, finalPrepFut, fut);
                        }
                    });

                    return;
                }
            }
        }

        if (!commit && prepFut != null) {
            try {
                prepFut.get();
            }
            catch (IgniteCheckedException e) {
                if (log.isDebugEnabled())
                    log.debug("Failed to prepare transaction [tx=" + this + ", e=" + e + ']');
            }
            finally {
                prepFut = null;
            }
        }

        try {
            if (prepFut != null)
                prepFut.get(); // Check for errors.

            boolean finished = localFinish(commit, false);

            if (!finished)
                err = new IgniteCheckedException("Failed to finish transaction [commit=" + commit +
                    ", tx=" + CU.txString(this) + ']');
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Failed to finish transaction [commit=" + commit + ", tx=" + this + ']', e);

            err = e;
        }
        catch (Throwable t) {
            fut.onDone(t);

            throw t;
        }

        if (primarySync)
            sendFinishReply(err);

        if (err != null)
            fut.rollbackOnError(err);
        else
            fut.finish(commit);
    }

    /**
     * @return Commit future.
     */
    public IgniteInternalFuture<IgniteInternalTx> commitDhtLocalAsync() {
        if (log.isDebugEnabled())
            log.debug("Committing dht local tx: " + this);

        final GridDhtTxFinishFuture fut = new GridDhtTxFinishFuture<>(cctx, this, true);

        cctx.mvcc().addFuture(fut, fut.futureId());

        GridDhtTxPrepareFuture prep = prepFut;

        if (prep != null) {
            if (prep.isDone())
                finishTx(true, prep, fut);
            else {
                prep.listen(new CI1<IgniteInternalFuture<?>>() {
                    @Override public void apply(IgniteInternalFuture<?> f) {
                        finishTx(true, f, fut);
                    }
                });
            }
        }
        else {
            assert optimistic();

            finishTx(true, null, fut);
        }

        return fut;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public IgniteInternalFuture<IgniteInternalTx> commitAsync() {
        return commitDhtLocalAsync();
    }

    /** {@inheritDoc} */
    @Override protected void clearPrepareFuture(GridDhtTxPrepareFuture fut) {
        assert optimistic();

        PREP_FUT_UPD.compareAndSet(this, fut, null);
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void rollbackDhtLocal() throws IgniteCheckedException {
        rollbackDhtLocalAsync().get();
    }

    /**
     * @return Rollback future.
     */
    public IgniteInternalFuture<IgniteInternalTx> rollbackDhtLocalAsync() {
        final GridDhtTxFinishFuture fut = new GridDhtTxFinishFuture<>(cctx, this, false);

        rollbackFuture(fut);

        cctx.mvcc().addFuture(fut, fut.futureId());

        GridDhtTxPrepareFuture prepFut = this.prepFut;

        if (prepFut != null) {
            prepFut.complete();

            prepFut.listen(new CI1<IgniteInternalFuture<?>>() {
                @Override public void apply(IgniteInternalFuture<?> f) {
                    finishTx(false, f, fut);
                }
            });
        }
        else
            finishTx(false, null, fut);

        return fut;
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<IgniteInternalTx> rollbackAsync() {
        return rollbackDhtLocalAsync();
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"CatchGenericClass", "ThrowableInstanceNeverThrown"})
    @Override public boolean localFinish(boolean commit, boolean clearThreadMap) throws IgniteCheckedException {
        assert nearFinFutId != null || isInvalidate() || !commit || isSystemInvalidate()
            || onePhaseCommit() || state() == PREPARED :
            "Invalid state [nearFinFutId=" + nearFinFutId + ", isInvalidate=" + isInvalidate() + ", commit=" + commit +
            ", sysInvalidate=" + isSystemInvalidate() + ", state=" + state() + ']';

        assert nearMiniId != 0;

        return super.localFinish(commit, clearThreadMap);
    }

    /** {@inheritDoc} */
    @Override protected void sendFinishReply(@Nullable Throwable err) {
        if (nearFinFutId != null) {
            if (nearNodeId.equals(cctx.localNodeId())) {
                if (log.isDebugEnabled())
                    log.debug("Skipping response sending to local node: " + this);

                return;
            }

            GridNearTxFinishResponse res = new GridNearTxFinishResponse(
                -1,
                nearXidVer,
                threadId,
                nearFinFutId,
                nearFinMiniId,
                err);

            try {
                cctx.io().send(nearNodeId, res, ioPolicy());

                if (cctx.txFinishMessageLogger().isDebugEnabled()) {
                    cctx.txFinishMessageLogger().debug("Sent near finish response [txId=" + nearXidVersion() +
                        ", dhtTxId=" + xidVersion() +
                        ", node=" + nearNodeId + ']');
                }
            }
            catch (ClusterTopologyCheckedException ignored) {
                if (cctx.txFinishMessageLogger().isDebugEnabled()) {
                    cctx.txFinishMessageLogger().debug("Failed to send near finish response, node left [txId=" + nearXidVersion() +
                        ", dhtTxId=" + xidVersion() +
                        ", node=" + nearNodeId() + ']');
                }
            }
            catch (Throwable ex) {
                U.error(log, "Failed to send finish response to node [txId=" + nearXidVersion() +
                    ", txState=" + state() +
                    ", dhtTxId=" + xidVersion() +
                    ", node=" + nearNodeId +
                    ", res=" + res + ']', ex);

                if (ex instanceof Error)
                    throw (Error)ex;
            }
        }
        else {
            if (cctx.txFinishMessageLogger().isDebugEnabled()) {
                cctx.txFinishMessageLogger().debug("Will not send finish reply because sender node has not sent finish " +
                    "request yet [txId=" + nearXidVersion() +
                    ", dhtTxId=" + xidVersion() +
                    ", node=" + nearNodeId() + ']');
            }
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Nullable @Override public IgniteInternalFuture<?> currentPrepareFuture() {
        return prepFut;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return GridToStringBuilder.toString(GridDhtTxLocal.class, this, "super", super.toString());
    }
}

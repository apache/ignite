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
import java.util.concurrent.atomic.AtomicReference;
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
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareResponse;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxEntry;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxKey;
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
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.GridCacheUtils.isNearEnabled;
import static org.apache.ignite.transactions.TransactionState.PREPARED;
import static org.apache.ignite.transactions.TransactionState.PREPARING;
import static org.apache.ignite.transactions.TransactionState.UNKNOWN;

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
    private IgniteUuid nearMiniId;

    /** Near future ID. */
    private IgniteUuid nearFinFutId;

    /** Near future ID. */
    private IgniteUuid nearFinMiniId;

    /** Near XID. */
    private GridCacheVersion nearXidVer;

    /** Future. */
    @GridToStringExclude
    private final AtomicReference<GridDhtTxPrepareFuture> prepFut =
        new AtomicReference<>();

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
        UUID nearNodeId,
        GridCacheVersion nearXidVer,
        IgniteUuid nearFutId,
        IgniteUuid nearMiniId,
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

        assert cctx != null;
        assert nearNodeId != null;
        assert nearFutId != null;
        assert nearMiniId != null;
        assert nearXidVer != null;

        this.nearNodeId = nearNodeId;
        this.nearXidVer = nearXidVer;
        this.nearFutId = nearFutId;
        this.nearMiniId = nearMiniId;
        this.txNodes = txNodes;

        threadId = nearThreadId;

        assert !F.eq(xidVer, nearXidVer);

        initResult();
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
     * @return Near future mini ID.
     */
    public IgniteUuid nearFinishMiniId() {
        return nearFinMiniId;
    }

    /**
     * @param nearFinMiniId Near future mini ID.
     */
    public void nearFinishMiniId(IgniteUuid nearFinMiniId) {
        this.nearFinMiniId = nearFinMiniId;
    }

    /** {@inheritDoc} */
    @Override @Nullable protected IgniteInternalFuture<Boolean> addReader(long msgId, GridDhtCacheEntry cached,
        IgniteTxEntry entry, AffinityTopologyVersion topVer) {
        // Don't add local node as reader.
        if (!cctx.localNodeId().equals(nearNodeId)) {
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
    @Override public IgniteInternalFuture<?> prepareAsync() {
        if (optimistic()) {
            assert isSystemInvalidate();

            return prepareAsync(
                null,
                null,
                Collections.<IgniteTxKey, GridCacheVersion>emptyMap(),
                0,
                nearMiniId,
                null,
                true);
        }

        // For pessimistic mode we don't distribute prepare request.
        GridDhtTxPrepareFuture fut = prepFut.get();

        if (fut == null) {
            // Future must be created before any exception can be thrown.
            if (!prepFut.compareAndSet(null, fut = new GridDhtTxPrepareFuture(
                cctx,
                this,
                nearMiniId,
                Collections.<IgniteTxKey, GridCacheVersion>emptyMap(),
                true,
                needReturnValue())))
                return prepFut.get();
        }
        else
            // Prepare was called explicitly.
            return fut;

        if (!state(PREPARING)) {
            if (setRollbackOnly()) {
                if (timedOut())
                    fut.onError(new IgniteTxTimeoutCheckedException("Transaction timed out and was rolled back: " + this));
                else
                    fut.onError(new IgniteCheckedException("Invalid transaction state for prepare [state=" + state() +
                        ", tx=" + this + ']'));
            }
            else
                fut.onError(new IgniteTxRollbackCheckedException("Invalid transaction state for prepare [state=" + state()
                    + ", tx=" + this + ']'));

            return fut;
        }

        try {
            userPrepare();

            if (!state(PREPARED)) {
                setRollbackOnly();

                fut.onError(new IgniteCheckedException("Invalid transaction state for commit [state=" + state() +
                    ", tx=" + this + ']'));

                return fut;
            }

            fut.complete();

            return fut;
        }
        catch (IgniteCheckedException e) {
            fut.onError(e);

            return fut;
        }
    }

    /**
     * Prepares next batch of entries in dht transaction.
     *
     * @param reads Read entries.
     * @param writes Write entries.
     * @param verMap Version map.
     * @param msgId Message ID.
     * @param nearMiniId Near mini future ID.
     * @param txNodes Transaction nodes mapping.
     * @param last {@code True} if this is last prepare request.
     * @return Future that will be completed when locks are acquired.
     */
    public IgniteInternalFuture<GridNearTxPrepareResponse> prepareAsync(
        @Nullable Collection<IgniteTxEntry> reads,
        @Nullable Collection<IgniteTxEntry> writes,
        Map<IgniteTxKey, GridCacheVersion> verMap,
        long msgId,
        IgniteUuid nearMiniId,
        Map<UUID, Collection<UUID>> txNodes,
        boolean last
    ) {
        // In optimistic mode prepare still can be called explicitly from salvageTx.
        GridDhtTxPrepareFuture fut = prepFut.get();

        if (fut == null) {
            init();

            // Future must be created before any exception can be thrown.
            if (!prepFut.compareAndSet(null, fut = new GridDhtTxPrepareFuture(
                cctx,
                this,
                nearMiniId,
                verMap,
                last,
                needReturnValue()))) {
                GridDhtTxPrepareFuture f = prepFut.get();

                assert f.nearMiniId().equals(nearMiniId) : "Wrong near mini id on existing future " +
                    "[futMiniId=" + f.nearMiniId() + ", miniId=" + nearMiniId + ", fut=" + f + ']';

                return chainOnePhasePrepare(f);
            }
        }
        else {
            assert fut.nearMiniId().equals(nearMiniId) : "Wrong near mini id on existing future " +
                "[futMiniId=" + fut.nearMiniId() + ", miniId=" + nearMiniId + ", fut=" + fut + ']';

            // Prepare was called explicitly.
            return chainOnePhasePrepare(fut);
        }

        if (state() != PREPARING) {
            if (!state(PREPARING)) {
                if (state() == PREPARED && isSystemInvalidate())
                    fut.complete();
                if (setRollbackOnly()) {
                    if (timedOut())
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
            if (reads != null) {
                for (IgniteTxEntry e : reads)
                    addEntry(msgId, e);
            }

            if (writes != null) {
                for (IgniteTxEntry e : writes)
                    addEntry(msgId, e);
            }

            userPrepare();

            // Make sure to add future before calling prepare on it.
            cctx.mvcc().addFuture(fut);

            if (isSystemInvalidate())
                fut.complete();
            else
                fut.prepare(reads, writes, txNodes);
        }
        catch (IgniteTxTimeoutCheckedException | IgniteTxOptimisticCheckedException e) {
            fut.onError(e);
        }
        catch (IgniteCheckedException e) {
            setRollbackOnly();

            fut.onError(new IgniteTxRollbackCheckedException("Failed to prepare transaction: " + this, e));

            try {
                rollback();
            }
            catch (IgniteTxOptimisticCheckedException e1) {
                if (log.isDebugEnabled())
                    log.debug("Failed optimistically to prepare transaction [tx=" + this + ", e=" + e1 + ']');

                fut.onError(e);
            }
            catch (IgniteCheckedException e1) {
                U.error(log, "Failed to rollback transaction: " + this, e1);
            }
        }

        return chainOnePhasePrepare(fut);
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"ThrowableInstanceNeverThrown"})
    @Override public IgniteInternalFuture<IgniteInternalTx> commitAsync() {
        if (log.isDebugEnabled())
            log.debug("Committing dht local tx: " + this);

        // In optimistic mode prepare was called explicitly.
        if (pessimistic())
            prepareAsync();

        final GridDhtTxFinishFuture fut = new GridDhtTxFinishFuture<>(cctx, this, /*commit*/true);

        cctx.mvcc().addFuture(fut, fut.futureId());

        GridDhtTxPrepareFuture prep = prepFut.get();

        if (prep != null) {
            if (prep.isDone()) {
                try {
                    prep.get(); // Check for errors of a parent future.

                    if (finish(true))
                        fut.finish();
                    else
                        fut.onError(new IgniteCheckedException("Failed to commit transaction: " + CU.txString(this)));
                }
                catch (IgniteTxOptimisticCheckedException e) {
                    if (log.isDebugEnabled())
                        log.debug("Failed to optimistically prepare transaction [tx=" + this + ", e=" + e + ']');

                    fut.onError(e);
                }
                catch (IgniteCheckedException e) {
                    U.error(log, "Failed to prepare transaction: " + this, e);

                    fut.onError(e);
                }
            }
            else
                prep.listen(new CI1<IgniteInternalFuture<?>>() {
                    @Override public void apply(IgniteInternalFuture<?> f) {
                        try {
                            f.get(); // Check for errors of a parent future.

                            if (finish(true))
                                fut.finish();
                            else
                                fut.onError(new IgniteCheckedException("Failed to commit transaction: " +
                                    CU.txString(GridDhtTxLocal.this)));
                        }
                        catch (IgniteTxOptimisticCheckedException e) {
                            if (log.isDebugEnabled())
                                log.debug("Failed optimistically to prepare transaction [tx=" + this + ", e=" + e + ']');

                            fut.onError(e);
                        }
                        catch (IgniteCheckedException e) {
                            U.error(log, "Failed to prepare transaction: " + this, e);

                            fut.onError(e);
                        }
                    }
                });
        }
        else {
            assert optimistic();

            try {
                if (finish(true))
                    fut.finish();
                else
                    fut.onError(new IgniteCheckedException("Failed to commit transaction: " + CU.txString(this)));
            }
            catch (IgniteTxOptimisticCheckedException e) {
                if (log.isDebugEnabled())
                    log.debug("Failed optimistically to prepare transaction [tx=" + this + ", e=" + e + ']');

                fut.onError(e);
            }
            catch (IgniteCheckedException e) {
                U.error(log, "Failed to commit transaction: " + this, e);

                fut.onError(e);
            }
        }

        return fut;
    }

    /** {@inheritDoc} */
    @Override protected void clearPrepareFuture(GridDhtTxPrepareFuture fut) {
        assert optimistic();

        prepFut.compareAndSet(fut, null);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<IgniteInternalTx> rollbackAsync() {
        GridDhtTxPrepareFuture prepFut = this.prepFut.get();

        final GridDhtTxFinishFuture fut = new GridDhtTxFinishFuture<>(cctx, this, /*rollback*/false);

        cctx.mvcc().addFuture(fut, fut.futureId());

        if (prepFut == null) {
            try {
                if (finish(false) || state() == UNKNOWN)
                    fut.finish();
                else
                    fut.onError(new IgniteCheckedException("Failed to rollback transaction: " + CU.txString(this)));
            }
            catch (IgniteTxOptimisticCheckedException e) {
                if (log.isDebugEnabled())
                    log.debug("Failed optimistically to prepare transaction [tx=" + this + ", e=" + e + ']');

                fut.onError(e);
            }
            catch (IgniteCheckedException e) {
                U.error(log, "Failed to rollback transaction (will make the best effort to rollback remote nodes): " +
                    this, e);

                fut.onError(e);
            }
        }
        else {
            prepFut.complete();

            prepFut.listen(new CI1<IgniteInternalFuture<?>>() {
                @Override public void apply(IgniteInternalFuture<?> f) {
                    try {
                        f.get(); // Check for errors of a parent future.
                    }
                    catch (IgniteCheckedException e) {
                        if (log.isDebugEnabled())
                            log.debug("Failed to prepare or rollback transaction [tx=" + this + ", e=" + e + ']');
                    }

                    try {
                        if (finish(false) || state() == UNKNOWN)
                            fut.finish();
                        else
                            fut.onError(new IgniteCheckedException("Failed to rollback transaction: " +
                                CU.txString(GridDhtTxLocal.this)));

                    }
                    catch (IgniteCheckedException e) {
                        U.error(log, "Failed to gracefully rollback transaction: " + CU.txString(GridDhtTxLocal.this),
                            e);

                        fut.onError(e);
                    }
                }
            });
        }

        return fut;
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"CatchGenericClass", "ThrowableInstanceNeverThrown"})
    @Override public boolean finish(boolean commit) throws IgniteCheckedException {
        assert nearFinFutId != null || isInvalidate() || !commit || isSystemInvalidate()
            || onePhaseCommit() || state() == PREPARED :
            "Invalid state [nearFinFutId=" + nearFinFutId + ", isInvalidate=" + isInvalidate() + ", commit=" + commit +
            ", sysInvalidate=" + isSystemInvalidate() + ", state=" + state() + ']';

        assert nearMiniId != null;

        return super.finish(commit);
    }

    /** {@inheritDoc} */
    @Override protected void sendFinishReply(boolean commit, @Nullable Throwable err) {
        if (nearFinFutId != null) {
            if (nearNodeId.equals(cctx.localNodeId())) {
                if (log.isDebugEnabled())
                    log.debug("Skipping response sending to local node: " + this);

                return;
            }

            GridNearTxFinishResponse res = new GridNearTxFinishResponse(nearXidVer, threadId, nearFinFutId,
                nearFinMiniId, err);

            try {
                cctx.io().send(nearNodeId, res, ioPolicy());
            }
            catch (ClusterTopologyCheckedException ignored) {
                if (log.isDebugEnabled())
                    log.debug("Node left before sending finish response (transaction was committed) [node=" +
                        nearNodeId + ", res=" + res + ']');
            }
            catch (Throwable ex) {
                U.error(log, "Failed to send finish response to node (transaction was " +
                    (commit ? "committed" : "rolledback") + ") [node=" + nearNodeId + ", res=" + res + ']', ex);

                if (ex instanceof Error)
                    throw (Error)ex;
            }
        }
        else {
            if (log.isDebugEnabled())
                log.debug("Will not send finish reply because sender node has not sent finish request yet: " + this);
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Nullable @Override public IgniteInternalFuture<?> currentPrepareFuture() {
        return prepFut.get();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return GridToStringBuilder.toString(GridDhtTxLocal.class, this, "super", super.toString());
    }
}

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

import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import org.apache.ignite.IgniteCacheRestartingException;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteFutureCancelledCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheStoppedException;
import org.apache.ignite.internal.processors.cache.GridCacheCompoundIdentityFuture;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.GridCacheMvccCandidate;
import org.apache.ignite.internal.processors.cache.GridCacheVersionedFuture;
import org.apache.ignite.internal.processors.cache.distributed.GridDistributedTxMapping;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTopologyFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxAbstractEnlistFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxLocalAdapter;
import org.apache.ignite.internal.processors.cache.mvcc.MvccSnapshot;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.timeout.GridTimeoutObjectAdapter;
import org.apache.ignite.internal.transactions.IgniteTxRollbackCheckedException;
import org.apache.ignite.internal.transactions.IgniteTxTimeoutCheckedException;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgniteReducer;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public abstract class GridNearTxAbstractEnlistFuture<T> extends GridCacheCompoundIdentityFuture<T> implements
    GridCacheVersionedFuture<T> {
    /** Done field updater. */
    private static final AtomicIntegerFieldUpdater<GridNearTxAbstractEnlistFuture> DONE_UPD =
        AtomicIntegerFieldUpdater.newUpdater(GridNearTxAbstractEnlistFuture.class, "done");

    /** Cache context. */
    @GridToStringExclude
    protected final GridCacheContext<?, ?> cctx;

    /** Transaction. */
    protected final GridNearTxLocal tx;

    /** */
    protected AffinityTopologyVersion topVer;

    /** MVCC snapshot. */
    protected MvccSnapshot mvccSnapshot;

    /** Logger. */
    @GridToStringExclude
    protected final IgniteLogger log;

    /** */
    protected long timeout;

    /** Initiated thread id. */
    protected final long threadId;

    /** Mvcc future id. */
    protected final IgniteUuid futId;

    /** Lock version. */
    protected final GridCacheVersion lockVer;

    /** */
    @GridToStringExclude
    private GridDhtTxAbstractEnlistFuture localEnlistFuture;

    /** */
    @SuppressWarnings("unused")
    @GridToStringExclude
    private volatile int done;

    /** Timeout object. */
    @GridToStringExclude
    protected LockTimeoutObject timeoutObj;

    /**
     * @param cctx Cache context.
     * @param tx Transaction.
     * @param timeout Timeout.
     * @param rdc Compound future reducer.
     */
    public GridNearTxAbstractEnlistFuture(
        GridCacheContext<?, ?> cctx, GridNearTxLocal tx, long timeout, @Nullable IgniteReducer<T, T> rdc) {
        super(rdc);

        assert cctx != null;
        assert tx != null;

        this.cctx = cctx;
        this.tx = tx;
        this.timeout = timeout;

        threadId = tx.threadId();
        lockVer = tx.xidVersion();
        futId = IgniteUuid.randomUuid();

        mvccSnapshot = tx.mvccSnapshot();

        assert mvccSnapshot != null;

        log = cctx.logger(getClass());
    }

    /**
     *
     */
    public void init() {
        if (timeout < 0) {
            // Time is out.
            onDone(timeoutException());

            return;
        }
        else if (timeout > 0)
            timeoutObj = new LockTimeoutObject();

        while (true) {
            IgniteInternalFuture<?> fut = tx.lockFuture();

            if (fut == GridDhtTxLocalAdapter.ROLLBACK_FUT) {
                onDone(tx.timedOut() ? tx.timeoutException() : tx.rollbackException());

                return;
            }
            else if (fut != null) {
                // Wait for previous future.
                assert fut instanceof GridNearTxAbstractEnlistFuture
                    || fut instanceof GridDhtTxAbstractEnlistFuture : fut;

                // Terminate this future if parent future is terminated by rollback.
                if (!fut.isDone()) {
                    fut.listen(new IgniteInClosure<IgniteInternalFuture>() {
                        @Override public void apply(IgniteInternalFuture fut) {
                            if (fut.error() != null)
                                onDone(fut.error());
                        }
                    });
                }
                else if (fut.error() != null)
                    onDone(fut.error());

                break;
            }
            else if (tx.updateLockFuture(null, this))
                break;
        }

        boolean added = cctx.mvcc().addFuture(this);

        assert added : this;

        if (isDone()) {
            cctx.mvcc().removeFuture(futId);

            return;
        }

        try {
            tx.addActiveCache(cctx, false);
        }
        catch (IgniteCheckedException e) {
            onDone(e);

            return;
        }

        if (timeoutObj != null)
            cctx.time().addTimeoutObject(timeoutObj);

        // Obtain the topology version to use.
        long threadId = Thread.currentThread().getId();

        AffinityTopologyVersion topVer = cctx.mvcc().lastExplicitLockTopologyVersion(threadId);

        // If there is another system transaction in progress, use it's topology version to prevent deadlock.
        if (topVer == null && tx.system())
            topVer = cctx.tm().lockedTopologyVersion(threadId, tx);

        if (topVer != null)
            tx.topologyVersion(topVer);

        if (topVer == null)
            topVer = tx.topologyVersionSnapshot();

        if (topVer != null) {
            for (GridDhtTopologyFuture fut : cctx.shared().exchange().exchangeFutures()) {
                if (fut.exchangeDone() && fut.topologyVersion().equals(topVer)) {
                    Throwable err = null;

                    // Before cache validation, make sure that this topology future is already completed.
                    try {
                        fut.get();
                    }
                    catch (IgniteCheckedException e) {
                        err = fut.error();
                    }

                    if (err == null)
                        err = fut.validateCache(cctx, false, false, null, null);

                    if (err != null) {
                        onDone(err);

                        return;
                    }

                    break;
                }
            }

            if (this.topVer == null)
                this.topVer = topVer;

            map(true);

            return;
        }

        mapOnTopology();
    }

    /**
     * @param node Primary node.
     * @throws IgniteCheckedException if future is already completed.
     */
    protected synchronized void updateMappings(ClusterNode node) throws IgniteCheckedException {
        checkCompleted();

        IgniteTxMappings m = tx.mappings();

        GridDistributedTxMapping mapping = m.get(node.id());

        if (mapping == null)
            m.put(mapping = new GridDistributedTxMapping(node));

        mapping.markQueryUpdate();

        if (node.isLocal())
            tx.colocatedLocallyMapped(true);

        checkCompleted();
    }

    /**
     * @param fut Local enlist future.
     * @throws IgniteCheckedException if future is already completed.
     */
    protected synchronized void updateLocalFuture(GridDhtTxAbstractEnlistFuture fut) throws IgniteCheckedException {
        checkCompleted();

        assert localEnlistFuture == null;

        localEnlistFuture = fut;
    }

    /**
     * @param fut Local enlist future.
     * @throws IgniteCheckedException if future is already completed.
     */
    protected synchronized void clearLocalFuture(GridDhtTxAbstractEnlistFuture fut) throws IgniteCheckedException {
        checkCompleted();

        if (localEnlistFuture == fut)
            localEnlistFuture = null;
    }

    /**
     * @throws IgniteCheckedException if future is already completed.
     */
    protected void checkCompleted() throws IgniteCheckedException {
        if (isDone())
            throw new IgniteCheckedException("Future is done.");
    }

    /**
     */
    private void mapOnTopology() {
        cctx.topology().readLock(); boolean topLocked = true;

        try {
            if (cctx.topology().stopping()) {
                onDone(
                    cctx.shared().cache().isCacheRestarting(cctx.name()) ?
                        new IgniteCacheRestartingException(cctx.name()) :
                        new CacheStoppedException(cctx.name()));

                return;
            }

            GridDhtTopologyFuture fut = cctx.topologyVersionFuture();

            cctx.topology().readUnlock(); topLocked = false;

            if (fut.isDone()) {
                Throwable err = fut.validateCache(cctx, false, false, null, null);

                if (err != null) {
                    onDone(err);

                    return;
                }

                AffinityTopologyVersion topVer = fut.topologyVersion();

                tx.topologyVersion(topVer);

                if (this.topVer == null)
                    this.topVer = topVer;

                map(false);
            }
            else {
                cctx.time().waitAsync(fut, tx.remainingTime(), (e, timedOut) -> {
                    try {
                        if (e != null || timedOut)
                            onDone(timedOut ? tx.timeoutException() : e);
                        else
                            mapOnTopology();
                    }
                    finally {
                        cctx.shared().txContextReset();
                    }
                });
            }
        }
        finally {
            if (topLocked)
                cctx.topology().readUnlock();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean onCancelled() {
        return onDone(null, asyncRollbackException(), false);
    }

    /** {@inheritDoc} */
    @Override public boolean onDone(@Nullable T res, @Nullable Throwable err, boolean cancelled) {
        if (!DONE_UPD.compareAndSet(this, 0, 1))
            return false;

        cctx.tm().txContext(tx);

        if (!cancelled && err == null)
            tx.clearLockFuture(this);
        else
            tx.setRollbackOnly();

        synchronized (this) {
            GridDhtTxAbstractEnlistFuture localFuture0 = localEnlistFuture;

            if (localFuture0 != null && (err != null || cancelled))
                localFuture0.onDone(cancelled ? new IgniteFutureCancelledCheckedException("Future was cancelled: " + localFuture0) : err);

            boolean done = super.onDone(res, err, cancelled);

            assert done;

            // Clean up.
            cctx.mvcc().removeVersionedFuture(this);

            if (timeoutObj != null)
                cctx.time().removeTimeoutObject(timeoutObj);

            return true;
        }
    }

    /** {@inheritDoc} */
    @Override protected void logError(IgniteLogger log, String msg, Throwable e) {
        // no-op
    }

    /** {@inheritDoc} */
    @Override protected void logDebug(IgniteLogger log, String msg) {
        // no-op
    }

    /** {@inheritDoc} */
    @Override public boolean trackable() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public void markNotTrackable() {
        // No-op.
    }

    @Override public GridCacheVersion version() {
        return lockVer;
    }

    @Override public boolean onOwnerChanged(GridCacheEntryEx entry, GridCacheMvccCandidate owner) {
        return false;
    }

    @Override public IgniteUuid futureId() {
        return futId;
    }

    /**
     * Gets remaining allowed time.
     *
     * @return Remaining time. {@code 0} if timeout isn't specified. {@code -1} if time is out.
     * @throws IgniteTxTimeoutCheckedException If timed out.
     */
    protected long remainingTime() throws IgniteTxTimeoutCheckedException {
        if (timeout <= 0)
            return 0;

        long timeLeft = timeout - (U.currentTimeMillis() - startTime());

        if (timeLeft <= 0)
            throw timeoutException();

        return timeLeft;
    }

    /**
     * @return Timeout exception.
     */
    @NotNull protected IgniteTxTimeoutCheckedException timeoutException() {
        return new IgniteTxTimeoutCheckedException("Failed to acquire lock within provided timeout for " +
            "transaction [timeout=" + timeout + ", tx=" + tx + ']');
    }

    /**
     * @return Async rollback exception.
     */
    @NotNull private IgniteTxRollbackCheckedException asyncRollbackException() {
        return new IgniteTxRollbackCheckedException("Transaction was asynchronously rolled back [tx=" + tx + ']');
    }

    /**
     * Start iterating the data rows and form batches.
     *
     * @param topLocked Whether topology was already locked.
     */
    protected abstract void map(boolean topLocked);

    /**
     * @return Nodes from which current future waits responses.
     */
    public abstract Set<UUID> pendingResponseNodes();

    /**
     * Lock request timeout object.
     */
    private class LockTimeoutObject extends GridTimeoutObjectAdapter {
        /**
         * Default constructor.
         */
        LockTimeoutObject() {
            super(timeout);
        }

        /** {@inheritDoc} */
        @Override public void onTimeout() {
            if (log.isDebugEnabled())
                log.debug("Timed out waiting for lock response: " + this);

            onDone(timeoutException());
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(LockTimeoutObject.class, this);
        }
    }
}

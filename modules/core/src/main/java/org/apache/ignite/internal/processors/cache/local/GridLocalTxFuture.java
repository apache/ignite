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

package org.apache.ignite.internal.processors.cache.local;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.cache.version.*;
import org.apache.ignite.internal.transactions.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.internal.processors.cache.transactions.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.internal.util.future.*;
import org.apache.ignite.internal.util.tostring.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.*;

import static org.apache.ignite.transactions.IgniteTxState.*;

/**
 * Replicated cache transaction future.
 */
final class GridLocalTxFuture<K, V> extends GridFutureAdapter<IgniteTxEx<K, V>>
    implements GridCacheMvccFuture<K, V, IgniteTxEx<K, V>> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Logger reference. */
    private static final AtomicReference<IgniteLogger> logRef = new AtomicReference<>();

    /** Future ID. */
    private IgniteUuid futId = IgniteUuid.randomUuid();

    /** Cache. */
    @GridToStringExclude
    private GridCacheSharedContext<K, V> cctx;

    /** Cache transaction. */
    @GridToStringExclude // Need to exclude due to circular dependencies.
    private GridLocalTx<K, V> tx;

    /** Error. */
    private AtomicReference<Throwable> err = new AtomicReference<>(null);

    /** Commit flag. */
    private AtomicBoolean commit = new AtomicBoolean(false);

    /** Logger. */
    @GridToStringExclude
    private IgniteLogger log;

    /** Trackable flag. */
    private boolean trackable = true;

    /**
     * Empty constructor required by {@link Externalizable}.
     */
    public GridLocalTxFuture() {
        // No-op.
    }

    /**
     * @param cctx Context.
     * @param tx Cache transaction.
     */
    GridLocalTxFuture(
        GridCacheSharedContext<K, V> cctx,
        GridLocalTx<K, V> tx) {
        super(cctx.kernalContext());

        assert cctx != null;
        assert tx != null;

        this.cctx = cctx;
        this.tx = tx;

        log = U.logger(ctx, logRef,  GridLocalTxFuture.class);
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
    @Override public Collection<? extends ClusterNode> nodes() {
        return Collections.emptyList();
    }

    /** {@inheritDoc} */
    @Override public boolean onNodeLeft(UUID nodeId) {
        // No-op.
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean trackable() {
        return trackable;
    }

    /** {@inheritDoc} */
    @Override public void markNotTrackable() {
        trackable = false;
    }

    /**
     * @return Lock version.
     */
    GridLocalTx<K, V> tx() {
        return tx;
    }

    /**
     *
     */
    void complete() {
        onComplete();
    }

    /**
     * @param e Error.
     */
    void onError(Throwable e) {
        if (err.compareAndSet(null, e)) {
            tx.setRollbackOnly();

            onComplete();
        }
    }

    /**
     * @param e Error.
     */
    @SuppressWarnings({"TypeMayBeWeakened"})
    void onError(IgniteTxOptimisticCheckedException e) {
        if (err.compareAndSet(null, e)) {
            tx.setRollbackOnly();

            onComplete();
        }
    }

    /**
     * @param e Error.
     */
    @SuppressWarnings({"TypeMayBeWeakened"})
    void onError(IgniteTxRollbackCheckedException e) {
        if (err.compareAndSet(null, e)) {
            // Attempt rollback.
            if (tx.setRollbackOnly()) {
                try {
                    tx.rollback();
                }
                catch (IgniteCheckedException ex) {
                    U.error(log, "Failed to rollback the transaction: " + tx, ex);
                }
            }

            onComplete();
        }
    }

    /**
     * Callback for whenever all replies are received.
     */
    @SuppressWarnings({"ThrowableInstanceNeverThrown"})
    void checkLocks() {
        for (IgniteTxEntry<K, V> txEntry : tx.writeMap().values()) {
            while (true) {
                try {
                    GridCacheEntryEx<K, V> entry = txEntry.cached();

                    if (entry == null) {
                        onError(new IgniteTxRollbackCheckedException("Failed to find cache entry for " +
                            "transaction key (will rollback) [key=" + txEntry.key() + ", tx=" + tx + ']'));

                        break;
                    }

                    // Another thread or transaction owns some lock.
                    if (!entry.lockedByThread(tx.threadId())) {
                        if (tx.pessimistic())
                            onError(new IgniteCheckedException("Pessimistic transaction does not own lock for commit: " + tx));

                        if (log.isDebugEnabled())
                            log.debug("Transaction entry is not locked by transaction (will wait) [entry=" + entry +
                                ", tx=" + tx + ']');

                        return;
                    }

                    break; // While.
                }
                // If entry cached within transaction got removed before lock.
                catch (GridCacheEntryRemovedException ignore) {
                    if (log.isDebugEnabled())
                        log.debug("Got removed entry in checkLocks method (will retry): " + txEntry);

                    txEntry.cached(txEntry.context().cache().entryEx(txEntry.key()), txEntry.keyBytes());
                }
            }
        }

        commit();
    }

    /**
     *
     * @param entry Entry.
     * @param owner Owner.
     */
    @SuppressWarnings({"ThrowableInstanceNeverThrown"})
    @Override public boolean onOwnerChanged(GridCacheEntryEx<K, V> entry, GridCacheMvccCandidate<K> owner) {
        if (log.isDebugEnabled())
            log.debug("Transaction future received owner changed callback [owner=" + owner + ", entry=" + entry + ']');

        for (IgniteTxEntry<K, V> txEntry : tx.writeMap().values()) {
            while (true) {
                try {
                    GridCacheEntryEx<K,V> cached = txEntry.cached();

                    if (entry == null) {
                        onError(new IgniteTxRollbackCheckedException("Failed to find cache entry for " +
                            "transaction key (will rollback) [key=" + txEntry.key() + ", tx=" + tx + ']'));

                        return true;
                    }

                    // Don't compare entry against itself.
                    if (cached != entry && !cached.lockedLocally(tx.xidVersion())) {
                        if (log.isDebugEnabled())
                            log.debug("Transaction entry is not locked by transaction (will wait) [entry=" + entry +
                                ", tx=" + tx + ']');

                        return true;
                    }

                    break;
                }
                // If entry cached within transaction got removed before lock.
                catch (GridCacheEntryRemovedException ignore) {
                    if (log.isDebugEnabled())
                        log.debug("Got removed entry in onOwnerChanged method (will retry): " + txEntry);

                    txEntry.cached(txEntry.context().cache().entryEx(txEntry.key()), txEntry.keyBytes());
                }
            }
        }

        commit();

        return false;
    }

    /**
     * Callback invoked when all locks succeeded.
     */
    @SuppressWarnings({"ThrowableInstanceNeverThrown"})
    private void commit() {
        if (commit.compareAndSet(false, true)) {
            try {
                tx.commit0();

                onComplete();
            }
            catch (IgniteTxTimeoutCheckedException e) {
                onError(e);
            }
            catch (IgniteCheckedException e) {
                if (tx.state() == UNKNOWN) {
                    onError(new IgniteTxHeuristicCheckedException("Commit only partially succeeded " +
                        "(entries will be invalidated on remote nodes once transaction timeout passes): " +
                        tx, e));
                }
                else {
                    onError(new IgniteTxRollbackCheckedException(
                        "Failed to commit transaction (will attempt rollback): " + tx, e));
                }
            }
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"ThrowableInstanceNeverThrown"})
    @Override public boolean cancel() {
        if (log.isDebugEnabled())
            log.debug("Attempting to cancel transaction: " + tx);

        // Attempt rollback.
        if (onCancelled()) {
            try {
                tx.rollback();
            }
            catch (IgniteCheckedException ex) {
                U.error(log, "Failed to rollback the transaction: " + tx, ex);
            }

            if (log.isDebugEnabled())
                log.debug("Transaction was cancelled and rolled back: " + tx);

            return true;
        }

        return isCancelled();
    }

    /**
     * Completeness callback.
     */
    private void onComplete() {
        if (onDone(tx, err.get()))
            cctx.mvcc().removeFuture(this);
    }

    /**
     * Checks for errors.
     *
     * @throws IgniteCheckedException If execution failed.
     */
    private void checkError() throws IgniteCheckedException {
        if (err.get() != null)
            throw U.cast(err.get());
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return GridToStringBuilder.toString(GridLocalTxFuture.class, this);
    }
}

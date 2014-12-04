/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.local;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.future.*;
import org.gridgain.grid.util.tostring.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.*;

import static org.gridgain.grid.cache.GridCacheTxState.*;

/**
 * Replicated cache transaction future.
 */
final class GridLocalTxFuture<K, V> extends GridFutureAdapter<GridCacheTxEx<K, V>>
    implements GridCacheMvccFuture<K, V, GridCacheTxEx<K, V>> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Logger reference. */
    private static final AtomicReference<GridLogger> logRef = new AtomicReference<>();

    /** Future ID. */
    private GridUuid futId = GridUuid.randomUuid();

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
    private GridLogger log;

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
    @Override public GridUuid futureId() {
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
    void onError(GridCacheTxOptimisticException e) {
        if (err.compareAndSet(null, e)) {
            tx.setRollbackOnly();

            onComplete();
        }
    }

    /**
     * @param e Error.
     */
    @SuppressWarnings({"TypeMayBeWeakened"})
    void onError(GridCacheTxRollbackException e) {
        if (err.compareAndSet(null, e)) {
            // Attempt rollback.
            if (tx.setRollbackOnly()) {
                try {
                    tx.rollback();
                }
                catch (GridException ex) {
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
        for (GridCacheTxEntry<K, V> txEntry : tx.writeMap().values()) {
            while (true) {
                try {
                    GridCacheEntryEx<K, V> entry = txEntry.cached();

                    if (entry == null) {
                        onError(new GridCacheTxRollbackException("Failed to find cache entry for " +
                            "transaction key (will rollback) [key=" + txEntry.key() + ", tx=" + tx + ']'));

                        break;
                    }

                    // Another thread or transaction owns some lock.
                    if (!entry.lockedByThread(tx.threadId())) {
                        if (tx.pessimistic())
                            onError(new GridException("Pessimistic transaction does not own lock for commit: " + tx));

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

        for (GridCacheTxEntry<K, V> txEntry : tx.writeMap().values()) {
            while (true) {
                try {
                    GridCacheEntryEx<K,V> cached = txEntry.cached();

                    if (entry == null) {
                        onError(new GridCacheTxRollbackException("Failed to find cache entry for " +
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
            catch (GridCacheTxTimeoutException e) {
                onError(e);
            }
            catch (GridException e) {
                if (tx.state() == UNKNOWN) {
                    onError(new GridCacheTxHeuristicException("Commit only partially succeeded " +
                        "(entries will be invalidated on remote nodes once transaction timeout passes): " +
                        tx, e));
                }
                else {
                    onError(new GridCacheTxRollbackException(
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
            catch (GridException ex) {
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
     * @throws GridException If execution failed.
     */
    private void checkError() throws GridException {
        if (err.get() != null)
            throw U.cast(err.get());
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return GridToStringBuilder.toString(GridLocalTxFuture.class, this);
    }
}

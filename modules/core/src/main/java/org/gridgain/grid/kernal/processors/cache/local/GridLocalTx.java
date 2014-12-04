/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.local;

import org.apache.ignite.lang.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.util.future.*;
import org.gridgain.grid.util.tostring.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.*;

import static org.gridgain.grid.cache.GridCacheTxState.*;

/**
 * Local cache transaction.
 */
class GridLocalTx<K, V> extends GridCacheTxLocalAdapter<K, V> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Transaction future. */
    private final AtomicReference<GridLocalTxFuture<K, V>> fut = new AtomicReference<>();

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public GridLocalTx() {
        // No-op.
    }

    /**
     * @param ctx Cache registry.
     * @param implicit {@code True} if transaction is implicitly created by the system,
     *      {@code false} if user explicitly created the transaction.
     * @param implicitSingle Implicit with single kye flag.
     * @param concurrency Concurrency.
     * @param isolation Isolation.
     * @param timeout Timeout.
     */
    GridLocalTx(
        GridCacheSharedContext<K, V> ctx,
        boolean implicit,
        boolean implicitSingle,
        GridCacheTxConcurrency concurrency,
        GridCacheTxIsolation isolation,
        long timeout,
        int txSize,
        @Nullable UUID subjId,
        int taskNameHash
    ) {
        super(ctx, ctx.versions().next(), implicit, implicitSingle, concurrency, isolation, timeout, txSize, null,
            false, subjId, taskNameHash);
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<GridCacheTxEx<K, V>> future() {
        return fut.get();
    }

    /** {@inheritDoc} */
    @Override public boolean onOwnerChanged(GridCacheEntryEx<K, V> entry, GridCacheMvccCandidate<K> owner) {
        GridLocalTxFuture<K, V> fut = this.fut.get();

        return fut != null && fut.onOwnerChanged(entry, owner);
    }

    /** {@inheritDoc} */
    @Override public void prepare() throws GridException {
        if (!state(PREPARING)) {
            GridCacheTxState state = state();

            // If other thread is doing "prepare", then no-op.
            if (state == PREPARING || state == PREPARED || state == COMMITTING || state == COMMITTED)
                return;

            setRollbackOnly();

            throw new GridException("Invalid transaction state for prepare [state=" + state + ", tx=" + this + ']');
        }

        try {
            userPrepare();

            state(PREPARED);
        }
        catch (GridException e) {
            setRollbackOnly();

            throw e;
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<GridCacheTxEx<K, V>> prepareAsync() {
        try {
            prepare();

            return new GridFinishedFuture<GridCacheTxEx<K, V>>(cctx.kernalContext(), this);
        }
        catch (GridException e) {
            return new GridFinishedFuture<>(cctx.kernalContext(), e);
        }
    }

    /**
     * Commits without prepare.
     *
     * @throws GridException If commit failed.
     */
    void commit0() throws GridException {
        if (state(COMMITTING)) {
            try {
                userCommit();
            }
            finally {
                if (!done()) {
                    if (isRollbackOnly()) {
                        state(ROLLING_BACK);

                        userRollback();

                        state(ROLLED_BACK);
                    }
                    else
                        state(COMMITTED);
                }
            }
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings( {"unchecked", "RedundantCast"})
    @Override public IgniteFuture<GridCacheTx> commitAsync() {
        try {
            prepare();
        }
        catch (GridException e) {
            state(UNKNOWN);

            return new GridFinishedFuture<>(cctx.kernalContext(), e);
        }

        GridLocalTxFuture<K, V> fut = this.fut.get();

        if (fut == null) {
            if (this.fut.compareAndSet(null, fut = new GridLocalTxFuture<>(cctx, this))) {
                cctx.mvcc().addFuture(fut);

                fut.checkLocks();

                return (IgniteFuture)fut;
            }
        }

        return (IgniteFuture)this.fut.get();
    }

    /** {@inheritDoc} */
    @Override public void rollback() throws GridException {
        rollbackAsync().get();
    }

    @Override public IgniteFuture<GridCacheTx> rollbackAsync() {
        try {
            state(ROLLING_BACK);

            userRollback();

            state(ROLLED_BACK);

            return new GridFinishedFuture<GridCacheTx>(cctx.kernalContext(), this);
        }
        catch (GridException e) {
            return new GridFinishedFuture<>(cctx.kernalContext(), e);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean finish(boolean commit) throws GridException {
        assert false;

        return false;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return GridToStringBuilder.toString(GridLocalTx.class, this, "super", super.toString());
    }
}

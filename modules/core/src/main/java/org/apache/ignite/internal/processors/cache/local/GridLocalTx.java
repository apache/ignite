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
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.transactions.*;
import org.apache.ignite.internal.processors.cache.transactions.*;
import org.apache.ignite.internal.util.future.*;
import org.apache.ignite.internal.util.tostring.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.*;

import static org.apache.ignite.transactions.IgniteTxState.*;

/**
 * Local cache transaction.
 */
class GridLocalTx<K, V> extends IgniteTxLocalAdapter<K, V> {
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
        IgniteTxConcurrency concurrency,
        IgniteTxIsolation isolation,
        long timeout,
        int txSize,
        @Nullable UUID subjId,
        int taskNameHash
    ) {
        super(ctx, ctx.versions().next(), implicit, implicitSingle, false, concurrency, isolation, timeout, false, true,
            txSize, null, false, subjId, taskNameHash);
    }

    /** {@inheritDoc} */
    @Override public boolean onOwnerChanged(GridCacheEntryEx<K, V> entry, GridCacheMvccCandidate<K> owner) {
        GridLocalTxFuture<K, V> fut = this.fut.get();

        return fut != null && fut.onOwnerChanged(entry, owner);
    }

    /** {@inheritDoc} */
    @Override public void prepare() throws IgniteCheckedException {
        if (!state(PREPARING)) {
            IgniteTxState state = state();

            // If other thread is doing "prepare", then no-op.
            if (state == PREPARING || state == PREPARED || state == COMMITTING || state == COMMITTED)
                return;

            setRollbackOnly();

            throw new IgniteCheckedException("Invalid transaction state for prepare [state=" + state + ", tx=" + this + ']');
        }

        try {
            userPrepare();

            state(PREPARED);
        }
        catch (IgniteCheckedException e) {
            setRollbackOnly();

            throw e;
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<IgniteInternalTx<K, V>> prepareAsync() {
        try {
            prepare();

            return new GridFinishedFuture<IgniteInternalTx<K, V>>(cctx.kernalContext(), this);
        }
        catch (IgniteCheckedException e) {
            return new GridFinishedFuture<>(cctx.kernalContext(), e);
        }
    }

    /**
     * Commits without prepare.
     *
     * @throws IgniteCheckedException If commit failed.
     */
    void commit0() throws IgniteCheckedException {
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
    @Override public IgniteInternalFuture<IgniteInternalTx> commitAsync() {
        try {
            prepare();
        }
        catch (IgniteCheckedException e) {
            state(UNKNOWN);

            return new GridFinishedFuture<>(cctx.kernalContext(), e);
        }

        GridLocalTxFuture<K, V> fut = this.fut.get();

        if (fut == null) {
            if (this.fut.compareAndSet(null, fut = new GridLocalTxFuture<>(cctx, this))) {
                cctx.mvcc().addFuture(fut);

                fut.checkLocks();

                return (IgniteInternalFuture)fut;
            }
        }

        return (IgniteInternalFuture)this.fut.get();
    }

    /** {@inheritDoc} */
    @Override public void rollback() throws IgniteCheckedException {
        rollbackAsync().get();
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<IgniteInternalTx> rollbackAsync() {
        try {
            state(ROLLING_BACK);

            userRollback();

            state(ROLLED_BACK);

            return new GridFinishedFuture<IgniteInternalTx>(cctx.kernalContext(), this);
        }
        catch (IgniteCheckedException e) {
            return new GridFinishedFuture<>(cctx.kernalContext(), e);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean finish(boolean commit) throws IgniteCheckedException {
        assert false;

        return false;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return GridToStringBuilder.toString(GridLocalTx.class, this, "super", super.toString());
    }
}

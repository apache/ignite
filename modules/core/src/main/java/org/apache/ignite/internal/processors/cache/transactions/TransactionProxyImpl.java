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

package org.apache.ignite.internal.processors.cache.transactions;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteClientDisconnectedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxLocal;
import org.apache.ignite.internal.processors.tracing.MTC;
import org.apache.ignite.internal.processors.tracing.Span;
import org.apache.ignite.internal.util.future.IgniteFinishedFutureImpl;
import org.apache.ignite.internal.util.future.IgniteFutureImpl;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.CX1;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteAsyncSupport;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.transactions.TransactionState;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.tracing.MTC.TraceSurroundings;
import static org.apache.ignite.internal.processors.tracing.SpanType.TX_CLOSE;
import static org.apache.ignite.internal.processors.tracing.SpanType.TX_COMMIT;
import static org.apache.ignite.internal.processors.tracing.SpanType.TX_RESUME;
import static org.apache.ignite.internal.processors.tracing.SpanType.TX_ROLLBACK;
import static org.apache.ignite.internal.processors.tracing.SpanType.TX_SUSPEND;
import static org.apache.ignite.transactions.TransactionState.SUSPENDED;

/**
 * Cache transaction proxy.
 */
@SuppressWarnings("unchecked")
public class TransactionProxyImpl<K, V> implements TransactionProxy, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Wrapped transaction. */
    @GridToStringInclude
    private GridNearTxLocal tx;

    /** Gateway. */
    @GridToStringExclude
    private GridCacheSharedContext<K, V> cctx;

    /** Async flag. */
    private boolean async;

    /** Async call result. */
    private IgniteFuture asyncRes;

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public TransactionProxyImpl() {
        // No-op.
    }

    /**
     * @param tx Transaction.
     * @param cctx Shared context.
     * @param async Async flag.
     */
    public TransactionProxyImpl(GridNearTxLocal tx, GridCacheSharedContext<K, V> cctx, boolean async) {
        assert tx != null;
        assert cctx != null;

        this.tx = tx;
        this.cctx = cctx;
        this.async = async;
    }

    /**
     * @return Transaction.
     */
    public GridNearTxLocal tx() {
        return tx;
    }

    /**
     * Enters a call.
     */
    private void enter() {
        enter(false);
    }

    /**
     * Enters a call.
     *
     * @param resume Flag to indicate that resume operation in progress.
     */
    private void enter(boolean resume) {
        if (!resume && state() == SUSPENDED)
            throw new IgniteException("Tx in SUSPENDED state. All operations except resume are prohibited.");

        if (cctx.deploymentEnabled())
            cctx.deploy().onEnter();

        tx.enterSystemSection();

        try {
            cctx.kernalContext().gateway().readLock();
        }
        catch (IllegalStateException | IgniteClientDisconnectedException e) {
            throw e;
        }
        catch (RuntimeException | Error e) {
            cctx.kernalContext().gateway().readUnlock();

            throw e;
        }
    }

    /**
     * Leaves a call.
     */
    private void leave() {
        try {
            CU.unwindEvicts(cctx);

            tx.leaveSystemSection();
        }
        finally {
            cctx.kernalContext().gateway().readUnlock();
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteUuid xid() {
        return tx.xid();
    }

    /** {@inheritDoc} */
    @Override public UUID nodeId() {
        if (async)
            save(tx.nodeId());

        return tx.nodeId();
    }

    /** {@inheritDoc} */
    @Override public long threadId() {
        if (async)
            save(tx.threadId());

        return tx.threadId();
    }

    /** {@inheritDoc} */
    @Override public long startTime() {
        if (async)
            save(tx.startTime());

        return tx.startTime();
    }

    /** {@inheritDoc} */
    @Override public TransactionIsolation isolation() {
        if (async)
            save(tx.isolation());

        return tx.isolation();
    }

    /** {@inheritDoc} */
    @Override public TransactionConcurrency concurrency() {
        if (async)
            save(tx.concurrency());

        return tx.concurrency();
    }

    /** {@inheritDoc} */
    @Override public boolean isInvalidate() {
        if (async)
            save(tx.isInvalidate());

        return tx.isInvalidate();
    }

    /** {@inheritDoc} */
    @Override public boolean implicit() {
        if (async)
            save(tx.implicit());

        return tx.implicit();
    }

    /** {@inheritDoc} */
    @Override public long timeout() {
        if (async)
            save(tx.timeout());

        return tx.timeout();
    }

    /** {@inheritDoc} */
    @Override public TransactionState state() {
        if (async)
            save(tx.state());

        return tx.state();
    }

    /** {@inheritDoc} */
    @Override public void suspend() throws IgniteException {
        try (TraceSurroundings ignored =
                 MTC.support(cctx.kernalContext().tracing().create(TX_SUSPEND, MTC.span()))) {
            enter();

            try {
                cctx.suspendTx(tx);
            }
            catch (IgniteCheckedException e) {
                throw U.convertException(e);
            }
            finally {
                leave();
            }
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override public String label() {
        if (async)
            save(tx.label());

        return tx.label();
    }

    /** {@inheritDoc} */
    @Override public long timeout(long timeout) {
        return tx.timeout(timeout);
    }

    /** {@inheritDoc} */
    @Override public IgniteAsyncSupport withAsync() {
        return new TransactionProxyImpl<>(tx, cctx, true);
    }

    /** {@inheritDoc} */
    @Override public boolean isAsync() {
        return async;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public <R> IgniteFuture<R> future() {
        return asyncRes;
    }

    /** {@inheritDoc} */
    @Override public boolean setRollbackOnly() {
        enter();

        try {
            return tx.setRollbackOnly();
        }
        finally {
            leave();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean isRollbackOnly() {
        enter();

        try {
            if (async)
                save(tx.isRollbackOnly());

            return tx.isRollbackOnly();
        }
        finally {
            leave();
        }
    }

    /** {@inheritDoc} */
    @Override public void commit() {
        Span span = MTC.span();

        try (TraceSurroundings ignored =
                 MTC.support(cctx.kernalContext().tracing().create(TX_COMMIT, span))) {
            enter();

            try {
                IgniteInternalFuture<IgniteInternalTx> commitFut = cctx.commitTxAsync(tx);

                if (async)
                    saveFuture(commitFut);
                else
                    commitFut.get();
            }
            catch (IgniteCheckedException e) {
                throw U.convertException(e);
            }
            finally {
                leave();
            }
        }
        finally {
            span.end();
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Void> commitAsync() throws IgniteException {
        Span span = MTC.span();

        try (TraceSurroundings ignored =
                 MTC.support(cctx.kernalContext().tracing().create(TX_COMMIT, span))) {
            enter();

            try {
                return (IgniteFuture<Void>)createFuture(cctx.commitTxAsync(tx));
            }
            finally {
                leave();
            }
        }
        finally {
            span.end();
        }
    }

    /** {@inheritDoc} */
    @Override public void close() {
        Span span = MTC.span();

        try (TraceSurroundings ignored =
                 MTC.support(cctx.kernalContext().tracing().create(TX_CLOSE, span))) {
            enter();

            try {
                cctx.endTx(tx);
            }
            catch (IgniteCheckedException e) {
                throw U.convertException(e);
            }
            finally {
                leave();
            }
        }
        finally {
            span.end();
        }
    }

    /** {@inheritDoc} */
    @Override public void rollback() {
        Span span = MTC.span();

        try (TraceSurroundings ignored =
                 MTC.support(cctx.kernalContext().tracing().create(TX_ROLLBACK, span))) {
            enter();

            try {
                IgniteInternalFuture rollbackFut = cctx.rollbackTxAsync(tx);

                if (async)
                    asyncRes = new IgniteFutureImpl(rollbackFut);
                else
                    rollbackFut.get();
            }
            catch (IgniteCheckedException e) {
                throw U.convertException(e);
            }
            finally {
                leave();
            }
        }
        finally {
            span.end();
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Void> rollbackAsync() throws IgniteException {
        Span span = MTC.span();

        try (TraceSurroundings ignored =
                 MTC.support(cctx.kernalContext().tracing().create(TX_ROLLBACK, span))) {
            enter();

            try {
                return (IgniteFuture<Void>)(new IgniteFutureImpl(cctx.rollbackTxAsync(tx)));
            }
            finally {
                leave();
            }
        }
        finally {
            span.end();
        }
    }

    /** {@inheritDoc} */
    @Override public void resume() throws IgniteException {
        try (TraceSurroundings ignored =
                 MTC.support(cctx.kernalContext().tracing().create(TX_RESUME, MTC.span()))) {
            enter(true);

            try {
                cctx.resumeTx(tx);
            }
            catch (IgniteCheckedException e) {
                throw U.convertException(e);
            }
            finally {
                leave();
            }
        }
    }

    /**
     * @param res Result to convert to finished future.
     */
    private void save(Object res) {
        asyncRes = new IgniteFinishedFutureImpl<>(res);
    }

    /**
     * @param fut Internal future.
     */
    private void saveFuture(IgniteInternalFuture<IgniteInternalTx> fut) {
        asyncRes = createFuture(fut);
    }

    /**
     * @param fut Internal future.
     * @return User future.
     */
    private IgniteFuture<?> createFuture(IgniteInternalFuture<IgniteInternalTx> fut) {
        IgniteInternalFuture<Transaction> fut0 = fut.chain(new CX1<IgniteInternalFuture<IgniteInternalTx>, Transaction>() {
            @Override public Transaction applyx(IgniteInternalFuture<IgniteInternalTx> fut) throws IgniteCheckedException {
                fut.get();

                return TransactionProxyImpl.this;
            }
        });

        return new IgniteFutureImpl(fut0);
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(tx);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        tx = (GridNearTxLocal)in.readObject();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(TransactionProxyImpl.class, this);
    }
}

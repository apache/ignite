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

import org.apache.ignite.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.util.future.*;
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.transactions.*;

import java.io.*;
import java.util.*;

/**
 * Cache transaction proxy.
 */
public class TransactionProxyImpl<K, V> implements TransactionProxy, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Wrapped transaction. */
    @GridToStringInclude
    private IgniteInternalTx tx;

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
    public TransactionProxyImpl(IgniteInternalTx tx, GridCacheSharedContext<K, V> cctx, boolean async) {
        assert tx != null;
        assert cctx != null;

        this.tx = tx;
        this.cctx = cctx;
        this.async = async;
    }

    /**
     * @return Transaction.
     */
    public IgniteInternalTx tx() {
        return tx;
    }

    /**
     * Enters a call.
     */
    private void enter() {
        if (cctx.deploymentEnabled())
            cctx.deploy().onEnter();

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

    /** {@inheritDoc} */
    @Override public void close() {
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

    /** {@inheritDoc} */
    @Override public void rollback() {
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
        IgniteInternalFuture<Transaction> fut0 = fut.chain(new CX1<IgniteInternalFuture<IgniteInternalTx>, Transaction>() {
            @Override public Transaction applyx(IgniteInternalFuture<IgniteInternalTx> fut) throws IgniteCheckedException {
                return fut.get().proxy();
            }
        });

        asyncRes = new IgniteFutureImpl(fut0);
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(tx);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        tx = (IgniteInternalTx)in.readObject();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(TransactionProxyImpl.class, this);
    }
}

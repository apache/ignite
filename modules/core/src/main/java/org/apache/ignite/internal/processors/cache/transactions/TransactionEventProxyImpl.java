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
import org.apache.ignite.IgniteException;
import org.apache.ignite.events.TransactionStateChangedEvent;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxLocal;
import org.apache.ignite.lang.IgniteAsyncSupport;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.transactions.TransactionState;
import org.jetbrains.annotations.Nullable;

/**
 * Transaction proxy used at tx events.
 *
 * @see TransactionStateChangedEvent
 */
public class TransactionEventProxyImpl implements TransactionProxy, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Unsupported operation message. */
    private static final String UNSUPPORTED_OPERATION_MESSAGE = "All operations changing transaction's params or " +
        "state are restricted inside event listener. " +
        "The only exception is setRollbackOnly(), use it in case transaction should be rolled back.";

    /** Xid. */
    private IgniteUuid xid;

    /** Tx. */
    private GridNearTxLocal tx;

    /** Proxy. */
    private TransactionProxy proxy;

    /**
     * Default constructor (required by Externalizable).
     */
    public TransactionEventProxyImpl() {
    }

    /**
     * @param tx Tx proxy.
     */
    public TransactionEventProxyImpl(GridNearTxLocal tx) {
        assert tx != null;

        this.tx = tx;
        this.xid = tx.xid();
    }

    /** {@inheritDoc} */
    @Override public IgniteUuid xid() {
        return xid;
    }

    /** {@inheritDoc} */
    @Override public UUID nodeId() {
        return tx().nodeId();
    }

    /** {@inheritDoc} */
    @Override public long threadId() {
        return tx().threadId();
    }

    /** {@inheritDoc} */
    @Override public long startTime() {
        return tx().startTime();
    }

    /** {@inheritDoc} */
    @Override public TransactionIsolation isolation() {
        return tx().isolation();
    }

    /** {@inheritDoc} */
    @Override public TransactionConcurrency concurrency() {
        return tx().concurrency();
    }

    /** {@inheritDoc} */
    @Override public boolean implicit() {
        return tx().implicit();
    }

    /** {@inheritDoc} */
    @Override public boolean isInvalidate() {
        return tx().isInvalidate();
    }

    /** {@inheritDoc} */
    @Override public TransactionState state() {
        return tx().state();
    }

    /** {@inheritDoc} */
    @Override public long timeout() {
        return tx().timeout();
    }

    /** {@inheritDoc} */
    @Override public long timeout(long timeout) {
        throw new UnsupportedOperationException(UNSUPPORTED_OPERATION_MESSAGE);
    }

    /** {@inheritDoc} */
    @Override public boolean setRollbackOnly() {
        return tx().setRollbackOnly();
    }

    /** {@inheritDoc} */
    @Override public boolean isRollbackOnly() {
        return tx().isRollbackOnly();
    }

    /** {@inheritDoc} */
    @Override public void commit() throws IgniteException {
        throw new UnsupportedOperationException(UNSUPPORTED_OPERATION_MESSAGE);
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Void> commitAsync() throws IgniteException {
        throw new UnsupportedOperationException(UNSUPPORTED_OPERATION_MESSAGE);
    }

    /** {@inheritDoc} */
    @Override public void close() throws IgniteException {
        throw new UnsupportedOperationException(UNSUPPORTED_OPERATION_MESSAGE);
    }

    /** {@inheritDoc} */
    @Override public void rollback() throws IgniteException {
        throw new UnsupportedOperationException(UNSUPPORTED_OPERATION_MESSAGE);
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Void> rollbackAsync() throws IgniteException {
        throw new UnsupportedOperationException(UNSUPPORTED_OPERATION_MESSAGE);
    }

    /** {@inheritDoc} */
    @Override public void resume() throws IgniteException {
        throw new UnsupportedOperationException(UNSUPPORTED_OPERATION_MESSAGE);
    }

    /** {@inheritDoc} */
    @Override public void suspend() throws IgniteException {
        throw new UnsupportedOperationException(UNSUPPORTED_OPERATION_MESSAGE);
    }

    /** {@inheritDoc} */
    @Nullable @Override public String label() {
        return tx().label();
    }

    /** {@inheritDoc} */
    @Override public IgniteAsyncSupport withAsync() {
        throw new UnsupportedOperationException("Operation deprecated.");
    }

    /** {@inheritDoc} */
    @Override public boolean isAsync() {
        throw new UnsupportedOperationException("Operation deprecated.");
    }

    /** {@inheritDoc} */
    @Override public <R> IgniteFuture<R> future() {
        throw new UnsupportedOperationException("Operation deprecated.");
    }

    /**
     * @return local transaction
     * @throws IgniteException in case tx was not found.
     */
    private TransactionProxy tx() throws IgniteException {
        if (tx == null)
            throw new IgniteException("Operation allowed only inside remote filter or " +
                "inside local listener registered on originating node. " +
                "Only xid() operation allowed in other case. ");

        if (proxy == null)
            proxy = tx.proxy();

        return proxy;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(xid);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        xid = (IgniteUuid)in.readObject();
    }
}

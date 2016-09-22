package org.apache.ignite.tests.utils;

import org.apache.ignite.lang.IgniteAsyncSupport;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.transactions.TransactionState;
import org.jetbrains.annotations.Nullable;

import java.util.UUID;

public class TestTransaction implements Transaction {
    /** */
    private final IgniteUuid xid = IgniteUuid.randomUuid();

    /** {@inheritDoc} */
    @Nullable
    @Override public IgniteUuid xid() {
        return xid;
    }

    /** {@inheritDoc} */
    @Nullable @Override public UUID nodeId() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public long threadId() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public long startTime() {
        return 0;
    }

    /** {@inheritDoc} */
    @Nullable @Override public TransactionIsolation isolation() {
        return null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public TransactionConcurrency concurrency() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean implicit() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean isInvalidate() {
        return false;
    }

    /** {@inheritDoc} */
    @Nullable @Override public TransactionState state() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public long timeout() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public long timeout(long timeout) {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public boolean setRollbackOnly() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean isRollbackOnly() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public void commit() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void close() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public IgniteAsyncSupport withAsync() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public boolean isAsync() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public <R> IgniteFuture<R> future() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void rollback() {
        // No-op.
    }
}

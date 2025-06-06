

package org.apache.ignite.console.db;

import java.util.UUID;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.util.future.IgniteFinishedFutureImpl;

import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.transactions.TransactionState;
import org.jetbrains.annotations.Nullable;

/**
 * Nested transaction.
 */
public class NestedTransaction implements Transaction {
    /** */
    private final Transaction delegate;

    /**
     * @param delegate Real transaction.
     */
    public NestedTransaction(Transaction delegate) {
        this.delegate = delegate;
    }

    /** {@inheritDoc} */
    @Override public IgniteUuid xid() {
        return delegate.xid();
    }

    /** {@inheritDoc} */
    @Override public UUID nodeId() {
        return delegate.nodeId();
    }

    /** {@inheritDoc} */
    @Override public long threadId() {
        return delegate.threadId();
    }

    /** {@inheritDoc} */
    @Override public long startTime() {
        return delegate.startTime();
    }

    /** {@inheritDoc} */
    @Override public TransactionIsolation isolation() {
        return delegate.isolation();
    }

    /** {@inheritDoc} */
    @Override public TransactionConcurrency concurrency() {
        return delegate.concurrency();
    }

    /** {@inheritDoc} */
    @Override public boolean implicit() {
        return delegate.implicit();
    }

    /** {@inheritDoc} */
    @Override public boolean isInvalidate() {
        return delegate.isInvalidate();
    }

    /** {@inheritDoc} */
    @Override public TransactionState state() {
        return delegate.state();
    }

    /** {@inheritDoc} */
    @Override public long timeout() {
        return delegate.timeout();
    }

    /** {@inheritDoc} */
    @Override public long timeout(long timeout) {
        return delegate.timeout(timeout);
    }

    /** {@inheritDoc} */
    @Override public boolean setRollbackOnly() {
        return delegate.setRollbackOnly();
    }

    /** {@inheritDoc} */
    @Override public boolean isRollbackOnly() {
        return delegate.isRollbackOnly();
    }

    /** {@inheritDoc} */
    @Override public void commit() throws IgniteException {
        // Nested transaction do nothing.
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Void> commitAsync() throws IgniteException {
        return new IgniteFinishedFutureImpl<>();
    }

    /** {@inheritDoc} */
    @Override public void close() throws IgniteException {
        // Nested transaction do nothing.
    }

    /** {@inheritDoc} */
    @Override public void rollback() throws IgniteException {
        // Nested transaction do nothing.
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Void> rollbackAsync() throws IgniteException {
        return new IgniteFinishedFutureImpl<>();
    }

    /** {@inheritDoc} */
    @Override public void resume() throws IgniteException {
        delegate.resume();
    }

    /** {@inheritDoc} */
    @Override public void suspend() throws IgniteException {
        delegate.suspend();
    }

    /** {@inheritDoc} */
    @Override public @Nullable String label() {
        return delegate.label();
    }

}

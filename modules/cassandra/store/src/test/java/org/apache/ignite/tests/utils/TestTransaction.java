/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.tests.utils;

import org.apache.ignite.IgniteException;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.transactions.TransactionState;
import org.jetbrains.annotations.Nullable;

import java.util.UUID;

/**
 * Dummy transaction for test purposes.
 */
public class TestTransaction implements Transaction {
    /** */
    private final IgniteUuid xid = IgniteUuid.randomUuid();

    /** {@inheritDoc} */
    @Nullable @Override public IgniteUuid xid() {
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
    @Override public IgniteFuture<Void> commitAsync() throws IgniteException {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void close() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void rollback() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Void> rollbackAsync() throws IgniteException {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void suspend() throws IgniteException{
        // No-op.
    }

    /** {@inheritDoc} */
    @Nullable @Override public String label() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void resume() throws IgniteException {
        // No-op.
    }
}

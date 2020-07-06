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

package org.apache.ignite.tests.utils;

import java.util.UUID;
import org.apache.ignite.IgniteException;
import org.apache.ignite.lang.IgniteAsyncSupport;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.transactions.TransactionState;
import org.jetbrains.annotations.Nullable;

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

/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.tests.utils;

import org.apache.ignite.IgniteException;
import org.apache.ignite.lang.IgniteAsyncSupport;
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

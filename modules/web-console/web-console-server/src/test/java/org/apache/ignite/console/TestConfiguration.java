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

package org.apache.ignite.console;

import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.console.repositories.AnnouncementRepository;
import org.apache.ignite.console.tx.TransactionManager;
import org.apache.ignite.internal.util.future.IgniteFinishedFutureImpl;
import org.apache.ignite.lang.IgniteAsyncSupport;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.testframework.junits.IgniteMock;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.transactions.TransactionState;
import org.jetbrains.annotations.Nullable;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test configuration with mocks.
 */
@Configuration
@Import(Application.class)
public class TestConfiguration {
    /** Announcement mock. */
    @Bean
    public AnnouncementRepository announcementRepository() {
        return mock(AnnouncementRepository.class);
    }

    /** Tx manager mock. */
    @Bean
    public TransactionManager transactionManager() {
        TransactionManager txMgr = mock(TransactionManager.class);

        when(txMgr.txStart(any(TransactionConcurrency.class), any(TransactionIsolation.class)))
            .thenReturn(new TransactionMock());

        when(txMgr.txStart())
            .thenReturn(new TestConfiguration.TransactionMock());

        return txMgr;
    }

    /** Ignite mock. */
    @Bean
    public Ignite igniteInstance() {
        return new IgniteMock("testGrid", null, null, null, null, null, null);
    }

    /**
     * Transaction mock.
     */
    private static class TransactionMock implements Transaction {
        /** {@inheritDoc} */
        @Override public IgniteUuid xid() {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        @Override public UUID nodeId() {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        @Override public long threadId() {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        @Override public long startTime() {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        @Override public TransactionIsolation isolation() {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        @Override public TransactionConcurrency concurrency() {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        @Override public boolean implicit() {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        @Override public boolean isInvalidate() {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        @Override public TransactionState state() {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        @Override public long timeout() {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        @Override public long timeout(long timeout) {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        @Override public boolean setRollbackOnly() {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        @Override public boolean isRollbackOnly() {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        @Override public void commit() throws IgniteException {
            // transaction do nothing.
        }

        /** {@inheritDoc} */
        @Override public IgniteFuture<Void> commitAsync() throws IgniteException {
            return new IgniteFinishedFutureImpl<>();
        }

        /** {@inheritDoc} */
        @Override public void close() throws IgniteException {
            // transaction do nothing.
        }

        /** {@inheritDoc} */
        @Override public void rollback() throws IgniteException {
            // transaction do nothing.
        }

        /** {@inheritDoc} */
        @Override public IgniteFuture<Void> rollbackAsync() throws IgniteException {
            return new IgniteFinishedFutureImpl<>();
        }

        /** {@inheritDoc} */
        @Override public void resume() throws IgniteException {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        @Override public void suspend() throws IgniteException {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        @Override public @Nullable String label() {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        @SuppressWarnings("deprecation")
        @Override public IgniteAsyncSupport withAsync() {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        @SuppressWarnings("deprecation")
        @Override public boolean isAsync() {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        @SuppressWarnings("deprecation")
        @Override public <R> IgniteFuture<R> future() {
            throw new UnsupportedOperationException();
        }
    }
}

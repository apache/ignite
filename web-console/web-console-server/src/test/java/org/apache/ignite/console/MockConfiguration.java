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

import java.util.Collections;
import java.util.Spliterators;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCluster;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteMessaging;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.console.repositories.AnnouncementRepository;
import org.apache.ignite.console.web.socket.AgentsService;
import org.apache.ignite.console.web.socket.TransitionService;
import org.apache.ignite.internal.util.future.IgniteFinishedFutureImpl;
import org.apache.ignite.lang.IgniteAsyncSupport;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.testframework.GridTestNode;
import org.apache.ignite.testframework.junits.IgniteMock;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.transactions.TransactionState;
import org.jetbrains.annotations.Nullable;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.event.ApplicationEventMulticaster;
import org.springframework.context.event.SimpleApplicationEventMulticaster;
import javax.cache.expiry.ExpiryPolicy;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test configuration with mocks.
 */
@TestConfiguration
@Import(Application.class)
public class MockConfiguration {
    /**
     * @return Application event multicaster.
     */
    @Bean(name = "applicationEventMulticaster")
    public ApplicationEventMulticaster simpleApplicationEventMulticaster() {
        return new SimpleApplicationEventMulticaster();
    }
    /** Announcement mock. */
    @Bean
    public AnnouncementRepository announcementRepository() {
        return mock(AnnouncementRepository.class);
    }

    /** Agents service mock. */
    @Bean
    public AgentsService agentsService() {
        return mock(AgentsService.class);
    }

    /** Transition service mock. */
    @Bean
    public TransitionService transitionService() {
        return mock(TransitionService.class);
    }

    /** Ignite mock. */
    @Bean
    public Ignite igniteInstance() {
        IgniteConfiguration cfg = new IgniteConfiguration();
        cfg.setClientMode(false);

        IgniteTransactions txs = mock(IgniteTransactions.class);

        when(txs.txStart(any(TransactionConcurrency.class), any(TransactionIsolation.class)))
            .thenReturn(new TransactionMock());

        when(txs.txStart())
            .thenReturn(new TransactionMock());

        IgniteCluster cluster = mock(IgniteCluster.class);

        GridTestNode locNode = new GridTestNode(UUID.randomUUID());

        when(cluster.nodes())
            .thenReturn(Collections.singleton(locNode));

        when(cluster.localNode()).thenReturn(locNode);

        return new IgniteMock("testGrid", null, null, null, null, null, null) {
            /** {@inheritDoc} */
            @Override public IgniteConfiguration configuration() {
                return cfg;
            }

            /** {@inheritDoc} */
            @Override public IgniteTransactions transactions() {
                return txs;
            }

            /** {@inheritDoc} */
            @Override public IgniteCluster cluster() {
                return cluster;
            }

            /** {@inheritDoc} */
            @SuppressWarnings("unchecked")
            @Override public <K, V> IgniteCache<K, V> getOrCreateCache(CacheConfiguration<K, V> cacheCfg) {
                IgniteCache<K, V> mockedCache = mock(IgniteCache.class);

                when(mockedCache.spliterator()).thenReturn(Spliterators.emptySpliterator());
                when(mockedCache.withExpiryPolicy(any(ExpiryPolicy.class))).thenReturn(mockedCache);

                return mockedCache;
            }

            @Override public IgniteMessaging message() {
                return mock(IgniteMessaging.class);
            }

            @Override public IgniteMessaging message(ClusterGroup prj) {
                return mock(IgniteMessaging.class);
            }
        };
    }

    /**
     * Transaction mock that do nothing.
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
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public IgniteFuture<Void> commitAsync() throws IgniteException {
            return new IgniteFinishedFutureImpl<>();
        }

        /** {@inheritDoc} */
        @Override public void close() throws IgniteException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void rollback() throws IgniteException {
            //No-op.
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

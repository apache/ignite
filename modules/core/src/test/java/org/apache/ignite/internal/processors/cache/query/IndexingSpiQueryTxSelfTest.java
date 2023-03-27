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

package org.apache.ignite.internal.processors.cache.query;

import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.Callable;
import javax.cache.Cache;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.GridCacheAbstractSelfTest;
import org.apache.ignite.internal.transactions.IgniteTxHeuristicCheckedException;
import org.apache.ignite.spi.IgniteSpiAdapter;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.indexing.IndexingQueryFilter;
import org.apache.ignite.spi.indexing.IndexingSpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.transactions.TransactionState;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

/**
 * Indexing Spi transactional query test
 */
public class IndexingSpiQueryTxSelfTest extends GridCacheAbstractSelfTest {
    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 4;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);
        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setForceServerMode(true);

        cfg.setIndexingSpi(new MyBrokenIndexingSpi());

        CacheConfiguration ccfg = cacheConfiguration(igniteInstanceName);
        ccfg.setName(DEFAULT_CACHE_NAME);
        ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);

        ccfg.setIndexedTypes(Integer.class, Integer.class);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /** */
    @Test
    public void testIndexingSpiWithTxClient() throws Exception {
        IgniteEx client = startClientGrid("client");

        assertNotNull(client.cache(DEFAULT_CACHE_NAME));

        doTestIndexingSpiWithTx(client, 0);
    }

    /** */
    @Test
    public void testIndexingSpiWithTxLocal() throws Exception {
        IgniteEx ignite = (IgniteEx)primaryNode(0, DEFAULT_CACHE_NAME);

        doTestIndexingSpiWithTx(ignite, 0);
    }

    /** */
    @Test
    public void testIndexingSpiWithTxNotLocal() throws Exception {
        IgniteEx ignite = (IgniteEx)primaryNode(0, DEFAULT_CACHE_NAME);

        doTestIndexingSpiWithTx(ignite, 1);
    }

    /**
     * @throws Exception If failed.
     */
    private void doTestIndexingSpiWithTx(IgniteEx ignite, int key) throws Exception {
        final IgniteCache<Integer, Integer> cache = ignite.cache(DEFAULT_CACHE_NAME);

        final IgniteTransactions txs = ignite.transactions();

        for (final TransactionConcurrency concurrency : TransactionConcurrency.values()) {
            for (final TransactionIsolation isolation : TransactionIsolation.values()) {
                System.out.println("Run in transaction: " + concurrency + " " + isolation);

                GridTestUtils.assertThrowsWithCause(new Callable<Void>() {
                    @Override public Void call() throws Exception {
                        Transaction tx;

                        try (Transaction tx0 = tx = txs.txStart(concurrency, isolation)) {
                            cache.put(key, key);

                            tx0.commit();
                        }

                        assertEquals(TransactionState.ROLLED_BACK, tx.state());

                        return null;
                    }
                }, IgniteTxHeuristicCheckedException.class);

                checkFutures();
            }
        }
    }

    /**
     * Indexing SPI implementation for test
     */
    private static class MyBrokenIndexingSpi extends IgniteSpiAdapter implements IndexingSpi {
        /** {@inheritDoc} */
        @Override public void spiStart(@Nullable String igniteInstanceName) throws IgniteSpiException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void spiStop() throws IgniteSpiException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public Iterator<Cache.Entry<?, ?>> query(@Nullable String cacheName, Collection<Object> params,
            @Nullable IndexingQueryFilter filters) throws IgniteSpiException {
            return null;
        }

        /** {@inheritDoc} */
        @Override public void store(@Nullable String cacheName, Object key, Object val, long expirationTime)
            throws IgniteSpiException {
            throw new IgniteSpiException("Test exception");
        }

        /** {@inheritDoc} */
        @Override public void remove(@Nullable String cacheName, Object key) throws IgniteSpiException {
            // No-op.
        }
    }
}

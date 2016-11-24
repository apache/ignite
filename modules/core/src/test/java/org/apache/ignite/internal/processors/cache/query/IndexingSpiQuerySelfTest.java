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

import junit.framework.TestCase;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SpiQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.CacheEntryImpl;
import org.apache.ignite.internal.transactions.IgniteTxHeuristicCheckedException;
import org.apache.ignite.spi.IgniteSpiAdapter;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.spi.indexing.IndexingQueryFilter;
import org.apache.ignite.spi.indexing.IndexingSpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.transactions.TransactionState;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import javax.cache.Cache;

/**
 * Indexing Spi query test
 */
public class IndexingSpiQuerySelfTest extends TestCase {
    /** {@inheritDoc} */
    @Override public void tearDown() throws Exception {
        Ignition.stopAll(true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testSimpleIndexingSpi() throws Exception {
        IgniteConfiguration cfg = configuration();

        cfg.setIndexingSpi(new MyIndexingSpi());

        Ignite ignite = Ignition.start(cfg);

        CacheConfiguration<Integer, Integer> ccfg = new CacheConfiguration<>("test-cache");

        ccfg.setIndexedTypes(Integer.class, Integer.class);

        IgniteCache<Integer, Integer> cache = ignite.createCache(ccfg);

        for (int i = 0; i < 10; i++)
            cache.put(i, i);

        QueryCursor<Cache.Entry<Integer, Integer>> cursor = cache.query(new SpiQuery<Integer, Integer>().setArgs(2, 5));

        for (Cache.Entry<Integer, Integer> entry : cursor)
            System.out.println(entry);
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    public void testIndexingSpiFailure() throws Exception {
        IgniteConfiguration cfg = configuration();

        cfg.setIndexingSpi(new MyBrokenIndexingSpi());

        Ignite ignite = Ignition.start(cfg);

        CacheConfiguration<Integer, Integer> ccfg = new CacheConfiguration<>("test-cache");

        ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        ccfg.setIndexedTypes(Integer.class, Integer.class);

        final IgniteCache<Integer, Integer> cache = ignite.createCache(ccfg);

        final IgniteTransactions txs = ignite.transactions();

        for (final TransactionConcurrency concurrency : TransactionConcurrency.values()) {
            for (final TransactionIsolation isolation : TransactionIsolation.values()) {
                System.out.println("Run in transaction: " + concurrency + " " + isolation);

                GridTestUtils.assertThrowsWithCause(new Callable<Void>() {
                    @Override public Void call() throws Exception {
                        Transaction tx;

                        try (Transaction tx0 = tx = txs.txStart(concurrency, isolation)) {
                            cache.put(1, 1);

                            tx0.commit();
                        }

                        assertEquals(TransactionState.ROLLED_BACK, tx.state());
                        return null;
                    }
                }, IgniteTxHeuristicCheckedException.class);
            }
        }
    }

    /**
     * @return Configuration.
     */
    private IgniteConfiguration configuration() {
        IgniteConfiguration cfg = new IgniteConfiguration();

        TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);
        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setMaxMissedHeartbeats(Integer.MAX_VALUE);

        disco.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(disco);

        return cfg;
    }

    /**
     * Indexing Spi implementation for test
     */
    private static class MyIndexingSpi extends IgniteSpiAdapter implements IndexingSpi {
        /** Index. */
        private final SortedMap<Object, Object> idx = new TreeMap<>();

        /** {@inheritDoc} */
        @Override public void spiStart(@Nullable String gridName) throws IgniteSpiException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void spiStop() throws IgniteSpiException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public Iterator<Cache.Entry<?, ?>> query(@Nullable String spaceName, Collection<Object> params,
            @Nullable IndexingQueryFilter filters) throws IgniteSpiException {
            if (params.size() < 2)
                throw new IgniteSpiException("Range parameters required.");

            Iterator<Object> paramsIt = params.iterator();

            Object from = paramsIt.next();
            Object to = paramsIt.next();

            SortedMap<Object, Object> map = idx.subMap(from, to);

            Collection<Cache.Entry<?, ?>> res = new ArrayList<>(map.size());

            for (Map.Entry<Object, Object> entry : map.entrySet())
                res.add(new CacheEntryImpl<>(entry.getKey(), entry.getValue()));

            return res.iterator();
        }

        /** {@inheritDoc} */
        @Override public void store(@Nullable String spaceName, Object key, Object val, long expirationTime)
            throws IgniteSpiException {
            idx.put(key, val);
        }

        /** {@inheritDoc} */
        @Override public void remove(@Nullable String spaceName, Object key) throws IgniteSpiException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void onSwap(@Nullable String spaceName, Object key) throws IgniteSpiException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void onUnswap(@Nullable String spaceName, Object key, Object val) throws IgniteSpiException {
            // No-op.
        }
    }

    /**
     * Broken Indexing Spi implementation for test
     */
    private class MyBrokenIndexingSpi extends MyIndexingSpi {
        /** {@inheritDoc} */
        @Override public void store(@Nullable String spaceName, Object key, Object val,
            long expirationTime) throws IgniteSpiException {
            throw new IgniteSpiException("Test exception");
        }
    }
}
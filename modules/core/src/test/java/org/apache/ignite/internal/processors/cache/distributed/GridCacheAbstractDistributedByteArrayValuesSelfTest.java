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

package org.apache.ignite.internal.processors.cache.distributed;

import java.util.Arrays;
import java.util.Collections;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.GridCacheAbstractByteArrayValuesSelfTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;
import static org.junit.Assert.assertArrayEquals;

/**
 * Tests for byte array values in distributed caches.
 */
public abstract class GridCacheAbstractDistributedByteArrayValuesSelfTest extends
    GridCacheAbstractByteArrayValuesSelfTest {
    /** Grids. */
    protected static Ignite[] ignites;

    /** Regular caches. */
    private static IgniteCache<Integer, Object>[] caches;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(igniteInstanceName);

        c.setCacheConfiguration(cacheConfiguration());

        c.setPeerClassLoadingEnabled(peerClassLoading());

        return c;
    }

    /**
     * @return Whether peer class loading is enabled.
     */
    protected abstract boolean peerClassLoading();

    /**
     * @return How many grids to start.
     */
    protected int gridCount() {
        return 3;
    }

    /**
     * @return Cache configuration.
     */
    protected CacheConfiguration cacheConfiguration() {
        CacheConfiguration cfg = cacheConfiguration0();

        cfg.setName(CACHE_REGULAR);

        return cfg;
    }

    /**
     * @return Internal cache configuration.
     */
    protected abstract CacheConfiguration cacheConfiguration0();

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected void beforeTestsStarted() throws Exception {
        int gridCnt = gridCount();

        assert gridCnt > 0;

        ignites = new Ignite[gridCnt];

        caches = new IgniteCache[gridCnt];

        for (int i = 0; i < gridCnt; i++) {
            ignites[i] = startGrid(i);

            caches[i] = ignites[i].cache(CACHE_REGULAR);
        }
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        caches = null;

        ignites = null;
    }

    /**
     * Check whether cache with byte array entry works correctly in PESSIMISTIC transaction.
     *
     * @throws Exception If failed.
     */
    public void testPessimistic() throws Exception {
        testTransaction0(caches, PESSIMISTIC, KEY_1, wrap(1));
    }

    /**
     * Check whether cache with byte array entry works correctly in PESSIMISTIC transaction.
     *
     * @throws Exception If failed.
     */
    public void testPessimisticMixed() throws Exception {
        testTransactionMixed0(caches, PESSIMISTIC, KEY_1, wrap(1), KEY_2, 1);
    }

    /**
     * Check whether cache with byte array entry works correctly in OPTIMISTIC transaction.
     *
     * @throws Exception If failed.
     */
    public void testOptimistic() throws Exception {
        testTransaction0(caches, OPTIMISTIC, KEY_1, wrap(1));
    }

    /**
     * Check whether cache with byte array entry works correctly in OPTIMISTIC transaction.
     *
     * @throws Exception If failed.
     */
    public void testOptimisticMixed() throws Exception {
        testTransactionMixed0(caches, OPTIMISTIC, KEY_1, wrap(1), KEY_2, 1);
    }

    /**
     * Test transaction behavior.
     *
     * @param caches Caches.
     * @param concurrency Concurrency.
     * @param key Key.
     * @param val Value.
     * @throws Exception If failed.
     */
    private void testTransaction0(IgniteCache<Integer, Object>[] caches, TransactionConcurrency concurrency,
        Integer key, byte[] val) throws Exception {
        testTransactionMixed0(caches, concurrency, key, val, null, null);
    }

    /**
     * Test transaction behavior.
     *
     * @param caches Caches.
     * @param concurrency Concurrency.
     * @param key1 Key 1.
     * @param val1 Value 1.
     * @param key2 Key 2.
     * @param val2 Value 2.
     * @throws Exception If failed.
     */
    private void testTransactionMixed0(IgniteCache<Integer, Object>[] caches, TransactionConcurrency concurrency,
        Integer key1, byte[] val1, @Nullable Integer key2, @Nullable Object val2) throws Exception {
        for (IgniteCache<Integer, Object> cache : caches) {
            info("Checking cache: " + cache.getName());

            Transaction tx = cache.unwrap(Ignite.class).transactions().txStart(concurrency, REPEATABLE_READ);

            try {
                cache.put(key1, val1);

                if (key2 != null)
                    cache.put(key2, val2);

                tx.commit();
            }
            finally {
                tx.close();
            }

            for (IgniteCache<Integer, Object> cacheInner : caches) {
                info("Getting value from cache: " + cacheInner.getName());

                tx = cacheInner.unwrap(Ignite.class).transactions().txStart(concurrency, REPEATABLE_READ);

                try {
                    assertArrayEquals(val1, (byte[])cacheInner.get(key1));

                    if (key2 != null) {
                        Object actual = cacheInner.get(key2);

                        assertEquals(val2, actual);
                    }

                    tx.commit();
                }
                finally {
                    tx.close();
                }
            }

            tx = cache.unwrap(Ignite.class).transactions().txStart(concurrency, REPEATABLE_READ);

            try {
                cache.remove(key1);

                if (key2 != null)
                    cache.remove(key2);

                tx.commit();
            }
            finally {
                tx.close();
            }

            assertNull(cache.get(key1));
        }
    }
}
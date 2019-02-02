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

package org.apache.ignite.internal.processors.cache.local;

import java.util.Arrays;
import java.util.Collections;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.GridCacheAbstractByteArrayValuesSelfTest;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.MvccFeatureChecker;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.jetbrains.annotations.Nullable;
import org.junit.Before;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.LOCAL;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Byte values test for LOCAL cache.
 */
public class GridCacheLocalByteArrayValuesSelfTest extends GridCacheAbstractByteArrayValuesSelfTest {
    /** Grid. */
    private static Ignite ignite;

    /** Regular cache. */
    private static IgniteCache<Integer, Object> cache;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        MvccFeatureChecker.skipIfNotSupported(MvccFeatureChecker.Feature.LOCAL_CACHE);

        IgniteConfiguration c = super.getConfiguration(igniteInstanceName);

        c.getTransactionConfiguration().setTxSerializableEnabled(true);

        CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        ccfg.setAtomicityMode(TRANSACTIONAL);
        ccfg.setCacheMode(LOCAL);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);

        c.setCacheConfiguration(ccfg);

        return c;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        ignite = startGrid(1);

        cache = ignite.cache(DEFAULT_CACHE_NAME);
    }
    /** */

    @Before
    public void beforeGridCacheLocalByteArrayValuesSelfTest() {
        MvccFeatureChecker.skipIfNotSupported(MvccFeatureChecker.Feature.LOCAL_CACHE);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        cache = null;
        ignite = null;
    }

    /**
     * Check whether cache with byte array entry works correctly in PESSIMISTIC transaction.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testPessimistic() throws Exception {
        testTransaction(cache, PESSIMISTIC, KEY_1, wrap(1));
    }

    /**
     * Check whether cache with byte array entry works correctly in PESSIMISTIC transaction.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testPessimisticMixed() throws Exception {
        testTransactionMixed(cache, PESSIMISTIC, KEY_1, wrap(1), KEY_2, 1);
    }

    /**
     * Check whether cache with byte array entry works correctly in OPTIMISTIC transaction.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testOptimistic() throws Exception {
        testTransaction(cache, OPTIMISTIC, KEY_1, wrap(1));
    }

    /**
     * Check whether cache with byte array entry works correctly in OPTIMISTIC transaction.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testOptimisticMixed() throws Exception {
        testTransactionMixed(cache, OPTIMISTIC, KEY_1, wrap(1), KEY_2, 1);
    }

    /**
     * Test byte array entry swapping.
     *
     * @throws Exception If failed.
     */
    @SuppressWarnings("TooBroadScope")
    @Test
    public void testSwap() throws Exception {
        // TODO GG-11148.
        // assert cache.getConfiguration(CacheConfiguration.class).isSwapEnabled();

        byte[] val1 = wrap(1);
        Object val2 = 2;

        cache.put(KEY_1, val1);
        cache.put(KEY_2, val2);

        assert Arrays.equals(val1, (byte[])cache.get(KEY_1));
        assert F.eq(val2, cache.get(KEY_2));

        cache.localEvict(Collections.singleton(KEY_1));
        cache.localEvict(Collections.singleton(KEY_2));

        assert cache.localPeek(KEY_1, CachePeekMode.ONHEAP) == null;
        assert cache.localPeek(KEY_2, CachePeekMode.ONHEAP) == null;
    }

    /**
     * Test transaction behavior.
     *
     * @param cache Cache.
     * @param concurrency Concurrency.
     * @param key Key.
     * @param val Value.
     * @throws Exception If failed.
     */
    private void testTransaction(IgniteCache<Integer, Object> cache, TransactionConcurrency concurrency,
        Integer key, byte[] val) throws Exception {
        testTransactionMixed(cache, concurrency, key, val, null, null);
    }

    /**
     * Test transaction behavior.
     *
     * @param cache Cache.
     * @param concurrency Concurrency.
     * @param key1 Key 1.
     * @param val1 Value 1.
     * @param key2 Key 2.
     * @param val2 Value 2.
     * @throws Exception If failed.
     */
    private void testTransactionMixed(IgniteCache<Integer, Object> cache, TransactionConcurrency concurrency,
        Integer key1, byte[] val1, @Nullable Integer key2, @Nullable Object val2) throws Exception {

        Transaction tx = ignite.transactions().txStart(concurrency, REPEATABLE_READ);

        try {
            cache.put(key1, val1);

            if (key2 != null)
                cache.put(key2, val2);

            tx.commit();
        }
        finally {
            tx.close();
        }

        tx = ignite.transactions().txStart(concurrency, REPEATABLE_READ);

        try {
            assert Arrays.equals(val1, (byte[])cache.get(key1));

            if (key2 != null)
                assert F.eq(val2, cache.get(key2));

            tx.commit();
        }
        finally {
            tx.close();
        }
    }
}

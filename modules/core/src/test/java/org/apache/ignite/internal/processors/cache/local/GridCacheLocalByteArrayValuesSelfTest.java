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

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.transactions.*;
import org.apache.ignite.spi.swapspace.file.*;
import org.apache.ignite.internal.util.typedef.*;
import org.jetbrains.annotations.*;

import java.util.*;

import static org.apache.ignite.cache.CacheAtomicityMode.*;
import static org.apache.ignite.cache.CacheMemoryMode.*;
import static org.apache.ignite.cache.CacheMode.*;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.*;
import static org.apache.ignite.transactions.IgniteTxConcurrency.*;
import static org.apache.ignite.transactions.IgniteTxIsolation.*;

/**
 * Byte values test for LOCAL cache.
 */
public class GridCacheLocalByteArrayValuesSelfTest extends GridCacheAbstractByteArrayValuesSelfTest {
    /** Grid. */
    private static Ignite ignite;

    /** Regular cache. */
    private static Cache<Integer, Object> cache;

    /** Offheap cache. */
    private static Cache<Integer, Object> cacheOffheap;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        c.getTransactionsConfiguration().setTxSerializableEnabled(true);

        CacheConfiguration cc1 = new CacheConfiguration();

        cc1.setName(CACHE_REGULAR);
        cc1.setAtomicityMode(TRANSACTIONAL);
        cc1.setCacheMode(LOCAL);
        cc1.setWriteSynchronizationMode(FULL_SYNC);
        cc1.setSwapEnabled(true);
        cc1.setEvictSynchronized(false);
        cc1.setEvictNearSynchronized(false);

        CacheConfiguration cc2 = new CacheConfiguration();

        cc2.setName(CACHE_OFFHEAP);
        cc2.setAtomicityMode(TRANSACTIONAL);
        cc2.setCacheMode(LOCAL);
        cc2.setWriteSynchronizationMode(FULL_SYNC);
        cc2.setMemoryMode(OFFHEAP_VALUES);
        cc2.setOffHeapMaxMemory(100 * 1024 * 1024);
        cc2.setQueryIndexEnabled(false);

        c.setCacheConfiguration(cc1, cc2);

        c.setSwapSpaceSpi(new FileSwapSpaceSpi());

        return c;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        ignite = startGrid(1);

        cache = ignite.cache(CACHE_REGULAR);
        cacheOffheap = ignite.cache(CACHE_OFFHEAP);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        cache = null;
        cacheOffheap = null;

        ignite = null;
    }

    /**
     * Check whether cache with byte array entry works correctly in PESSIMISTIC transaction.
     *
     * @throws Exception If failed.
     */
    public void testPessimistic() throws Exception {
        testTransaction(cache, PESSIMISTIC, KEY_1, wrap(1));
    }

    /**
     * Check whether cache with byte array entry works correctly in PESSIMISTIC transaction.
     *
     * @throws Exception If failed.
     */
    public void testPessimisticMixed() throws Exception {
        testTransactionMixed(cache, PESSIMISTIC, KEY_1, wrap(1), KEY_2, 1);
    }

    /**
     * Check whether offheap cache with byte array entry works correctly in PESSIMISTIC transaction.
     *
     * @throws Exception If failed.
     */
    public void testPessimisticOffheap() throws Exception {
        testTransaction(cacheOffheap, PESSIMISTIC, KEY_1, wrap(1));
    }

    /**
     * Check whether offheap cache with byte array entry works correctly in PESSIMISTIC transaction.
     *
     * @throws Exception If failed.
     */
    public void testPessimisticOffheapMixed() throws Exception {
        testTransactionMixed(cacheOffheap, PESSIMISTIC, KEY_1, wrap(1), KEY_2, 1);
    }

    /**
     * Check whether cache with byte array entry works correctly in OPTIMISTIC transaction.
     *
     * @throws Exception If failed.
     */
    public void testOptimistic() throws Exception {
        testTransaction(cache, OPTIMISTIC, KEY_1, wrap(1));
    }

    /**
     * Check whether cache with byte array entry works correctly in OPTIMISTIC transaction.
     *
     * @throws Exception If failed.
     */
    public void testOptimisticMixed() throws Exception {
        testTransactionMixed(cache, OPTIMISTIC, KEY_1, wrap(1), KEY_2, 1);
    }

    /**
     * Check whether offheap cache with byte array entry works correctly in OPTIMISTIC transaction.
     *
     * @throws Exception If failed.
     */
    public void testOptimisticOffheap() throws Exception {
        testTransaction(cacheOffheap, OPTIMISTIC, KEY_1, wrap(1));
    }

    /**
     * Check whether offheap cache with byte array entry works correctly in OPTIMISTIC transaction.
     *
     * @throws Exception If failed.
     */
    public void testOptimisticOffheapMixed() throws Exception {
        testTransactionMixed(cacheOffheap, OPTIMISTIC, KEY_1, wrap(1), KEY_2, 1);
    }

    /**
     * Test byte array entry swapping.
     *
     * @throws Exception If failed.
     */
    @SuppressWarnings("TooBroadScope")
    public void testSwap() throws Exception {
        assert cache.configuration().isSwapEnabled();

        byte[] val1 = wrap(1);
        Object val2 = 2;

        cache.put(KEY_1, val1);
        cache.put(KEY_2, val2);

        assert Arrays.equals(val1, (byte[])cache.get(KEY_1));
        assert F.eq(val2, cache.get(KEY_2));

        assert cache.evict(KEY_1);
        assert cache.evict(KEY_2);

        assert cache.peek(KEY_1) == null;
        assert cache.peek(KEY_2) == null;

        assert Arrays.equals(val1, (byte[])cache.promote(KEY_1));
        assert F.eq(val2, cache.promote(KEY_2));
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
    private void testTransaction(Cache<Integer, Object> cache, IgniteTxConcurrency concurrency,
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
    private void testTransactionMixed(Cache<Integer, Object> cache, IgniteTxConcurrency concurrency,
        Integer key1, byte[] val1, @Nullable Integer key2, @Nullable Object val2) throws Exception {

        IgniteTx tx = cache.txStart(concurrency, REPEATABLE_READ);

        try {
            cache.put(key1, val1);

            if (key2 != null)
                cache.put(key2, val2);

            tx.commit();
        }
        finally {
            tx.close();
        }

        tx = cache.txStart(concurrency, REPEATABLE_READ);

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

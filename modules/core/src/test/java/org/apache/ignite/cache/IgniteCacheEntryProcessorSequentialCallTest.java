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

package org.apache.ignite.cache;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.transactions.TransactionOptimisticException;
import org.junit.Test;

/**
 */
public class IgniteCacheEntryProcessorSequentialCallTest extends GridCommonAbstractTest {
    /** */
    private static final String CACHE = "cache";

    /** */
    private static final String MVCC_CACHE = "mvccCache";

    /** */
    private String cacheName;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrids(2);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cacheName = CACHE;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration ccfg = cacheConfiguration(CACHE);

        CacheConfiguration mvccCfg = cacheConfiguration(MVCC_CACHE)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT);

        cfg.setCacheConfiguration(ccfg, mvccCfg);

        return cfg;
    }

    /**
     *
     * @return Cache configuration.
     * @param name Cache name.
     */
    private CacheConfiguration cacheConfiguration(String name) {
        CacheConfiguration cacheCfg = new CacheConfiguration(name);

        cacheCfg.setCacheMode(CacheMode.PARTITIONED);
        cacheCfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        cacheCfg.setRebalanceMode(CacheRebalanceMode.SYNC);
        cacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        cacheCfg.setMaxConcurrentAsyncOperations(0);
        cacheCfg.setBackups(0);
        return cacheCfg;
    }

    /**
     *
     */
    @Test
    public void testOptimisticSerializableTxInvokeSequentialCall() throws Exception {
        transactionInvokeSequentialCallOnPrimaryNode(TransactionConcurrency.OPTIMISTIC, TransactionIsolation.SERIALIZABLE);

        transactionInvokeSequentialCallOnNearNode(TransactionConcurrency.OPTIMISTIC, TransactionIsolation.SERIALIZABLE);
    }

    /**
     *
     */
    @Test
    public void testOptimisticRepeatableReadTxInvokeSequentialCall() throws Exception {
        transactionInvokeSequentialCallOnPrimaryNode(TransactionConcurrency.OPTIMISTIC, TransactionIsolation.REPEATABLE_READ);

        transactionInvokeSequentialCallOnNearNode(TransactionConcurrency.OPTIMISTIC, TransactionIsolation.REPEATABLE_READ);
    }

    /**
     *
     */
    @Test
    public void testOptimisticReadCommittedTxInvokeSequentialCall() throws Exception {
        transactionInvokeSequentialCallOnPrimaryNode(TransactionConcurrency.OPTIMISTIC, TransactionIsolation.READ_COMMITTED);

        transactionInvokeSequentialCallOnNearNode(TransactionConcurrency.OPTIMISTIC, TransactionIsolation.READ_COMMITTED);
    }

    /**
     *
     */
    @Test
    public void testPessimisticSerializableTxInvokeSequentialCall() throws Exception {
        transactionInvokeSequentialCallOnPrimaryNode(TransactionConcurrency.PESSIMISTIC, TransactionIsolation.SERIALIZABLE);

        transactionInvokeSequentialCallOnNearNode(TransactionConcurrency.PESSIMISTIC, TransactionIsolation.SERIALIZABLE);
    }

    /**
     *
     */
    @Test
    public void testPessimisticRepeatableReadTxInvokeSequentialCall() throws Exception {
        transactionInvokeSequentialCallOnPrimaryNode(TransactionConcurrency.PESSIMISTIC, TransactionIsolation.REPEATABLE_READ);

        transactionInvokeSequentialCallOnNearNode(TransactionConcurrency.PESSIMISTIC, TransactionIsolation.REPEATABLE_READ);
    }

    /**
     *
     */
    @Test
    public void testPessimisticReadCommittedTxInvokeSequentialCall() throws Exception {
        transactionInvokeSequentialCallOnPrimaryNode(TransactionConcurrency.PESSIMISTIC, TransactionIsolation.READ_COMMITTED);

        transactionInvokeSequentialCallOnNearNode(TransactionConcurrency.PESSIMISTIC, TransactionIsolation.READ_COMMITTED);
    }

    /**
     *
     */
    @Test
    public void testMvccTxInvokeSequentialCall() throws Exception {
        cacheName = MVCC_CACHE;

        transactionInvokeSequentialCallOnPrimaryNode(TransactionConcurrency.PESSIMISTIC, TransactionIsolation.REPEATABLE_READ);

        transactionInvokeSequentialCallOnNearNode(TransactionConcurrency.PESSIMISTIC, TransactionIsolation.REPEATABLE_READ);
    }

    /**
     * Test for sequential entry processor invoking not null value on primary cache.
     * In this test entry processor gets value from local node.
     *
     * @param transactionConcurrency Transaction concurrency.
     * @param transactionIsolation Transaction isolation.
     */
    public void transactionInvokeSequentialCallOnPrimaryNode(TransactionConcurrency transactionConcurrency,
        TransactionIsolation transactionIsolation) throws Exception {
        TestKey key = new TestKey(1L);
        TestValue val = new TestValue();
        val.value("1");

        Ignite primaryIgnite;

        if (ignite(0).affinity(cacheName).isPrimary(ignite(0).cluster().localNode(), key))
            primaryIgnite = ignite(0);
        else
            primaryIgnite = ignite(1);

        IgniteCache<TestKey, TestValue> cache = primaryIgnite.cache(cacheName);

        cache.put(key, val);

        NotNullCacheEntryProcessor cacheEntryProcessor = new NotNullCacheEntryProcessor();

        try (Transaction transaction = primaryIgnite.transactions().txStart(transactionConcurrency,
            transactionIsolation)) {

            cache.invoke(key, cacheEntryProcessor);
            cache.invoke(key, cacheEntryProcessor);

            transaction.commit();
        }

        cache.remove(key);
    }

    /**
     * Test for sequential entry processor invoking not null value on near cache.
     * In this test entry processor fetches value from remote node.
     *
     * @param transactionConcurrency Transaction concurrency.
     * @param transactionIsolation Transaction isolation.
     */
    public void transactionInvokeSequentialCallOnNearNode(TransactionConcurrency transactionConcurrency,
        TransactionIsolation transactionIsolation) throws Exception {
        TestKey key = new TestKey(1L);
        TestValue val = new TestValue();
        val.value("1");

        Ignite nearIgnite;
        Ignite primaryIgnite;

        if (ignite(0).affinity(cacheName).isPrimary(ignite(0).cluster().localNode(), key)) {
            primaryIgnite = ignite(0);

            nearIgnite = ignite(1);
        }
        else {
            primaryIgnite = ignite(1);

            nearIgnite = ignite(0);
        }

        primaryIgnite.cache(cacheName).put(key, val);

        IgniteCache<TestKey, TestValue> nearCache = nearIgnite.cache(cacheName);

        NotNullCacheEntryProcessor cacheEntryProcessor = new NotNullCacheEntryProcessor();

        try (Transaction transaction = nearIgnite.transactions().txStart(transactionConcurrency,
            transactionIsolation)) {

            nearCache.invoke(key, cacheEntryProcessor);
            nearCache.invoke(key, cacheEntryProcessor);

            transaction.commit();
        }

        primaryIgnite.cache(cacheName).remove(key);
    }

    /**
     * Test for sequential entry processor invocation. During transaction value is changed externally, which leads to
     * optimistic conflict exception.
     */
    @Test
    @SuppressWarnings("ThrowableNotThrown")
    public void testTxInvokeSequentialOptimisticConflict() throws Exception {
        TestKey key = new TestKey(1L);

        IgniteCache<TestKey, TestValue> cache = ignite(0).cache(CACHE);

        CountDownLatch latch = new CountDownLatch(1);

        cache.put(key, new TestValue("1"));

        multithreadedAsync(new Runnable() {
            @Override public void run() {
                try {
                    latch.await();
                }
                catch (InterruptedException e) {
                    fail();
                }

                cache.put(key, new TestValue("2"));
            }
        }, 1);

        Transaction tx = ignite(0).transactions().txStart(TransactionConcurrency.OPTIMISTIC, TransactionIsolation.SERIALIZABLE);

        cache.invoke(key, new NotNullCacheEntryProcessor());

        latch.countDown();

        Thread.sleep(1_000);

        cache.invoke(key, new NotNullCacheEntryProcessor());

        GridTestUtils.assertThrowsWithCause(new Callable<Object>() {
            @Override public Object call() throws Exception {
                tx.commit();

                return null;
            }
        }, TransactionOptimisticException.class);

        cache.remove(key);
    }

    /**
     * Cache entry processor checking whether entry has got non-null value.
     */
    public static class NotNullCacheEntryProcessor implements CacheEntryProcessor<TestKey, TestValue, Object> {
        /** {@inheritDoc} */
        @Override public Object process(MutableEntry entry, Object... arguments) throws EntryProcessorException {
            assertNotNull(entry.getValue());

            return null;
        }
    }

    /**
     *
     */
    public static class TestKey {
        /** Value. */
        private final Long val;

        /**
         * @param val Value.
         */
        public TestKey(Long val) {
            this.val = val;
        }
    }

    /**
     *
     */
    public static class TestValue {
        /** Value. */
        private String val;

        /**
         * Default constructor.
         */
        public TestValue() {
        }

        /**
         * @param val Value.
         */
        public TestValue(String val) {
            this.val = val;
        }

        /**
         * @return Value.
         */
        public String value() {
            return val;
        }

        /**
         * @param val New value.
         */
        public void value(String val) {
            this.val = val;
        }
    }
}

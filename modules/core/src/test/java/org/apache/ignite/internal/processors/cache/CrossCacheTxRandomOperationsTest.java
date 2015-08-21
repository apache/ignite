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

package org.apache.ignite.internal.processors.cache;

import junit.framework.*;
import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cache.affinity.fair.*;
import org.apache.ignite.cache.affinity.rendezvous.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.util.lang.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.testframework.*;
import org.apache.ignite.testframework.junits.common.*;
import org.apache.ignite.transactions.*;
import org.jetbrains.annotations.*;

import javax.cache.processor.*;
import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import static org.apache.ignite.cache.CacheAtomicityMode.*;
import static org.apache.ignite.cache.CacheMode.*;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.*;
import static org.apache.ignite.transactions.TransactionConcurrency.*;
import static org.apache.ignite.transactions.TransactionIsolation.*;

/**
 *
 */
public class CrossCacheTxRandomOperationsTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final String CACHE1 = "cache1";

    /** */
    private static final String CACHE2 = "cache2";

    /** */
    private static final int GRID_CNT = 5;

    /** */
    private static final int KEY_RANGE = 1000;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(IP_FINDER);

        if (gridName.equals(getTestGridName(GRID_CNT - 1)))
            cfg.setClientMode(true);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGridsMultiThreaded(GRID_CNT);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxOperations() throws Exception {
        txOperations(PARTITIONED, FULL_SYNC, false, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testCrossCacheTxOperations() throws Exception {
        txOperations(PARTITIONED, FULL_SYNC, true, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testCrossCacheTxOperationsPrimarySync() throws Exception {
        txOperations(PARTITIONED, PRIMARY_SYNC, true, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void _testCrossCacheTxOperationsFairAffinity() throws Exception {
        txOperations(PARTITIONED, FULL_SYNC, true, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testCrossCacheTxOperationsReplicated() throws Exception {
        txOperations(REPLICATED, FULL_SYNC, true, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testCrossCacheTxOperationsReplicatedPrimarySync() throws Exception {
        txOperations(REPLICATED, PRIMARY_SYNC, true, false);
    }

    /**
     * @param name Cache name.
     * @param cacheMode Cache mode.
     * @param writeSync Write synchronization mode.
     * @param fairAff If {@code true} uses {@link FairAffinityFunction}, otherwise {@link RendezvousAffinityFunction}.
     * @return Cache configuration.
     */
    private CacheConfiguration cacheConfiguration(String name,
        CacheMode cacheMode,
        CacheWriteSynchronizationMode writeSync,
        boolean fairAff) {
        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setName(name);
        ccfg.setCacheMode(cacheMode);
        ccfg.setAtomicityMode(TRANSACTIONAL);
        ccfg.setWriteSynchronizationMode(writeSync);

        if (cacheMode == PARTITIONED)
            ccfg.setBackups(1);

        ccfg.setAffinity(fairAff ? new FairAffinityFunction() : new RendezvousAffinityFunction());

        return ccfg;
    }

    /**
     * @param cacheMode Cache mode.
     * @param writeSync Write synchronization mode.
     * @param crossCacheTx If {@code true} uses cross cache transaction.
     * @param fairAff If {@code true} uses {@link FairAffinityFunction}, otherwise {@link RendezvousAffinityFunction}.
     * @throws Exception If failed.
     */
    private void txOperations(CacheMode cacheMode,
        CacheWriteSynchronizationMode writeSync,
        boolean crossCacheTx,
        boolean fairAff) throws Exception {
        Ignite ignite = ignite(0);

        try {
            ignite.createCache(cacheConfiguration(CACHE1, cacheMode, writeSync, fairAff));
            ignite.createCache(cacheConfiguration(CACHE2, cacheMode, writeSync, fairAff));

            txOperations(PESSIMISTIC, REPEATABLE_READ, crossCacheTx, false);
            txOperations(PESSIMISTIC, REPEATABLE_READ, crossCacheTx, true);

            txOperations(OPTIMISTIC, REPEATABLE_READ, crossCacheTx, false);
            txOperations(OPTIMISTIC, REPEATABLE_READ, crossCacheTx, true);
        }
        finally {
            ignite.destroyCache(CACHE1);
            ignite.destroyCache(CACHE2);
        }
    }

    /**
     * @param concurrency Transaction concurrency.
     * @param isolation Transaction isolation.
     * @param crossCacheTx If {@code true} uses cross cache transaction.
     * @param client If {@code true} uses client node.
     * @throws Exception If failed.
     */
    private void txOperations(TransactionConcurrency concurrency,
        TransactionIsolation isolation,
        boolean crossCacheTx,
        boolean client) throws Exception {
        final Map<TestKey, TestValue> expData1 = new HashMap<>();
        final Map<TestKey, TestValue> expData2 = new HashMap<>();

        Ignite ignite = client ? ignite(GRID_CNT - 1) : ignite(0);

        assertEquals(client, (boolean)ignite.configuration().isClientMode());

        IgniteCache<TestKey, TestValue> cache1 = ignite.cache(CACHE1);
        IgniteCache<TestKey, TestValue> cache2 = ignite.cache(CACHE2);

        assertNotNull(cache1);
        assertNotNull(cache2);
        assertNotSame(cache1, cache2);

        try {
            ThreadLocalRandom rnd = ThreadLocalRandom.current();

            long seed = System.currentTimeMillis();

            log.info("Test tx operations [concurrency=" + concurrency +
                ", isolation=" + isolation +
                ", client=" + client +
                ", seed=" + seed + ']');

            IgniteTransactions txs = ignite.transactions();

            final List<TestKey> keys = new ArrayList<>();

            for (int i = 0; i < KEY_RANGE; i++)
                keys.add(new TestKey(i));

            for (int i = 0; i < 10_000; i++) {
                if (i % 100 == 0)
                    log.info("Iteration: " + i);

                boolean rollback = i % 10 == 0;

                try (Transaction tx = txs.txStart(concurrency, isolation)) {
                    cacheOperation(expData1, rnd, cache1, concurrency == OPTIMISTIC, rollback);

                    if (crossCacheTx)
                        cacheOperation(expData2, rnd, cache2, concurrency == OPTIMISTIC, rollback);

                    if (rollback)
                        tx.rollback();
                    else
                        tx.commit();
                }
            }

            final List<IgniteCache<TestKey, TestValue>> caches1 = new ArrayList<>();
            final List<IgniteCache<TestKey, TestValue>> caches2 = new ArrayList<>();

            for (int i = 0; i < GRID_CNT; i++) {
                caches1.add(ignite(i).<TestKey, TestValue>cache(CACHE1));
                caches2.add(ignite(i).<TestKey, TestValue>cache(CACHE2));
            }

            CacheConfiguration ccfg = cache1.getConfiguration(CacheConfiguration.class);

            if (ccfg.getWriteSynchronizationMode() == FULL_SYNC) {
                checkData(caches1, keys, expData1);
                checkData(caches2, keys, expData2);
            }
            else {
                boolean pass = GridTestUtils.waitForCondition(new GridAbsPredicate() {
                    @Override public boolean apply() {
                        try {
                            checkData(caches1, keys, expData1);
                            checkData(caches2, keys, expData2);
                        }
                        catch (AssertionFailedError e) {
                            log.info("Data check failed, will retry.");
                        }

                        return true;
                    }
                }, 5000);

                if (!pass) {
                    checkData(caches1, keys, expData1);
                    checkData(caches2, keys, expData2);
                }
            }
        }
        finally {
            cache1.removeAll();
            cache2.removeAll();
        }
    }

    /**
     * @param caches Caches.
     * @param keys Keys.
     * @param expData Expected data.
     */
    private void checkData(List<IgniteCache<TestKey, TestValue>> caches,
        List<TestKey> keys, Map<TestKey, TestValue> expData) {
        for (IgniteCache<TestKey, TestValue> cache : caches) {
            for (TestKey key : keys) {
                TestValue val = cache.get(key);
                TestValue expVal = expData.get(key);

                assertEquals(expVal, val);
            }
        }
    }

    /**
     * @param expData Expected cache data.
     * @param rnd Random.
     * @param cache Cache.
     * @param optimistic {@code True} if test uses optimistic transaction.
     * @param willRollback {@code True} if will rollback transaction.
     */
    private void cacheOperation(
        Map<TestKey, TestValue> expData,
        ThreadLocalRandom rnd,
        IgniteCache<TestKey, TestValue> cache,
        boolean optimistic,
        boolean willRollback) {
        TestKey key = key(rnd);
        TestValue val = new TestValue(rnd.nextLong());

        switch (rnd.nextInt(8)) {
            case 0: {
                cache.put(key, val);

                if (!willRollback)
                    expData.put(key, val);

                break;
            }

            case 1: {
                TestValue oldVal = cache.getAndPut(key, val);

                TestValue expOld = expData.get(key);

                if (!optimistic)
                    assertEquals(expOld, oldVal);

                if (!willRollback)
                    expData.put(key, val);

                break;
            }

            case 2: {
                boolean rmv = cache.remove(key);

                if (!optimistic)
                    assertEquals(expData.containsKey(key), rmv);

                if (!willRollback)
                    expData.remove(key);

                break;
            }

            case 3: {
                TestValue oldVal = cache.getAndRemove(key);

                TestValue expOld = expData.get(key);

                if (!optimistic)
                    assertEquals(expOld, oldVal);

                if (!willRollback)
                    expData.remove(key);

                break;
            }

            case 4: {
                boolean put = cache.putIfAbsent(key, val);

                boolean expPut = !expData.containsKey(key);

                if (!optimistic)
                    assertEquals(expPut, put);

                if (expPut && !willRollback)
                    expData.put(key, val);

                break;
            }

            case 5: {
                TestValue oldVal = cache.invoke(key, new TestEntryProcessor(val.value()));
                TestValue expOld = expData.get(key);

                if (!optimistic)
                    assertEquals(expOld, oldVal);

                if (!willRollback)
                    expData.put(key, val);

                break;
            }

            case 6: {
                TestValue oldVal = cache.invoke(key, new TestEntryProcessor(null));
                TestValue expOld = expData.get(key);

                if (!optimistic)
                    assertEquals(expOld, oldVal);

                break;
            }

            case 7: {
                TestValue oldVal = cache.get(key);
                TestValue expOld = expData.get(key);

                assertEquals(expOld, oldVal);

                break;
            }

            default:
                assert false;
        }
    }

    /**
     * @param rnd Random.
     * @return Key.
     */
    private TestKey key(ThreadLocalRandom rnd) {
        return new TestKey(rnd.nextInt(KEY_RANGE));
    }

    /**
     *
     */
    private static class TestKey implements Serializable {
        /** */
        private long key;

        /**
         * @param key Key.
         */
        public TestKey(long key) {
            this.key = key;
        }

        /**
         * @return Key.
         */
        public long key() {
            return key;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            TestKey other = (TestKey)o;

            return key == other.key;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return (int)(key ^ (key >>> 32));
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(TestKey.class, this);
        }
    }

    /**
     *
     */
    private static class TestValue implements Serializable {
        /** */
        private long val;

        /**
         * @param val Value.
         */
        public TestValue(long val) {
            this.val = val;
        }

        /**
         * @return Value.
         */
        public long value() {
            return val;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            TestValue other = (TestValue)o;

            return val == other.val;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(TestValue.class, this);
        }
    }

    /**
     *
     */
    private static class TestEntryProcessor implements CacheEntryProcessor<TestKey, TestValue, TestValue> {
        /** */
        private Long val;

        /**
         * @param val Value.
         */
        public TestEntryProcessor(@Nullable Long val) {
            this.val = val;
        }

        /** {@inheritDoc} */
        @Override public TestValue process(MutableEntry<TestKey, TestValue> e, Object... args) {
            TestValue old = e.getValue();

            if (val != null)
                e.setValue(new TestValue(val));

            return old;
        }
    }
}

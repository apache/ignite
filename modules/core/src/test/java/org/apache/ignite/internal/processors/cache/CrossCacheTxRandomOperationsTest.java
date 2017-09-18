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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.fair.FairAffinityFunction;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.PRIMARY_SYNC;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;
import static org.apache.ignite.transactions.TransactionIsolation.SERIALIZABLE;

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
    @Override protected long getTestTimeout() {
        return 6 * 60 * 1000;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGridsMultiThreaded(GRID_CNT - 1);

        startGrid(GRID_CNT - 1);
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
    public void testCrossCacheTxOperationsFairAffinity() throws Exception {
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
    protected CacheConfiguration cacheConfiguration(String name,
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
     * @param fairAff Fair affinity flag.
     * @param ignite Node to use.
     * @param name Cache name.
     */
    protected void createCache(CacheMode cacheMode,
        CacheWriteSynchronizationMode writeSync,
        boolean fairAff,
        Ignite ignite,
        String name) {
        ignite.createCache(cacheConfiguration(name, cacheMode, writeSync, fairAff));
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
            createCache(cacheMode, writeSync, fairAff, ignite, CACHE1);
            createCache(cacheMode, writeSync, fairAff, ignite, CACHE2);

            txOperations(PESSIMISTIC, REPEATABLE_READ, crossCacheTx, false);
            txOperations(PESSIMISTIC, REPEATABLE_READ, crossCacheTx, true);

            txOperations(OPTIMISTIC, REPEATABLE_READ, crossCacheTx, false);
            txOperations(OPTIMISTIC, REPEATABLE_READ, crossCacheTx, true);

            if (writeSync == FULL_SYNC) {
                txOperations(OPTIMISTIC, SERIALIZABLE, crossCacheTx, false);
                txOperations(OPTIMISTIC, SERIALIZABLE, crossCacheTx, true);
            }
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

        final List<IgniteCache<TestKey, TestValue>> caches1 = new ArrayList<>();
        final List<IgniteCache<TestKey, TestValue>> caches2 = new ArrayList<>();

        for (int i = 0; i < GRID_CNT; i++) {
            caches1.add(ignite(i).<TestKey, TestValue>cache(CACHE1));
            caches2.add(ignite(i).<TestKey, TestValue>cache(CACHE2));
        }

        IgniteCache<TestKey, TestValue> cache1 = ignite.cache(CACHE1);
        IgniteCache<TestKey, TestValue> cache2 = ignite.cache(CACHE2);

        assertNotNull(cache1);
        assertNotNull(cache2);
        assertNotSame(cache1, cache2);

        try {
            Random rnd = new Random();

            long seed = System.currentTimeMillis();

            rnd.setSeed(seed);

            log.info("Test tx operations [concurrency=" + concurrency +
                ", isolation=" + isolation +
                ", client=" + client +
                ", seed=" + seed + ']');

            IgniteTransactions txs = ignite.transactions();

            final List<TestKey> keys = new ArrayList<>();

            for (int i = 0; i < KEY_RANGE; i++)
                keys.add(new TestKey(i));

            CacheConfiguration ccfg = cache1.getConfiguration(CacheConfiguration.class);

            boolean fullSync = ccfg.getWriteSynchronizationMode() == FULL_SYNC;
            boolean optimistic = concurrency == OPTIMISTIC;

            boolean checkData = fullSync && !optimistic;

            long stopTime = System.currentTimeMillis() + 10_000;

            for (int i = 0; i < 10_000; i++) {
                if (i % 100 == 0) {
                    if (System.currentTimeMillis() > stopTime) {
                        log.info("Stop on timeout, iteration: " + i);

                        break;
                    }

                    log.info("Iteration: " + i);
                }

                boolean rollback = i % 10 == 0;

                try (Transaction tx = txs.txStart(concurrency, isolation)) {
                    cacheOperation(expData1, rnd, cache1, checkData, rollback);

                    if (crossCacheTx)
                        cacheOperation(expData2, rnd, cache2, checkData, rollback);

                    if (rollback)
                        tx.rollback();
                    else
                        tx.commit();
                }
            }

            if (fullSync) {
                checkData(caches1, keys, expData1);
                checkData(caches2, keys, expData2);

                cache1.removeAll();
                cache2.removeAll();

                checkData(caches1, keys, new HashMap<TestKey, TestValue>());
                checkData(caches2, keys, new HashMap<TestKey, TestValue>());
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
     * @param checkData If {@code true} checks data.
     * @param willRollback {@code True} if will rollback transaction.
     */
    private void cacheOperation(
        Map<TestKey, TestValue> expData,
        Random rnd,
        IgniteCache<TestKey, TestValue> cache,
        boolean checkData,
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

                if (checkData)
                    assertEquals(expOld, oldVal);

                if (!willRollback)
                    expData.put(key, val);

                break;
            }

            case 2: {
                boolean rmv = cache.remove(key);

                if (checkData)
                    assertEquals(expData.containsKey(key), rmv);

                if (!willRollback)
                    expData.remove(key);

                break;
            }

            case 3: {
                TestValue oldVal = cache.getAndRemove(key);

                TestValue expOld = expData.get(key);

                if (checkData)
                    assertEquals(expOld, oldVal);

                if (!willRollback)
                    expData.remove(key);

                break;
            }

            case 4: {
                boolean put = cache.putIfAbsent(key, val);

                boolean expPut = !expData.containsKey(key);

                if (checkData)
                    assertEquals(expPut, put);

                if (expPut && !willRollback)
                    expData.put(key, val);

                break;
            }

            case 5: {
                TestValue oldVal = cache.invoke(key, new TestEntryProcessor(val.value()));
                TestValue expOld = expData.get(key);

                if (checkData)
                    assertEquals(expOld, oldVal);

                if (!willRollback)
                    expData.put(key, val);

                break;
            }

            case 6: {
                TestValue oldVal = cache.invoke(key, new TestEntryProcessor(null));
                TestValue expOld = expData.get(key);

                if (checkData)
                    assertEquals(expOld, oldVal);

                break;
            }

            case 7: {
                TestValue oldVal = cache.get(key);
                TestValue expOld = expData.get(key);

                if (checkData)
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
    private TestKey key(Random rnd) {
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

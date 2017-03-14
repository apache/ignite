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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ThreadLocalRandom;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMemoryMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.eviction.EvictionPolicy;
import org.apache.ignite.cache.eviction.lru.LruEvictionPolicy;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.cache.CacheAtomicWriteOrderMode.PRIMARY;
import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMemoryMode.OFFHEAP_TIERED;
import static org.apache.ignite.cache.CacheMemoryMode.ONHEAP_TIERED;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 *
 */
public class CacheRandomOperationsMultithreadedTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final int KEYS = 1000;

    /** */
    private static final int NODES = 4;

    /** */
    private boolean client;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(ipFinder);

        cfg.setClientMode(client);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGridsMultiThreaded(NODES - 1);

        client = true;

        startGrid(NODES - 1);

        super.beforeTestsStarted();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        super.afterTestsStopped();
    }

    /**
     * @throws Exception If failed.
     */
    public void testAtomicOffheapTiered() throws Exception {
        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(PARTITIONED,
            ATOMIC,
            OFFHEAP_TIERED,
            null,
            false);

        randomOperations(ccfg);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAtomicOffheapTieredIndexing() throws Exception {
        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(PARTITIONED,
            ATOMIC,
            OFFHEAP_TIERED,
            null,
            true);

        randomOperations(ccfg);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAtomicOffheapEviction() throws Exception {
        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(PARTITIONED,
            ATOMIC,
            ONHEAP_TIERED,
            new LruEvictionPolicy<>(10),
            false);

        randomOperations(ccfg);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAtomicOffheapEvictionIndexing() throws Exception {
        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(PARTITIONED,
            ATOMIC,
            ONHEAP_TIERED,
            new LruEvictionPolicy<>(10),
            true);

        randomOperations(ccfg);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxOffheapTiered() throws Exception {
        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(PARTITIONED,
            TRANSACTIONAL,
            OFFHEAP_TIERED,
            null,
            false);

        randomOperations(ccfg);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxOffheapTieredIndexing() throws Exception {
        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(PARTITIONED,
            TRANSACTIONAL,
            OFFHEAP_TIERED,
            null,
            true);

        randomOperations(ccfg);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxOffheapEviction() throws Exception {
        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(PARTITIONED,
            TRANSACTIONAL,
            ONHEAP_TIERED,
            new LruEvictionPolicy<>(10),
            false);

        randomOperations(ccfg);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxOffheapEvictionIndexing() throws Exception {
        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(PARTITIONED,
            TRANSACTIONAL,
            ONHEAP_TIERED,
            new LruEvictionPolicy<>(10),
            true);

        randomOperations(ccfg);
    }

    /**
     * @param ccfg CacheConfiguration.
     * @throws Exception If failed.
     */
    private void randomOperations(final CacheConfiguration<Object, Object> ccfg) throws Exception {
        ignite(0).createCache(ccfg);

        try {
            final long stopTime = U.currentTimeMillis() + 30_000;

            final boolean indexing = !F.isEmpty(ccfg.getIndexedTypes()) ||
                !F.isEmpty(ccfg.getQueryEntities());

            GridTestUtils.runMultiThreaded(new IgniteInClosure<Integer>() {
                @Override public void apply(Integer idx) {
                    Ignite ignite = ignite(idx % NODES);

                    IgniteCache<Object, Object> cache = ignite.cache(ccfg.getName());

                    ThreadLocalRandom rnd = ThreadLocalRandom.current();

                    while (U.currentTimeMillis() < stopTime)
                        randomOperation(rnd, cache, indexing);
                }
            }, 1, "test-thread");
        }
        finally {
            ignite(0).destroyCache(ccfg.getName());
        }
    }

    /**
     * @param rnd Random generator.
     * @param cache Cache.
     * @param indexing Indexing flag.
     */
    private void randomOperation(ThreadLocalRandom rnd,
        IgniteCache<Object, Object> cache,
        boolean indexing) {
        int r0 = rnd.nextInt(100);

        if (r0 == 0)
            cache.clear();
        else if (r0 == 1)
            cache.size();

        switch (rnd.nextInt(14)) {
            case 0: {
                cache.put(key(rnd), value(rnd));

                break;
            }

            case 1: {
                cache.getAndPut(key(rnd), value(rnd));

                break;
            }

            case 2: {
                cache.get(key(rnd));

                break;
            }

            case 3: {
                cache.remove(key(rnd));

                break;
            }

            case 4: {
                cache.getAndRemove(key(rnd));

                break;
            }

            case 5: {
                Map<Object, Object> map = new TreeMap<>();

                for (int i = 0; i < 50; i++)
                    map.put(key(rnd), value(rnd));

                cache.putAll(map);

                break;
            }

            case 6: {
                cache.getAll(keys(50, rnd));

                break;
            }

            case 7: {
                cache.removeAll(keys(50, rnd));

                break;
            }

            case 8: {
                cache.putIfAbsent(key(rnd), value(rnd));

                break;
            }

            case 9: {
                cache.getAndPutIfAbsent(key(rnd), value(rnd));

                break;
            }

            case 10: {
                cache.replace(key(rnd), value(rnd));

                break;
            }

            case 11: {
                cache.getAndReplace(key(rnd), value(rnd));

                break;
            }

            case 12: {
                ScanQuery<Object, Object> qry = new ScanQuery<>();
                qry.setFilter(new TestFilter());

                List<Cache.Entry<Object, Object>> res = cache.query(qry).getAll();

                assertTrue(res.size() >= 0);

                break;
            }

            case 13: {
                if (indexing) {
                    SqlQuery<Object, Object> qry = new SqlQuery<>(TestData.class, "where val1 > ?");
                    qry.setArgs(KEYS / 2);

                    List<Cache.Entry<Object, Object>> res = cache.query(qry).getAll();

                    assertTrue(res.size() >= 0);
                }

                break;
            }

            default:
                fail();
        }
    }

    /**
     * @param cnt Number of keys.
     * @param rnd Random generator.
     * @return Keys.
     */
    private Set<Object> keys(int cnt, ThreadLocalRandom rnd) {
        TreeSet<Object> keys = new TreeSet<>();

        for (int i = 0; i < cnt; i++)
            keys.add(key(rnd));

        return keys;
    }

    /**
     * @param rnd Random generator.
     * @return Key.
     */
    private Object key(ThreadLocalRandom rnd) {
        return new TestKey(rnd.nextInt(KEYS), rnd);
    }

    /**
     * @param rnd Random generator.
     * @return Value.
     */
    private Object value(ThreadLocalRandom rnd) {
        return new TestData(rnd);
    }

    /**
     * @param cacheMode Cache mode.
     * @param atomicityMode Cache atomicity mode.
     * @param memoryMode Cache memory mode.
     * @param evictionPlc Eviction policy.
     * @param indexing Indexing flag.
     * @return Cache configuration.
     */
    private CacheConfiguration<Object, Object> cacheConfiguration(
        CacheMode cacheMode,
        CacheAtomicityMode atomicityMode,
        CacheMemoryMode memoryMode,
        @Nullable  EvictionPolicy<Object, Object> evictionPlc,
        boolean indexing) {
        CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>();

        ccfg.setAtomicityMode(atomicityMode);
        ccfg.setCacheMode(cacheMode);
        ccfg.setMemoryMode(memoryMode);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setAtomicWriteOrderMode(PRIMARY);
        ccfg.setEvictionPolicy(evictionPlc);
        ccfg.setOffHeapMaxMemory(0);

        if (cacheMode == PARTITIONED)
            ccfg.setBackups(1);

        if (indexing)
            ccfg.setIndexedTypes(TestKey.class, TestData.class);

        return ccfg;
    }

    /**
     *
     */
    static class TestFilter implements IgniteBiPredicate<Object, Object> {
        /** {@inheritDoc} */
        @Override public boolean apply(Object key, Object val) {
            return ThreadLocalRandom.current().nextInt(10) == 0;
        }
    }

    /**
     *
     */
    static class TestKey implements Serializable, Comparable<TestKey> {
        /** */
        private int key;

        /** */
        private byte[] byteVal;

        /** {@inheritDoc} */
        @Override public int compareTo(TestKey o) {
            return Integer.compare(key, o.key);
        }

        /**
         * @param key Key.
         * @param rnd Random generator.
         */
        public TestKey(int key, ThreadLocalRandom rnd) {
            this.key = key;
            byteVal = new byte[rnd.nextInt(100)];
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            TestKey testKey = (TestKey)o;

            return key == testKey.key;
       }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return key;
        }
    }

    /**
     *
     */
    static class TestData implements Serializable {
        /** */
        @QuerySqlField(index = true)
        private int val1;

        /** */
        private long val2;

        /** */
        @QuerySqlField(index = true)
        private String val3;

        /** */
        private byte[] val4;

        /**
         * @param rnd Random generator.
         */
        public TestData(ThreadLocalRandom rnd) {
            val1 = rnd.nextInt();
            val2 = val1;
            val3 = String.valueOf(val1);
            val4 = new byte[rnd.nextInt(1024)];
        }
    }
}

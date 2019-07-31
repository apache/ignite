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

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.spi.metric.LongMetric;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.MvccFeatureChecker;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Assume;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheRebalanceMode.ASYNC;
import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.cacheMetricsRegistryName;

/**
 *
 */
public class CacheRemoveWithTombstonesLoadTest extends GridCommonAbstractTest {
    /** */
    private boolean persistence;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        DataStorageConfiguration dsCfg = new DataStorageConfiguration();

        if (persistence) {
            dsCfg.setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration().setMaxSize(100L * 1024 * 1024).setPersistenceEnabled(true))
                    .setWalMode(WALMode.LOG_ONLY);
        }

        dsCfg.setPageSize(1024);

        cfg.setDataStorageConfiguration(dsCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        Assume.assumeFalse(MvccFeatureChecker.forcedMvcc());
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        super.afterTest();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRemoveAndRebalance() throws Exception {
        removeAndRebalance();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRemoveAndRebalanceWithPersistence() throws Exception {
        persistence = true;

        testRemoveAndRebalance();
    }

    /**
     * @throws Exception If failed.
     */
    private void removeAndRebalance() throws Exception {
        IgniteEx ignite0 = startGrid(0);

        if (persistence)
            ignite0.cluster().active(true);

        final int pageSize = ignite0.configuration().getDataStorageConfiguration().getPageSize();

        assert pageSize > 0;

        IgniteCache<TestKey, TestValue> cache0 = ignite0.createCache(cacheConfiguration());

        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        List<TestKey> keys = new ArrayList<>();

        Map<TestKey, TestValue> data = new HashMap<>();

        final int KEYS = persistence ? 5_000 : 10_000;
        final int ADD_NODES = persistence ? 2 : 3;

        for (int i = 0; i < KEYS; i++) {
            TestKey key = new TestKey(i, new byte[rnd.nextInt(pageSize * 3)]);

            keys.add(key);

            data.put(key, new TestValue(new byte[rnd.nextInt(pageSize * 3)]));
        }

        cache0.putAll(data);

        AtomicInteger nodeIdx = new AtomicInteger();

        for (int iter = 0; iter < ADD_NODES; iter++) {
            IgniteInternalFuture<?> fut = GridTestUtils.runAsync(new Callable<Object>() {
                @Override public Object call() throws Exception {
                    int idx = nodeIdx.incrementAndGet();

                    info("Start node: " + idx);

                    return startGrid(idx);
                }
            });

            long endTime = System.currentTimeMillis() + 2500;

            while (System.currentTimeMillis() < endTime) {
                for (int i = 0; i < 100; i++) {
                    TestKey key = keys.get(rnd.nextInt(keys.size()));

                    if (rnd.nextBoolean()) {
                        cache0.remove(key);

                        data.remove(key);
                    } else {
                        TestValue val = new TestValue(new byte[rnd.nextInt(pageSize * 3)]);

                        cache0.put(key, val);
                        data.put(key, val);
                    }

                    Thread.sleep(10);
                }
            }

            fut.get(30_000);

            checkData(keys, data);

            waitTombstoneCleanup();

            checkData(keys, data);
        }

        awaitPartitionMapExchange();

        for (int iter = 0; iter < ADD_NODES; iter++) {
            IgniteInternalFuture<?> fut = GridTestUtils.runAsync(new Callable<Object>() {
                @Override public Object call() throws Exception {
                    int idx = nodeIdx.getAndDecrement();

                    info("Stop node: " + idx);

                    stopGrid(idx);

                    awaitPartitionMapExchange();

                    return null;
                }
            });

            long endTime = System.currentTimeMillis() + 2500;

            while (System.currentTimeMillis() < endTime) {
                for (int i = 0; i < 100; i++) {
                    TestKey key = keys.get(rnd.nextInt(keys.size()));

                    if (rnd.nextBoolean()) {
                        cache0.remove(key);

                        data.remove(key);
                    } else {
                        TestValue val = new TestValue(new byte[rnd.nextInt(pageSize * 3)]);

                        cache0.put(key, val);
                        data.put(key, val);
                    }
                }

                Thread.sleep(10);
            }

            fut.get(30_000);

            checkData(keys, data);

            waitTombstoneCleanup();

            checkData(keys, data);
        }
    }

    /**
     * @param keys Keys to check.
     * @param data Expected data.
     */
    private void checkData(List<TestKey> keys, Map<TestKey, TestValue> data) {
        for (Ignite node : Ignition.allGrids()) {
            if (!node.name().endsWith("CacheRemoveWithTombstonesLoadTest1"))
                continue;

            info("Check node: " + node.name());

            IgniteCache<TestKey, TestValue> cache = node.cache(DEFAULT_CACHE_NAME);

            for (TestKey key : keys) {
                TestValue expVal = data.get(key);
                TestValue val = cache.get(key);

                if (expVal == null)
                    assertNull(val);
                else {
                    assertNotNull(val);
                    assertTrue(Arrays.equals(expVal.dummyData, val.dummyData));
                }
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    private void waitTombstoneCleanup() throws Exception {
        for (Ignite node : Ignition.allGrids()) {
            final LongMetric tombstones =  ((IgniteEx)node).context().metric().registry(
                cacheMetricsRegistryName(DEFAULT_CACHE_NAME, false)).findMetric("Tombstones");

            GridTestUtils.waitForCondition(new GridAbsPredicate() {
                @Override public boolean apply() {
                    return tombstones.get() == 0;
                }
            }, 30_000);

            assertEquals("Failed to wait for tombstone cleanup: " + node.name(), 0, tombstones.get());
        }
    }

    /**
     * @return Cache configuration.
     */
    private CacheConfiguration<TestKey, TestValue> cacheConfiguration() {
        CacheConfiguration<TestKey, TestValue> ccfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);

        ccfg.setAtomicityMode(TRANSACTIONAL);
        ccfg.setCacheMode(PARTITIONED);
        ccfg.setBackups(1);
        ccfg.setRebalanceMode(ASYNC);
        ccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);

        return ccfg;
    }

    /**
     *
     */
    static class TestKey {
        /** */
        private final int id;

        /** */
        private final byte[] dummyData;

        /**
         * @param id ID.
         * @param dummyData Dummy byte array (to test with various key sizes).
         */
        public TestKey(int id, byte[] dummyData) {
            this.id = id;
            this.dummyData = dummyData;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            TestKey testKey = (TestKey) o;

            return id == testKey.id && Arrays.equals(dummyData, testKey.dummyData);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            int result = Objects.hash(id);
            result = 31 * result + Arrays.hashCode(dummyData);
            return result;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "TestKey [id=" + id + "]";
        }
    }

    /**
     *
     */
    static class TestValue {
        /** */
        private final byte[] dummyData;

        /**
         * @param dummyData Dummy byte array (to test with various value sizes).
         */
        public TestValue(byte[] dummyData) {
            this.dummyData = dummyData;
        }
    }
}

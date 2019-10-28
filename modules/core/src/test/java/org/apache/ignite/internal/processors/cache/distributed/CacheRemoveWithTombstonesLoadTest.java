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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.metric.impl.MetricUtils;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.metric.LongMetric;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.MvccFeatureChecker;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheRebalanceMode.SYNC;

/**
 *
 */
@RunWith(Parameterized.class)
public class CacheRemoveWithTombstonesLoadTest extends GridCommonAbstractTest {
    /** Dummy data. */
    private static final byte[] DUMMY_DATA = {};

    /** Test parameters. */
    @Parameterized.Parameters(name = "persistenceEnabled={0}, historicalRebalance={1}")
    public static Collection parameters() {
        List<Object[]> res = new ArrayList<>();

        for (boolean persistenceEnabled : new boolean[] {false, true}) {
            for (boolean histRebalance : new boolean[] {false, true}) {
                if (!persistenceEnabled && histRebalance)
                    continue;

                res.add(new Object[]{persistenceEnabled, histRebalance});
            }
        }

        return res;
    }

    /** */
    @Parameterized.Parameter(0)
    public boolean persistence;

    /** */
    @Parameterized.Parameter(1)
    public boolean histRebalance;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setConsistentId(gridName);

        DataStorageConfiguration dsCfg = new DataStorageConfiguration();

        if (persistence) {
            dsCfg.setDefaultDataRegionConfiguration(
                new DataRegionConfiguration()
                    .setInitialSize(256L * 1024 * 1024)
                    .setMaxSize(256L * 1024 * 1024)
                    .setPersistenceEnabled(true))
                .setWalMode(WALMode.LOG_ONLY);
        }

        dsCfg.setPageSize(1024);

        cfg.setDataStorageConfiguration(dsCfg);

        // Throttle rebalance.
        cfg.setRebalanceThrottle(100);

        return cfg;
    }

    /**
     *
     */
    @BeforeClass
    public static void beforeTests() {
        Assume.assumeFalse(MvccFeatureChecker.forcedMvcc());
    }

    /**
     *
     */
    @Before
    public void before() throws Exception {
        cleanPersistenceDir();

        stopAllGrids();

        if (histRebalance)
            System.setProperty(IgniteSystemProperties.IGNITE_PDS_WAL_REBALANCE_THRESHOLD, "0");
    }

    /**
     *
     */
    @After
    public void after() throws Exception {
        if (histRebalance)
            System.clearProperty(IgniteSystemProperties.IGNITE_PDS_WAL_REBALANCE_THRESHOLD);

        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IgniteSystemProperties.IGNITE_BASELINE_AUTO_ADJUST_ENABLED, value = "false")
    public void removeAndRebalance() throws Exception {
        IgniteEx ignite0 = startGrid(0);

        IgniteCache<TestKey, TestValue> cache0;

        final int ADD_NODES = persistence ? 2 : 3;
        final int KEYS = persistence ? 5_000 : 10_000;

        if (persistence) {
            // Preload initial data to all nodes to have start point for WAL rebalance.
            for (int i = 0, idx = 1; i < ADD_NODES; i++, idx++)
                startGrid(idx);

            ignite0.cluster().active(true);

            awaitPartitionMapExchange();

            cache0 = ignite0.getOrCreateCache(cacheConfiguration());

            for (int k = 0; k < KEYS; k++)
                cache0.put(new TestKey(k, DUMMY_DATA), new TestValue(DUMMY_DATA));

            forceCheckpoint();

            for (int i = 0, idx = 1; i < ADD_NODES; i++, idx++) {
                stopGrid(idx);

                awaitPartitionMapExchange();
            }
        }

        final int pageSize = ignite0.configuration().getDataStorageConfiguration().getPageSize();

        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        List<TestKey> keys = new ArrayList<>();

        Map<TestKey, TestValue> data = new HashMap<>();

        for (int i = 0; i < KEYS; i++) {
            TestKey key = new TestKey(i, new byte[rnd.nextInt(pageSize * 3)]);

            keys.add(key);

            data.put(key, new TestValue(new byte[rnd.nextInt(pageSize * 3)]));
        }

        cache0 = ignite0.getOrCreateCache(cacheConfiguration());

        cache0.putAll(data);

        AtomicInteger nodeIdx = new AtomicInteger();

        for (int iter = 0; iter < ADD_NODES; iter++) {
            IgniteInternalFuture<?> nodeStartFut = GridTestUtils.runAsync(() -> {
                int idx = nodeIdx.incrementAndGet();

                info("Start node: " + idx);

                U.sleep(500);

                return startGrid(idx);
            });

            long endTime = U.currentTimeMillis() + 5_000;

            while (U.currentTimeMillis() < endTime) {
                for (int i = 0; i < 100; i++) {
                    TestKey key = keys.get(rnd.nextInt(keys.size()));

                    if (rnd.nextBoolean()) {
                        cache0.remove(key);

                        data.remove(key);
                    }
                    else {
                        TestValue val = new TestValue(new byte[rnd.nextInt(pageSize * 3)]);

                        cache0.put(key, val);
                        data.put(key, val);
                    }

                    U.sleep(10);
                }
            }

            nodeStartFut.get(30_000);

            checkData(keys, data);

            waitTombstoneCleanup();

            checkData(keys, data);
        }

        awaitPartitionMapExchange();

        for (int iter = 0; iter < ADD_NODES; iter++) {
            IgniteInternalFuture<?> nodeStopFut = GridTestUtils.runAsync(() -> {
                int idx = nodeIdx.getAndDecrement();

                info("Stop node: " + idx);

                stopGrid(idx);

                awaitPartitionMapExchange();

                return null;
            });

            long endTime = U.currentTimeMillis() + 2_500;

            while (U.currentTimeMillis() < endTime) {
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

                U.sleep(10);
            }

            nodeStopFut.get(30_000);

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
                MetricUtils.cacheGroupMetricsRegistryName(DEFAULT_CACHE_NAME)).findMetric("Tombstones");

            GridTestUtils.waitForCondition(() -> tombstones.value() == 0, 30_000);

            assertEquals("Failed to wait for tombstone cleanup: " + node.name(), 0, tombstones.value());
        }
    }

    /**
     * @return Cache configuration.
     */
    private CacheConfiguration<TestKey, TestValue> cacheConfiguration() {
        CacheConfiguration<TestKey, TestValue> ccfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);

        ccfg.setAtomicityMode(TRANSACTIONAL);
        ccfg.setCacheMode(PARTITIONED);
        ccfg.setBackups(2);
        ccfg.setRebalanceMode(SYNC);
        ccfg.setReadFromBackup(true);
        ccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        ccfg.setAffinity(new RendezvousAffinityFunction(false, 64));

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

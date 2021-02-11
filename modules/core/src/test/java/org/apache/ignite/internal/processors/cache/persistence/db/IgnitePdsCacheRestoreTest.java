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

package org.apache.ignite.internal.processors.cache.persistence.db;

import java.util.Arrays;
import java.util.List;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 *
 */
public class IgnitePdsCacheRestoreTest extends GridCommonAbstractTest {
    /** Non-persistent data region name. */
    private static final String NO_PERSISTENCE_REGION = "no-persistence-region";

    /** */
    private CacheConfiguration[] ccfgs;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (ccfgs != null) {
            cfg.setCacheConfiguration(ccfgs);

            ccfgs = null;
        }

        long regionMaxSize = 20L * 1024 * 1024;

        DataStorageConfiguration memCfg = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration().setMaxSize(regionMaxSize).setPersistenceEnabled(true))
            .setWalMode(WALMode.LOG_ONLY);

        memCfg.setDataRegionConfigurations(new DataRegionConfiguration()
            .setMaxSize(regionMaxSize)
            .setName(NO_PERSISTENCE_REGION)
            .setPersistenceEnabled(false));

        cfg.setDataStorageConfiguration(memCfg);

        return cfg;
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
    public void testRestoreAndNewCache1() throws Exception {
        restoreAndNewCache(false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRestoreAndNewCache2() throws Exception {
        restoreAndNewCache(true);
    }

    /**
     * @param createNew If {@code true} need cache is added while node is stopped.
     * @throws Exception If failed.
     */
    private void restoreAndNewCache(boolean createNew) throws Exception {
        for (int i = 0; i < 3; i++) {
            ccfgs = configurations1();

            startGrid(i);
        }

        ignite(0).active(true);

        IgniteCache<Object, Object> cache1 = ignite(2).cache("c1");

        List<Integer> keys = primaryKeys(cache1, 10);

        for (Integer key : keys)
            cache1.put(key, key);

        stopGrid(2);

        if (createNew) {
            // New cache is added when node is stopped.
            ignite(0).getOrCreateCaches(Arrays.asList(configurations2()));
        }
        else {
            // New cache is added on node restart.
            ccfgs = configurations2();
        }

        IgniteEx g2 = startGrid(2);

        g2.resetLostPartitions(Arrays.asList("c1", "c2", "c3"));

        cache1 = ignite(2).cache("c1");

        IgniteCache<Object, Object> cache2 = ignite(2).cache("c2");

        IgniteCache<Object, Object> cache3 = ignite(2).cache("c3");

        for (Integer key : keys) {
            assertEquals(key, cache1.get(key));

            assertNull(cache2.get(key));

            assertNull(cache3.get(key));

            cache2.put(key, key);

            assertEquals(key, cache2.get(key));

            cache3.put(key, key);

            assertEquals(key, cache3.get(key));
        }

        List<Integer> nearKeys = nearKeys(cache1, 10, 0);

        for (Integer key : nearKeys) {
            assertNull(cache1.get(key));
            assertNull(cache2.get(key));
            assertNull(cache3.get(key));

            cache3.put(key, key);
            assertEquals(key, cache3.get(key));

            cache2.put(key, key);
            assertEquals(key, cache2.get(key));

            cache1.put(key, key);
            assertEquals(key, cache1.get(key));
        }

        startGrid(3);

        awaitPartitionMapExchange();

        for (Integer key : nearKeys) {
            assertEquals(key, cache3.get(key));

            assertEquals(key, cache2.get(key));

            assertEquals(key, cache1.get(key));
        }
    }

    /**
     * @return Configurations set 1.
     */
    private CacheConfiguration[] configurations1() {
        CacheConfiguration[] ccfgs = new CacheConfiguration[1];

        ccfgs[0] = cacheConfiguration("c1");

        return ccfgs;
    }

    /**
     * @return Configurations set 1.
     */
    private CacheConfiguration[] configurations2() {
        CacheConfiguration[] ccfgs = new CacheConfiguration[3];

        ccfgs[0] = cacheConfiguration("c1");
        ccfgs[1] = cacheConfiguration("c2");
        ccfgs[2] = cacheConfiguration("c3");

        ccfgs[2].setDataRegionName(NO_PERSISTENCE_REGION);
        ccfgs[2].setDiskPageCompression(null);

        return ccfgs;
    }

    /**
     * @param name Cache name.
     * @return Cache configuration.
     */
    private CacheConfiguration cacheConfiguration(String name) {
        CacheConfiguration ccfg = new CacheConfiguration(name);

        ccfg.setWriteSynchronizationMode(FULL_SYNC);

        return ccfg;
    }
}

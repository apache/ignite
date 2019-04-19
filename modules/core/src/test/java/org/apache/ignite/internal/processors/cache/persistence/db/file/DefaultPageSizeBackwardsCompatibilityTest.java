/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.internal.processors.cache.persistence.db.file;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 *
 */
@RunWith(JUnit4.class)
public class DefaultPageSizeBackwardsCompatibilityTest extends GridCommonAbstractTest {
    /** Client mode. */
    private boolean set2kPageSize = true;

    /** Entries count. */
    public static final int ENTRIES_COUNT = 300;

    /** Cache name. */
    public static final String CACHE_NAME = "cache1";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        DataStorageConfiguration memCfg = new DataStorageConfiguration();

        if (set2kPageSize)
            memCfg.setPageSize(2048);

        DataRegionConfiguration memPlcCfg = new DataRegionConfiguration();
        memPlcCfg.setMaxSize(100L * 1000 * 1000);
        memPlcCfg.setName("dfltDataRegion");
        memPlcCfg.setPersistenceEnabled(true);

        memCfg.setDefaultDataRegionConfiguration(memPlcCfg);
        memCfg.setCheckpointFrequency(3_000);

        cfg.setDataStorageConfiguration(memCfg);

        CacheConfiguration ccfg1 = new CacheConfiguration();

        ccfg1.setName(CACHE_NAME);
        ccfg1.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        ccfg1.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        ccfg1.setAffinity(new RendezvousAffinityFunction(false, 32));

        cfg.setCacheConfiguration(ccfg1);

        cfg.setConsistentId(gridName);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testStartFrom16kDefaultStore() throws Exception {
        startGrids(2);

        Ignite ig = ignite(0);

        ig.active(true);

        awaitPartitionMapExchange();

        IgniteCache<Integer, Integer> cache = ig.getOrCreateCache(CACHE_NAME);

        for (int i = 0; i < ENTRIES_COUNT; i++)
            cache.put(i, i);

        Thread.sleep(5_000); // Await for checkpoint to happen.

        stopAllGrids();

        set2kPageSize = false;

        startGrids(2);

        ig = ignite(0);

        ig.active(true);

        awaitPartitionMapExchange();

        cache = ig.getOrCreateCache(CACHE_NAME);

        for (int i = 0; i < ENTRIES_COUNT; i++)
            assertEquals((Integer)i, cache.get(i));
    }
}

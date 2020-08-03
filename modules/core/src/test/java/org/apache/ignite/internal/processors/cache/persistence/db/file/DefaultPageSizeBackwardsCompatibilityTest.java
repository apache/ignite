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

/**
 *
 */
public class DefaultPageSizeBackwardsCompatibilityTest extends GridCommonAbstractTest {
    /** Client mode. */
    private boolean set16kPageSize = true;

    /** Entries count. */
    public static final int ENTRIES_COUNT = 300;

    /** Cache name. */
    public static final String CACHE_NAME = "cache1";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        DataStorageConfiguration memCfg = new DataStorageConfiguration();

        if (set16kPageSize)
            memCfg.setPageSize(16 * 1024);
        else
            memCfg.setPageSize(0); // Enforce default.

        DataRegionConfiguration memPlcCfg = new DataRegionConfiguration();
        memPlcCfg.setMaxSize(100L * 1000 * 1000);
        memPlcCfg.setName("dfltDataRegion");
        memPlcCfg.setPersistenceEnabled(true);

        memCfg.setDefaultDataRegionConfiguration(memPlcCfg);
        memCfg.setCheckpointFrequency(500);

        cfg.setDataStorageConfiguration(memCfg);

        CacheConfiguration ccfg1 = new CacheConfiguration();

        ccfg1.setName(CACHE_NAME);
        ccfg1.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        ccfg1.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        ccfg1.setAffinity(new RendezvousAffinityFunction(false, 32));

        if (!set16kPageSize)
            ccfg1.setDiskPageCompression(null);

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

        Thread.sleep(1500); // Await for checkpoint to happen.

        stopAllGrids();

        set16kPageSize = false;

        startGrids(2);

        ig = ignite(0);

        ig.active(true);

        awaitPartitionMapExchange();

        cache = ig.getOrCreateCache(CACHE_NAME);

        for (int i = 0; i < ENTRIES_COUNT; i++)
            assertEquals((Integer)i, cache.get(i));
    }
}

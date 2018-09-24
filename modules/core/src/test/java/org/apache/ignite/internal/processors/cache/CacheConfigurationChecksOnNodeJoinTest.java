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

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 *
 */
public class CacheConfigurationChecksOnNodeJoinTest extends GridCommonAbstractTest {
    /** Number records in cache. */
    private static final int NUMBER_RECORDS = 30;

    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        DataRegionConfiguration drCfg = new DataRegionConfiguration().setPersistenceEnabled(true);

        DataStorageConfiguration dsCfg = new DataStorageConfiguration().setDefaultDataRegionConfiguration(drCfg);

        cfg.setDataStorageConfiguration(dsCfg);

        return cfg;
    }

    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    public void testStartNodeAfterCacheDestroy() throws Exception {
        IgniteEx ignite0 = startGrid(0);
        IgniteEx ignite1 = startGrid(1);

        CacheConfiguration<Long, Long> cacheCfg = new CacheConfiguration<Long, Long>(DEFAULT_CACHE_NAME).setBackups(0);

        ignite1.cluster().active(true);

        IgniteCache<Long, Long> cache0 = ignite0.getOrCreateCache(cacheCfg);

        for (int i = 0; i < NUMBER_RECORDS; i++)
            cache0.put(1L << i, 1L << i);

        IgniteCache<Long, Long> cache1 = ignite1.cache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < NUMBER_RECORDS; i++)
            assertTrue(cache1.containsKey(1L << i));

        stopGrid(1);

        cache0.destroy();

        // Starting grid with stored DEFAULT_CACHE_NAME configuration after DEFAULT_CACHE_NAME cache was destroyed.
        startGrid(1);

        awaitPartitionMapExchange();
    }
}
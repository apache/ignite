/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence;

import java.io.Serializable;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.processors.database.IgniteDbDynamicCacheSelfTest;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.MvccFeatureChecker;
import org.junit.Test;

/**
 *
 */
public class IgnitePdsDynamicCacheTest extends IgniteDbDynamicCacheSelfTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        DataStorageConfiguration memCfg = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration().setMaxSize(200L * 1024 * 1024).setPersistenceEnabled(true))
            .setWalMode(WALMode.LOG_ONLY);

        cfg.setDataStorageConfiguration(memCfg);

        return cfg;
    }


    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        System.setProperty(GridCacheDatabaseSharedManager.IGNITE_PDS_CHECKPOINT_TEST_SKIP_SYNC, "true");

        super.beforeTest();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        System.clearProperty(GridCacheDatabaseSharedManager.IGNITE_PDS_CHECKPOINT_TEST_SKIP_SYNC);

        cleanPersistenceDir();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRestartAndCreate() throws Exception {
        startGrids(3);

        Ignite ignite = ignite(0);

        ignite.active(true);

        CacheConfiguration ccfg1 = new CacheConfiguration();

        ccfg1.setName("cache1");
        ccfg1.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        ccfg1.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        ccfg1.setAffinity(new RendezvousAffinityFunction(false, 32));

        if (MvccFeatureChecker.forcedMvcc())
            ccfg1.setRebalanceDelay(Long.MAX_VALUE);
        else
            ccfg1.setRebalanceMode(CacheRebalanceMode.NONE);

        CacheConfiguration ccfg2 = new CacheConfiguration();

        ccfg2.setName("cache2");
        ccfg2.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        ccfg2.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        ccfg2.setAffinity(new RendezvousAffinityFunction(false, 32));
        ccfg2.setIndexedTypes(Integer.class, Value.class);

        if (MvccFeatureChecker.forcedMvcc())
            ccfg2.setRebalanceDelay(Long.MAX_VALUE);
        else
            ccfg2.setRebalanceMode(CacheRebalanceMode.NONE);

        CacheConfiguration ccfg3 = new CacheConfiguration();

        ccfg3.setName("cache3");
        ccfg3.setAtomicityMode(CacheAtomicityMode.ATOMIC);
        ccfg3.setCacheMode(CacheMode.LOCAL);

        ignite.createCache(ccfg1);
        ignite.createCache(ccfg2);
        ignite.createCache(ccfg3).put(2, 3);

        int iterations = 20;

        long stopTime = U.currentTimeMillis() + 20_000;

        for (int k = 0; k < iterations && U.currentTimeMillis() < stopTime; k++) {
            log.info("Iteration: " + k);

            stopAllGrids();

            startGrids(3);

            ignite = ignite(0);

            ignite.active(true);

            ignite.getOrCreateCache(ccfg1);

            ignite.getOrCreateCache(ccfg2);

            assertEquals(1, ignite.cache(ccfg3.getName()).size());
            assertEquals(3, ignite.cache(ccfg3.getName()).get(2));

            ignite.destroyCache(ccfg2.getName());

            ignite.getOrCreateCache(ccfg2);

            ignite.destroyCache(ccfg1.getName());
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDynamicCacheSavingOnNewNode() throws Exception {
        Ignite ignite = startGrid(0);

        ignite.active(true);

        CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        ccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        ccfg.setRebalanceMode(CacheRebalanceMode.SYNC);
        ccfg.setAffinity(new RendezvousAffinityFunction(false, 32));

        IgniteCache cache = ignite.getOrCreateCache(ccfg);

        for (int i = 0; i < 160; i++)
            cache.put(i, i);

        ignite = startGrid(1);

        awaitPartitionMapExchange();

        cache = ignite.cache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < 160; i++)
            assertEquals(i, cache.get(i));

        stopAllGrids(true);

        startGrid(0);
        ignite = startGrid(1);

        ignite.active(true);

        cache = ignite.cache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < 160; i++)
            assertEquals(i, cache.get(i));
    }

    /**
     *
     */
    static class Value implements Serializable {
        /** */
        @QuerySqlField(index = true, groups = "full_name")
        String fName;

        /** */
        @QuerySqlField(index = true, groups = "full_name")
        String lName;
    }
}

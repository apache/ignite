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
 *
 */

package org.apache.ignite.internal.processors.database;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.MvccFeatureChecker;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 *
 */
public class IgniteDbDynamicCacheSelfTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        DataStorageConfiguration memCfg = new DataStorageConfiguration().setDefaultDataRegionConfiguration(
            new DataRegionConfiguration().setMaxSize(200L * 1024 * 1024).setPersistenceEnabled(true));

        cfg.setDataStorageConfiguration(memCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 10 * 60 * 1000;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCreate() throws Exception {
        int iterations = 200;

        startGrids(3);

        Ignite ignite = ignite(0);

        ignite.active(true);

        CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        ccfg.setName("cache1");
        ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        ccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        ccfg.setAffinity(new RendezvousAffinityFunction(false, 32));

        if (MvccFeatureChecker.forcedMvcc())
            ccfg.setRebalanceDelay(Long.MAX_VALUE);
        else
            ccfg.setRebalanceMode(CacheRebalanceMode.NONE);

        for (int k = 0; k < iterations; k++) {
            System.out.println("Iteration: " + k);

            IgniteCache cache = ignite.createCache(ccfg);

            awaitPartitionMapExchange();

            ignite.destroyCache(ccfg.getName());

            awaitPartitionMapExchange();
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMultipleDynamicCaches() throws Exception {
        int caches = 10;

        int entries = 10;

        startGrids(1);

        Ignite ignite = ignite(0);

        ignite.active(true);

        CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        ccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        ccfg.setAffinity(new RendezvousAffinityFunction(false, 32));

        if (MvccFeatureChecker.forcedMvcc())
            ccfg.setRebalanceDelay(Long.MAX_VALUE);
        else
            ccfg.setRebalanceMode(CacheRebalanceMode.NONE);

        ccfg.setIndexedTypes(Integer.class, String.class);

        long finishTime = U.currentTimeMillis() + 20_000;

        int iteration = 0;

        while (U.currentTimeMillis() < finishTime ) {
            System.out.println("Iteration: " + iteration);

            for (int i = 0; i < caches; i++) {
                ccfg.setName("cache" + i);

                IgniteCache cache = ignite.createCache(ccfg);

                for (int j = 0; j < entries; j++)
                    cache.put(j, "val " + j);

                ignite.destroyCache("cache" + i);
            }

            iteration++;
        }
    }
}

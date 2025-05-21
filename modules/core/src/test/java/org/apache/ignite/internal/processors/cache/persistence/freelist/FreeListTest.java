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

package org.apache.ignite.internal.processors.cache.persistence.freelist;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import java.util.Random;

import static org.apache.ignite.testframework.GridTestUtils.runMultiThreaded;

/**
 * Test freelists.
 */
public class FreeListTest extends GridCommonAbstractTest {
    private static final int KEYS_COUNT = 5_000;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConsistentId(igniteInstanceName);

        cfg.setMetricsLogFrequency(2000);

        DataStorageConfiguration dsCfg = new DataStorageConfiguration();

        int pageSize = dsCfg.getPageSize() == 0 ? DataStorageConfiguration.DFLT_PAGE_SIZE : dsCfg.getPageSize();

        dsCfg.setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setPersistenceEnabled(false)
                .setMaxSize(pageSize * 3L * KEYS_COUNT));

        cfg.setDataStorageConfiguration(dsCfg);

        return cfg;
    }

    /**
     *
     */
    @Test
    public void testFreeList() throws Exception {
        IgniteEx ignite = startGrid(0);

        int partCnt = 1;

        IgniteCache<Object, Object> cache = ignite.createCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
            .setAffinity(new RendezvousAffinityFunction().setPartitions(partCnt))
            .setAtomicityMode(CacheAtomicityMode.ATOMIC));

        Random random = new Random(3793);

        for (int i = 0; i < KEYS_COUNT; i++)
            cache.put(i, new byte[random.nextInt(3000, 12000)]);


        runMultiThreaded(() -> {
            for (int i = 0; i < KEYS_COUNT * 100; i++) {
                if (i % 50000 == 0)
                    ignite.log().info(String.format("%s: i=%d; size=%d", Thread.currentThread().getName(), i, cache.size()));

                cache.put(random.nextInt(KEYS_COUNT),
                    new byte[random.nextInt(3000, 12000)]);

                int del = random.nextInt(KEYS_COUNT);

                for (int j = del; j < del + random.nextInt(5); j++)
                    cache.remove(j);
            }
        },24,"update-remove");
    }
}

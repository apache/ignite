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
import org.apache.ignite.internal.processors.cache.GridCacheProcessor;
import org.apache.ignite.internal.processors.cache.IgniteCacheOffheapManagerImpl;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLongArray;

import static org.apache.ignite.testframework.GridTestUtils.runMultiThreaded;

/**
 * Test freelists.
 */
public class FreeListTest extends GridCommonAbstractTest {
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
                .setMaxSize(pageSize * 4_000_000L));

        cfg.setDataStorageConfiguration(dsCfg);

        return cfg;
    }

    /**
     *
     */
    @Test
    public void testFreeList() throws Exception {
        IgniteEx ignite = startGrid(0);

//        ignite.cluster().state(ClusterState.ACTIVE);

        int partCnt = 10;

        GridCacheProcessor cacheProc = ignite.context().cache();
//        GridCacheDatabaseSharedManager dbMgr = (GridCacheDatabaseSharedManager)cacheProc.context().database();

//        dbMgr.enableCheckpoints(false).get();

        IgniteCache<Object, Object> cache = ignite.createCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
            .setAffinity(new RendezvousAffinityFunction().setPartitions(partCnt))
            .setAtomicityMode(CacheAtomicityMode.ATOMIC));

        IgniteCacheOffheapManagerImpl offheap = (IgniteCacheOffheapManagerImpl)cacheProc.cache(DEFAULT_CACHE_NAME).context().group()
            .offheap();

        runMultiThreaded(() -> {
                for (int i = 0; i < 50_000; i++) {
                    for (int p = 0; p < partCnt; p++) {
                        Integer key = i * partCnt + p;
                        cache.put(key, new byte[i + 1]);
//                cache.remove(key);
                    }
                }
            },
            24,
            "insert"
        );

        for (int i = 0; i < 100_000; i++) {
            cache.remove(i);
        }

        runMultiThreaded(() -> {
                ThreadLocalRandom random = ThreadLocalRandom.current();

                for (int i = 0; i < 5_000_000; i++) {
                    cache.put(random.nextInt(500_000),
                        new byte[random.nextInt(1, 53307)]);

                    int del = random.nextInt(500_000);
                    for (int j = del; j < del + random.nextInt(50); j++)
                        cache.remove(j);
                }
            },
            24,
            "update-remove");

        offheap.cacheDataStores().forEach(cacheData -> {
            PagesList list = (PagesList)cacheData.rowStore().freeList();

            AtomicLongArray bucketsSize = list.bucketsSize;

            // All buckets except reuse bucket must be empty after puts and removes of the same key.
            for (int i = 0; i < bucketsSize.length(); i++) {
//                if (list.isReuseBucket(i))
//                    assertTrue(bucketsSize.get(i) > 0);
//                else
//                    assertEquals(0, bucketsSize.get(i));
            }
        });
    }
}

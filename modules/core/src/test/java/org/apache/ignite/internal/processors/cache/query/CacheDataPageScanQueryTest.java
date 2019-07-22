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

package org.apache.ignite.internal.processors.cache.query;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.Cache;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.CacheGroupMetricsImpl;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.persistence.DataRegionMetricsImpl;
import org.apache.ignite.internal.processors.cache.tree.CacheDataTree;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Ignore;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_DATA_REGION_INITIAL_SIZE;

/**
 */
public class CacheDataPageScanQueryTest extends GridCommonAbstractTest {
    /** */
    private static final String CACHE = "test";

    /** */
    private static final int PARTS = 1;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String instanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(instanceName);

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration()
                        .setPersistenceEnabled(true)
                        .setMaxSize(DFLT_DATA_REGION_INITIAL_SIZE)
                ));

        cfg.setCacheConfiguration(
            new CacheConfiguration(CACHE)
                .setAtomicityMode(ATOMIC)
                .setAffinity(
                    new RendezvousAffinityFunction()
                        .setPartitions(PARTS)
                )
        );

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids(true);
        cleanPersistenceDir();
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("ConstantConditions")
    @Test
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-11998")
    public void testDataPageScanWithRestart() throws Exception {
        IgniteEx ignite = startGrid(0);
        ignite.cluster().active(true);

        IgniteInternalCache<Long, String> cache = ignite.cachex(CACHE);
        CacheGroupMetricsImpl metrics = cache.context().group().metrics();
        DataRegionMetricsImpl rmx = cache.context().dataRegion().memoryMetrics();

        long maxKey = 10_000;

        Map<Long,String> map = new ConcurrentHashMap<>();

        int threads = 16;
        AtomicInteger threadShift = new AtomicInteger();

        multithreaded((Callable<Void>)() -> {
            ThreadLocalRandom rnd = ThreadLocalRandom.current();

            int shift = threadShift.getAndIncrement();

            for (int i = shift; i < maxKey; i += threads) {
                Long k = (long)i;
                String v = GridTestUtils.randomString(rnd, 6 * 1024); // Bigger than single page.

                cache.put(k, v);
                map.put(k, v);
            }
            return null;
        }, threads);

        assertEquals(map.size(), cache.size());

        info("Page mem  : " + rmx.getPhysicalMemorySize());
        info("Alloc size: " + metrics.getTotalAllocatedSize());
        info("Store size: " + metrics.getStorageSize());

        HashMap<Long,String> map2 = new HashMap<>(map);

        IgniteCache<Long,String> c = ignite.cache(CACHE);
        for (Cache.Entry<Long,String> e : c.query(new ScanQuery<Long,String>()).getAll())
            assertEquals(e.getValue(), map.remove(e.getKey()));

        assertTrue(map.isEmpty());
        assertTrue(CacheDataTree.isLastFindWithDataPageScan());

        stopAllGrids(true);

        ignite = startGrid(0);
        ignite.cluster().active(true);

        c = ignite.cache(CACHE);
        for (Cache.Entry<Long,String> e : c.query(new ScanQuery<Long,String>()).getAll())
            assertEquals(e.getValue(), map2.remove(e.getKey()));

        assertTrue(map2.isEmpty());
        assertTrue(CacheDataTree.isLastFindWithDataPageScan());
    }
}

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

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
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
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.persistence.DataRegionMetricsImpl;
import org.apache.ignite.mxbean.CacheGroupMetricsMXBean;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_DATA_REGION_INITIAL_SIZE;

public class CacheDirtyScanQueryTest extends GridCommonAbstractTest {
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

//    /** {@inheritDoc} */
//    @Override protected long getTestTimeout() {
//        return 15 * 60 * 1000;
//    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("ConstantConditions")
    public void testDirtyScan() throws Exception {
        IgniteEx ignite = startGrid(0);
        ignite.cluster().active(true);

        IgniteInternalCache<Long, String> cache = ignite.cachex(CACHE);
        CacheGroupMetricsMXBean gmx = cache.context().group().mxBean();
        DataRegionMetricsImpl rmx = cache.context().dataRegion().memoryMetrics();

        long maxKey = 1_000_000;

        Map<Long,String> map = new ConcurrentHashMap<>();

        int threads = 32;
        AtomicInteger threadShift = new AtomicInteger();

        multithreaded((Callable<Void>)() -> {
            int shift = threadShift.getAndIncrement();

            for (int i = shift; i < maxKey; i += threads) {
                Long k = (long)i;
                String v = String.valueOf(i);

                cache.put(k, v);
                map.put(k, v);
            }
            return null;
        }, threads);

//        forceCheckpoint(ignite);

        assertEquals(map.size(), cache.size());

        info("Page mem  : " + rmx.getPhysicalMemorySize());
        info("Alloc size: " + gmx.getTotalAllocatedSize());
        info("Store size: " + gmx.getStorageSize());

        IgniteCache<Long,String> c = ignite.cache(CACHE);
        ScanQuery<Long,String> qry = new ScanQuery<>();

        qry.setLocal(true)
//            .setFilter((k, v) -> k == -1)
            .setPartition(0);

        for (Cache.Entry<Long,String> e : c.query(qry))
            assertEquals(e.getValue(), map.remove(e.getKey()));

        assertTrue(map.isEmpty());

//        for (int i = 0; i < 1000; i++) {
//            long start = System.nanoTime();
//
//            assertEquals(0, c.query(qry).getAll().size());
//
//            info("Scan time: " + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));
//        }
    }
}

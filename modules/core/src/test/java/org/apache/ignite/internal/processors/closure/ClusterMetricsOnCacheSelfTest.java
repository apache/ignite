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

package org.apache.ignite.internal.processors.closure;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMemoryMode;
import org.apache.ignite.cache.eviction.fifo.FifoEvictionPolicy;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.GridCacheAbstractSelfTest;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.testframework.GridTestUtils;

/**
 * Tests cluster metrics affected by cache state.
 */
public class ClusterMetricsOnCacheSelfTest extends GridCacheAbstractSelfTest {
    /** */
    private static final int VAL_SIZE = 512 * 1024; // bytes

    /** */
    private static final int MAX_VALS_AMOUNT = 100;

    /** */
    private static final int NODES_CNT = 2;

    /** Init caches only once */
    private boolean cacheInit = false;

    /** With OFFHEAP_VALUES policy. */
    private final String OFF_HEAP_VALUE_NAME = "offHeapValuesCfg";

    /** With ONHEAP_TIERED policy. */
    private final String ON_HEAP_TIERED_NAME = "onHeapTieredCfg";

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return NODES_CNT;
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setIncludeProperties();
        cfg.setMetricsUpdateFrequency(0);

        if (!cacheInit) {
            CacheConfiguration<Integer, Object> offHeapValuesCfg = cacheConfiguration(gridName);
            offHeapValuesCfg.setName(OFF_HEAP_VALUE_NAME);
            offHeapValuesCfg.setStatisticsEnabled(true);
            offHeapValuesCfg.setMemoryMode(CacheMemoryMode.OFFHEAP_VALUES);
            offHeapValuesCfg.setOffHeapMaxMemory(MAX_VALS_AMOUNT * VAL_SIZE);

            CacheConfiguration<Integer, Object> onHeapTieredCfg = cacheConfiguration(gridName);
            onHeapTieredCfg.setName(ON_HEAP_TIERED_NAME);
            onHeapTieredCfg.setStatisticsEnabled(true);
            onHeapTieredCfg.setMemoryMode(CacheMemoryMode.ONHEAP_TIERED);
            onHeapTieredCfg.setOffHeapMaxMemory(MAX_VALS_AMOUNT * VAL_SIZE);

            FifoEvictionPolicy plc = new FifoEvictionPolicy();
            plc.setMaxMemorySize(MAX_VALS_AMOUNT * VAL_SIZE);
            plc.setMaxSize(VAL_SIZE);

            onHeapTieredCfg.setEvictionPolicy(plc);

            cacheInit = true;

            return cfg.setCacheConfiguration(offHeapValuesCfg, onHeapTieredCfg);
        }

        return cfg;
    }

    public void testAllocatedMemory() throws Exception {
        final IgniteCache tiered = grid(0).getOrCreateCache(ON_HEAP_TIERED_NAME);
        final IgniteCache values = grid(0).getOrCreateCache(OFF_HEAP_VALUE_NAME);

        long prevTieredOffHeapSize = tiered.metrics().getOffHeapAllocatedSize();
        long prevValuesOffHeapSize = values.metrics().getOffHeapAllocatedSize();

        assertEquals(0, prevTieredOffHeapSize);
        assertEquals(0, prevValuesOffHeapSize);

        long prevClusterNonHeapMemoryUsed = grid(0).cluster().metrics().getNonHeapMemoryUsed();

        fillCache(tiered, getTestTimeout());

        assertTrue(tiered.metrics().getOffHeapAllocatedSize() > (MAX_VALS_AMOUNT - 5) * VAL_SIZE + prevTieredOffHeapSize);
        assertEquals(prevValuesOffHeapSize, values.metrics().getOffHeapAllocatedSize());
        // prevClusterNonHeapMemoryUsed = 69_277_368
        // (MAX_VALS_AMOUNT - 5) * VAL_SIZE = 49_807_360
        // tiered.metrics().getOffHeapAllocatedSize() = 51_012_531

        assertTrue((MAX_VALS_AMOUNT - 5) * VAL_SIZE + prevClusterNonHeapMemoryUsed < grid(0).cluster().metrics().getNonHeapMemoryUsed());

        prevClusterNonHeapMemoryUsed = grid(0).cluster().metrics().getNonHeapMemoryUsed();
        prevTieredOffHeapSize = tiered.metrics().getOffHeapAllocatedSize();

        fillCache(values, getTestTimeout());

        assertTrue(values.metrics().getOffHeapAllocatedSize() > (MAX_VALS_AMOUNT - 5) * VAL_SIZE + prevValuesOffHeapSize);
        assertEquals(prevTieredOffHeapSize, tiered.metrics().getOffHeapAllocatedSize());
        assertTrue((MAX_VALS_AMOUNT - 5) * VAL_SIZE + prevClusterNonHeapMemoryUsed < grid(0).cluster().metrics().getNonHeapMemoryUsed());
    }

    /** Fill cache with values. */
    private static void fillCache(final IgniteCache<Integer, Object> cache, long timeout) throws Exception{
        final int THREAD_COUNT = 4;
        final byte[] val = new byte[VAL_SIZE];
        final AtomicInteger keyStart = new AtomicInteger(0);
        final CountDownLatch latch = new CountDownLatch(THREAD_COUNT);

        GridTestUtils.runMultiThreaded(new Callable<Void>() {
            @Override public Void call() throws Exception {
                final int start = keyStart.addAndGet(MAX_VALS_AMOUNT);

                for (int i = start; i < start + MAX_VALS_AMOUNT; i++)
                    cache.put(i, val);

                latch.countDown();

                return null;
            }
        }, THREAD_COUNT, "test");

        latch.await(timeout, TimeUnit.SECONDS);
        IgniteUtils.sleep(2_000);
    }

}

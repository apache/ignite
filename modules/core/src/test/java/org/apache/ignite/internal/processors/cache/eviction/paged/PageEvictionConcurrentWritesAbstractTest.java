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

package org.apache.ignite.internal.processors.cache.eviction.paged;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

/**
 * Concurrent deadlock test for size-aware page eviction.
 * <p>
 * The region is first filled with a large number of small entries (so there is plenty of evictable page space), then
 * several threads concurrently insert large rows (larger than the empty-pages pool). Each large insert goes through
 * the size-aware reserve and, for the single-row path, eviction under the new entry lock with the non-blocking
 * {@code tryLockEntry}. The average data volume is kept within the region capacity, so eviction frees already-stored
 * small entries rather than overrunning the free list. The test asserts that no deadlock occurs (all threads finish
 * within a global deadline).
 */
public abstract class PageEvictionConcurrentWritesAbstractTest extends PageEvictionAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        return super.getConfiguration(gridName).setDataStorageConfiguration(new DataStorageConfiguration());
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * Concurrent large inserts into a region pre-filled with small entries must complete within the deadline without
     * deadlock, and without corrupting the free list (eviction frees small entries rather than overrunning the region).
     *
     * @throws Exception If failed.
     */
    @Test
    public void testConcurrentLargeWritesNoDeadlock() throws Exception {
         // Number of small pre-fill entries, leaving a buffer that is exceeded by the total of the large writes, so that
         // the last of them can only be stored by freeing pages via size-aware eviction. The large records are small
         // enough that concurrent size-aware eviction reliably frees the required pages (no spurious guard OOM).
        int smallEntries = 48_000;

         // Large rows inserted per thread. Their total (threads x rows) exceeds the buffer left by the pre-fill, so the
         // last large writes overflow the region and require size-aware eviction to free small entry pages.
        int largeRowsPerThread = 20;

        IgniteEx ignite = startGrid(1);

        IgniteCache<Integer, Object> cache = createCache(ignite, DEFAULT_CACHE_NAME);

        // Pre-fill the region with many small entries so that eviction always has evictable pages to free.
        for (int i = 0; i < smallEntries; i++)
            cache.put(i, new byte[4096]);

        byte[] largeVal = new byte[2 * 1024 * 1024];

        CountDownLatch startLatch = new CountDownLatch(1);

        AtomicInteger threadIdx = new AtomicInteger();

        IgniteInternalFuture<?> fut = GridTestUtils.runMultiThreadedAsync(() -> {
            U.awaitQuiet(startLatch);

            int idx = threadIdx.getAndIncrement();

            for (int k = 0; k < largeRowsPerThread; k++)
                cache.put(smallEntries + idx * largeRowsPerThread + k, largeVal);
            }, 10, "paged-writer");

        startLatch.countDown();

        fut.get(TimeUnit.MINUTES.toMillis(3));
    }
}

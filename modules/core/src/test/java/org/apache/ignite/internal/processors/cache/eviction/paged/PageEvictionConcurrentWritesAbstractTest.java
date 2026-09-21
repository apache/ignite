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
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_PAGE_SIZE;

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
public abstract class PageEvictionConcurrentWritesAbstractTest extends GridCommonAbstractTest {
    /** Off-heap region size. */
    private static final int SIZE = 256 * 1024 * 1024;

    /** Partition count (kept low so that index-tree structures do not exhaust the region). */
    private static final int PARTITIONS = 32;

    /** Large record size (larger than the empty-pages pool so that each write is size-aware). */
    private static final int LARGE_RECORD_SIZE = 2 * 1024 * 1024;

    /** Small record size used to pre-fill the region with evictable data. */
    private static final int SMALL_RECORD_SIZE = 4096;

    /** Empty pages pool size. */
    private static final int POOL_SIZE = 100;

    /** Number of small pre-fill entries, leaving a buffer that is exceeded by the total of the large writes, so that
     * the last of them can only be stored by freeing pages via size-aware eviction. The large records are small
     * enough that concurrent size-aware eviction reliably frees the required pages (no spurious guard OOM). */
    private static final int SMALL_ENTRIES = 48_000;

    /** Number of writer threads. */
    private static final int THREADS = 10;

    /** Large rows inserted per thread. Their total (threads x rows) exceeds the buffer left by the pre-fill, so the
     * last large writes overflow the region and require size-aware eviction to free small entry pages. */
    private static final int LARGE_ROWS_PER_THREAD = 20;

    /** Global deadline for the whole test (protects against a deadlock/busy-spin hang). */
    private static final long DEADLINE = TimeUnit.MINUTES.toMillis(3);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        return super.getConfiguration(gridName)
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                    .setInitialSize(SIZE)
                    .setMaxSize(SIZE)
                    .setEmptyPagesPoolSize(POOL_SIZE))
                .setPageSize(DFLT_PAGE_SIZE));
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @param ignite Ignite node.
     * @return Cache with a small partition count (reduces structural page overhead).
     */
    private IgniteCache<Integer, Object> createCache(IgniteEx ignite) {
        return ignite.createCache(new CacheConfiguration<Integer, Object>(DEFAULT_CACHE_NAME)
            .setAffinity(new RendezvousAffinityFunction(false, PARTITIONS)));
    }

    /**
     * Concurrent large inserts into a region pre-filled with small entries must complete within the deadline without
     * deadlock, and without corrupting the free list (eviction frees small entries rather than overrunning the region).
     *
     * @throws Exception If failed.
     */
    @Test
    public void testConcurrentLargeWritesNoDeadlock() throws Exception {
        IgniteEx ignite = startGrid(1);

        IgniteCache<Integer, Object> cache = createCache(ignite);

        // Pre-fill the region with many small entries so that eviction always has evictable pages to free.
        for (int i = 0; i < SMALL_ENTRIES; i++)
            cache.put(i, new byte[SMALL_RECORD_SIZE]);

        byte[] largeVal = new byte[LARGE_RECORD_SIZE];

        CountDownLatch startLatch = new CountDownLatch(1);

        AtomicInteger threadIdx = new AtomicInteger();

        IgniteInternalFuture<?> fut = GridTestUtils.runMultiThreadedAsync(() -> {
                U.awaitQuiet(startLatch);

                int idx = threadIdx.getAndIncrement();

                for (int k = 0; k < LARGE_ROWS_PER_THREAD; k++)
                    cache.put(SMALL_ENTRIES + idx * LARGE_ROWS_PER_THREAD + k, largeVal);
            },
            THREADS, "paged-writer");

        startLatch.countDown();

        fut.get(DEADLINE);
    }
}

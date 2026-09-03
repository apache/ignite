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
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
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
    private static final int SIZE = 128 * 1024 * 1024;

    /** Partition count (kept low so that index-tree structures do not exhaust the region). */
    private static final int PARTITIONS = 32;

    /** Large record size (much larger than the empty-pages pool). */
    private static final int LARGE_RECORD_SIZE = 4 * 1024 * 1024;

    /** Small record size used to pre-fill the region with evictable data. */
    private static final int SMALL_RECORD_SIZE = 4096;

    /** Empty pages pool size. */
    private static final int POOL_SIZE = 100;

    /** Number of small pre-fill entries. */
    private static final int SMALL_ENTRIES = 10_000;

    /** Number of writer threads. */
    private static final int THREADS = 4;

    /** Large rows inserted per thread (moderate total, kept within region capacity after eviction of small rows). */
    private static final int LARGE_ROWS_PER_THREAD = 3;

    /** Global deadline for the whole test (protects against a deadlock/busy-spin hang). */
    private static final long DEADLINE = TimeUnit.MINUTES.toMillis(3);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        return super.getConfiguration(gridName)
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                    .setInitialSize(SIZE)
                    .setMaxSize(SIZE)
                    .setEmptyPagesPoolSize(POOL_SIZE)
                )
                .setPageSize(DFLT_PAGE_SIZE)
            );
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

        AtomicLong errors = new AtomicLong();

        CountDownLatch startLatch = new CountDownLatch(1);

        long deadline = System.currentTimeMillis() + DEADLINE;

        Thread[] threads = new Thread[THREADS];

        for (int i = 0; i < THREADS; i++) {
            final int threadIdx = i;

            threads[i] = new Thread(() -> {
                try {
                    startLatch.await();

                    for (int k = 0; k < LARGE_ROWS_PER_THREAD; k++)
                        cache.put(SMALL_ENTRIES + threadIdx * LARGE_ROWS_PER_THREAD + k, largeVal);
                }
                catch (Throwable e) {
                    errors.incrementAndGet();

                    log.error("Unexpected error in writer thread", e);
                }
            }, "paged-writer-" + i);

            threads[i].start();
        }

        startLatch.countDown();

        for (Thread t : threads)
            t.join(Math.max(1, deadline - System.currentTimeMillis()));

        // The core assertion of this deadlock test: every writer must have completed (no thread is stuck waiting on
        // an entry lock held by size-aware eviction running under another entry lock).
        for (Thread t : threads)
            assertFalse("Writer thread " + t.getName() + " did not finish (possible deadlock)", t.isAlive());

        assertEquals("Writer threads reported errors", 0, errors.get());
    }
}

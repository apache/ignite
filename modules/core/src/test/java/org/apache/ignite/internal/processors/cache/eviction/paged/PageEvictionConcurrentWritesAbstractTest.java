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

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

/** Concurrent deadlock test for size-aware page eviction. */
public abstract class PageEvictionConcurrentWritesAbstractTest extends PageEvictionAbstractTest {
    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * Concurrent large inserts into a region pre-filled with small entries must complete within the deadline without
     * deadlock, and without corrupting the free list (eviction frees small entries rather than overrunning the region).
     * After the concurrent phase, the cache must remain functional: a new put+get must succeed, and at least some of
     * the large entries written by concurrent threads must be readable.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testConcurrentLargeWritesNoDeadlock() throws Exception {
        int smallEntries = 1_000;
        int largeRowsPerThread = 20;
        int threads = 10;

        IgniteEx ignite = startGrid(1);

        IgniteCache<Integer, Object> cache = createCache(ignite, DEFAULT_CACHE_NAME);

        long regionMax = regionMaxSize(ignite);

        // Small value: sized so that 'smallEntries' of them fill ~80% of the region, staying under the 90% eviction threshold.
        byte[] smallVal = new byte[(int)(regionMax * 0.8 / smallEntries)];

        for (int i = 0; i < smallEntries; i++)
            cache.put(i, smallVal);

        assertFalse("Eviction must not have started during pre-fill", isEvictionsStarted(ignite));

        CountDownLatch startLatch = new CountDownLatch(1);
        AtomicInteger threadIdx = new AtomicInteger();

        // Large value: 5% of the region — large enough to trigger eviction during concurrent writes.
        byte[] largeVal = new byte[(int)(regionMax / 20)];

        IgniteInternalFuture<?> fut = GridTestUtils.runMultiThreadedAsync(() -> {
            U.awaitQuiet(startLatch);

            int idx = threadIdx.getAndIncrement();

            for (int k = 0; k < largeRowsPerThread; k++)
                cache.put(smallEntries + idx * largeRowsPerThread + k, largeVal);
        }, threads, "paged-writer");

        startLatch.countDown();

        fut.get(TimeUnit.MINUTES.toMillis(3));

        assertTrue("Eviction must have started during concurrent large writes", isEvictionsStarted(ignite));

        // Verify the free list is not corrupted: a new put+get must succeed.
        byte[] probeVal = new byte[pageSize(ignite) - 200];

        Arrays.fill(probeVal, (byte)1);

        int probeKey = smallEntries + threads * largeRowsPerThread + 1;

        cache.put(probeKey, probeVal);

        byte[] read = (byte[])cache.get(probeKey);

        assertNotNull(read);
        assertTrue(Arrays.equals(probeVal, read));

        int readableLarge = 0;

        for (int t = 0; t < threads; t++) {
            for (int k = 0; k < largeRowsPerThread; k++) {
                int key = smallEntries + t * largeRowsPerThread + k;

                if (cache.get(key) != null)
                    readableLarge++;
            }
        }

        assertTrue(readableLarge > 0);
    }
}

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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

/** Concurrent deadlock and data-integrity tests for size-aware page eviction. */
public abstract class PageEvictionConcurrentWritesAbstractTest extends PageEvictionAbstractTest {
    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** */
    @Test
    public void testConcurrentLargeWritesNoDeadlock() throws Exception {
        int smallEntries = 1_000;
        int largeRowsPerThread = 20;
        int threads = 10;

        IgniteEx ignite = startGrid(1);

        IgniteCache<Integer, Object> cache = createCache(ignite, DEFAULT_CACHE_NAME);

        long regionMax = regionMaxSize(ignite);

        int smallValSize = (int)(regionMax * 0.8 / smallEntries);

        for (int i = 0; i < smallEntries; i++)
            cache.put(i, valueFor(i, smallValSize));

        assertFalse("Eviction must not have started during pre-fill", isEvictionsStarted(ignite));

        CountDownLatch startLatch = new CountDownLatch(1);
        AtomicInteger threadIdx = new AtomicInteger();

        int largeValSize = (int)(regionMax / 20);

        IgniteInternalFuture<?> fut = GridTestUtils.runMultiThreadedAsync(() -> {
            U.awaitQuiet(startLatch);

            int idx = threadIdx.getAndIncrement();

            for (int k = 0; k < largeRowsPerThread; k++) {
                int key = smallEntries + idx * largeRowsPerThread + k;

                cache.put(key, valueFor(key, largeValSize));
            }
        }, threads, "paged-writer");

        startLatch.countDown();

        fut.get(TimeUnit.MINUTES.toMillis(3));

        assertTrue("Eviction must have started during concurrent large writes", isEvictionsStarted(ignite));

        int probeKey = smallEntries + threads * largeRowsPerThread + 1;
        byte[] probeVal = valueFor(probeKey, pageSize(ignite) - 200);

        cache.put(probeKey, probeVal);

        byte[] read = (byte[])cache.get(probeKey);

        assertNotNull(read);
        assertTrue(Arrays.equals(probeVal, read));

        verifyContent(cache, 0, smallEntries, smallValSize);

        int survivedLarge = verifyContent(cache, smallEntries, threads * largeRowsPerThread, largeValSize);

        assertTrue("At least one large entry must survive", survivedLarge > 0);
    }

    /** */
    @Test
    public void testRebalanceBatchInsertsWithEviction() throws Exception {
        IgniteEx ignite1 = startGrid(1);

        int largeValSize = (int)(regionMaxSize(ignite1) / 20);
        int largeRows = 25;

        IgniteCache<Integer, Object> cache = ignite1.createCache(new CacheConfiguration<Integer, Object>(DEFAULT_CACHE_NAME)
            .setAffinity(new RendezvousAffinityFunction(false, 32)).setBackups(1));

        for (int k = 0; k < largeRows; k++)
            cache.put(k, valueFor(k, largeValSize));

        byte[] smallVal = new byte[pageSize(ignite1) - 200];

        int smallKeyBase = largeRows + 1;

        for (int i = 0; i < totalPages(ignite1); i++) {
            cache.put(smallKeyBase + i, smallVal);

            if (isEvictionsStarted(ignite1))
                break;
        }

        assertTrue("Eviction must start during overflow fill", isEvictionsStarted(ignite1));

        IgniteEx ignite2 = startGrid(2);

        // Writers keep adding large rows on the coordinator while the joining node stores rebalance batches.
        CountDownLatch startLatch = new CountDownLatch(1);
        AtomicInteger threadIdx = new AtomicInteger();

        int writers = 4;
        int rowsPerWriter = 5;
        int writerKeyBase = smallKeyBase + (int)totalPages(ignite1);

        IgniteInternalFuture<?> fut = GridTestUtils.runMultiThreadedAsync(() -> {
            U.awaitQuiet(startLatch);

            int idx = threadIdx.getAndIncrement();

            for (int k = 0; k < rowsPerWriter; k++) {
                int key = writerKeyBase + idx * rowsPerWriter + k;

                cache.put(key, valueFor(key, largeValSize));
            }
        }, writers, "paged-writer");

        startLatch.countDown();

        awaitPartitionMapExchange();

        cache.rebalance().get(TimeUnit.MINUTES.toMillis(2));

        fut.get(TimeUnit.MINUTES.toMillis(2));

        assertTrue("Eviction must have started on the rebalanced node", isEvictionsStarted(ignite2));

        int survived = verifyContent(cache, 0, largeRows, largeValSize) +
            verifyContent(cache, writerKeyBase, writers * rowsPerWriter, largeValSize);

        assertTrue("At least one large entry must survive", survived > 0);
    }

    /** */
    @Test
    public void testConcurrentPutsAndPutAllNoDeadlock() throws Exception {
        IgniteEx ignite = startGrid(0);

        IgniteCache<Integer, byte[]> cache = ignite.createCache(new CacheConfiguration<Integer, byte[]>(DEFAULT_CACHE_NAME)
            .setAffinity(new RendezvousAffinityFunction(false, 32))
            .setBackups(0)
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.PRIMARY_SYNC));

        int valSize = pageSize(ignite) / 4 - 50;
        int smallEntries = (int)(regionMaxSize(ignite) * 0.7 / valSize);

        for (int i = 0; i < smallEntries; i++)
            cache.put(i, valueFor(i, valSize));

        AtomicInteger keyGen = new AtomicInteger(smallEntries);
        CountDownLatch startLatch = new CountDownLatch(1);
        AtomicInteger threadIdx = new AtomicInteger();

        IgniteInternalFuture<?> fut = GridTestUtils.runMultiThreadedAsync(() -> {
            U.awaitQuiet(startLatch);

            // Even threads do putAll batches, odd threads do single puts.
            boolean isBatch = (threadIdx.getAndIncrement() & 1) == 0;

            for (int op = 0; op < 50; op++) {
                if (isBatch) {
                    Map<Integer, byte[]> batch = new HashMap<>();

                    for (int i = 0; i < 20; i++) {
                        int key = keyGen.incrementAndGet();

                        batch.put(key, valueFor(key, valSize));
                    }

                    cache.putAll(batch);
                }
                else {
                    int key = keyGen.incrementAndGet();

                    cache.put(key, valueFor(key, valSize));
                }
            }
        }, 6, "mixed-writer");

        startLatch.countDown();

        fut.get(TimeUnit.MINUTES.toMillis(3));

        assertTrue("Eviction must have started", isEvictionsStarted(ignite));

        int probeKey = keyGen.incrementAndGet();
        byte[] probeVal = valueFor(probeKey, valSize);

        cache.put(probeKey, probeVal);

        byte[] read = cache.get(probeKey);

        assertNotNull(read);
        assertTrue(Arrays.equals(probeVal, read));
    }

    /** */
    private static int verifyContent(IgniteCache<Integer, ?> cache, int base, int cnt, int valSize) {
        int survived = 0;

        for (int k = base; k < base + cnt; k++) {
            byte[] read = (byte[])cache.get(k);

            if (read == null)
                continue; // Evicted.

            assertTrue("Content mismatch for key " + k, Arrays.equals(valueFor(k, valSize), read));

            survived++;
        }

        return survived;
    }

    /** @return Deterministic per-key value content. */
    private static byte[] valueFor(int key, int size) {
        byte[] val = new byte[size];

        for (int i = 0; i < size; i++)
            val[i] = (byte)(key * 31 + i);

        return val;
    }
}

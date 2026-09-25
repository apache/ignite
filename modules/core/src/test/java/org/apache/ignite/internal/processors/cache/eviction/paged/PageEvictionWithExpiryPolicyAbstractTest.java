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
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/** Tests the synergy between ExpiryPolicy (TTL cleanup) and size-aware page eviction on an in-memory data region. */
public abstract class PageEvictionWithExpiryPolicyAbstractTest extends PageEvictionAbstractTest {
    /** Short TTL applied to some entries. */
    private static final long TTL = 2000;

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * Concurrent page eviction and TTL cleanup on the same data region must not deadlock.
     * Multiple threads insert large records into a plain cache (triggering eviction) while small
     * entries in a separate TTL cache (sharing the same data region) expire and are cleaned up
     * by the eager-TTL background worker.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testLargePutWithExpiryNoDeadlock() throws Exception {
        IgniteEx ignite = startGrid(1);

        IgniteCache<Integer, Object> plainCache = createCache(ignite, "plain-cache");
        IgniteCache<Integer, Object> ttlCache = createCache(ignite, "ttl-cache", TTL, false);

        int smallEntries = 1_000;

        for (int i = 0; i < smallEntries; i++)
            plainCache.put(i, new byte[16 * 1024]);

        assertFalse("Eviction must not have started during pre-fill", isEvictionsStarted(ignite));

        int ttlEntries = 10;
        byte[] ttlVal = new byte[100];

        for (int i = 0; i < ttlEntries; i++)
            ttlCache.put(i, ttlVal);

        int largeRowsPerThread = 40;
        byte[] largeVal = new byte[1024 * 1024];

        CountDownLatch startLatch = new CountDownLatch(1);
        AtomicInteger threadIdx = new AtomicInteger();

        IgniteInternalFuture<?> fut = GridTestUtils.runMultiThreadedAsync(() -> {
            U.awaitQuiet(startLatch);

            int idx = threadIdx.getAndIncrement();

            for (int k = 0; k < largeRowsPerThread; k++)
                plainCache.put(smallEntries + idx * largeRowsPerThread + k, largeVal);
        }, 10, "paged-writer");

        startLatch.countDown();

        assertTrue("TTL entries must expire after TTL duration",
            waitForCondition(() -> {
                for (int i = 0; i < ttlEntries; i++) {
                    if (ttlCache.get(i) != null)
                        return false;
                }

                return true;
            }, TTL + 10_000));

        fut.get(TimeUnit.MINUTES.toMillis(3));

        assertTrue("Eviction must have started during concurrent large writes", isEvictionsStarted(ignite));
    }

    /**
     * After short-TTL entries expire and free their pages, putting the same amount of data to a non-expiring cache
     * must succeed, and the pre-filled small entries must remain intact (not evicted).
     *
     * @throws Exception If failed.
     */
    @Test
    public void testTtlFreedSpaceAccountedForByEviction() throws Exception {
        IgniteEx ignite = startGrid(1);

        IgniteCache<Integer, Object> plainCache = createCache(ignite, "plain-cache");
        IgniteCache<Integer, Object> ttlCache = createCache(ignite, "ttl-cache", TTL, false);

        int ttlEntries = 2;

        int smallEntries = (int)(totalPages(ignite) * 0.3);

        byte[] small = new byte[pageSize(ignite) - 200];

        for (int i = 0; i < smallEntries; i++)
            plainCache.put(i, small);

        Object val = new byte[(int)(regionMaxSize(ignite) / 5)];

        for (int i = 0; i < ttlEntries; i++)
            ttlCache.put(i, val);

        assertNotNull("TTL entry must be present before expiry", ttlCache.get(0));

        assertTrue(waitForCondition(() -> ttlCache.size() == 0, 10_000));

        for (int i = 0; i < ttlEntries; i++)
            plainCache.put(smallEntries + i, val);

        for (int i = 0; i < ttlEntries; i++)
            assertNotNull("Fresh large record must be present", plainCache.get(smallEntries + i));

        for (int i = 0; i < smallEntries; i++)
            assertNotNull("Pre-filled entry " + i + " must not be evicted", plainCache.get(i));
    }
}

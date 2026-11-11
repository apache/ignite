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
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_EXPIRED;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/** Tests the synergy between ExpiryPolicy (TTL cleanup) and size-aware page eviction on an in-memory data region. */
public abstract class PageEvictionWithExpiryPolicyAbstractTest extends PageEvictionAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        return super.getConfiguration(gridName).setIncludeEventTypes(EVT_CACHE_OBJECT_EXPIRED);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        assertNoOomFailures();
    }

    /** Entries expire while concurrent large puts evict other entries: neither must deadlock or fail. */
    @Test
    public void testLargePutWithExpiryNoDeadlock() throws Exception {
        IgniteEx ignite = startGrid(1);

        IgniteCache<Integer, Object> ttlCache = createCache(ignite, "ttl-cache", 100, false);

        AtomicInteger expired = new AtomicInteger();

        ignite.events().localListen(evt -> {
            expired.incrementAndGet();

            return true;
        }, EVT_CACHE_OBJECT_EXPIRED);

        byte[] largeVal = new byte[1024 * 1024];

        AtomicInteger keyGen = new AtomicInteger();

        long stopTime = U.currentTimeMillis() + 2_000;

        GridTestUtils.runMultiThreadedAsync(() -> {
            while (U.currentTimeMillis() < stopTime) {
                ttlCache.put(keyGen.incrementAndGet(), largeVal);

                // The region refills slower than the TTL, so some entries live long enough to expire.
                doSleep(100);
            }
        }, 10, "ttl-writer").get(TIMEOUT);

        assertTrue("Eviction must have started during concurrent large writes", isEvictionsStarted(ignite));
        assertTrue("Entries must expire during concurrent large writes", expired.get() > 0);
        assertTrue("Remaining entries must expire", waitForCondition(() -> ttlCache.size() == 0, 10_000));
    }

    /** */
    @Test
    public void testTtlFreedSpaceAccountedForByEviction() throws Exception {
        IgniteEx ignite = startGrid(1);

        IgniteCache<Integer, Object> plainCache = createCache(ignite, "plain-cache");
        IgniteCache<Integer, Object> ttlCache = createCache(ignite, "ttl-cache", 2000, false);

        // Pre-fill + two 10% TTL rows + the empty pages pool must stay below the eviction threshold, which applies
        // to every insert including the size of the row being inserted. Without reusing the space freed by TTL, the two
        // fresh rows of the same size would not fit into the region at all.
        int smallEntries = fillRegion(ignite, plainCache, 0.6);

        assertFalse("Eviction must not start during pre-fill", isEvictionsStarted(ignite));

        int ttlEntries = 2;
        byte[] val = new byte[(int)(regionMaxSize(ignite) * 0.1)];

        for (int i = 0; i < ttlEntries; i++)
            ttlCache.put(i, val);

        assertNotNull("TTL entry must be present before expiry", ttlCache.get(0));

        assertTrue("TTL entries must expire", waitForCondition(() -> ttlCache.size() == 0, 10_000));

        for (int i = 0; i < ttlEntries; i++)
            plainCache.put(smallEntries + i, val);

        for (int i = 0; i < ttlEntries; i++) {
            byte[] read = (byte[])plainCache.get(smallEntries + i);

            assertNotNull("Fresh large record must be present", read);
            assertTrue("Fresh large record must match", Arrays.equals(val, read));
        }

        for (int i = 0; i < smallEntries; i++)
            assertNotNull("Pre-filled entry " + i + " must not be evicted", plainCache.get(i));

        assertFalse("The space freed by TTL must be reused without the eviction", isEvictionsStarted(ignite));
    }
}

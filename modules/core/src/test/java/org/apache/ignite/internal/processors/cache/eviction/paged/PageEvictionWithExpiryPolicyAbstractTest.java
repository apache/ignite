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

import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

/** Tests the synergy between ExpiryPolicy (TTL cleanup) and size-aware page eviction on an in-memory data region. */
public abstract class PageEvictionWithExpiryPolicyAbstractTest extends PageEvictionAbstractTest {
    /** Off-heap region size. */
    private static final int SIZE = 128 * 1024 * 1024;

    /**
     * Large record size (much larger than the empty-pages pool, and larger than the space left free when the region
     * is held at the eviction threshold {@code (1 - threshold) * totalPages}) so that a large put cannot take the
     * fast path of {@code ensureFreeSpaceForInsert} and must actually run the size-aware eviction reserve.
     */
    private static final int RECORD_SIZE = 32 * 1024 * 1024;

    /** Short TTL applied to some entries. */
    private static final long TTL = 2000;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        return super.getConfiguration(gridName)
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                    .setInitialSize(SIZE)
                    .setMaxSize(SIZE)));
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * Concurrent TTL cleanup and eviction must not deadlock, and a large record (larger than the empty-pages pool)
     * must still be stored on a region with enabled eviction even in the presence of short-TTL entries.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testLargePutWithExpiryNoDeadlock() throws Exception {
        IgniteEx ignite = startGrid(1);

        IgniteCache<Integer, Object> cache = createCache(ignite, DEFAULT_CACHE_NAME, TTL, false);

        Object val = new byte[RECORD_SIZE];

        for (int i = 0; i < 60; i++)
            cache.put(i, val);

        assertNotNull("Cache must remain responsive after concurrent expiry and eviction", cache.get(59));
    }

    /**
     * After short-TTL entries expire and free their pages, putting the same amount of data to a non-expiring cache
     * must succeed, and the pre-filled small entries must remain intact (not evicted).
     *
     * @throws Exception If failed.
     */
    @Test
    public void testTtlFreedSpaceAccountedForByEviction() throws Exception {
        int ttlEntries = 2;
        int smallEntries = 6_000;

        IgniteEx ignite = startGrid(1);

        IgniteCache<Integer, Object> plainCache = createCache(ignite, "plain-cache");
        IgniteCache<Integer, Object> ttlCache = createCache(ignite, "ttl-cache", TTL, false);

        // Pre-fill the region with small non-expiring entries, but leave enough room for the large TTL entries.
        byte[] small = new byte[1024];

        for (int i = 0; i < smallEntries; i++)
            plainCache.put(i, small);

        Object val = new byte[RECORD_SIZE];

        for (int i = 0; i < ttlEntries; i++)
            ttlCache.put(i, val);

        assertNotNull("TTL entry must be present before expiry", ttlCache.get(0));

        GridTestUtils.waitForCondition(() -> ttlCache.get(0) == null, 10_000);

        for (int i = 0; i < ttlEntries; i++)
            plainCache.put(smallEntries + i, val);

        for (int i = 0; i < ttlEntries; i++)
            assertNotNull("Fresh large record must be present", plainCache.get(smallEntries + i));

        for (int i = 0; i < smallEntries; i++)
            assertNotNull("Pre-filled entry " + i + " must not be evicted", plainCache.get(i));
    }
}

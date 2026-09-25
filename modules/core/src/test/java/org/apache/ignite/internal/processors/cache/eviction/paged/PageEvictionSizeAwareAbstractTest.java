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
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.mem.IgniteOutOfMemoryException;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

/** Tests size-aware page eviction on in-memory (non-persistent) data regions. */
public abstract class PageEvictionSizeAwareAbstractTest extends PageEvictionAbstractTest {
    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * A record larger than the whole region must fail (not hang) even when size-aware eviction is enabled.
     * A batch {@code putAll} of records whose total size equals the region capacity must also fail with OOM because
     * structural pages (free list, index tree) leave no room for the data (exercises the batch store path
     * {@code RowStore.addRows} → {@code ensureFreeSpaceForInsert}).
     *
     * @throws Exception If failed.
     */
    @Test
    public void testRecordLargerThanRegionOom() throws Exception {
        IgniteEx ignite = startGrid(1);

        IgniteCache<Integer, Object> cache = createCache(ignite, DEFAULT_CACHE_NAME);

        long regionSize = regionMaxSize(ignite);

        GridTestUtils.assertThrowsWithCause(
            () -> cache.put(1, new byte[(int)(regionSize * 2)]),
            IgniteOutOfMemoryException.class
        );

        GridTestUtils.assertThrowsWithCause(
            () -> {
                Map<Integer, Object> batch = new HashMap<>();

                Object val = new byte[(int)(regionSize / 4)];

                for (int i = 0; i < 4; i++)
                    batch.put(i, val);

                cache.putAll(batch);
            },
            IgniteOutOfMemoryException.class
        );
    }

    /**
     * A batch putAll of several large records (each larger than the empty-pages pool) must be stored successfully when
     * page eviction is enabled. Exercises the size-aware reserve in the batch store path ({@code RowStore.addRows}).
     *
     * @throws Exception If failed.
     */
    @Test
    public void testPutAllLargeRows() throws Exception {
        IgniteEx ignite = startGrid(1);

        IgniteCache<Integer, Object> cache = createCache(ignite, DEFAULT_CACHE_NAME);

        // Pre-fill to ~85% of capacity so that less than one large row remains available.
        int smallEntries = (int)(totalPages(ignite) * 0.85);

        byte[] small = new byte[pageSize(ignite) - 200];

        for (int i = 0; i < smallEntries; i++)
            cache.put(smallEntries + i, small);

        assertTrue("Pre-fill must load >80% of pages", loadedPages(ignite) > totalPages(ignite) * 0.80);
        assertFalse("Eviction must not start during pre-fill", isEvictionsStarted(ignite));

        int putAllLargeRows = 3;

        Map<Integer, Object> large = new HashMap<>();

        Object val = new byte[(int)(regionMaxSize(ignite) / 4)];

        for (int i = 0; i < putAllLargeRows; i++)
            large.put(i, val);

        cache.putAll(large);

        assertTrue("Eviction must start after putAll of large rows near capacity", isEvictionsStarted(ignite));

        for (int i = 0; i < putAllLargeRows; i++)
            assertNotNull("Large row " + i + " must be readable after putAll", cache.get(i));
    }

    /**
     * Updating a record from a small to a large value (larger than the empty-pages pool) must succeed with page
     * eviction enabled: the update goes through the same size-aware reserve as an insert. The region is pre-filled
     * to near capacity so that the grown value cannot fit without evicting pre-filled entries.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testUpdateRowGrows() throws Exception {
        IgniteEx ignite = startGrid(1);

        IgniteCache<Integer, Object> cache = createCache(ignite, DEFAULT_CACHE_NAME);

        int smallEntries = (int)(totalPages(ignite) * 0.85);

        byte[] small = new byte[pageSize(ignite) - 200];

        for (int i = 0; i < smallEntries; i++)
            cache.put(smallEntries + i, small);

        // Insert key 1 with a small value, then update it to a large value that requires size-aware eviction.
        cache.put(1, new byte[1024]);

        byte[] big = new byte[(int)(regionMaxSize(ignite) / 4)];

        Arrays.fill(big, (byte)7);

        cache.put(1, big);

        assertTrue("Eviction must have started after growing a row near capacity", isEvictionsStarted(ignite));

        byte[] read = (byte[])cache.get(1);

        assertNotNull("Updated large value must be readable", read);
        assertTrue("Updated value must equal the stored value", Arrays.equals(big, read));
    }

    /**
     * Verifies the fast path in {@code ensureFreeSpaceForEviction}: when the region is below the
     * eviction threshold, a large row (exceeding the empty-pages pool) that fits in the remaining headroom is written
     * successfully <b>without triggering eviction</b> — the size-aware reserve trusts headroom
     * and skips the eviction loop. If the fast path were broken, size-aware eviction would run unnecessarily,
     * evicting pre-filled entries and setting the evictions-started flag.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testLargeRowBelowThresholdUsesHeadroomNoEviction() throws Exception {
        IgniteEx ignite = startGrid(1);

        IgniteCache<Integer, Object> cache = createCache(ignite, DEFAULT_CACHE_NAME);

        // Pre-fill to ~40% of total pages — well below the eviction threshold, leaving ~50% headroom.
        int preFill = (int)(totalPages(ignite) * 0.4);

        byte[] small = new byte[pageSize(ignite) - 200];

        for (int i = 0; i < preFill; i++)
            cache.put(i, small);

        long loadedPages = loadedPages(ignite);
        long totalPages = totalPages(ignite);

        assertTrue("Pre-fill must load ~40% of pages", loadedPages > totalPages * 0.30 && loadedPages < totalPages * 0.50);
        assertFalse("Eviction must not have started during pre-fill below threshold", isEvictionsStarted(ignite));

        // Write a large row that fits in the remaining headroom. After pre-fill at ~40%, ~60% remains.
        // Use ~30% of region so total ~70% stays below the 90% threshold.
        byte[] val = new byte[(int)(regionMaxSize(ignite) * 0.3)];

        Arrays.fill(val, (byte)1);

        cache.put(preFill, val);

        assertFalse("Eviction must not start when the region stays below the threshold", isEvictionsStarted(ignite));

        for (int i = 0; i < preFill; i++)
            assertNotNull("Pre-filled entry " + i + " must not be evicted below threshold", cache.get(i));

        byte[] read = (byte[])cache.get(preFill);

        assertNotNull("Large row must be readable", read);
        assertTrue("Value read back must equal the stored value", Arrays.equals(val, read));
    }

    /**
     * Verifies that a large row (exceeding the empty-pages pool) is written successfully when the region is already
     * at or above the eviction threshold — the size-aware eviction loop evicts enough
     * pre-filled entries to free the required pages, and to write succeeds.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testLargeRowAboveThresholdEvictsAndSucceeds() throws Exception {
        IgniteEx ignite = startGrid(1);

        IgniteCache<Integer, Object> cache = createCache(ignite, DEFAULT_CACHE_NAME);

        // Pre-fill to ~85% of capacity — above the 0.9 eviction threshold (with page overhead).
        int smallEntries = (int)(totalPages(ignite) * 0.85);

        byte[] small = new byte[pageSize(ignite) - 200];

        for (int i = 0; i < smallEntries; i++)
            cache.put(i, small);

        assertTrue("Pre-fill must load >80% of pages", loadedPages(ignite) > totalPages(ignite) * 0.80);

        // Write a large row that exceeds the combined headroom + empty-pages pool, so the size-aware
        // reserve must actually evict pre-filled entries to make room.
        byte[] val = new byte[(int)(regionMaxSize(ignite) / 4)];

        Arrays.fill(val, (byte)1);

        int largeKey = smallEntries + 1;

        cache.put(largeKey, val);

        assertTrue("Eviction must have started after a large put near capacity", isEvictionsStarted(ignite));

        byte[] read = (byte[])cache.get(largeKey);

        assertNotNull("Large row must be readable after eviction", read);
        assertTrue("Value read back must equal the stored value", Arrays.equals(val, read));
    }

    /**
     * Verifies that the data region can be filled beyond the eviction threshold (default 0.9) — at least to
     * 95% of total pages — without triggering eviction. Before the fix, the headroom gate
     * caused the size-aware reserve to ignore headroom above the threshold, so the region effectively stopped
     * growing at ~90% and evicted pre-filled entries unnecessarily.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testRegionFillsBeyondEvictionThreshold() throws Exception {
        IgniteEx ignite = startGrid(1);

        IgniteCache<Integer, Object> cache = createCache(ignite, DEFAULT_CACHE_NAME);

        long total = totalPages(ignite);

        byte[] small = new byte[pageSize(ignite) - 200];

        // Put entries until the region is at least 95% full.
        for (int i = 0; i < total; i++) {
            cache.put(i, small);

            if (loadedPages(ignite) >= total * 0.95)
                break;
        }

        assertTrue("Region must fill beyond the 0.9 eviction threshold", loadedPages(ignite) >= total * 0.95);
        assertFalse("Eviction must not start when headroom is still available", isEvictionsStarted(ignite));

        stopGrid(1);
        ignite = startGrid(1);

        assertEquals(0, loadedPages(ignite));

        cache = createCache(ignite, DEFAULT_CACHE_NAME);

        int batchSize = 100;

        // Put entries in batches until the region is at least 95% full.
        for (int base = 0; base < total; base += batchSize) {
            Map<Integer, Object> batch = new HashMap<>();

            for (int i = 0; i < batchSize && base + i < total; i++)
                batch.put(base + i, small);

            cache.putAll(batch);

            if (loadedPages(ignite) >= total * 0.95)
                break;
        }

        assertTrue("Region must fill beyond the 0.9 eviction threshold via putAll", loadedPages(ignite) >= total * 0.95);
        assertFalse("Eviction must not start when headroom is still available", isEvictionsStarted(ignite));
    }

    /** @return Loaded pages. */
    private long loadedPages(IgniteEx ignite) throws IgniteCheckedException {
        return defaultRegion(ignite).pageMemory().loadedPages();
    }
}

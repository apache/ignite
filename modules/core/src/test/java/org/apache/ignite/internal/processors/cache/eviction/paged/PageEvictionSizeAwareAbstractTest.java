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
     * A record larger than the whole region, and a {@code putAll} whose total size equals the region capacity, must
     * both fail with OOM (structural pages leave no room for the data).
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
     * A {@code putAll} of large records (each larger than the empty-pages pool) must succeed with eviction enabled.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testPutAllLargeRows() throws Exception {
        IgniteEx ignite = startGrid(1);

        IgniteCache<Integer, Object> cache = createCache(ignite, DEFAULT_CACHE_NAME);

        // Pre-fill to ~85% of capacity so less than one large row remains available.
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
     * Updating a small record to a large value near capacity must succeed via the size-aware reserve.
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
     * Fast path: below the eviction threshold, a large row that fits in the remaining headroom is written without
     * triggering eviction.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testLargeRowBelowThresholdUsesHeadroomNoEviction() throws Exception {
        IgniteEx ignite = startGrid(1);

        IgniteCache<Integer, Object> cache = createCache(ignite, DEFAULT_CACHE_NAME);

        // Pre-fill to ~40% of total pages, leaving ~50% headroom.
        int preFill = fillRegion(ignite, cache, 0.4);

        long loadedPages = loadedPages(ignite);
        long totalPages = totalPages(ignite);

        assertTrue("Pre-fill must load ~40% of pages", loadedPages > totalPages * 0.30 && loadedPages < totalPages * 0.50);
        assertFalse("Eviction must not have started during pre-fill below threshold", isEvictionsStarted(ignite));

        // ~30% of the region fits in the remaining headroom (total ~70% < 0.9 threshold).
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
     * Above the eviction threshold, a large row exceeding the headroom + empty-pages pool is written by evicting
     * pre-filled entries.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testLargeRowAboveThresholdEvictsAndSucceeds() throws Exception {
        IgniteEx ignite = startGrid(1);

        IgniteCache<Integer, Object> cache = createCache(ignite, DEFAULT_CACHE_NAME);

        // Pre-fill to ~85% of capacity (above the 0.9 threshold with page overhead).
        int smallEntries = fillRegion(ignite, cache, 0.85);

        assertTrue("Pre-fill must load >80% of pages", loadedPages(ignite) > totalPages(ignite) * 0.80);

        // Large row exceeding headroom + empty-pages pool → reserve must evict to make room.
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
     * Threshold-gate semantics: once loaded pages cross the 0.9 threshold, the gate evicts on every put, so the region
     * caps near the threshold. Exercises both single-put and {@code putAll} paths.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testRegionEvictsAtThreshold() throws Exception {
        IgniteEx ignite = startGrid(1);

        IgniteCache<Integer, Object> cache = createCache(ignite, DEFAULT_CACHE_NAME);

        long total = totalPages(ignite);

        int cap = (int)total * 2;

        byte[] small = new byte[pageSize(ignite) - 200];

        // Fill until eviction starts.
        for (int i = 0; i < cap; i++) {
            cache.put(i, small);

            if (isEvictionsStarted(ignite))
                break;
        }

        assertTrue("Eviction must start once the region crosses the 0.9 threshold", isEvictionsStarted(ignite));
        assertTrue("Region must reach at least ~85% before eviction engages", loadedPages(ignite) >= total * 0.85);

        stopGrid(1);
        ignite = startGrid(1);

        assertEquals(0, loadedPages(ignite));

        cache = createCache(ignite, DEFAULT_CACHE_NAME);

        int batchSize = 100;

        // Fill via putAll batches until eviction starts.
        for (int base = 0; base < cap && !isEvictionsStarted(ignite); base += batchSize) {
            Map<Integer, Object> batch = new HashMap<>();

            for (int i = 0; i < batchSize; i++)
                batch.put(base + i, small);

            cache.putAll(batch);
        }

        assertTrue("Eviction must start via putAll once the region crosses the 0.9 threshold",
            isEvictionsStarted(ignite));
        assertTrue("Region must reach at least ~85% before eviction engages via putAll",
            loadedPages(ignite) >= total * 0.85);
    }

    /** @return Loaded pages. */
    private long loadedPages(IgniteEx ignite) throws IgniteCheckedException {
        return defaultRegion(ignite).pageMemory().loadedPages();
    }
}

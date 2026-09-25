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
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.mem.IgniteOutOfMemoryException;
import org.apache.ignite.internal.processors.cache.persistence.DataRegionMetricsImpl;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

/** Tests size-aware page eviction on in-memory (non-persistent) data regions. */
public abstract class PageEvictionSizeAwareAbstractTest extends PageEvictionAbstractTest {
    /** Off-heap region size (large enough to hold cache structural pages with the configured partition count). */
    private static final int SIZE = 128 * 1024 * 1024;

    /**
     * Record size: chosen so that a single row requires more pages than are left free when the region is kept at the
     * eviction threshold ({@code (1 - threshold) * totalPages}). This guarantees a large put cannot take the fast
     * path of {@code ensureFreeSpaceForInsert} and must actually run the size-aware eviction reserve
     * ({@code ensureFreeSpaceForEviction}), which is the scenario these tests are meant to cover.
     */
    private static final int RECORD_SIZE = 32 * 1024 * 1024;

    /** Small record size used to pre-fill the region with evictable data (for putAll tests). */
    private static final int SMALL_RECORD_SIZE = 4096;

    /**
     * Small pre-fill entries count. Chosen to fill the 128 MiB region close to capacity so that less than one large
     * record ({@link #RECORD_SIZE}) remains available, forcing {@code ensureFreeSpaceForInsert} to actually evict
     * pre-filled entries rather than taking its fast path.
     */
    private static final int SMALL_ENTRIES = 28_000;

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

        GridTestUtils.assertThrowsWithCause(
            () -> cache.put(1, new byte[SIZE * 2]),
            IgniteOutOfMemoryException.class
        );

        GridTestUtils.assertThrowsWithCause(
            () -> {
                Map<Integer, Object> batch = new HashMap<>();

                Object val = new byte[RECORD_SIZE];

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
        int putAllLargeRows = 3;

        IgniteEx ignite = startGrid(1);

        IgniteCache<Integer, Object> cache = createCache(ignite, DEFAULT_CACHE_NAME);

        // Pre-fill the region to near capacity with small evictable entries so that less than one large row remains
        // available. This forces ensureFreeSpaceForInsert to evict pre-filled entries rather than taking its fast path.
        byte[] small = new byte[SMALL_RECORD_SIZE];

        for (int i = 0; i < SMALL_ENTRIES; i++)
            cache.put(SMALL_ENTRIES + i, small);

        Map<Integer, Object> large = new HashMap<>();

        Object val = new byte[RECORD_SIZE];

        for (int i = 0; i < putAllLargeRows; i++)
            large.put(i, val);

        cache.putAll(large);

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

        // Pre-fill the region to near capacity with small evictable entries so that the grown value below cannot
        // fit without eviction.
        byte[] small = new byte[SMALL_RECORD_SIZE];

        for (int i = 0; i < SMALL_ENTRIES; i++)
            cache.put(SMALL_ENTRIES + i, small);

        // Insert key 1 with a small value, then update it to a large value that requires size-aware eviction.
        cache.put(1, new byte[1024]);

        byte[] big = new byte[RECORD_SIZE];

        Arrays.fill(big, (byte)7);

        cache.put(1, big);

        byte[] read = (byte[])cache.get(1);

        assertNotNull("Updated large value must be readable", read);
        assertTrue("Updated value must equal the stored value", Arrays.equals(big, read));
    }

    /**
     * Verifies the {@code evictionRegime} gate in {@code ensureFreeSpaceForEviction}: when the region is below the
     * eviction threshold, a large row (exceeding the empty-pages pool) that fits in the remaining headroom is written
     * successfully <b>without triggering eviction</b> — the size-aware reserve trusts headroom
     * ({@code evictionRegime = false}) and skips the eviction loop. If the gate were broken (always {@code true}),
     * size-aware eviction would run unnecessarily, evicting pre-filled entries and setting the evictions-started flag.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testLargeRowBelowThresholdUsesHeadroomNoEviction() throws Exception {
        IgniteEx ignite = startGrid(1);

        IgniteCache<Integer, Object> cache = createCache(ignite, DEFAULT_CACHE_NAME);

        // Pre-fill to ~80 MiB — below the 0.9 * 128 MiB = 115.2 MiB threshold, leaving ~48 MiB of headroom.
        // After pre-fill, loadedPages is well below threshold, so evictionRegime = false.
        byte[] small = new byte[SMALL_RECORD_SIZE];

        int preFill = 20_000;

        for (int i = 0; i < preFill; i++)
            cache.put(i, small);

        DataRegionMetricsImpl metrics = ignite.context().cache().context().database().dataRegion(null).metrics();

        assertFalse("Eviction must not have started during pre-fill below threshold", metrics.isEvictionsStarted());

        // Write a 32 MiB row that fits in the remaining headroom (~48 MiB). Total ~112 MiB stays below the 115.2 MiB
        // threshold, so evictionRegime remains false throughout and no eviction is triggered.
        byte[] val = new byte[RECORD_SIZE];

        Arrays.fill(val, (byte)1);

        cache.put(preFill, val);

        // Eviction must not have started: the region never crossed the threshold, so the size-aware reserve
        // trusted headroom and the normal threshold eviction in insertDataRows never fired.
        assertFalse("Eviction must not start when the region stays below the threshold", metrics.isEvictionsStarted());

        for (int i = 0; i < preFill; i++)
            assertNotNull("Pre-filled entry " + i + " must not be evicted below threshold", cache.get(i));

        byte[] read = (byte[])cache.get(preFill);

        assertNotNull("Large row must be readable", read);
        assertTrue("Value read back must equal the stored value", Arrays.equals(val, read));
    }

    /**
     * Verifies that a large row (exceeding the empty-pages pool) is written successfully when the region is already
     * at or above the eviction threshold — the size-aware eviction loop ({@code evictionRegime = true}) evicts enough
     * pre-filled entries to free the required pages, and the write succeeds.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testLargeRowAboveThresholdEvictsAndSucceeds() throws Exception {
        IgniteEx ignite = startGrid(1);

        IgniteCache<Integer, Object> cache = createCache(ignite, DEFAULT_CACHE_NAME);

        // Pre-fill to near capacity with small evictable entries. 28 000 x 4 KiB ~ 112 MiB of data; with page
        // overhead loadedPages is at or above the 0.9 threshold, so evictionRegime = true.
        byte[] small = new byte[SMALL_RECORD_SIZE];

        for (int i = 0; i < SMALL_ENTRIES; i++)
            cache.put(i, small);

        // Write a 32 MiB row that exceeds the empty-pages pool. Since the region is at or above the eviction
        // threshold, the size-aware reserve cannot trust headroom and must actually evict pre-filled entries.
        byte[] val = new byte[RECORD_SIZE];

        Arrays.fill(val, (byte)1);

        int largeKey = SMALL_ENTRIES + 1;

        cache.put(largeKey, val);

        DataRegionMetricsImpl metrics = ignite.context().cache().context().database().dataRegion(null).metrics();

        assertTrue("Eviction must have started after a large put near capacity", metrics.isEvictionsStarted());

        byte[] read = (byte[])cache.get(largeKey);

        assertNotNull("Large row must be readable after eviction", read);
        assertTrue("Value read back must equal the stored value", Arrays.equals(val, read));
    }
}

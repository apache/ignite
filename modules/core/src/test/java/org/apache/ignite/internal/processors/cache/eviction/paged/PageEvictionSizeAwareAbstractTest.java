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
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

/**
 * Tests size-aware page eviction on in-memory (non-persistent) data regions.
 * <p>
 * Verifies that a row larger than the configured {@code emptyPagesPoolSize} (in pages) is still written successfully
 * when page eviction is enabled, by evicting old entries to free enough space. Also verifies that a row
 * which fundamentally cannot fit into the region fails with OOM instead of hanging in an infinite eviction loop.
 * The batch path ({@code putAll} of large rows) and the update path (growing a row) are covered as well.
 */
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
     * A batch {@code putAll} of records whose total size exceeds the region must also fail with OOM (exercises the
     * batch store path {@code RowStore.addRows} → {@code ensureFreeSpaceForInsert}).
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
     * Verifies the {@code !evictionRegime} fast path in {@code ensureFreeSpaceForEviction}: when the region is below
     * the eviction threshold, a large row (exceeding the empty-pages pool) that fits in the remaining headroom must be
     * written successfully — the size-aware reserve trusts headroom and does not trigger size-aware eviction.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testGrowBeyondEvictionThreshold() throws Exception {
        IgniteEx ignite = startGrid(1);

        IgniteCache<Integer, Object> cache = createCache(ignite, DEFAULT_CACHE_NAME);

        // Pre-fill to ~80 MiB — below the 0.9 * 128 MiB = 115.2 MiB threshold, leaving ~48 MiB of headroom.
        byte[] small = new byte[SMALL_RECORD_SIZE];

        int preFill = 20_000;

        for (int i = 0; i < preFill; i++)
            cache.put(i, small);

        // Write a 32 MiB row that fits in the remaining headroom. The size-aware reserve sees loadedPages below the
        // threshold, trusts headroom (evictionRegime = false), and skips size-aware eviction.
        byte[] val = new byte[RECORD_SIZE];

        Arrays.fill(val, (byte)1);

        cache.put(preFill, val);

        byte[] read = (byte[])cache.get(preFill);

        assertNotNull("Large row must be readable after writing below threshold", read);
        assertTrue("Value read back must equal the stored value", Arrays.equals(val, read));
    }
}

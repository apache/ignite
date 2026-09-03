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
 * Tests size-aware page eviction on in-memory (non-persistent) data regions.
 *
 * Verifies that a row larger than the configured {@code emptyPagesPoolSize} (in pages) is still written successfully
 * when page eviction is enabled, by evicting old entries to free enough space. Also verifies that a row
 * which fundamentally cannot fit into the region fails with OOM instead of hanging in an infinite eviction loop.
 *
 * Note: the atomic DHT batch path (putAll of many large rows overflowing a small region) is out of scope here — it is
 * handled by a separate size-aware reserve in the batch store path and already fails on the original code.
 */
public abstract class PageEvictionSizeAwareAbstractTest extends GridCommonAbstractTest {
    /** Off-heap region size (large enough to hold cache structural pages with the configured partition count). */
    private static final int SIZE = 128 * 1024 * 1024;

    /** Partition count (kept low so that index-tree structures do not exhaust the region). */
    private static final int PARTITIONS = 32;

    /** Record size: chosen to be much larger than {@code emptyPagesPoolSize} pages. */
    private static final int RECORD_SIZE = 4 * 1024 * 1024;

    /** Empty pages pool size. */
    private static final int POOL_SIZE = 100;

    /** Entry count to accumulate beyond the region capacity. */
    private static final int ENTRIES = 40;

    /** Small record size used to pre-fill the region with evictable data (for putAll tests). */
    private static final int SMALL_RECORD_SIZE = 4096;

    /** Small pre-fill entries count. */
    private static final int SMALL_ENTRIES = 8000;

    /** Large rows written via putAll. */
    private static final int PUT_ALL_LARGE_ROWS = 3;

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
     * A large record (larger than the empty-pages pool) must be stored without OOM when there is evictable data,
     * by evicting previously stored records to free enough space.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testPutLargeObjectsDoesNotOom() throws Exception {
        IgniteEx ignite = startGrids(2);

        IgniteCache<Integer, Object> cache = createCache(ignite);

        Object val = new byte[RECORD_SIZE];

        // Total data (ENTRIES * RECORD_SIZE) exceeds the region size, so at least some records must be evicted.
        for (Integer key : primaryKeys(grid(1).cache(DEFAULT_CACHE_NAME), ENTRIES))
            cache.put(key, val);

        // Eviction must have bounded the number of resident entries.
        assertTrue("Expected some entries to be evicted, but cache.size()=" + cache.size(),
            cache.size() > 0 && cache.size() < ENTRIES);
    }

    /**
     * A large record written must be readable right away (the just-written entry is the most recently used and is not
     * a candidate for eviction before the write completes).
     *
     * @throws Exception If failed.
     */
    @Test
    public void testLargeObjectReadBack() throws Exception {
        IgniteEx ignite = startGrid(1);

        IgniteCache<Integer, Object> cache = createCache(ignite);

        byte[] val = new byte[RECORD_SIZE];

        Arrays.fill(val, (byte)42);

        cache.put(1, val);

        byte[] read = (byte[])cache.get(1);

        assertNotNull("Large value must be readable after put", read);

        assertTrue("Value read back must equal the stored value", Arrays.equals(val, read));
    }

    /**
     * A record larger than the whole region must fail (not hang) even when size-aware eviction is enabled.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testRecordLargerThanRegionOom() throws Exception {
        IgniteEx ignite = startGrid(1);

        IgniteCache<Integer, Object> cache = createCache(ignite);

        boolean rejected = false;

        try {
            cache.put(1, new byte[SIZE * 2]);
        }
        catch (Exception e) {
            // OOM (possibly wrapped) because the row cannot fit into the region.
            rejected = true;
        }

        assertTrue("Record larger than the region must be rejected (no hang), but put succeeded", rejected);
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

        IgniteCache<Integer, Object> cache = createCache(ignite);

        // Pre-fill with small evictable entries so large rows below region capacity fit via the reserve path.
        byte[] small = new byte[SMALL_RECORD_SIZE];

        for (int i = 0; i < SMALL_ENTRIES; i++)
            cache.put(SMALL_ENTRIES + i, small);

        Map<Integer, Object> large = new HashMap<>();

        Object val = new byte[RECORD_SIZE];

        for (int i = 0; i < PUT_ALL_LARGE_ROWS; i++)
            large.put(i, val);

        cache.putAll(large);

        for (int i = 0; i < PUT_ALL_LARGE_ROWS; i++)
            assertNotNull("Large row " + i + " must be readable after putAll", cache.get(i));
    }

    /**
     * Updating a record from a small to a large value (larger than the empty-pages pool) must succeed with page
     * eviction enabled: the update goes through the same size-aware reserve as an insert.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testUpdateRowGrows() throws Exception {
        IgniteEx ignite = startGrid(1);

        IgniteCache<Integer, Object> cache = createCache(ignite);

        cache.put(1, new byte[1024]);

        byte[] big = new byte[RECORD_SIZE];

        Arrays.fill(big, (byte)7);

        cache.put(1, big);

        byte[] read = (byte[])cache.get(1);

        assertNotNull("Updated large value must be readable", read);

        assertTrue("Updated value must equal the stored value", Arrays.equals(big, read));
    }
}

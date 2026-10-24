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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataPageEvictionMode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.metric.IoStatisticsHolderNoOp;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.IgniteCacheOffheapManager.CacheDataStore;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.persistence.freelist.FreeList;
import org.apache.ignite.internal.processors.cache.tree.DataRow;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.getFieldValue;

/**
 * Tests the page reservation accounting of an in-memory free list with enabled page eviction: the reservation is fully
 * released after batch inserts (whole-page fragments, fill-up-handler rows, whole-page-aligned rows, concurrent batch
 * inserts), concurrent puts and putAll on a backup node, and read-through loads; the empty pages pool is shared by
 * all concurrent reservations instead of being added to each of them.
 */
public class PageEvictionReservedPagesSelfTest extends PageEvictionAbstractTest {
    /** Empty pages pool size: close to the allowed maximum of 10% of the region. */
    private static final int LARGE_EMPTY_PAGES_POOL_SIZE = 900;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = setEvictionMode(DataPageEvictionMode.RANDOM_LRU, super.getConfiguration(gridName));

        cfg.getDataStorageConfiguration().getDefaultDataRegionConfiguration().setEmptyPagesPoolSize(LARGE_EMPTY_PAGES_POOL_SIZE);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** */
    @Test
    public void testReservedPagesReleasedAfterBatchInserts() throws Exception {
        IgniteEx ignite = startGrid(0);

        createCache(ignite, DEFAULT_CACHE_NAME).put(0, 0);

        FreeList<CacheDataRow> freeList = freeList(ignite);

        // Phase 1: small rows that fit into a single page (fill-up path, no whole-page fragments).
        insertBatchesAndCheckReservedPages(ignite, freeList, 10);

        // Phase 2: large rows that span multiple pages (whole-page fragments + fill-up remainder).
        insertBatchesAndCheckReservedPages(ignite, freeList, 10_000);

        // Phase 3: rows sized to an exact multiple of the whole-page fragment size (whole-page fragments only).
        int minPageSize = getFieldValue(freeList, "MIN_SIZE_FOR_DATA_PAGE");

        int fixedSize = row(cacheContext(ignite), 0, 0).size();

        insertBatchesAndCheckReservedPages(ignite, freeList, 2 * minPageSize - fixedSize);
    }

    /** */
    @Test
    public void testReservedPagesReleasedAfterConcurrentBatchInserts() throws Exception {
        IgniteEx ignite = startGrid(0);

        createCache(ignite, DEFAULT_CACHE_NAME).put(0, 0);

        FreeList<CacheDataRow> freeList = freeList(ignite);

        GridCacheContext<?, ?> cctx = cacheContext(ignite);

        int rowsPerThread = 200;

        AtomicInteger keyGen = new AtomicInteger();

        GridTestUtils.runMultiThreaded(() -> {
            try {
                Collection<CacheDataRow> rows = new ArrayList<>(rowsPerThread);

                for (int i = 0; i < rowsPerThread; i++)
                    rows.add(row(cctx, keyGen.incrementAndGet(), 100));

                freeList.insertDataRows(rows, IoStatisticsHolderNoOp.INSTANCE);
            }
            catch (IgniteCheckedException e) {
                throw new RuntimeException(e);
            }
        }, 8, "concurrent-batch-insert");

        assertAllReleased(10_000);
    }

    /** */
    @Test
    public void testReservedPagesReleasedAfterConcurrentPuts() throws Exception {
        IgniteCache<Integer, byte[]> cache = startGridsWithBackupCache();

        AtomicInteger keyGen = new AtomicInteger();

        GridTestUtils.runMultiThreaded(() -> {
            for (int i = 0; i < 300; i++)
                cache.put(keyGen.incrementAndGet(), new byte[500]);
        }, 8, "concurrent-put");

        assertAllReleased(10_000);
    }

    /** */
    @Test
    public void testReservedPagesReleasedAfterConcurrentPutAll() throws Exception {
        IgniteCache<Integer, byte[]> cache = startGridsWithBackupCache();

        AtomicInteger keyGen = new AtomicInteger();

        GridTestUtils.runMultiThreaded(() -> {
            for (int b = 0; b < 10; b++) {
                Map<Integer, byte[]> batch = new HashMap<>();

                for (int i = 0; i < 100; i++)
                    batch.put(keyGen.incrementAndGet(), new byte[300]);

                cache.putAll(batch);
            }
        }, 4, "concurrent-putAll");

        assertAllReleased(10_000);
    }

    /** Read-through loads reserve pages before the entry lock and must release them, also on the backup. */
    @Test
    public void testReservedPagesReleasedAfterReadThrough() throws Exception {
        startGrids(2);

        IgniteCache<Integer, byte[]> cache = grid(0).getOrCreateCache(new CacheConfiguration<Integer, byte[]>(DEFAULT_CACHE_NAME)
            .setAffinity(new RendezvousAffinityFunction(false, 32))
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
            .setBackups(1)
            .setReadThrough(true)
            .setCacheStoreFactory(new TestStoreFactory()));

        awaitPartitionMapExchange();

        for (int i = 1; i <= 15_000; i++)
            cache.get(i);

        assertTrue("Eviction must have started during read-through loads", isEvictionsStarted(grid(0)));

        assertEquals(PAGE_SIZE - 200, cache.get(1).length);

        assertAllReleased(10_000);
    }

    /**
     * Holds reservations of single-page rows of many writers at once in an empty region: the sum of per-writer pools
     * would exceed the region (and there is nothing to evict), but a single shared pool leaves room for all of them.
     */
    @Test
    public void testSimultaneousReservationsShareThePool() throws Exception {
        IgniteEx ignite = startGrid(0);

        createCache(ignite, DEFAULT_CACHE_NAME).put(0, 0);

        int writers = 16;

        assertTrue(writers * LARGE_EMPTY_PAGES_POOL_SIZE > totalPages(ignite));

        FreeList<CacheDataRow> freeList = freeList(ignite);

        long[] reserved = new long[writers];

        try {
            for (int i = 0; i < writers; i++) {
                reserved[i] = freeList.reserve(pageSize(ignite) / 2, false);

                assertEquals(1, reserved[i]);
            }
        }
        finally {
            for (long pages : reserved)
                freeList.releaseReserved(pages);
        }

        assertEquals(0, reservedPages(freeList));
    }

    /** @return Cache with a backup on two started nodes. */
    private IgniteCache<Integer, byte[]> startGridsWithBackupCache() throws Exception {
        startGrids(2);

        IgniteCache<Integer, byte[]> cache = grid(0).createCache(new CacheConfiguration<Integer, byte[]>(DEFAULT_CACHE_NAME)
            .setAffinity(new RendezvousAffinityFunction(false, 32))
            .setBackups(1)
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC));

        awaitPartitionMapExchange();

        return cache;
    }

    /**
     * Asserts that {@code reservedPages} is {@code 0} on every data store of every started node, waiting up to
     * {@code timeoutMs} for the reservation to drain.
     *
     * @param timeoutMs Wait timeout in milliseconds.
     * @throws Exception If the reservation does not drain in time.
     */
    private static void assertAllReleased(long timeoutMs) throws Exception {
        for (Ignite ignite : Ignition.allGrids()) {
            for (CacheDataStore store : cacheContext((IgniteEx)ignite).offheap().cacheDataStores()) {
                FreeList<CacheDataRow> freeList = (FreeList<CacheDataRow>)store.rowStore().freeList();

                assertTrue(GridTestUtils.waitForCondition(() -> reservedPages(freeList) == 0, timeoutMs));
            }
        }
    }

    /** */
    private static long reservedPages(FreeList<CacheDataRow> freeList) {
        return ((AtomicLong)getFieldValue(freeList, "reservedPages")).get();
    }

    /** */
    private static FreeList<CacheDataRow> freeList(IgniteEx ignite) {
        return (FreeList<CacheDataRow>)cacheContext(ignite).offheap().cacheDataStores().iterator().next().rowStore().freeList();
    }

    /** */
    private static void insertBatchesAndCheckReservedPages(
        IgniteEx ignite,
        FreeList<CacheDataRow> freeList,
        int payloadSize
    ) throws IgniteCheckedException {
        int batchSize = 200;

        for (int batch = 0; batch < 3; batch++) {
            GridCacheContext<?, ?> cctx = cacheContext(ignite);

            Collection<CacheDataRow> rows = new ArrayList<>(batchSize);

            for (int i = 0; i < batchSize; i++)
                rows.add(row(cctx, batch * batchSize + i, payloadSize));

            freeList.insertDataRows(rows, IoStatisticsHolderNoOp.INSTANCE);

            assertEquals(0, reservedPages(freeList));
        }
    }

    /** @return Context of the default cache. */
    private static GridCacheContext<?, ?> cacheContext(IgniteEx ignite) {
        return ignite.context().cache().cache(DEFAULT_CACHE_NAME).context();
    }

    /** @return Data row with the given key and a byte array value of the given size. */
    private static CacheDataRow row(GridCacheContext<?, ?> cctx, int key, int payloadSize) throws IgniteCheckedException {
        KeyCacheObject keyObj = cctx.toCacheKeyObject(key);
        CacheObject valObj = cctx.toCacheObject(new byte[payloadSize]);

        keyObj.valueBytes(cctx.cacheObjectContext());
        valObj.valueBytes(cctx.cacheObjectContext());

        return new DataRow(keyObj, valObj, new GridCacheVersion(), 0, 0L, cctx.cacheId(), false);
    }
}

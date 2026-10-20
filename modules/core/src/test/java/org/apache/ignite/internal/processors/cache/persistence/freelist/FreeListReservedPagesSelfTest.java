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

package org.apache.ignite.internal.processors.cache.persistence.freelist;

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
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.metric.IoStatisticsHolderNoOp;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.IgniteCacheOffheapManager.CacheDataStore;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.tree.DataRow;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.getFieldValue;

/**
 * Tests that the atomic page reservation of an in-memory free list with enabled page eviction is fully released after
 * batch inserts: whole-page fragments, fill-up-handler rows, whole-page-aligned rows, concurrent batch inserts, and
 * concurrent single-row puts on a backup node.
 */
public class FreeListReservedPagesSelfTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setInitialSize(10 * 1024 * 1024L)
                .setMaxSize(20 * 1024 * 1024L)
                .setPageEvictionMode(DataPageEvictionMode.RANDOM_LRU)));

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

        createCache(ignite);

        FreeList<CacheDataRow> freeList = freeList(ignite, DEFAULT_CACHE_NAME);

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

        createCache(ignite);

        final FreeList<CacheDataRow> freeList = freeList(ignite, DEFAULT_CACHE_NAME);

        final GridCacheContext<?, ?> cctx = cacheContext(ignite);

        int rowsPerThread = 200;

        final AtomicInteger keyGen = new AtomicInteger();

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

        assertTrue(GridTestUtils.waitForCondition(() -> reservedPages(freeList) == 0, 10_000));
    }

    /** */
    @Test
    public void testReservedPagesReleasedAfterConcurrentPuts() throws Exception {
        startGrids(2);

        IgniteCache<Integer, byte[]> cache = grid(0).createCache(new CacheConfiguration<Integer, byte[]>(DEFAULT_CACHE_NAME)
            .setAffinity(new RendezvousAffinityFunction(false, 32))
            .setBackups(1)
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC));

        awaitPartitionMapExchange();

        AtomicInteger keyGen = new AtomicInteger();

        GridTestUtils.runMultiThreaded(() -> {
            for (int i = 0; i < 300; i++) {
                int key = keyGen.incrementAndGet();

                cache.put(key, new byte[500]);
            }
        }, 8, "concurrent-put");

        assertAllReleased(DEFAULT_CACHE_NAME, 10_000);
    }

    /** */
    @Test
    public void testReservedPagesReleasedAfterConcurrentPutAll() throws Exception {
        startGrids(2);

        IgniteCache<Integer, byte[]> cache = grid(0).createCache(new CacheConfiguration<Integer, byte[]>(DEFAULT_CACHE_NAME)
            .setAffinity(new RendezvousAffinityFunction(false, 32))
            .setBackups(1)
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC));

        awaitPartitionMapExchange();

        AtomicInteger keyGen = new AtomicInteger();

        GridTestUtils.runMultiThreaded(() -> {
            for (int b = 0; b < 10; b++) {
                Map<Integer, byte[]> batch = new HashMap<>();

                for (int i = 0; i < 100; i++) {
                    int key = keyGen.incrementAndGet();

                    batch.put(key, new byte[300]);
                }

                cache.putAll(batch);
            }
        }, 4, "concurrent-putAll");

        assertAllReleased(DEFAULT_CACHE_NAME, 10_000);
    }

    /**
     * Asserts that {@code reservedPages} is {@code 0} on every data store of every started node, waiting up to
     * {@code timeoutMs} for the reservation to drain.
     *
     * @param cacheName Cache name.
     * @param timeoutMs Wait timeout in milliseconds.
     * @throws Exception If the reservation does not drain in time.
     */
    public static void assertAllReleased(String cacheName, long timeoutMs) throws Exception {
        for (Ignite ignite : Ignition.allGrids()) {
            for (CacheDataStore store : ((IgniteEx)ignite).context().cache().cache(cacheName).context().offheap().cacheDataStores()) {
                FreeList<CacheDataRow> freeList = (FreeList<CacheDataRow>)store.rowStore().freeList();

                if (reservedPages(freeList) == 0)
                    continue;

                assertTrue(GridTestUtils.waitForCondition(() -> reservedPages(freeList) == 0, timeoutMs));
            }
        }
    }

    /** */
    private static long reservedPages(FreeList<CacheDataRow> freeList) {
        return ((AtomicLong)getFieldValue(freeList, "reservedPages")).get();
    }

    /** */
    private static FreeList<CacheDataRow> freeList(IgniteEx ignite, String cacheName) {
        return (FreeList<CacheDataRow>)ignite.context().cache().cache(cacheName).context().offheap()
            .cacheDataStores().iterator().next().rowStore().freeList();
    }

    /** */
    private static void insertBatchesAndCheckReservedPages(
        IgniteEx ignite,
        FreeList<CacheDataRow> freeList,
        int payloadSize
    ) throws IgniteCheckedException {
        int batchSize = 500;

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

    /** */
    private static void createCache(IgniteEx ignite) {
        IgniteCache<Integer, Object> cache = ignite.createCache(new CacheConfiguration<Integer, Object>(DEFAULT_CACHE_NAME)
            .setAffinity(new RendezvousAffinityFunction(false, 32)).setBackups(0));

        cache.put(0, 0);
    }

    /** @return Data row with the given key and a byte array value of the given size. */
    private static CacheDataRow row(GridCacheContext<?, ?> cctx, int key, int payloadSize) throws IgniteCheckedException {
        KeyCacheObject keyObj = cctx.toCacheKeyObject(key);
        CacheObject valObj = cctx.toCacheObject(new byte[payloadSize]);

        keyObj.valueBytes(cctx.cacheObjectContext());
        valObj.valueBytes(cctx.cacheObjectContext());

        return new DataRow(keyObj, valObj, new GridCacheVersion(), 0, 0L, cctx.cacheId());
    }
}

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
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
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
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.tree.DataRow;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.getFieldValue;

/**
 * Tests that the atomic page reservation of an in-memory free list with enabled page eviction is fully released
 * after batch inserts: covers the whole-page fragment path, rows consumed by the page fill-up handler, rows left
 * unfinished by the fill-up handler and rows sized as an exact multiple of the whole-page fragment size.
 */
public class FreeListReservedPagesSelfTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        DataStorageConfiguration dsCfg = new DataStorageConfiguration();

        dsCfg.setDefaultDataRegionConfiguration(new DataRegionConfiguration()
            .setInitialSize(20 * 1024 * 1024L / 2)
            .setMaxSize(20 * 1024 * 1024L)
            .setPageEvictionMode(DataPageEvictionMode.RANDOM_LRU));

        cfg.setDataStorageConfiguration(dsCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /** */
    @Test
    public void testReservedPagesReleasedAfterSmallRowsBatch() throws Exception {
        assertReservedPagesReleasedAfterBatch(10);
    }

    /** */
    @Test
    public void testReservedPagesReleasedAfterLargeRowsBatch() throws Exception {
        assertReservedPagesReleasedAfterBatch(10_000);
    }

    /** */
    @Test
    public void testReservedPagesReleasedAfterWholePageAlignedRowsBatch() throws Exception {
        IgniteEx ignite = startGrid(0);

        createCache(ignite);

        FreeList<CacheDataRow> freeList = freeList(ignite);

        int minPageSize = getFieldValue(freeList, "MIN_SIZE_FOR_DATA_PAGE");

        // Probe the size fixed part (key bytes, version and the link slot) to size the value so that the whole row
        // is an exact multiple of the whole-page fragment size.
        int fixedSize = row(cacheContext(ignite), 0, 0).size();

        insertBatches(ignite, freeList, 2 * minPageSize - fixedSize);

        assertEquals(0, reservedPages(freeList));
    }

    /** */
    private void assertReservedPagesReleasedAfterBatch(int payloadSize) throws Exception {
        IgniteEx ignite = startGrid(0);

        createCache(ignite);

        insertBatches(ignite, freeList(ignite), payloadSize);
    }

    /** */
    private static void insertBatches(IgniteEx ignite, FreeList<CacheDataRow> freeList, int payloadSize) throws IgniteCheckedException {
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

    /**
     * @param freeList Free list.
     * @return Pages currently reserved by in-flight inserts.
     */
    private static long reservedPages(FreeList<CacheDataRow> freeList) {
        return ((AtomicLong)getFieldValue(freeList, "reservedPages")).get();
    }

    /**
     * @param ignite Ignite.
     * @return Free list of the first partition data store.
     */
    private static FreeList<CacheDataRow> freeList(IgniteEx ignite) {
        return (FreeList<CacheDataRow>)cacheContext(ignite).offheap().cacheDataStores().iterator().next().rowStore().freeList();
    }

    /**
     * @param ignite Ignite.
     * @return Context of the default cache.
     */
    private static GridCacheContext<?, ?> cacheContext(IgniteEx ignite) {
        return ignite.context().cache().cache(DEFAULT_CACHE_NAME).context();
    }

    /**
     * @param ignite Ignite.
     */
    private static void createCache(IgniteEx ignite) {
        IgniteCache<Integer, Object> cache = ignite.createCache(new CacheConfiguration<Integer, Object>(DEFAULT_CACHE_NAME)
            .setAffinity(new RendezvousAffinityFunction(false, 32)).setBackups(0));

        cache.put(0, 0);
    }

    /**
     * @param cctx Cache context.
     * @param key Key.
     * @param payloadSize Value size.
     * @return Data row with the given key and a byte array value of the given size.
     */
    private static CacheDataRow row(GridCacheContext<?, ?> cctx, int key, int payloadSize) throws IgniteCheckedException {
        KeyCacheObject keyObj = cctx.toCacheKeyObject(key);
        CacheObject valObj = cctx.toCacheObject(new byte[payloadSize]);

        keyObj.valueBytes(cctx.cacheObjectContext());
        valObj.valueBytes(cctx.cacheObjectContext());

        return new DataRow(keyObj, valObj, new GridCacheVersion(), 0, 0L, cctx.cacheId());
    }
}

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

package org.apache.ignite.internal.processors.database;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.internal.mem.unsafe.UnsafeMemoryProvider;
import org.apache.ignite.internal.pagemem.PageIdAllocator;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.pagemem.impl.PageMemoryNoStoreImpl;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.processors.cache.persistence.DataRegion;
import org.apache.ignite.internal.processors.cache.persistence.DataRegionMetricsImpl;
import org.apache.ignite.internal.processors.cache.persistence.Storable;
import org.apache.ignite.internal.processors.cache.persistence.evict.NoOpPageEvictionTracker;
import org.apache.ignite.internal.processors.cache.persistence.freelist.DefaultFreeList;
import org.apache.ignite.internal.processors.cache.persistence.freelist.PagesList;
import org.apache.ignite.internal.processors.cache.persistence.freelist.SecondaryCacheDataRow;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.AbstractDataPageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.reuse.ReuseList;
import org.apache.ignite.internal.stat.IoStatisticsHolderNoOp;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 */
public class FreeListSpaceReuseSelfTest extends GridCommonAbstractTest {
    /** */
    private static final long MB = 1024L * 1024L;

    /** */
    private PageMemory pageMem;

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        if (pageMem != null)
            pageMem.stop(true);

        pageMem = null;
    }

    /** */
    @Test
    public void testCacheDataRow_1() throws Exception {
        int pageSize = 1024;

        testInsertRemoveAll(pageSize, size -> new CacheFreeListImplSelfTest.TestDataRow(size / 3, size * 2 / 3),
            0,
            100, 100, 100, 100);
    }

    /** */
    @Test
    public void testCacheDataRow_2() throws Exception {
        int pageSize = 1024;

        testInsertRemoveAll(pageSize, size -> new CacheFreeListImplSelfTest.TestDataRow(size / 3, size * 2 / 3),
            1,
            pageSize * 3 / 2);
    }

    /** */
    @Test
    public void testCacheDataRow_3() throws Exception {
        int pageSize = 1024;

        testInsertRemoveAll(pageSize, size -> new CacheFreeListImplSelfTest.TestDataRow(size / 3, size * 2 / 3),
            1,
            pageSize * 3 / 2, 100);
    }

    /** */
    @Test
    public void testSecondaryCacheDataRow_1() throws Exception {
        int pageSize = 1024;

        testInsertRemoveAll(pageSize, size -> new SecondaryCacheDataRow(0, new byte[size]),
            3,
            100, 100, 100, 100);
    }

    /** */
    @Test
    public void testSecondaryCacheDataRow_2() throws Exception {
        int pageSize = 1024;

        testInsertRemoveAll(pageSize, size -> new SecondaryCacheDataRow(0, new byte[size]),
            1,
            pageSize * 3 / 2);
    }

    /** */
    @Test
    public void testSecondaryCacheDataRow_3() throws Exception {
        int pageSize = 1024;

        testInsertRemoveAll(pageSize, size -> new SecondaryCacheDataRow(0, new byte[size]),
            2,
            pageSize * 3 / 2, 100);
    }

    /**
     * @param sizes Sizes.
     */
    private void testInsertRemoveAll(int pageSize, IgniteClosure<Integer, Storable> producer,
        int expEmptyCnt, int... sizes) throws Exception {
        int usableSize = pageSize - AbstractDataPageIO.MIN_DATA_PAGE_OVERHEAD;

        TestFreeList list = createFreeList(pageSize);

        List<Long> links = new ArrayList<>(sizes.length);

        for (int i = 0; i < sizes.length; i++) {
            int size = sizes[i];

            Storable row = producer.apply(size);

            list.insertDataRow(row, IoStatisticsHolderNoOp.INSTANCE);
            assertTrue(row.link() != 0);

            links.add(row.link());
        }

        for (Long link : links)
            list.removeDataRowByLink(link, IoStatisticsHolderNoOp.INSTANCE);

        // All buckets must be empty.
        list.assertEmpty(DefaultFreeList.REUSE_BUCKET);

        // Reuse bucket must not be empty.
        assertTrue(expEmptyCnt == 0 || !list.isEmpty(DefaultFreeList.REUSE_BUCKET));

        assertEquals("Free pages count is not as expected", expEmptyCnt, list.bucketCount(DefaultFreeList.REUSE_BUCKET));

        /** NOTE: first free page will be used for reused pages list. */
    }

    /**
     * @return Page memory.
     */
    protected PageMemory createPageMemory(int pageSize, DataRegionConfiguration plcCfg) throws Exception {
        PageMemory pageMem = new PageMemoryNoStoreImpl(log,
            new UnsafeMemoryProvider(log),
            null,
            pageSize,
            plcCfg,
            new DataRegionMetricsImpl(plcCfg),
            true);

        pageMem.start();

        return pageMem;
    }

    /**
     * @param pageSize Page size.
     * @return Free list.
     * @throws Exception If failed.
     */
    protected TestFreeList createFreeList(int pageSize) throws Exception {
        DataRegionConfiguration plcCfg = new DataRegionConfiguration()
            .setInitialSize(1024 * MB)
            .setMaxSize(1024 * MB);

        pageMem = createPageMemory(pageSize, plcCfg);

        long metaPageId = pageMem.allocatePage(1, 1, PageIdAllocator.FLAG_DATA);

        DataRegionMetricsImpl regionMetrics = new DataRegionMetricsImpl(plcCfg);

        DataRegion dataRegion = new DataRegion(pageMem, plcCfg, regionMetrics, new NoOpPageEvictionTracker());

        return new TestFreeList(1, "freelist", regionMetrics, dataRegion, null, null, metaPageId, true);
    }

    /** */
    static class TestFreeList extends DefaultFreeList {
        /**
         * @param cacheId Cache id.
         * @param name Name.
         * @param memMetrics Mem metrics.
         * @param memPlc Mem policy.
         * @param reuseList Reuse list.
         * @param wal Wal.
         * @param metaPageId Meta page id.
         * @param initNew Initialize new.
         */
        public TestFreeList(int cacheId, String name,
            DataRegionMetricsImpl memMetrics, DataRegion memPlc,
            ReuseList reuseList,
            IgniteWriteAheadLogManager wal, long metaPageId,
            boolean initNew) throws IgniteCheckedException {
            super(cacheId, name, memMetrics, memPlc, reuseList, wal, metaPageId, initNew);
        }

        /**
         * @param bucket Bucket.
         */
        public Stripe[] bucket(int bucket) {
            return super.getBucket(bucket);
        }

        /**
         * @param bucket Bucket.
         */
        public boolean isEmpty(int bucket) {
            PagesList.Stripe[] stripes = bucket(bucket);

            if (stripes != null) {
                for (PagesList.Stripe stripe : stripes) {
                    if (!stripe.empty)
                        return false;
                }
            }

            return true;
        }

        /**
         * @param bucket Bucket.
         */
        public long bucketCount(int bucket) {
            return bucketsSize[bucket].get();
        }

        /**
         * @param exclude Exclude.
         */
        public void assertEmpty(int... exclude) {
            if (exclude != null)
                Arrays.sort(exclude);

            for (int b = 0; b < DefaultFreeList.BUCKETS - 1; b++) {
                if (exclude != null && Arrays.binarySearch(exclude, b) >= 0)
                    continue;

                assertTrue("Bucket not empty " + b, isEmpty(b));
                assertEquals("Bucket count is not zero", 0, bucketCount(b));
            }
        }
    }
}

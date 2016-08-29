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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.mem.unsafe.UnsafeMemoryProvider;
import org.apache.ignite.internal.pagemem.Page;
import org.apache.ignite.internal.pagemem.PageIdAllocator;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.pagemem.impl.PageMemoryNoStoreImpl;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.processors.cache.database.CacheDataRow;
import org.apache.ignite.internal.processors.cache.database.CacheDataRowAdapter;
import org.apache.ignite.internal.processors.cache.database.DataStructure;
import org.apache.ignite.internal.processors.cache.database.freelist.FreeList;
import org.apache.ignite.internal.processors.cache.database.freelist.PagesList;
import org.apache.ignite.internal.processors.cache.database.tree.io.DataPageIO;
import org.apache.ignite.internal.processors.cache.database.tree.reuse.ReuseBag;
import org.apache.ignite.internal.processors.cache.database.tree.reuse.ReuseList;
import org.apache.ignite.internal.processors.cache.database.tree.reuse.ReuseListNew;
import org.apache.ignite.internal.processors.cache.database.tree.util.PageHandler;
import org.apache.ignite.internal.util.GridLongList;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import static org.apache.ignite.internal.processors.cache.database.tree.util.PageHandler.initPage;
import static org.apache.ignite.internal.processors.cache.database.tree.util.PageHandler.writePage;

/**
 *
 */
public class PagesListTest extends GridCommonAbstractTest {
    /** */
    protected static final long MB = 1024 * 1024;

    /** */
    protected static final int CPUS = Runtime.getRuntime().availableProcessors();

    /** */
    private int pageSize = 128;

    /** */
    private PageMemory pageMem;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        DataStructure.rnd = new Random(0);

        super.beforeTest();

        pageMem = createPageMemory(pageSize);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        pageMem.stop();

        super.afterTest();
    }

    /**
     * @throws Exception If failed.
     */
    public void testReuseList() throws Exception {
        testReuseList(false);

        testReuseList(true);
    }

    /**
     * @throws Exception If failed.
     */
    private void testReuseList(boolean singleBag) throws Exception {
        ReuseListNew list = new ReuseListNew(0, pageMem, null);

        final int PAGES = 1000;

        Set<Long> addPages = new HashSet<>();

        if (singleBag) {
            TestBag bag = new TestBag();

            for (int i = 0; i < PAGES; i++) {
                long pageId = pageMem.allocatePage(0, i, PageIdAllocator.FLAG_IDX);

                bag.add(pageId);

                assertTrue(addPages.add(pageId));
            }

            list.addForRecycle(bag);
        }
        else {
            for (int i = 0; i < PAGES; i++) {
                long pageId = pageMem.allocatePage(0, i, PageIdAllocator.FLAG_IDX);

                TestBag bag = new TestBag();

                bag.add(pageId);

                assertTrue(addPages.add(pageId));

                list.addForRecycle(bag);
            }
        }

        Set<Long> takePages = new HashSet<>();

        long pageId;

        while ((pageId = list.takeRecycledPage(null, null)) != 0L) {
            assertTrue(takePages.add(pageId));
        }

        assertEquals(addPages.size(), takePages.size());

        for (long pageId0 : takePages) {
            TestBag bag = new TestBag();

            bag.add(pageId0);

            list.addForRecycle(bag);
        }

        takePages.clear();

        while ((pageId = list.takeRecycledPage(null, null)) != 0L) {
            assertTrue(takePages.add(pageId));
        }

        assertEquals(addPages.size(), takePages.size());
    }

    /**
     *
     */
    static class TestBag extends GridLongList implements ReuseBag {
        /** {@inheritDoc} */
        @Override public void addFreePage(long pageId) {
            add(pageId);
        }

        /** {@inheritDoc} */
        @Override public long pollFreePage() {
            return isEmpty() ? 0 : remove();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testFreeList() throws Exception {
        TestFreeList list = new TestFreeList(0, pageMem, null);

        List<Long> pages = new ArrayList<>();

        for (int i = 0; i < 10; i++) {
            long pageId = list.allocateAndAdd();

            pages.add(pageId);
        }

        for (int i = pages.size() - 1; i >= 0; i--)
            list.remove(pages.get(i));

//        for (Long pageId : pages)
//            list.remove(pageId);

//        List<Long> links = new ArrayList<>();
//
//        for (int i = 0; i < 3; i++) {
//            CacheDataRowAdapter row = new CacheDataRowAdapter(0);
//
//            list.insertDataRow(row);
//
//            assert row.link() != 0;
//
//            links.add(row.link());
//        }
//
//        System.out.println();
    }

    /**
     *
     */
    static class TestFreeList extends PagesList implements FreeList {
        /** */
        private static final AtomicReferenceFieldUpdater<TestFreeList, long[]> bucketUpdater =
                AtomicReferenceFieldUpdater.newUpdater(TestFreeList.class, long[].class, "bucket");

        /** */
        private volatile long[] bucket;

        public TestFreeList(int cacheId, PageMemory pageMem, IgniteWriteAheadLogManager wal) {
            super(cacheId, pageMem, wal);

            this.reuseList = new ReuseList() {
                @Override
                public void addForRecycle(ReuseBag bag) throws IgniteCheckedException {

                }

                @Override
                public long takeRecycledPage(DataStructure client, ReuseBag bag) throws IgniteCheckedException {
                    return 0;
                }

                @Override
                public long recycledPagesCount() throws IgniteCheckedException {
                    return 0;
                }
            };
        }

        /** {@inheritDoc} */
        @Override public void insertDataRow(CacheDataRow row) throws IgniteCheckedException {
            int written = 0;

            do {
                int bucket = 0;

                long pageId = takePage(bucket);

                boolean newPage = pageId == 0;

                if (newPage)
                    System.out.println("Allocate new page");
                else
                    System.out.println("Found page " + pageId + ", bucket=" + bucket);

                try (Page page = newPage ? allocateDataPage(0) : pageMem.page(cacheId, pageId)) {
                    // If it is an existing page, we do not need to initialize it.
                    DataPageIO init = newPage ? DataPageIO.VERSIONS.latest() : null;

                    written = writePage(page.id(), page, writeRow, init, wal, row, written);
                }
            }
            while (written == 0);
        }

        /**
         * @param part Partition.
         * @return Page.
         * @throws IgniteCheckedException If failed.
         */
        private Page allocateDataPage(int part) throws IgniteCheckedException {
            long pageId = pageMem.allocatePage(0, part, PageIdAllocator.FLAG_DATA);

            return pageMem.page(0, pageId);
        }

        public long allocateAndAdd() throws IgniteCheckedException {
            try (Page page = allocateDataPage(0)) {
                initPage(page.id(), page, DataPageIO.VERSIONS.latest(), null);

                ByteBuffer buf = page.getForWrite();

                put(null, buf, 0);

                page.releaseWrite(true);

                return page.id();
            }
        }

        public void remove(long pageId) throws IgniteCheckedException {
            try (Page page = pageMem.page(0, pageId)) {
                ByteBuffer buf = page.getForWrite();

                removeDataPage(buf, 0);

                page.releaseWrite(true);
            }
        }

        /** {@inheritDoc} */
        @Override public void removeDataRowByLink(long link) throws IgniteCheckedException {
            assert link != 0;

            //removeDataPage();
        }

        /** {@inheritDoc} */
        @Override protected long[] getBucket(int bucket) {
            return this.bucket;
        }

        /** {@inheritDoc} */
        @Override protected boolean casBucket(int bucket, long[] exp, long[] upd) {
            return bucketUpdater.compareAndSet(this, exp, upd);
        }

        /** {@inheritDoc} */
        @Override protected boolean isReuseBucket(int bucket) {
            return false;
        }

        /** */
        private final PageHandler<CacheDataRow, DataPageIO, Integer> writeRow =
                new PageHandler<CacheDataRow, DataPageIO, Integer>() {
                    @Override public Integer run(long pageId, Page page, DataPageIO io, ByteBuffer buf, CacheDataRow row, int written)
                            throws IgniteCheckedException {
                        int rowSize = 10;

                        int oldFreeSpace = buf.getInt(DataPageIO.COMMON_HEADER_END + 64);

                        if (oldFreeSpace == 0)
                            oldFreeSpace = 21;

                        if (oldFreeSpace > rowSize) {
                            buf.putInt(DataPageIO.COMMON_HEADER_END + 64, oldFreeSpace - rowSize);

                            int bucket = 0;

                            put(null, buf, bucket);

                            // row.link(pageId);

                            return 1;
                        }

                        return 0;
                    }
                };

        /** */
        private final PageHandler<Void, DataPageIO, Long> rmvRow = new PageHandler<Void, DataPageIO, Long>() {
            @Override public Long run(long pageId, Page page, DataPageIO io, ByteBuffer buf, Void arg, int itemId)
                    throws IgniteCheckedException {
                int oldFreeSpace = buf.getInt(DataPageIO.COMMON_HEADER_END);

                assert oldFreeSpace >= 0 : oldFreeSpace;

                int newFreeSpace = oldFreeSpace + 10;

                buf.putInt(DataPageIO.COMMON_HEADER_END, newFreeSpace);

                if (newFreeSpace > 0) {
                    put(null, buf, 0);
                }

                return 0L;
            }
        };
    }

    /**
     * @return Page memory.
     */
    private PageMemory createPageMemory(int pageSize) throws Exception {
        long[] sizes = new long[CPUS];

        for (int i = 0; i < sizes.length; i++)
            sizes[i] = 64 * MB / CPUS;

        PageMemory pageMem = new PageMemoryNoStoreImpl(log, new UnsafeMemoryProvider(sizes), null, pageSize);

        pageMem.start();

        return pageMem;
    }
}

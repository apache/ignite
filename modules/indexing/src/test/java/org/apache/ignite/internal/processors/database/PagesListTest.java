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

import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import org.apache.ignite.internal.mem.unsafe.UnsafeMemoryProvider;
import org.apache.ignite.internal.pagemem.PageIdAllocator;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.pagemem.impl.PageMemoryNoStoreImpl;
import org.apache.ignite.internal.processors.cache.database.DataStructure;
import org.apache.ignite.internal.processors.cache.database.tree.reuse.ReuseBag;
import org.apache.ignite.internal.processors.cache.database.tree.reuse.ReuseListNew;
import org.apache.ignite.internal.util.GridLongList;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

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
        ReuseListNew list = new ReuseListNew(0, pageMem, null);

        TestBag bag = new TestBag();

        final int PAGES = 100;

        Set<Long> pages = new HashSet<>();

        for (int i = 0; i < PAGES; i++) {
            long pageId = pageMem.allocatePage(0, i, PageIdAllocator.FLAG_IDX);

            bag.add(pageId);

            System.out.println("Add " + pageId);

            assertTrue(pages.add(pageId));
        }

        list.addForRecycle(bag);

        for (int i = 0; i < PAGES; i++) {
            long pageId = list.takeRecycledPage(null, null);

            assert pageId != 0;

            System.out.println("Remove " + pageId);

            assertTrue("Invalid pageId: " + pageId, pages.remove(pageId));
        }

        assertTrue(pages.isEmpty());
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

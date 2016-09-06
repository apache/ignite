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

package org.apache.ignite.internal.pagemem.impl;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.mem.DirectMemoryProvider;
import org.apache.ignite.internal.mem.file.MappedFileMemoryProvider;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.Page;
import org.apache.ignite.internal.pagemem.PageIdAllocator;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.processors.cache.database.tree.io.PageIO;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 *
 */
public class PageMemoryNoLoadSelfTest extends GridCommonAbstractTest {
    /** */
    protected static final int PAGE_SIZE = 8 * 1024;

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        deleteRecursively(U.resolveWorkDirectory("pagemem", false));
    }

    /**
     * @throws Exception If failed.
     */
    public void testPageTearingInner() throws Exception {
        PageMemory mem = memory();

        mem.start();

        try {
            FullPageId fullId1 = allocatePage(mem);
            FullPageId fullId2 = allocatePage(mem);

            Page page1 = mem.page(fullId1.cacheId(), fullId1.pageId());

            try {
                Page page2 = mem.page(fullId2.cacheId(), fullId2.pageId());

                info("Allocated pages [page1=" + page1 + ", page2=" + page2 + ']');

                try {
                    writePage(page1, 1);
                    writePage(page2, 2);

                    readPage(page1, 1);
                    readPage(page2, 2);

                    // Check read after read.
                    readPage(page1, 1);
                    readPage(page2, 2);
                }
                finally {
                    mem.releasePage(page2);
                }
            }
            finally {
                mem.releasePage(page1);
            }
        }
        finally {
            mem.stop();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testPageTearingSequential() throws Exception {
        PageMemory mem = memory();

        mem.start();

        try {
            int pagesCnt = 1024;

            List<FullPageId> pages = new ArrayList<>(pagesCnt);

            for (int i = 0; i < pagesCnt; i++) {
                FullPageId fullId = allocatePage(mem);

                pages.add(fullId);

                Page page = mem.page(fullId.cacheId(), fullId.pageId());

                try {
                    if (i % 64 == 0)
                        info("Writing page [idx=" + i + ", page=" + page + ']');

                    writePage(page, i + 1);
                }
                finally {
                    mem.releasePage(page);
                }
            }

            for (int i = 0; i < pagesCnt; i++) {
                FullPageId fullId = pages.get(i);

                Page page = mem.page(fullId.cacheId(), fullId.pageId());

                try {
                    if (i % 64 == 0)
                        info("Reading page [idx=" + i + ", page=" + page + ']');

                    readPage(page, i + 1);
                }
                finally {
                    mem.releasePage(page);
                }
            }
        }
        finally {
            mem.stop();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testPageHandleDeallocation() throws Exception {
        PageMemory mem = memory();

        mem.start();

        try {
            int pages = 3 * 1024 * 1024 / (8 * 1024);

            Collection<FullPageId> handles = new HashSet<>();

            for (int i = 0; i < pages; i++)
                handles.add(allocatePage(mem));

            for (FullPageId fullId : handles)
                mem.freePage(fullId.cacheId(), fullId.pageId());

            for (int i = 0; i < pages; i++)
                assertFalse(handles.add(allocatePage(mem)));
        }
        finally {
            mem.stop();
        }
    }

    /**
     * @return Page memory implementation.
     */
    protected PageMemory memory() throws Exception {
        File memDir = U.resolveWorkDirectory("pagemem", false);

        long[] sizes = new long[10];

        for (int i = 0; i < sizes.length; i++)
            sizes[i] = 1024 * 1024;

        DirectMemoryProvider provider = new MappedFileMemoryProvider(log(), memDir, true,
            sizes);

        return new PageMemoryNoStoreImpl(log(), provider, null, PAGE_SIZE);
    }

    /**
     * @param page Page to write.
     * @param val Value to write.
     */
    private void writePage(Page page, int val) {
        ByteBuffer bytes = page.getForWrite();

        try {
            for (int i = PageIO.COMMON_HEADER_END; i < PAGE_SIZE; i++)
                bytes.put(i, (byte)val);
        }
        finally {
            page.releaseWrite(true);
        }
    }

    /**
     * @param page Page to read.
     * @param expVal Expected value.
     */
    private void readPage(Page page, int expVal) {
        expVal &= 0xFF;

        ByteBuffer bytes = page.getForRead();

        try {
            for (int i = PageIO.COMMON_HEADER_END; i < PAGE_SIZE; i++) {
                int val = bytes.get(i) & 0xFF;

                assertEquals("Unexpected value at position: " + i, expVal, val);
            }
        }
        finally {
            page.releaseRead();
        }
    }

    /**
     * @param mem Memory.
     * @return Page.
     */
    public static FullPageId allocatePage(PageIdAllocator mem) throws IgniteCheckedException {
        return new FullPageId(mem.allocatePage(0, -1, PageIdAllocator.FLAG_DATA), 0);
    }
}

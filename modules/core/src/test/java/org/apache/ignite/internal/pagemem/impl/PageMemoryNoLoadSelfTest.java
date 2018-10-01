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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.internal.mem.DirectMemoryProvider;
import org.apache.ignite.internal.mem.IgniteOutOfMemoryException;
import org.apache.ignite.internal.mem.file.MappedFileMemoryProvider;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.PageIdAllocator;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.pagemem.PageUtils;
import org.apache.ignite.internal.processors.cache.persistence.DataRegionMetricsImpl;
import org.apache.ignite.internal.processors.cache.persistence.DummyPageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 *
 */
public class PageMemoryNoLoadSelfTest extends GridCommonAbstractTest {
    /** */
    protected static final int PAGE_SIZE = 8 * 1024;

    /** */
    private static final int MAX_MEMORY_SIZE = 10 * 1024 * 1024;

    /** */
    private static final PageIO PAGE_IO = new DummyPageIO();

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        U.delete(U.resolveWorkDirectory(U.defaultWorkDirectory(), "pagemem", false));
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

            long page1 = mem.acquirePage(fullId1.groupId(), fullId1.pageId());

            try {
                long page2 = mem.acquirePage(fullId2.groupId(), fullId2.pageId());

                info("Allocated pages [page1Id=" + fullId1.pageId() + ", page1=" + page1 +
                    ", page2Id=" + fullId2.pageId() + ", page2=" + page2 + ']');

                try {
                    writePage(mem, fullId1.pageId(), page1, 1);
                    writePage(mem, fullId2.pageId(), page2, 2);

                    readPage(mem, fullId1.pageId(), page1, 1);
                    readPage(mem, fullId2.pageId(), page2, 2);

                    // Check read after read.
                    readPage(mem, fullId1.pageId(), page1, 1);
                    readPage(mem, fullId2.pageId(), page2, 2);
                }
                finally {
                    mem.releasePage(fullId2.groupId(), fullId2.pageId(), page2);
                }
            }
            finally {
                mem.releasePage(fullId1.groupId(), fullId1.pageId(), page1);
            }
        }
        finally {
            mem.stop(true);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testLoadedPagesCount() throws Exception {
        PageMemory mem = memory();

        mem.start();

        int expPages = MAX_MEMORY_SIZE / mem.systemPageSize();

        try {
            for (int i = 0; i < expPages * 2; i++)
                allocatePage(mem);
        }
        catch (IgniteOutOfMemoryException e) {
            e.printStackTrace();
            // Expected.

            assertEquals(mem.loadedPages(), expPages);
        }
        finally {
            mem.stop(true);
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

                long page = mem.acquirePage(fullId.groupId(), fullId.pageId());

                try {
                    if (i % 64 == 0)
                        info("Writing page [idx=" + i + ", pageId=" + fullId.pageId() + ", page=" + page + ']');

                    writePage(mem, fullId.pageId(), page, i + 1);
                }
                finally {
                    mem.releasePage(fullId.groupId(), fullId.pageId(), page);
                }
            }

            for (int i = 0; i < pagesCnt; i++) {
                FullPageId fullId = pages.get(i);

                long page = mem.acquirePage(fullId.groupId(), fullId.pageId());

                try {
                    if (i % 64 == 0)
                        info("Reading page [idx=" + i + ", pageId=" + fullId.pageId() + ", page=" + page + ']');

                    readPage(mem, fullId.pageId(), page, i + 1);
                }
                finally {
                    mem.releasePage(fullId.groupId(), fullId.pageId(), page);
                }
            }
        }
        finally {
            mem.stop(true);
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
                mem.freePage(fullId.groupId(), fullId.pageId());

            for (int i = 0; i < pages; i++)
                assertFalse(handles.add(allocatePage(mem)));
        }
        finally {
            mem.stop(true);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testPageIdRotation() throws Exception {
        PageMemory mem = memory();

        mem.start();

        try {
            int pages = 5;

            Collection<FullPageId> old = new ArrayList<>();
            Collection<FullPageId> updated = new ArrayList<>();

            for (int i = 0; i < pages; i++)
                old.add(allocatePage(mem));

            // Check that initial pages are accessible.
            for (FullPageId id : old) {
                long pageApsPtr = mem.acquirePage(id.groupId(), id.pageId());
                try {
                    long pageAddr = mem.writeLock(id.groupId(), id.pageId(), pageApsPtr);

                    assertNotNull(pageAddr);

                    try {
                        PAGE_IO.initNewPage(pageAddr, id.pageId(), mem.pageSize());

                        long updId = PageIdUtils.rotatePageId(id.pageId());

                        PageIO.setPageId(pageAddr, updId);

                        updated.add(new FullPageId(updId, id.groupId()));
                    }
                    finally {
                        mem.writeUnlock(id.groupId(), id.pageId(), pageApsPtr, null, true);
                    }
                }
                finally {
                    mem.releasePage(id.groupId(), id.pageId(), pageApsPtr);
                }
            }

            // Check that updated pages are inaccessible using old IDs.
            for (FullPageId id : old) {
                long pageApsPtr = mem.acquirePage(id.groupId(), id.pageId());
                try {
                    long pageAddr = mem.writeLock(id.groupId(), id.pageId(), pageApsPtr);

                    if (pageAddr != 0L) {
                        mem.writeUnlock(id.groupId(), id.pageId(), pageApsPtr, null, false);

                        fail("Was able to acquire page write lock.");
                    }

                    mem.readLock(id.groupId(), id.pageId(), pageApsPtr);

                    if (pageAddr != 0) {
                        mem.readUnlock(id.groupId(), id.pageId(), pageApsPtr);

                        fail("Was able to acquire page read lock.");
                    }
                }
                finally {
                    mem.releasePage(id.groupId(), id.pageId(), pageApsPtr);
                }
            }

            // Check that updated pages are accessible using new IDs.
            for (FullPageId id : updated) {
                long pageApsPtr = mem.acquirePage(id.groupId(), id.pageId());
                try {
                    long pageAddr = mem.writeLock(id.groupId(), id.pageId(), pageApsPtr);

                    assertNotSame(0L, pageAddr);

                    try {
                        assertEquals(id.pageId(), PageIO.getPageId(pageAddr));
                    }
                    finally {
                        mem.writeUnlock(id.groupId(), id.pageId(), pageApsPtr, null, false);
                    }

                    pageAddr = mem.readLock(id.groupId(), id.pageId(), pageApsPtr);

                    assertNotSame(0L, pageAddr);

                    try {
                        assertEquals(id.pageId(), PageIO.getPageId(pageAddr));
                    }
                    finally {
                        mem.readUnlock(id.groupId(), id.pageId(), pageApsPtr);
                    }
                }
                finally {
                    mem.releasePage(id.groupId(), id.pageId(), pageApsPtr);
                }
            }
        }
        finally {
            mem.stop(true);
        }
    }

    /**
     * @return Page memory implementation.
     * @throws Exception If failed.
     */
    protected PageMemory memory() throws Exception {
        File memDir = U.resolveWorkDirectory(U.defaultWorkDirectory(), "pagemem", false);

        DataRegionConfiguration plcCfg = new DataRegionConfiguration()
            .setMaxSize(MAX_MEMORY_SIZE).setInitialSize(MAX_MEMORY_SIZE);

        DirectMemoryProvider provider = new MappedFileMemoryProvider(log(), memDir);

        return new PageMemoryNoStoreImpl(
            log(),
            provider,
            null,
            PAGE_SIZE,
            plcCfg,
            new DataRegionMetricsImpl(plcCfg),
            true);
    }

    /**
     * @param mem Page memory.
     * @param pageId Page ID.
     * @param page Page pointer.
     * @param val Value to write.
     */
    private void writePage(PageMemory mem, long pageId, long page, int val) {
        long pageAddr = mem.writeLock(-1, pageId, page);

        try {
            PAGE_IO.initNewPage(pageAddr, pageId, mem.pageSize());

            for (int i = PageIO.COMMON_HEADER_END; i < PAGE_SIZE; i++)
                PageUtils.putByte(pageAddr, i, (byte)val);
        }
        finally {
            mem.writeUnlock(-1, pageId, page, null, true);
        }
    }

    /**
     * @param mem Page memory.
     * @param pageId Page ID.
     * @param page Page pointer.
     * @param expVal Expected value.
     */
    private void readPage(PageMemory mem, long pageId, long page, int expVal) {
        expVal &= 0xFF;

        long pageAddr = mem.readLock(-1, pageId, page);

        assert pageAddr != 0;

        try {
            for (int i = PageIO.COMMON_HEADER_END; i < PAGE_SIZE; i++) {
                int val = PageUtils.getByte(pageAddr, i) & 0xFF;

                assertEquals("Unexpected value at position: " + i, expVal, val);
            }
        }
        finally {
            mem.readUnlock(-1, pageId, page);
        }
    }

    /**
     * @param mem Memory.
     * @return Page.
     * @throws IgniteCheckedException If failed.
     */
    public static FullPageId allocatePage(PageIdAllocator mem) throws IgniteCheckedException {
        return new FullPageId(mem.allocatePage(-1, 1, PageIdAllocator.FLAG_DATA), -1);
    }
}

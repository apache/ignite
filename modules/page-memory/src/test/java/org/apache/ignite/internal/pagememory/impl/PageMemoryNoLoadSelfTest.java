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

package org.apache.ignite.internal.pagememory.impl;

import static org.apache.ignite.internal.configuration.ConfigurationTestUtils.fixConfiguration;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import org.apache.ignite.configuration.schemas.store.DataRegionConfiguration;
import org.apache.ignite.configuration.schemas.store.PageMemoryDataRegionChange;
import org.apache.ignite.configuration.schemas.store.PageMemoryDataRegionConfiguration;
import org.apache.ignite.configuration.schemas.store.PageMemoryDataRegionConfigurationSchema;
import org.apache.ignite.configuration.schemas.store.UnsafeMemoryAllocatorConfigurationSchema;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.pagememory.FullPageId;
import org.apache.ignite.internal.pagememory.PageIdAllocator;
import org.apache.ignite.internal.pagememory.PageMemory;
import org.apache.ignite.internal.pagememory.TestPageIoModule.TestPageIo;
import org.apache.ignite.internal.pagememory.io.PageIo;
import org.apache.ignite.internal.pagememory.io.PageIoRegistry;
import org.apache.ignite.internal.pagememory.mem.DirectMemoryProvider;
import org.apache.ignite.internal.pagememory.mem.IgniteOutOfMemoryException;
import org.apache.ignite.internal.pagememory.mem.unsafe.UnsafeMemoryProvider;
import org.apache.ignite.internal.pagememory.util.PageIdUtils;
import org.apache.ignite.internal.pagememory.util.PageUtils;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.lang.IgniteInternalCheckedException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Tests {@link PageMemoryNoStoreImpl}.
 */
@ExtendWith(ConfigurationExtension.class)
public class PageMemoryNoLoadSelfTest extends BaseIgniteAbstractTest {
    protected static final int PAGE_SIZE = 8 * 1024;

    private static final int MAX_MEMORY_SIZE = 10 * 1024 * 1024;

    private static final PageIo PAGE_IO = new TestPageIo();

    @InjectConfiguration(
            value = "mock.type = pagemem",
            polymorphicExtensions = {
                PageMemoryDataRegionConfigurationSchema.class,
                UnsafeMemoryAllocatorConfigurationSchema.class
            })
    private DataRegionConfiguration dataRegionCfg;

    @Test
    public void testPageTearingInner() throws Exception {
        PageMemory mem = memory();

        mem.start();

        try {
            FullPageId fullId1 = allocatePage(mem);
            FullPageId fullId2 = allocatePage(mem);

            long page1 = mem.acquirePage(fullId1.groupId(), fullId1.pageId());

            try {
                long page2 = mem.acquirePage(fullId2.groupId(), fullId2.pageId());

                log.info("Allocated pages [page1Id=" + fullId1.pageId() + ", page1=" + page1
                        + ", page2Id=" + fullId2.pageId() + ", page2=" + page2 + ']');

                try {
                    writePage(mem, fullId1, page1, 1);
                    writePage(mem, fullId2, page2, 2);

                    readPage(mem, fullId1.pageId(), page1, 1);
                    readPage(mem, fullId2.pageId(), page2, 2);

                    // Check read after read.
                    readPage(mem, fullId1.pageId(), page1, 1);
                    readPage(mem, fullId2.pageId(), page2, 2);
                } finally {
                    mem.releasePage(fullId2.groupId(), fullId2.pageId(), page2);
                }
            } finally {
                mem.releasePage(fullId1.groupId(), fullId1.pageId(), page1);
            }
        } finally {
            mem.stop(true);
        }
    }

    @Test
    public void testLoadedPagesCount() throws Exception {
        PageMemory mem = memory();

        mem.start();

        int expPages = MAX_MEMORY_SIZE / mem.systemPageSize();

        try {
            for (int i = 0; i < expPages * 2; i++) {
                allocatePage(mem);
            }
        } catch (IgniteOutOfMemoryException e) {
            log.error(e.getMessage(), e);

            // Expected.
            assertEquals(mem.loadedPages(), expPages);
        } finally {
            mem.stop(true);
        }
    }

    @Test
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
                    if (i % 64 == 0) {
                        log.info("Writing page [idx=" + i + ", pageId=" + fullId.pageId() + ", page=" + page + ']');
                    }

                    writePage(mem, fullId, page, i + 1);
                } finally {
                    mem.releasePage(fullId.groupId(), fullId.pageId(), page);
                }
            }

            for (int i = 0; i < pagesCnt; i++) {
                FullPageId fullId = pages.get(i);

                long page = mem.acquirePage(fullId.groupId(), fullId.pageId());

                try {
                    if (i % 64 == 0) {
                        log.info("Reading page [idx=" + i + ", pageId=" + fullId.pageId() + ", page=" + page + ']');
                    }

                    readPage(mem, fullId.pageId(), page, i + 1);
                } finally {
                    mem.releasePage(fullId.groupId(), fullId.pageId(), page);
                }
            }
        } finally {
            mem.stop(true);
        }
    }

    @Test
    public void testPageHandleDeallocation() throws Exception {
        PageMemory mem = memory();

        mem.start();

        try {
            int pages = 3 * 1024 * 1024 / (8 * 1024);

            Collection<FullPageId> handles = new HashSet<>();

            for (int i = 0; i < pages; i++) {
                handles.add(allocatePage(mem));
            }

            for (FullPageId fullId : handles) {
                mem.freePage(fullId.groupId(), fullId.pageId());
            }

            for (int i = 0; i < pages; i++) {
                assertFalse(handles.add(allocatePage(mem)));
            }
        } finally {
            mem.stop(true);
        }
    }

    @Test
    public void testPageIdRotation() throws Exception {
        PageMemory mem = memory();

        mem.start();

        try {
            int pages = 5;

            Collection<FullPageId> old = new ArrayList<>();
            Collection<FullPageId> updated = new ArrayList<>();

            for (int i = 0; i < pages; i++) {
                old.add(allocatePage(mem));
            }

            // Check that initial pages are accessible.
            for (FullPageId id : old) {
                long pageApsPtr = mem.acquirePage(id.groupId(), id.pageId());
                try {
                    long pageAddr = mem.writeLock(id.groupId(), id.pageId(), pageApsPtr);

                    assertNotNull(pageAddr);

                    try {
                        PAGE_IO.initNewPage(pageAddr, id.pageId(), mem.realPageSize(id.groupId()));

                        long updId = PageIdUtils.rotatePageId(id.pageId());

                        PageIo.setPageId(pageAddr, updId);

                        updated.add(new FullPageId(updId, id.groupId()));
                    } finally {
                        mem.writeUnlock(id.groupId(), id.pageId(), pageApsPtr, true);
                    }
                } finally {
                    mem.releasePage(id.groupId(), id.pageId(), pageApsPtr);
                }
            }

            // Check that updated pages are inaccessible using old IDs.
            for (FullPageId id : old) {
                long pageApsPtr = mem.acquirePage(id.groupId(), id.pageId());
                try {
                    long pageAddr = mem.writeLock(id.groupId(), id.pageId(), pageApsPtr);

                    if (pageAddr != 0L) {
                        mem.writeUnlock(id.groupId(), id.pageId(), pageApsPtr, false);

                        fail("Was able to acquire page write lock.");
                    }

                    mem.readLock(id.groupId(), id.pageId(), pageApsPtr);

                    if (pageAddr != 0) {
                        mem.readUnlock(id.groupId(), id.pageId(), pageApsPtr);

                        fail("Was able to acquire page read lock.");
                    }
                } finally {
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
                        assertEquals(id.pageId(), PageIo.getPageId(pageAddr));
                    } finally {
                        mem.writeUnlock(id.groupId(), id.pageId(), pageApsPtr, false);
                    }

                    pageAddr = mem.readLock(id.groupId(), id.pageId(), pageApsPtr);

                    assertNotSame(0L, pageAddr);

                    try {
                        assertEquals(id.pageId(), PageIo.getPageId(pageAddr));
                    } finally {
                        mem.readUnlock(id.groupId(), id.pageId(), pageApsPtr);
                    }
                } finally {
                    mem.releasePage(id.groupId(), id.pageId(), pageApsPtr);
                }
            }
        } finally {
            mem.stop(true);
        }
    }

    /**
     * Creates new page memory instance.
     *
     * @return Page memory implementation.
     * @throws Exception If failed.
     */
    protected PageMemory memory() throws Exception {
        dataRegionCfg.change(cfg ->
                cfg.convert(PageMemoryDataRegionChange.class)
                        .changePageSize(PAGE_SIZE)
                        .changeInitSize(MAX_MEMORY_SIZE)
                        .changeMaxSize(MAX_MEMORY_SIZE)
        );

        DirectMemoryProvider provider = new UnsafeMemoryProvider(null);

        PageIoRegistry ioRegistry = new PageIoRegistry();

        ioRegistry.loadFromServiceLoader();

        return new PageMemoryNoStoreImpl(
            provider,
            (PageMemoryDataRegionConfiguration) fixConfiguration(dataRegionCfg),
            ioRegistry
        );
    }

    /**
     * Fills page with passed value.
     *
     * @param mem Page memory.
     * @param fullId Page ID.
     * @param page Page pointer.
     * @param val Value to write.
     */
    private void writePage(PageMemory mem, FullPageId fullId, long page, int val) {
        long pageAddr = mem.writeLock(-1, fullId.pageId(), page);

        try {
            PAGE_IO.initNewPage(pageAddr, fullId.pageId(), mem.realPageSize(fullId.groupId()));

            for (int i = PageIo.COMMON_HEADER_END; i < PAGE_SIZE; i++) {
                PageUtils.putByte(pageAddr, i, (byte) val);
            }
        } finally {
            mem.writeUnlock(-1, fullId.pageId(), page, true);
        }
    }

    /**
     * Reads a page from page memory and asserts that it is full of expected values.
     *
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
            for (int i = PageIo.COMMON_HEADER_END; i < PAGE_SIZE; i++) {
                int val = PageUtils.getByte(pageAddr, i) & 0xFF;

                assertEquals(expVal, val, "Unexpected value at position: " + i);
            }
        } finally {
            mem.readUnlock(-1, pageId, page);
        }
    }

    /**
     * Allocates page.
     *
     * @param mem Memory.
     * @return Page.
     * @throws IgniteCheckedException If failed.
     */
    public static FullPageId allocatePage(PageIdAllocator mem) throws IgniteInternalCheckedException {
        return new FullPageId(mem.allocatePage(-1, 1, PageIdAllocator.FLAG_DATA), -1);
    }
}

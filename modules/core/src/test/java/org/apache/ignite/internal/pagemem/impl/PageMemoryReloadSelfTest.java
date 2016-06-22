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
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.mem.file.MappedFileMemoryProvider;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.Page;
import org.apache.ignite.internal.pagemem.PageIdAllocator;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jsr166.ThreadLocalRandom8;

/**
 *
 */
public class PageMemoryReloadSelfTest extends GridCommonAbstractTest {
    /** */
    public static final int PAGE_SIZE = 8 * 1024;

    /** */
    private static File allocationPath;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        allocationPath = U.resolveWorkDirectory("pagemem", false);
    }

    /**
     *
     */
    public void testPageContentReload() throws Exception {
        PageMemory mem = memory(true);

        Collection<FullPageId> pages = new ArrayList<>();

        mem.start();

        try {
            for (int i = 0; i < 512; i++)
                pages.add(allocatePage(mem));

            for (FullPageId fullId : pages) {
                Page page = mem.page(fullId.cacheId(), fullId.pageId());

                try {
                    ByteBuffer writeBuf = page.getForWrite();

                    try {
                        for (int i = 0; i < 1024; i++)
                            writeBuf.putLong(fullId.pageId() * 1024 + i);
                    }
                    finally {
                        page.releaseWrite(true);
                    }
                }
                finally {
                    mem.releasePage(page);
                }
            }

            for (FullPageId fullId : pages) {
                Page page = mem.page(fullId.cacheId(), fullId.pageId());

                try {
                    ByteBuffer readBuf = page.getForRead();

                    try {
                        for (int i = 0; i < 1024; i++)
                            assertEquals(fullId.pageId() * 1024 + i, readBuf.getLong());

                        assertTrue(page.isDirty());
                    }
                    finally {
                        page.releaseRead();
                    }
                }
                finally {
                    mem.releasePage(page);
                }
            }
        }
        finally {
            mem.stop();
        }

        mem = memory(false);

        mem.start();

        try {
            for (FullPageId fullId : pages) {
                Page page = mem.page(fullId.cacheId(), fullId.pageId());

                try {
                    ByteBuffer readBuf = page.getForRead();

                    try {
                        for (int i = 0; i < 1024; i++)
                            assertEquals(fullId.pageId() * 1024 + i, readBuf.getLong());
                    }
                    finally {
                        page.releaseRead();
                    }
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
    public void testFreeList() throws Exception {
        PageMemory mem = memory(true);

        Collection<FullPageId> pages = new HashSet<>();

        mem.start();

        try {
            for (int i = 0; i < 512; i++)
                pages.add(allocatePage(mem));

            for (FullPageId fullId : pages)
                mem.freePage(fullId.cacheId(), fullId.pageId());
        }
        finally {
            mem.stop();
        }

        mem = memory(false);

        mem.start();

        try {
            Collection<FullPageId> rePages = new HashSet<>();

            for (int i = 0; i < 512; i++)
                rePages.add(allocatePage(mem));

            assertTrue(pages.containsAll(rePages));
            assertTrue(rePages.containsAll(pages));

            for (FullPageId fullId : rePages) {
                Page page = mem.page(fullId.cacheId(), fullId.pageId());

                try {
                    assertFalse(page.isDirty());
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
    public void testConcurrentReadWrite() throws Exception {
        final List<FullPageId> pages = new ArrayList<>();

        final Map<FullPageId, Long> map = new ConcurrentHashMap<>();

        {
            final PageMemory mem = memory(true);

            mem.start();

            try {
                for (int i = 0; i < 512; i++)
                    pages.add(allocatePage(mem));

                for (FullPageId fullId : pages) {
                    Page page = mem.page(fullId.cacheId(), fullId.pageId());

                    try {
                        writePage(page, 0, map);

                        map.put(fullId, 0L);
                    }
                    finally {
                        mem.releasePage(page);
                    }
                }

                final AtomicBoolean stop = new AtomicBoolean(false);
                final AtomicInteger ops = new AtomicInteger();

                IgniteInternalFuture<Long> fut = GridTestUtils.runMultiThreadedAsync(new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        ThreadLocalRandom8 rnd = ThreadLocalRandom8.current();

                        while (!stop.get()) {
                            FullPageId fullId = pages.get(rnd.nextInt(pages.size()));

                            Page page = mem.page(fullId.cacheId(), fullId.pageId());

                            try {
                                if (rnd.nextBoolean()) {
                                    long val = rnd.nextLong();

                                    writePage(page, val, map);
                                }
                                else
                                    readPage(page, map);

                                ops.incrementAndGet();
                            }
                            finally {
                                mem.releasePage(page);
                            }
                        }

                        return null;
                    }
                }, 8, "async-runner");

                for (int i = 0; i < 60; i++) {
                    U.sleep(1_000);

                    info("Ops/sec: " + ops.getAndSet(0));
                }

                stop.set(true);

                fut.get();

                for (FullPageId fullId : pages) {
                    Page page = mem.page(fullId.cacheId(), fullId.pageId());

                    try {
                        readPage(page, map);
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

        {
            PageMemory mem0 = memory(false);

            mem0.start();

            try {
                for (FullPageId fullId : pages) {
                    Page page = mem0.page(fullId.cacheId(), fullId.pageId());

                    try {
                        readPage(page, map);
                    }
                    finally {
                        mem0.releasePage(page);
                    }
                }
            }
            finally {
                mem0.stop();
            }
        }
    }

    /**
     * @param clean Clean flag. If {@code true}, will clean previous memory state and allocate
     *      new empty page memory.
     * @return Page memory instance.
     */
    private PageMemory memory(boolean clean) {
        MappedFileMemoryProvider provider = new MappedFileMemoryProvider(log(), allocationPath, clean,
            10 * 1024 * 1024, 1024 * 1024);

        return new PageMemoryImpl(log, provider, null, PAGE_SIZE, Runtime.getRuntime().availableProcessors());
    }

    /**
     * @param page Page to write.
     * @param val Value to write.
     */
    private void writePage(Page page, long val, Map<FullPageId, Long> updateMap) {
        ByteBuffer bytes = page.getForWrite();

        try {
            for (int i = 0; i < 1024; i++)
                bytes.putLong(val);

            updateMap.put(page.fullId(), val);
        }
        finally {
            page.releaseWrite(true);
        }
    }

    /**
     * @param page Page to read.
     * @param valMap Value map with control values.
     */
    private void readPage(Page page, Map<FullPageId, Long> valMap) {
        ByteBuffer bytes = page.getForRead();

        try {
            long expVal = valMap.get(page.fullId());

            for (int i = 0; i < 1024; i++) {
                long val = bytes.getLong();

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

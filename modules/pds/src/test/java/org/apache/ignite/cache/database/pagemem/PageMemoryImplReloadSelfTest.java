/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.cache.database.pagemem;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
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
import org.apache.ignite.internal.pagemem.PageIdAllocator;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.pagemem.PageUtils;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.database.CheckpointLockStateChecker;
import org.apache.ignite.internal.processors.cache.database.IgniteCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.database.pagemem.PageMemoryEx;
import org.apache.ignite.internal.processors.cache.database.pagemem.PageMemoryImpl;
import org.apache.ignite.internal.processors.cache.database.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.database.tree.io.TrackingPageIO;
import org.apache.ignite.internal.util.lang.GridInClosure3X;
import org.apache.ignite.internal.util.typedef.CIX3;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.GridTestKernalContext;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jsr166.ThreadLocalRandom8;

/**
 *
 */
public class PageMemoryImplReloadSelfTest extends GridCommonAbstractTest {
    /** */
    public static final int PAGE_SIZE = 8 * 1024;

    /** */
    private static File allocationPath;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        allocationPath = U.resolveWorkDirectory(U.defaultWorkDirectory(), "pagemem", false);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        deleteRecursively(allocationPath);
    }

    /**
     *
     */
    public void testPageContentReload() throws Exception {
        fail(); //TODO @Ed

        PageMemory mem = memory(true);

        Collection<FullPageId> pages = new ArrayList<>();

        mem.start();

        try {
            for (int i = 0; i < 512; i++)
                pages.add(allocatePage(mem));

            for (FullPageId fullId : pages) {
                assert TrackingPageIO.VERSIONS.latest().trackingPageFor(fullId.pageId(), PAGE_SIZE) != fullId.pageId();

                long page = mem.acquirePage(fullId.cacheId(), fullId.pageId());

                try {
                    long writeBuf = mem.writeLock(fullId.cacheId(), fullId.pageId(), page);

                    PageIO.setPageId(writeBuf, fullId.pageId());

                    try {
                        for (int i = PageIO.COMMON_HEADER_END; i < 1024; i++)
                            PageUtils.putLong(writeBuf, PageIO.COMMON_HEADER_END + 8 * (i - PageIO.COMMON_HEADER_END),
                                fullId.pageId() * 1024 + i);
                    }
                    finally {
                        mem.writeUnlock(fullId.cacheId(), fullId.pageId(), page, null, true);
                    }
                }
                finally {
                    mem.releasePage(fullId.cacheId(), fullId.pageId(), page);
                }
            }

            for (FullPageId fullId : pages) {
                long page = mem.acquirePage(fullId.cacheId(), fullId.pageId());
                try {
                    long pageAddr = mem.writeLock(fullId.cacheId(), fullId.pageId(), page);

                    try {
                        for (int i = PageIO.COMMON_HEADER_END; i < 1024; i++)
                            assertEquals(fullId.pageId() * 1024 + i,
                                PageUtils.getLong(pageAddr, PageIO.COMMON_HEADER_END + 8 * (i - PageIO.COMMON_HEADER_END)));

                        assertTrue(mem.isDirty(fullId.cacheId(), fullId.pageId(), page));
                    }
                    finally {
                        mem.readUnlock(fullId.cacheId(), fullId.pageId(), page);
                    }
                }
                finally {
                    mem.releasePage(fullId.cacheId(), fullId.pageId(), page);
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
                long page = mem.acquirePage(fullId.cacheId(), fullId.pageId());
                try {
                    long pageAddr = mem.writeLock(fullId.cacheId(), fullId.pageId(), page);

                    try {
                        for (int i = PageIO.COMMON_HEADER_END; i < 1024; i++)
                            assertEquals(fullId.pageId() * 1024 + i,
                                PageUtils.getLong(pageAddr, PageIO.COMMON_HEADER_END + 8 * (i - PageIO.COMMON_HEADER_END)));
                    }
                    finally {
                        mem.readUnlock(fullId.cacheId(), fullId.pageId(), page);
                    }
                }
                finally {
                    mem.releasePage(fullId.cacheId(), fullId.pageId(), page);
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
        fail(); //TODO @Ed
        final List<FullPageId> pages = new ArrayList<>();

        final Map<FullPageId, Long> map = new ConcurrentHashMap<>();

        {
            final PageMemory mem = memory(true);

            mem.start();

            try {
                for (int i = 0; i < 512; i++)
                    pages.add(allocatePage(mem));

                for (FullPageId fullId : pages) {
                    long page = mem.acquirePage(fullId.cacheId(), fullId.pageId());

                    try {
                        writePage(mem, fullId.cacheId(), fullId.pageId(), page, 0, map);

                        map.put(fullId, 0L);
                    }
                    finally {
                        mem.releasePage(fullId.cacheId(), fullId.pageId(), page);
                    }
                }

                final AtomicBoolean stop = new AtomicBoolean(false);
                final AtomicInteger ops = new AtomicInteger();

                IgniteInternalFuture<Long> fut = GridTestUtils.runMultiThreadedAsync(new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        ThreadLocalRandom8 rnd = ThreadLocalRandom8.current();

                        while (!stop.get()) {
                            FullPageId fullId = pages.get(rnd.nextInt(pages.size()));

                            long page = mem.acquirePage(fullId.cacheId(), fullId.pageId());

                            try {
                                if (rnd.nextBoolean()) {
                                    long val = rnd.nextLong();

                                    writePage(mem, fullId.cacheId(), fullId.pageId(), page, val, map);
                                }
                                else
                                    readPage(mem, fullId.cacheId(), fullId.pageId(), page, map);

                                ops.incrementAndGet();
                            }
                            finally {
                                mem.releasePage(fullId.cacheId(), fullId.pageId(), page);
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
                    long page = mem.acquirePage(fullId.cacheId(), fullId.pageId());

                    try {
                        readPage(mem, fullId.cacheId(), fullId.pageId(), page, map);
                    }
                    finally {
                        mem.releasePage(fullId.cacheId(), fullId.pageId(), page);
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
                    long page = mem0.acquirePage(fullId.cacheId(), fullId.pageId());

                    try {
                        readPage(mem0, fullId.cacheId(), fullId.pageId(), page, map);
                    }
                    finally {
                        mem0.releasePage(fullId.cacheId(), fullId.pageId(), page);
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
    private PageMemory memory(boolean clean) throws Exception {
        long[] sizes = new long[10];

        for (int i = 0; i < sizes.length; i++)
            sizes[i] = 5 * 1024 * 1024;

        MappedFileMemoryProvider provider = new MappedFileMemoryProvider(log(), allocationPath, clean,
            sizes);

        GridCacheSharedContext<Object, Object> sharedCtx = new GridCacheSharedContext<>(
            new GridTestKernalContext(log),
            null,
            null,
            null,
            new NoOpPageStoreManager(),
            new NoOpWALManager(),
            new IgniteCacheDatabaseSharedManager(),
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null
        );

        return new PageMemoryImpl(provider, sharedCtx, PAGE_SIZE,
            new CIX3<FullPageId, ByteBuffer, Integer>() {
                @Override public void applyx(FullPageId fullPageId, ByteBuffer byteBuf, Integer tag) {
                    assert false : "No evictions should happen during the test";
                }
            }, new GridInClosure3X<Long, FullPageId, PageMemoryEx>() {
            @Override public void applyx(Long page, FullPageId fullPageId, PageMemoryEx pageMem) {
            }
        }, new CheckpointLockStateChecker() {
            @Override public boolean checkpointLockIsHeldByThread() {
                return true;
            }
        });
    }

    /**
     * @param val Value to write.
     */
    private void writePage(PageMemory pageMem, int cacheId, long pageId, long page, long val, Map<FullPageId, Long> updateMap) {
        long pageAddr = pageMem.writeLock(cacheId, pageId, page);

        try {
            PageIO.setPageId(pageAddr, pageId);

            for (int i = PageIO.COMMON_HEADER_END; i < 1024; i++)
                PageUtils.putLong(pageAddr, PageIO.COMMON_HEADER_END + 8 * (i - PageIO.COMMON_HEADER_END), val);

            updateMap.put(new FullPageId(pageId, cacheId), val);
        }
        finally {
            pageMem.writeUnlock(cacheId, pageId, page, null, true);
        }
    }

    /**
     * @param valMap Value map with control values.
     */
    private void readPage(PageMemory pageMem, int cacheId, long pageId, long page, Map<FullPageId, Long> valMap) {
        long pageAddr = pageMem.readLock(cacheId, pageId, page);

        try {
            long expVal = valMap.get(new FullPageId(pageId, cacheId));

            for (int i = PageIO.COMMON_HEADER_END; i < 1024; i++) {
                long val = PageUtils.getLong(pageAddr, PageIO.COMMON_HEADER_END + 8 * (i - PageIO.COMMON_HEADER_END));

                assertEquals("Unexpected value at position: " + i, expVal, val);
            }
        }
        finally {
            pageMem.readUnlock(cacheId, pageId, page);
        }
    }

    /**
     * @param mem Memory.
     * @return Page.
     */
    public static FullPageId allocatePage(PageIdAllocator mem) throws IgniteCheckedException {
        return new FullPageId(mem.allocatePage(0, 1, PageIdAllocator.FLAG_DATA), 0);
    }
}

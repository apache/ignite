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

package org.apache.ignite.internal.processors.cache.persistence.db.file;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataPageEvictionMode;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.pagemem.PageUtils;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.persistence.IgniteCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.DummyPageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Test for page replacement (rotation with disk) process with enabled persistence.
 * A lot of reader threads tries to acquire page and checkpointer threads write data.
 */
public class IgnitePdsPageReplacementTest extends GridCommonAbstractTest {
    /** */
    private static final int NUMBER_OF_SEGMENTS = 64;

    /** */
    private static final int PAGE_SIZE = 4 * 1024;

    /** */
    private static final long CHUNK_SIZE = 1024 * 1024;

    /** */
    private static final long MEMORY_LIMIT = 10 * CHUNK_SIZE;

    /** */
    private static final int PAGES_NUM = 128_000;

    /** Cache name. */
    private final String cacheName = "cache";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        final IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setDataStorageConfiguration(createDbConfig());

        cfg.setCacheConfiguration(new CacheConfiguration<>(cacheName));

        return cfg;
    }

    /**
     * @return DB config.
     */
    private DataStorageConfiguration createDbConfig() {
        final DataStorageConfiguration memCfg = new DataStorageConfiguration();

        DataRegionConfiguration memPlcCfg = new DataRegionConfiguration();
        memPlcCfg.setInitialSize(MEMORY_LIMIT);
        memPlcCfg.setMaxSize(MEMORY_LIMIT);
        memPlcCfg.setPageEvictionMode(DataPageEvictionMode.RANDOM_LRU);
        memPlcCfg.setName("dfltDataRegion");
        memPlcCfg.setPersistenceEnabled(true);

        memCfg.setPageSize(PAGE_SIZE);
        memCfg.setConcurrencyLevel(NUMBER_OF_SEGMENTS);
        memCfg.setDefaultDataRegionConfiguration(memPlcCfg);
        memCfg.setWalMode(WALMode.LOG_ONLY);

        return memCfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * @throws Exception If fail.
     */
    public void testPageReplacement() throws Exception {
        final IgniteEx ig = startGrid(0);

        ig.active(true);

        final PageMemory memory = getMemory(ig);

        writeData(ig, memory, CU.cacheId(cacheName));
    }

    /**
     * @param memory Page memory.
     * @param cacheId Cache id.
     * @throws IgniteCheckedException If failed.
     */
    private void writeData(final IgniteEx ignite, final PageMemory memory, final int cacheId) throws Exception {
        final int pagesNum = getPagesNum();

        final List<FullPageId> pageIds = new ArrayList<>(pagesNum);

        IgniteCacheDatabaseSharedManager db = ignite.context().cache().context().database();

        PageIO pageIO = new DummyPageIO();

        // Allocate.
        for (int i = 0; i < pagesNum; i++) {
            db.checkpointReadLock();
            try {
                final FullPageId fullId = new FullPageId(memory.allocatePage(cacheId, i % 256, PageMemory.FLAG_DATA),
                    cacheId);

                initPage(memory, pageIO, fullId);

                pageIds.add(fullId);
            }
            finally {
                db.checkpointReadUnlock();
            }
        }

        System.out.println("Allocated pages: " + pageIds.size());

        // Write data. (Causes evictions.)
        final int part = pagesNum / NUMBER_OF_SEGMENTS;

        final Collection<IgniteInternalFuture> futs = new ArrayList<>();

        for (int i = 0; i < pagesNum; i += part)
            futs.add(runWriteInThread(ignite, i, i + part, memory, pageIds));

        for (final IgniteInternalFuture fut : futs)
            fut.get();

        System.out.println("Wrote pages: " + pageIds.size());

        // Read data. (Causes evictions.)
        futs.clear();

        for (int i = 0; i < pagesNum; i += part)
            futs.add(runReadInThread(ignite, i, i + part, memory, pageIds));

        for (final IgniteInternalFuture fut : futs)
            fut.get();

        System.out.println("Read pages: " + pageIds.size());
    }

    /**
     * @return num of pages to operate in test.
     */
    protected int getPagesNum() {
        return PAGES_NUM;
    }

    /**
     * Initializes page.
     * @param mem page memory implementation.
     * @param pageIO page io implementation.
     * @param fullId full page id.
     * @throws IgniteCheckedException if error occurs.
     */
    private void initPage(PageMemory mem, PageIO pageIO, FullPageId fullId) throws IgniteCheckedException {
        long page = mem.acquirePage(fullId.groupId(), fullId.pageId());

        try {
            final long pageAddr = mem.writeLock(fullId.groupId(), fullId.pageId(), page);

            try {
                pageIO.initNewPage(pageAddr, fullId.pageId(), mem.realPageSize(fullId.groupId()));
            }
            finally {
                mem.writeUnlock(fullId.groupId(), fullId.pageId(), page, null, true);
            }
        }
        finally {
            mem.releasePage(fullId.groupId(), fullId.pageId(), page);
        }
    }

    /**
     * @param start Start index.
     * @param end End index.
     * @param memory PageMemory.
     * @param pageIds Allocated pages.
     * @return Future.
     * @throws Exception If fail.
     */
    private IgniteInternalFuture runWriteInThread(
        final IgniteEx ignite,
        final int start,
        final int end,
        final PageMemory memory,
        final List<FullPageId> pageIds
    ) throws Exception {

        return GridTestUtils.runAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                IgniteCacheDatabaseSharedManager db = ignite.context().cache().context().database();

                for (int i = start; i < end; i++) {
                    db.checkpointReadLock();

                    try {
                        FullPageId fullId = pageIds.get(i);

                        long page = memory.acquirePage(fullId.groupId(), fullId.pageId());

                        try {
                            final long pageAddr = memory.writeLock(fullId.groupId(), fullId.pageId(), page);

                            try {
                                PageIO.setPageId(pageAddr, fullId.pageId());

                                PageUtils.putLong(pageAddr, PageIO.COMMON_HEADER_END, i * 2);
                            }
                            finally {
                                memory.writeUnlock(fullId.groupId(), fullId.pageId(), page, null, true);
                            }
                        }
                        finally {
                            memory.releasePage(fullId.groupId(), fullId.pageId(), page);
                        }
                    }
                    finally {
                        db.checkpointReadUnlock();
                    }
                }

                return null;
            }
        });
    }

    /**
     * @param start Start index.
     * @param end End index.
     * @param memory PageMemory.
     * @param pageIds Allocated pages.
     * @return Future.
     * @throws Exception If fail.
     */
    private IgniteInternalFuture runReadInThread(final IgniteEx ignite, final int start, final int end,
        final PageMemory memory,
        final List<FullPageId> pageIds) throws Exception {
        return GridTestUtils.runAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                IgniteCacheDatabaseSharedManager db = ignite.context().cache().context().database();

                for (int i = start; i < end; i++) {
                    db.checkpointReadLock();

                    try {
                        final FullPageId fullId = pageIds.get(i);

                        long page = memory.acquirePage(fullId.groupId(), fullId.pageId());
                        try {
                            final long pageAddr = memory.readLock(fullId.groupId(), fullId.pageId(), page);

                            try {
                                assertEquals(i * 2, PageUtils.getLong(pageAddr, PageIO.COMMON_HEADER_END));
                            }
                            finally {
                                memory.readUnlock(fullId.groupId(), fullId.pageId(), page);
                            }
                        }
                        finally {
                            memory.releasePage(fullId.groupId(), fullId.pageId(), page);
                        }
                    }
                    finally {
                        db.checkpointReadUnlock();
                    }
                }

                return null;
            }
        });
    }

    /**
     * @param ig Ignite instance.
     * @return Memory and store.
     * @throws Exception If failed to initialize the store.
     */
    private PageMemory getMemory(IgniteEx ig) throws Exception {
        final GridCacheSharedContext<Object, Object> sharedCtx = ig.context().cache().context();

        final IgniteCacheDatabaseSharedManager db = sharedCtx.database();

        return db.dataRegion(null).pageMemory();
    }
}

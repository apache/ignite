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

package org.apache.ignite.internal.processors.cache.persistence.pagemem;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.NoOpFailureHandler;
import org.apache.ignite.internal.IgniteFutureTimeoutCheckedException;
import org.apache.ignite.internal.managers.encryption.GridEncryptionManager;
import org.apache.ignite.internal.mem.DirectMemoryProvider;
import org.apache.ignite.internal.mem.IgniteOutOfMemoryException;
import org.apache.ignite.internal.mem.unsafe.UnsafeMemoryProvider;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.PageUtils;
import org.apache.ignite.internal.pagemem.store.IgnitePageStoreManager;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.persistence.CheckpointLockStateChecker;
import org.apache.ignite.internal.processors.cache.persistence.CheckpointWriteProgressSupplier;
import org.apache.ignite.internal.processors.cache.persistence.DataRegionMetricsImpl;
import org.apache.ignite.internal.processors.cache.persistence.DummyPageIO;
import org.apache.ignite.internal.processors.cache.persistence.IgniteCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.PageStoreWriter;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.failure.FailureProcessor;
import org.apache.ignite.internal.processors.metric.GridMetricManager;
import org.apache.ignite.internal.processors.plugin.IgnitePluginProcessor;
import org.apache.ignite.internal.processors.subscription.GridInternalSubscriptionProcessor;
import org.apache.ignite.internal.util.GridMultiCollectionWrapper;
import org.apache.ignite.internal.util.lang.GridInClosure3X;
import org.apache.ignite.plugin.PluginProvider;
import org.apache.ignite.spi.encryption.noop.NoopEncryptionSpi;
import org.apache.ignite.spi.metric.noop.NoopMetricExporterSpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.GridTestKernalContext;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.logger.GridTestLog4jLogger;
import org.junit.Test;
import org.mockito.Mockito;

import static org.apache.ignite.internal.pagemem.PageIdAllocator.FLAG_IDX;
import static org.apache.ignite.internal.pagemem.PageIdAllocator.INDEX_PARTITION;
import static org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMemoryImpl.CHECKPOINT_POOL_OVERFLOW_ERROR_MSG;
import static org.apache.ignite.internal.processors.database.DataRegionMetricsSelfTest.NO_OP_METRICS;

/**
 *
 */
public class PageMemoryImplTest extends GridCommonAbstractTest {
    /** Mb. */
    private static final long MB = 1024 * 1024;

    /** Page size. */
    private static final int PAGE_SIZE = 1024;

    /** Max memory size. */
    private static final int MAX_SIZE = 128;

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testThatAllocationTooMuchPagesCauseToOOMException() throws Exception {
        PageMemoryImpl memory = createPageMemory(PageMemoryImpl.ThrottlingPolicy.DISABLED);

        try {
            while (!Thread.currentThread().isInterrupted())
                memory.allocatePage(1, INDEX_PARTITION, FLAG_IDX);
        }
        catch (IgniteOutOfMemoryException ignore) {
            //Success
        }

        assertFalse(memory.safeToUpdate());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCheckpointBufferOverusageDontCauseWriteLockLeak() throws Exception {
        PageMemoryImpl memory = createPageMemory(PageMemoryImpl.ThrottlingPolicy.DISABLED);

        List<FullPageId> pages = new ArrayList<>();

        try {
            while (!Thread.currentThread().isInterrupted()) {
                long pageId = memory.allocatePage(1, INDEX_PARTITION, FLAG_IDX);

                FullPageId fullPageId = new FullPageId(pageId, 1);

                pages.add(fullPageId);

                acquireAndReleaseWriteLock(memory, fullPageId); //to set page id, otherwise we would fail with assertion error
            }
        }
        catch (IgniteOutOfMemoryException ignore) {
            //Success
        }

        memory.beginCheckpoint();

        final AtomicReference<FullPageId> lastPage = new AtomicReference<>();

        try {
            for (FullPageId fullPageId : pages) {
                lastPage.set(fullPageId);

                acquireAndReleaseWriteLock(memory, fullPageId);
            }
        }
        catch (Exception ex) {
            assertTrue(ex.getMessage().startsWith(CHECKPOINT_POOL_OVERFLOW_ERROR_MSG));
        }

        memory.finishCheckpoint();

        GridTestUtils.runAsync(new Runnable() {
            @Override public void run() {
                try {
                    acquireAndReleaseWriteLock(memory, lastPage.get()); //we should be able get lock again
                }
                catch (IgniteCheckedException e) {
                    throw new AssertionError(e);
                }
            }
        }).get(getTestTimeout());
    }

    /**
     * Tests that checkpoint buffer won't be overflowed with enabled CHECKPOINT_BUFFER_ONLY throttling.
     * @throws Exception If failed.
     */
    @Test
    public void testCheckpointBufferCantOverflowMixedLoad() throws Exception {
        testCheckpointBufferCantOverflowWithThrottlingMixedLoad(PageMemoryImpl.ThrottlingPolicy.CHECKPOINT_BUFFER_ONLY);
    }

    /**
     * Tests that checkpoint buffer won't be overflowed with enabled SPEED_BASED throttling.
     * @throws Exception If failed.
     */
    @Test
    public void testCheckpointBufferCantOverflowMixedLoadSpeedBased() throws Exception {
        testCheckpointBufferCantOverflowWithThrottlingMixedLoad(PageMemoryImpl.ThrottlingPolicy.SPEED_BASED);
    }

    /**
     * Tests that checkpoint buffer won't be overflowed with enabled TARGET_RATIO_BASED throttling.
     * @throws Exception If failed.
     */
    @Test
    public void testCheckpointBufferCantOverflowMixedLoadRatioBased() throws Exception {
        testCheckpointBufferCantOverflowWithThrottlingMixedLoad(PageMemoryImpl.ThrottlingPolicy.TARGET_RATIO_BASED);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testCheckpointProtocolWriteDirtyPageAfterWriteUnlock() throws Exception {
        TestPageStoreManager pageStoreMgr = new TestPageStoreManager();

        // Create a 1 mb page memory.
        PageMemoryImpl memory = createPageMemory(
            1,
            PageMemoryImpl.ThrottlingPolicy.TARGET_RATIO_BASED,
            pageStoreMgr,
            pageStoreMgr
        );

        int initPageCnt = 10;

        List<FullPageId> allocated = new ArrayList<>(initPageCnt);

        for (int i = 0; i < initPageCnt; i++) {
            long id = memory.allocatePage(1, INDEX_PARTITION, FLAG_IDX);

            FullPageId fullId = new FullPageId(id, 1);

            allocated.add(fullId);

            writePage(memory, fullId, (byte)1);
        }

        doCheckpoint(memory.beginCheckpoint(), memory, pageStoreMgr);

        FullPageId cowPageId = allocated.get(0);

        // Mark some pages as dirty.
        writePage(memory, cowPageId, (byte)2);

        GridMultiCollectionWrapper<FullPageId> cpPages = memory.beginCheckpoint();

        assertEquals(1, cpPages.size());

        // At this point COW mechanics kicks in.
        writePage(memory, cowPageId, (byte)3);

        doCheckpoint(cpPages, memory, pageStoreMgr);

        byte[] data = pageStoreMgr.storedPages.get(cowPageId);

        for (int i = PageIO.COMMON_HEADER_END; i < PAGE_SIZE; i++)
            assertEquals(2, data[i]);
    }

    /**
     * @param cpPages Checkpoint pages acuiqred by {@code beginCheckpoint()}.
     * @param memory Page memory.
     * @param pageStoreMgr Test page store manager.
     * @throws Exception If failed.
     */
    private void doCheckpoint(
        GridMultiCollectionWrapper<FullPageId> cpPages,
        PageMemoryImpl memory,
        TestPageStoreManager pageStoreMgr
    ) throws Exception {
        PageStoreWriter pageStoreWriter = (fullPageId, buf, tag) -> {
            assertNotNull(tag);

            pageStoreMgr.write(fullPageId.groupId(), fullPageId.pageId(), buf, 1);
        };

        for (FullPageId cpPage : cpPages) {
            byte[] data = new byte[PAGE_SIZE];

            ByteBuffer buf = ByteBuffer.wrap(data);

            memory.checkpointWritePage(cpPage, buf, pageStoreWriter, null);
        }

        memory.finishCheckpoint();
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testCheckpointProtocolCannotReplaceUnwrittenPage() throws Exception {
        TestPageStoreManager pageStoreMgr = new TestPageStoreManager();

        // Create a 1 mb page memory.
        PageMemoryImpl memory = createPageMemory(
            1,
            PageMemoryImpl.ThrottlingPolicy.TARGET_RATIO_BASED,
            pageStoreMgr,
            pageStoreMgr);

        int initPageCnt = 500;

        List<FullPageId> allocated = new ArrayList<>(initPageCnt);

        for (int i = 0; i < initPageCnt; i++) {
            long id = memory.allocatePage(1, INDEX_PARTITION, FLAG_IDX);

            FullPageId fullId = new FullPageId(id, 1);
            allocated.add(fullId);

            writePage(memory, fullId, (byte)1);
        }

        // CP Write lock.
        memory.beginCheckpoint();
        // CP Write unlock.

        byte[] buf = new byte[PAGE_SIZE];

        memory.checkpointWritePage(allocated.get(0), ByteBuffer.wrap(buf),
            (fullPageId, buf0, tag) -> {
                assertNotNull(tag);

                boolean oom = false;

                try {
                    // Try force page replacement.
                    while (true) {
                        memory.allocatePage(1, INDEX_PARTITION, FLAG_IDX);
                    }
                }
                catch (IgniteOutOfMemoryException ex) {
                    oom = true;
                }

                assertTrue("Should oom before check replaced page.", oom);

                assertTrue("Missing page: " + fullPageId, memory.hasLoadedPage(fullPageId));
            }
            , null);
    }

    /**
     * @param mem Page memory.
     * @param fullPageId Full page ID to write.
     * @param val Value to write.
     * @throws Exception If failed.
     */
    private void writePage(PageMemoryImpl mem, FullPageId fullPageId, byte val) throws Exception {
        int grpId = fullPageId.groupId();
        long pageId = fullPageId.pageId();
        long page = mem.acquirePage(grpId, pageId);

        try {
            long ptr = mem.writeLock(grpId, pageId, page);

            try {
                DummyPageIO.VERSIONS.latest().initNewPage(ptr, pageId, PAGE_SIZE);

                for (int i = PageIO.COMMON_HEADER_END; i < mem.pageSize(); i++)
                    PageUtils.putByte(ptr, i, val);
            }
            finally {
                mem.writeUnlock(grpId, pageId, page, Boolean.FALSE, true);
            }
        }
        finally {
            mem.releasePage(grpId, pageId, page);
        }
    }

    /**
     * @throws Exception If failed.
     */
    private void testCheckpointBufferCantOverflowWithThrottlingMixedLoad(PageMemoryImpl.ThrottlingPolicy plc) throws Exception {
        PageMemoryImpl memory = createPageMemory(plc);

        List<FullPageId> pages = new ArrayList<>();

        for (int i = 0; i < (MAX_SIZE - 10) * MB / PAGE_SIZE / 2; i++) {
            long pageId = memory.allocatePage(1, INDEX_PARTITION, FLAG_IDX);

            FullPageId fullPageId = new FullPageId(pageId, 1);

            pages.add(fullPageId);

            acquireAndReleaseWriteLock(memory, fullPageId);
        }

        memory.beginCheckpoint();

        CheckpointMetricsTracker mockTracker = Mockito.mock(CheckpointMetricsTracker.class);

        for (FullPageId checkpointPage : pages)
            memory.checkpointWritePage(checkpointPage, ByteBuffer.allocate(PAGE_SIZE),
                (fullPageId, buffer, tag) -> {
                    // No-op.
                }, mockTracker);

        memory.finishCheckpoint();

        for (int i = (int)((MAX_SIZE - 10) * MB / PAGE_SIZE / 2); i < (MAX_SIZE - 20) * MB / PAGE_SIZE; i++) {
            long pageId = memory.allocatePage(1, INDEX_PARTITION, FLAG_IDX);

            FullPageId fullPageId = new FullPageId(pageId, 1);

            pages.add(fullPageId);

            acquireAndReleaseWriteLock(memory, fullPageId);
        }

        memory.beginCheckpoint();

        Collections.shuffle(pages); // Mix pages in checkpoint with clean pages

        AtomicBoolean stop = new AtomicBoolean(false);

        try {
            GridTestUtils.runAsync(new Runnable() {
                @Override public void run() {
                    for (FullPageId page : pages) {
                        if (ThreadLocalRandom.current().nextDouble() < 0.5) // Mark dirty 50% of pages
                            try {
                                acquireAndReleaseWriteLock(memory, page);

                                if (stop.get())
                                    break;
                            }
                            catch (IgniteCheckedException e) {
                                log.error("runAsync ended with exception", e);

                                fail();
                            }
                    }
                }
            }).get(5_000);
        }
        catch (IgniteFutureTimeoutCheckedException ex) {
            // Expected.
        }
        finally {
            stop.set(true);
        }

        memory.finishCheckpoint();
    }

    /**
     * @param memory Memory.
     * @param fullPageId Full page id.
     * @throws IgniteCheckedException If acquiring lock failed.
     */
    private void acquireAndReleaseWriteLock(PageMemoryImpl memory, FullPageId fullPageId) throws IgniteCheckedException {
        long page = memory.acquirePage(1, fullPageId.pageId());

        long address = memory.writeLock(1, fullPageId.pageId(), page);

        PageIO.setPageId(address, fullPageId.pageId());

        PageIO.setType(address, PageIO.T_BPLUS_META);

        PageUtils.putShort(address, PageIO.VER_OFF, (short)1);

        memory.writeUnlock(1, fullPageId.pageId(), page, Boolean.FALSE, true);

        memory.releasePage(1, fullPageId.pageId(), page);
    }

    /**
     * @param throttlingPlc Throttling Policy.
     * @throws Exception If creating mock failed.
     */
    private PageMemoryImpl createPageMemory(
        PageMemoryImpl.ThrottlingPolicy throttlingPlc) throws Exception {
        return createPageMemory(
            MAX_SIZE,
            throttlingPlc,
            new NoOpPageStoreManager(),
            (fullPageId, byteBuf, tag) -> {
                assert false : "No page replacement (rotation with disk) should happen during the test";
            });
    }

    /**
     * @param throttlingPlc Throttling Policy.
     * @throws Exception If creating mock failed.
     */
    private PageMemoryImpl createPageMemory(
        int maxSize,
        PageMemoryImpl.ThrottlingPolicy throttlingPlc,
        IgnitePageStoreManager mgr,
        PageStoreWriter replaceWriter
    ) throws Exception {
        long[] sizes = new long[5];

        for (int i = 0; i < sizes.length; i++)
            sizes[i] = maxSize * MB / 4;

        sizes[4] = 5 * MB;

        DirectMemoryProvider provider = new UnsafeMemoryProvider(log);

        IgniteConfiguration igniteCfg = new IgniteConfiguration();
        igniteCfg.setDataStorageConfiguration(new DataStorageConfiguration());
        igniteCfg.setFailureHandler(new NoOpFailureHandler());
        igniteCfg.setEncryptionSpi(new NoopEncryptionSpi());
        igniteCfg.setMetricExporterSpi(new NoopMetricExporterSpi());

        GridTestKernalContext kernalCtx = new GridTestKernalContext(new GridTestLog4jLogger(), igniteCfg);

        kernalCtx.add(new IgnitePluginProcessor(kernalCtx, igniteCfg, Collections.<PluginProvider>emptyList()));
        kernalCtx.add(new GridInternalSubscriptionProcessor(kernalCtx));
        kernalCtx.add(new GridEncryptionManager(kernalCtx));
        kernalCtx.add(new GridMetricManager(kernalCtx));

        FailureProcessor failureProc = new FailureProcessor(kernalCtx);

        failureProc.start();

        kernalCtx.add(failureProc);

        GridCacheSharedContext<Object, Object> sharedCtx = new GridCacheSharedContext<>(
            kernalCtx,
            null,
            null,
            null,
            mgr,
            new NoOpWALManager(),
            null,
            new IgniteCacheDatabaseSharedManager(),
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null
        );

        CheckpointWriteProgressSupplier noThrottle = Mockito.mock(CheckpointWriteProgressSupplier.class);

        Mockito.when(noThrottle.currentCheckpointPagesCount()).thenReturn(1_000_000);
        Mockito.when(noThrottle.evictedPagesCntr()).thenReturn(new AtomicInteger(0));
        Mockito.when(noThrottle.syncedPagesCounter()).thenReturn(new AtomicInteger(1_000_000));
        Mockito.when(noThrottle.writtenPagesCounter()).thenReturn(new AtomicInteger(1_000_000));

        PageMemoryImpl mem = new PageMemoryImpl(
            provider,
            sizes,
            sharedCtx,
            PAGE_SIZE,
            replaceWriter,
            new GridInClosure3X<Long, FullPageId, PageMemoryEx>() {
                @Override public void applyx(Long page, FullPageId fullId, PageMemoryEx pageMem) {
                }
            }, new CheckpointLockStateChecker() {
                @Override public boolean checkpointLockIsHeldByThread() {
                    return true;
                }
            },
            new DataRegionMetricsImpl(igniteCfg.getDataStorageConfiguration().getDefaultDataRegionConfiguration(),
                kernalCtx.metric(),
                NO_OP_METRICS),
            throttlingPlc,
            noThrottle
        );

        mem.start();

        return mem;
    }

    /**
     *
     */
    private static class TestPageStoreManager extends NoOpPageStoreManager implements PageStoreWriter {
        /** */
        private Map<FullPageId, byte[]> storedPages = new HashMap<>();

        /** {@inheritDoc} */
        @Override public void read(int grpId, long pageId, ByteBuffer pageBuf) throws IgniteCheckedException {
            FullPageId fullPageId = new FullPageId(pageId, grpId);

            byte[] bytes = storedPages.get(fullPageId);

            if (bytes != null)
                pageBuf.put(bytes);
            else
                pageBuf.put(new byte[PAGE_SIZE]);
        }

        /** {@inheritDoc} */
        @Override public void write(int grpId, long pageId, ByteBuffer pageBuf, int tag) throws IgniteCheckedException {
            byte[] data = new byte[PAGE_SIZE];

            pageBuf.get(data);

            storedPages.put(new FullPageId(pageId, grpId), data);
        }

        /** {@inheritDoc} */
        @Override public void writePage(FullPageId fullPageId, ByteBuffer pageBuf, int tag) throws IgniteCheckedException {
            byte[] data = new byte[PAGE_SIZE];

            pageBuf.get(data);

            storedPages.put(fullPageId, data);
        }
    }
}

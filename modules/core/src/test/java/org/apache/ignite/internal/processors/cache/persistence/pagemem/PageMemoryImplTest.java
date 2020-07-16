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
import java.nio.ByteOrder;
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
import org.apache.ignite.internal.managers.eventstorage.GridEventStorageManager;
import org.apache.ignite.internal.managers.systemview.GridSystemViewManager;
import org.apache.ignite.internal.mem.DirectMemoryProvider;
import org.apache.ignite.internal.mem.IgniteOutOfMemoryException;
import org.apache.ignite.internal.mem.unsafe.UnsafeMemoryProvider;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.PageUtils;
import org.apache.ignite.internal.pagemem.store.IgnitePageStoreManager;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.persistence.CheckpointLockStateChecker;
import org.apache.ignite.internal.processors.cache.persistence.DataRegionMetricsImpl;
import org.apache.ignite.internal.processors.cache.persistence.DummyPageIO;
import org.apache.ignite.internal.processors.cache.persistence.IgniteCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.PageStoreWriter;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointProgress;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointProgressImpl;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.failure.FailureProcessor;
import org.apache.ignite.internal.processors.metric.GridMetricManager;
import org.apache.ignite.internal.processors.metric.impl.LongAdderMetric;
import org.apache.ignite.internal.processors.plugin.IgnitePluginProcessor;
import org.apache.ignite.internal.processors.subscription.GridInternalSubscriptionProcessor;
import org.apache.ignite.internal.util.GridMultiCollectionWrapper;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.lang.GridInClosure3X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgniteOutClosure;
import org.apache.ignite.plugin.PluginProvider;
import org.apache.ignite.spi.encryption.noop.NoopEncryptionSpi;
import org.apache.ignite.spi.eventstorage.NoopEventStorageSpi;
import org.apache.ignite.spi.metric.noop.NoopMetricExporterSpi;
import org.apache.ignite.spi.systemview.jmx.JmxSystemViewExporterSpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.GridTestKernalContext;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.logger.GridTestLog4jLogger;
import org.jetbrains.annotations.Nullable;
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
        PageMemoryImpl memory = createPageMemory(PageMemoryImpl.ThrottlingPolicy.DISABLED, null);

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
        PageMemoryImpl memory = createPageMemory(PageMemoryImpl.ThrottlingPolicy.DISABLED, null);

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

        memory.beginCheckpoint(new GridFinishedFuture());

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
     * Tests that with throttling enabled emptify cp buffer primarily with enabled CHECKPOINT_BUFFER_ONLY throttling.
     * @throws Exception If failed.
     */
    @Test
    public void testThrottlingEmptifyCpBufFirst() throws Exception {
        runThrottlingEmptifyCpBufFirst(PageMemoryImpl.ThrottlingPolicy.CHECKPOINT_BUFFER_ONLY);
    }

    /**
     * Tests that with throttling enabled emptify cp buffer primarily with enabled SPEED_BASED throttling.
     * @throws Exception If failed.
     */
    @Test
    public void testThrottlingEmptifyCpBufFirstSpeedBased() throws Exception {
        runThrottlingEmptifyCpBufFirst(PageMemoryImpl.ThrottlingPolicy.SPEED_BASED);
    }

    /**
     * Tests that with throttling enabled emptify cp buffer primarily with enabled TARGET_RATIO_BASED throttling.
     * @throws Exception If failed.
     */
    @Test
    public void testThrottlingEmptifyCpBufFirstRatioBased() throws Exception {
        runThrottlingEmptifyCpBufFirst(PageMemoryImpl.ThrottlingPolicy.TARGET_RATIO_BASED);
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
            pageStoreMgr,
            null
        );

        int initPageCnt = 10;

        List<FullPageId> allocated = new ArrayList<>(initPageCnt);

        for (int i = 0; i < initPageCnt; i++) {
            long id = memory.allocatePage(1, INDEX_PARTITION, FLAG_IDX);

            FullPageId fullId = new FullPageId(id, 1);

            allocated.add(fullId);

            writePage(memory, fullId, (byte)1);
        }

        doCheckpoint(memory.beginCheckpoint(new GridFinishedFuture()), memory, pageStoreMgr);

        FullPageId cowPageId = allocated.get(0);

        // Mark some pages as dirty.
        writePage(memory, cowPageId, (byte)2);

        GridMultiCollectionWrapper<FullPageId> cpPages = memory.beginCheckpoint(new GridFinishedFuture());

        assertEquals(1, cpPages.size());

        // At this point COW mechanics kicks in.
        writePage(memory, cowPageId, (byte)3);

        doCheckpoint(cpPages, memory, pageStoreMgr);

        byte[] data = pageStoreMgr.storedPages.get(cowPageId);

        for (int i = PageIO.COMMON_HEADER_END; i < PAGE_SIZE; i++)
            assertEquals(2, data[i]);
    }

    /**
     * @throws Exception if failed.
     */
    public void runThrottlingEmptifyCpBufFirst(PageMemoryImpl.ThrottlingPolicy plc) throws Exception {
        TestPageStoreManager pageStoreMgr = new TestPageStoreManager();

        final List<FullPageId> allocated = new ArrayList<>();

        int pagesForStartThrottling = 10;

        // Create a 1 mb page memory.
        PageMemoryImpl memory = createPageMemory(
            1,
            plc,
            pageStoreMgr,
            pageStoreMgr,
            new IgniteInClosure<FullPageId>() {
                @Override public void apply(FullPageId fullPageId) {
                    assertTrue(allocated.contains(fullPageId));
                }
            }
        );

        assert pagesForStartThrottling < memory.checkpointBufferPagesSize() / 3;

        for (int i = 0; i < pagesForStartThrottling + (memory.checkpointBufferPagesSize() * 2 / 3); i++) {
            long id = memory.allocatePage(1, INDEX_PARTITION, FLAG_IDX);

            FullPageId fullId = new FullPageId(id, 1);

            allocated.add(fullId);

            writePage(memory, fullId, (byte)1);
        }

        GridMultiCollectionWrapper<FullPageId> markedPages = memory.beginCheckpoint(new GridFinishedFuture());

        for (int i = 0; i < 10 + (memory.checkpointBufferPagesSize() * 2 / 3); i++)
            writePage(memory, allocated.get(i), (byte)1);

        doCheckpoint(markedPages, memory, pageStoreMgr);
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

            while (memory.shouldThrottle()) {
                FullPageId cpPageId = memory.pullPageFromCpBuffer();

                if (cpPageId.equals(FullPageId.NULL_PAGE))
                    break;

                ByteBuffer tmpWriteBuf = ByteBuffer.allocateDirect(memory.pageSize());

                tmpWriteBuf.order(ByteOrder.nativeOrder());

                tmpWriteBuf.rewind();

                memory.checkpointWritePage(cpPageId, tmpWriteBuf, pageStoreWriter, null);
            }
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
            pageStoreMgr,
            null);

        int initPageCnt = 500;

        List<FullPageId> allocated = new ArrayList<>(initPageCnt);

        for (int i = 0; i < initPageCnt; i++) {
            long id = memory.allocatePage(1, INDEX_PARTITION, FLAG_IDX);

            FullPageId fullId = new FullPageId(id, 1);
            allocated.add(fullId);

            writePage(memory, fullId, (byte)1);
        }

        // CP Write lock.
        memory.beginCheckpoint(new GridFinishedFuture());
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
            }, null);
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
        PageMemoryImpl memory = createPageMemory(plc, null);

        List<FullPageId> pages = new ArrayList<>();

        for (int i = 0; i < (MAX_SIZE - 10) * MB / PAGE_SIZE / 2; i++) {
            long pageId = memory.allocatePage(1, INDEX_PARTITION, FLAG_IDX);

            FullPageId fullPageId = new FullPageId(pageId, 1);

            pages.add(fullPageId);

            acquireAndReleaseWriteLock(memory, fullPageId);
        }

        memory.beginCheckpoint(new GridFinishedFuture());

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

        memory.beginCheckpoint(new GridFinishedFuture());

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

        LongAdderMetric totalThrottlingTime = U.field(memory.metrics(), "totalThrottlingTime");

        assertNotNull(totalThrottlingTime);

        assertTrue(totalThrottlingTime.value() > 0);
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
        PageMemoryImpl.ThrottlingPolicy throttlingPlc,
        @Nullable IgniteInClosure<FullPageId> cpBufChecker) throws Exception {
        return createPageMemory(
            MAX_SIZE,
            throttlingPlc,
            new NoOpPageStoreManager(),
            (fullPageId, byteBuf, tag) -> {
                assert false : "No page replacement (rotation with disk) should happen during the test";
            },
            cpBufChecker);
    }

    /**
     * @param throttlingPlc Throttling Policy.
     * @throws Exception If creating mock failed.
     */
    private PageMemoryImpl createPageMemory(
        int maxSize,
        PageMemoryImpl.ThrottlingPolicy throttlingPlc,
        IgnitePageStoreManager mgr,
        PageStoreWriter replaceWriter,
        @Nullable IgniteInClosure<FullPageId> cpBufChecker
    ) throws Exception {
        long[] sizes = new long[5];

        for (int i = 0; i < sizes.length; i++)
            sizes[i] = maxSize * MB / 4;

        sizes[4] = maxSize * MB / 4;

        DirectMemoryProvider provider = new UnsafeMemoryProvider(log);

        IgniteConfiguration igniteCfg = new IgniteConfiguration();
        igniteCfg.setDataStorageConfiguration(new DataStorageConfiguration());
        igniteCfg.setFailureHandler(new NoOpFailureHandler());
        igniteCfg.setEncryptionSpi(new NoopEncryptionSpi());
        igniteCfg.setMetricExporterSpi(new NoopMetricExporterSpi());
        igniteCfg.setSystemViewExporterSpi(new JmxSystemViewExporterSpi());
        igniteCfg.setEventStorageSpi(new NoopEventStorageSpi());

        GridTestKernalContext kernalCtx = new GridTestKernalContext(new GridTestLog4jLogger(), igniteCfg);

        kernalCtx.add(new IgnitePluginProcessor(kernalCtx, igniteCfg, Collections.<PluginProvider>emptyList()));
        kernalCtx.add(new GridInternalSubscriptionProcessor(kernalCtx));
        kernalCtx.add(new GridEncryptionManager(kernalCtx));
        kernalCtx.add(new GridMetricManager(kernalCtx));
        kernalCtx.add(new GridSystemViewManager(kernalCtx));
        kernalCtx.add(new GridEventStorageManager(kernalCtx));

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
            null,
            null
        );

        CheckpointProgressImpl cl0 = Mockito.mock(CheckpointProgressImpl.class);

        IgniteOutClosure<CheckpointProgress> noThrottle = Mockito.mock(IgniteOutClosure.class);
        Mockito.when(noThrottle.apply()).thenReturn(cl0);

        Mockito.when(cl0.currentCheckpointPagesCount()).thenReturn(1_000_000);
        Mockito.when(cl0.evictedPagesCounter()).thenReturn(new AtomicInteger(0));
        Mockito.when(cl0.syncedPagesCounter()).thenReturn(new AtomicInteger(1_000_000));
        Mockito.when(cl0.writtenPagesCounter()).thenReturn(new AtomicInteger(1_000_000));

        PageMemoryImpl mem = cpBufChecker == null ? new PageMemoryImpl(
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
        ) : new PageMemoryImpl(
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
        ) {
            @Override public FullPageId pullPageFromCpBuffer() {
                FullPageId pageId = super.pullPageFromCpBuffer();

                cpBufChecker.apply(pageId);

                return pageId;
            }
        };

        mem.metrics().enableMetrics();

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

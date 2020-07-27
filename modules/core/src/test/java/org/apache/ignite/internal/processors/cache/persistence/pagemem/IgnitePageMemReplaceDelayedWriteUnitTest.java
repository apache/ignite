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
package org.apache.ignite.internal.processors.cache.persistence.pagemem;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.managers.encryption.GridEncryptionManager;
import org.apache.ignite.internal.managers.eventstorage.GridEventStorageManager;
import org.apache.ignite.internal.mem.DirectMemoryProvider;
import org.apache.ignite.internal.mem.unsafe.UnsafeMemoryProvider;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.PageIdAllocator;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.persistence.DataRegionMetricsImpl;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.IgniteCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.PageStoreWriter;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointProgress;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointProgressImpl;
import org.apache.ignite.internal.processors.metric.GridMetricManager;
import org.apache.ignite.internal.processors.subscription.GridInternalSubscriptionProcessor;
import org.apache.ignite.internal.util.GridMultiCollectionWrapper;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteOutClosure;
import org.apache.ignite.logger.NullLogger;
import org.apache.ignite.spi.encryption.noop.NoopEncryptionSpi;
import org.apache.ignite.spi.eventstorage.NoopEventStorageSpi;
import org.apache.ignite.spi.metric.noop.NoopMetricExporterSpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static org.apache.ignite.internal.processors.database.DataRegionMetricsSelfTest.NO_OP_METRICS;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit test for delayed page replacement mode.
 */
public class IgnitePageMemReplaceDelayedWriteUnitTest {
    /** Per test timeout */
    @Rule
    public Timeout globalTimeout = new Timeout((int) GridTestUtils.DFLT_TEST_TIMEOUT);

    /** CPU count. */
    private static final int CPUS = 32;

    /** 1 megabyte in bytes */
    private static final long MB = 1024L * 1024;

    /** Logger. */
    private IgniteLogger log = new NullLogger();

    /** Page memory, published here for backward call from replacement write callback. */
    private PageMemoryImpl pageMemory;

    /**
     * Test delayed eviction causes locking in page reads
     * @throws IgniteCheckedException if failed.
     */
    @Test
    public void testReplacementWithDelayCausesLockForRead() throws IgniteCheckedException {
        IgniteConfiguration cfg = getConfiguration(16 * MB);

        AtomicInteger totalEvicted = new AtomicInteger();

        PageStoreWriter pageWriter = (FullPageId fullPageId, ByteBuffer byteBuf, int tag) -> {
            log.info("Evicting " + fullPageId);

            assert getLockedPages(fullPageId).contains(fullPageId);

            assert !getSegment(fullPageId).writeLock().isHeldByCurrentThread();

            totalEvicted.incrementAndGet();
        };

        int pageSize = 4096;
        PageMemoryImpl memory = createPageMemory(cfg, pageWriter, pageSize);

        this.pageMemory = memory;

        long pagesTotal = cfg.getDataStorageConfiguration().getDefaultDataRegionConfiguration().getMaxSize() / pageSize;
        long markDirty = pagesTotal * 2 / 3;
        for (int i = 0; i < markDirty; i++) {
            long pageId = memory.allocatePage(1, 1, PageIdAllocator.FLAG_DATA);
            long ptr = memory.acquirePage(1, pageId);

            memory.releasePage(1, pageId, ptr);
        }

        GridMultiCollectionWrapper<FullPageId> ids = memory.beginCheckpoint(new GridFinishedFuture());
        int cpPages = ids.size();
        log.info("Started CP with [" + cpPages + "] pages in it, created [" + markDirty + "] pages");

        for (int i = 0; i < cpPages; i++) {
            long pageId = memory.allocatePage(1, 1, PageIdAllocator.FLAG_DATA);
            long ptr = memory.acquirePage(1, pageId);
            memory.releasePage(1, pageId, ptr);
        }

        List<Collection<FullPageId>> stripes = getAllLockedPages();

        assert !stripes.isEmpty();

        for (Collection<FullPageId> pageIds : stripes) {
            assert pageIds.isEmpty();
        }

        assert totalEvicted.get() > 0;

        memory.stop(true);
    }

    /**
     * Test delayed eviction causes locking in page reads
     * @throws IgniteCheckedException if failed.
     */
    @Test
    public void testBackwardCompatibilityMode() throws IgniteCheckedException {
        IgniteConfiguration cfg = getConfiguration(16 * MB);

        AtomicInteger totalEvicted = new AtomicInteger();

        PageStoreWriter pageWriter = (FullPageId fullPageId, ByteBuffer byteBuf, int tag) -> {
            log.info("Evicting " + fullPageId);

            assert getSegment(fullPageId).writeLock().isHeldByCurrentThread();

            totalEvicted.incrementAndGet();
        };

        System.setProperty(IgniteSystemProperties.IGNITE_DELAYED_REPLACED_PAGE_WRITE, "false");
        int pageSize = 4096;
        PageMemoryImpl memory;

        try {
            memory = createPageMemory(cfg, pageWriter, pageSize);
        }
        finally {
            System.clearProperty(IgniteSystemProperties.IGNITE_DELAYED_REPLACED_PAGE_WRITE);
        }

        this.pageMemory = memory;

        long pagesTotal = cfg.getDataStorageConfiguration().getDefaultDataRegionConfiguration().getMaxSize() / pageSize;
        long markDirty = pagesTotal * 2 / 3;
        for (int i = 0; i < markDirty; i++) {
            long pageId = memory.allocatePage(1, 1, PageIdAllocator.FLAG_DATA);
            long ptr = memory.acquirePage(1, pageId);

            memory.releasePage(1, pageId, ptr);
        }

        GridMultiCollectionWrapper<FullPageId> ids = memory.beginCheckpoint(new GridFinishedFuture());
        int cpPages = ids.size();
        log.info("Started CP with [" + cpPages + "] pages in it, created [" + markDirty + "] pages");

        for (int i = 0; i < cpPages; i++) {
            long pageId = memory.allocatePage(1, 1, PageIdAllocator.FLAG_DATA);
            long ptr = memory.acquirePage(1, pageId);
            memory.releasePage(1, pageId, ptr);
        }

        assert totalEvicted.get() > 0;

        memory.stop(true);
    }

    /**
     * @param fullPageId page ID to determine segment for
     * @return segment related
     */
    private ReentrantReadWriteLock getSegment(FullPageId fullPageId) {
        ReentrantReadWriteLock[] segments = U.field(pageMemory, "segments");

        int idx = PageMemoryImpl.segmentIndex(fullPageId.groupId(), fullPageId.pageId(),
            segments.length);

        return segments[idx];
    }

    /**
     * @param cfg configuration
     * @param pageWriter writer for page replacement.
     * @param pageSize page size
     * @return implementation for test
     */
    @NotNull
    private PageMemoryImpl createPageMemory(IgniteConfiguration cfg, PageStoreWriter pageWriter, int pageSize) {
        IgniteCacheDatabaseSharedManager db = mock(GridCacheDatabaseSharedManager.class);

        when(db.checkpointLockIsHeldByThread()).thenReturn(true);

        GridCacheSharedContext sctx = Mockito.mock(GridCacheSharedContext.class);

        when(sctx.gridConfig()).thenReturn(cfg);
        when(sctx.pageStore()).thenReturn(new NoOpPageStoreManager());
        when(sctx.wal()).thenReturn(new NoOpWALManager());
        when(sctx.database()).thenReturn(db);
        when(sctx.logger(any(Class.class))).thenReturn(log);

        GridKernalContext kernalCtx = mock(GridKernalContext.class);

        when(kernalCtx.config()).thenReturn(cfg);
        when(kernalCtx.log(any(Class.class))).thenReturn(log);
        when(kernalCtx.internalSubscriptionProcessor()).thenAnswer(new Answer<Object>() {
            @Override public Object answer(InvocationOnMock mock) throws Throwable {
                return new GridInternalSubscriptionProcessor(kernalCtx);
            }
        });
        when(kernalCtx.encryption()).thenAnswer(new Answer<Object>() {
            @Override public Object answer(InvocationOnMock mock) throws Throwable {
                return new GridEncryptionManager(kernalCtx);
            }
        });
        when(kernalCtx.metric()).thenAnswer(new Answer<Object>() {
            @Override public Object answer(InvocationOnMock mock) throws Throwable {
                return new GridMetricManager(kernalCtx);
            }
        });

        when(sctx.kernalContext()).thenReturn(kernalCtx);

        when(sctx.gridEvents()).thenAnswer(new Answer<Object>() {
            @Override public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                return new GridEventStorageManager(kernalCtx);
            }
        });

        DataRegionConfiguration regCfg = cfg.getDataStorageConfiguration().getDefaultDataRegionConfiguration();

        DataRegionMetricsImpl memMetrics = new DataRegionMetricsImpl(regCfg, kernalCtx.metric(), NO_OP_METRICS);

        long[] sizes = prepareSegmentSizes(regCfg.getMaxSize());

        DirectMemoryProvider provider = new UnsafeMemoryProvider(log);

        IgniteOutClosure<CheckpointProgress> clo = new IgniteOutClosure<CheckpointProgress>() {
            @Override public CheckpointProgress apply() {
                return Mockito.mock(CheckpointProgressImpl.class);
            }
        };

        PageMemoryImpl memory = new PageMemoryImpl(provider, sizes, sctx, pageSize,
            pageWriter, null, () -> true, memMetrics, PageMemoryImpl.ThrottlingPolicy.DISABLED,
            clo);

        memory.start();
        return memory;
    }

    /**
     * @param overallSize default region size in bytes
     * @return configuration for test.
     */
    @NotNull private IgniteConfiguration getConfiguration(long overallSize) {
        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setEncryptionSpi(new NoopEncryptionSpi());

        cfg.setMetricExporterSpi(new NoopMetricExporterSpi());

        cfg.setEventStorageSpi(new NoopEventStorageSpi());

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration()
                        .setPersistenceEnabled(true)
                        .setMaxSize(overallSize)));

        return cfg;
    }

    /**
     * @return collection with pages locked
     * @param fullPageId page id to check lock.
     */
    private Collection<FullPageId> getLockedPages(FullPageId fullPageId) {
        Object tracker = delayedReplacementTracker();

        Object[] stripes = U.field(tracker, "stripes");

        int idx = PageMemoryImpl.segmentIndex(fullPageId.groupId(), fullPageId.pageId(),
            stripes.length);

        return U.field(stripes[idx], "locked");
    }

    /**
     * @return delayed write tracked from page memory.
     */
    @NotNull private Object delayedReplacementTracker() {
        Object tracker = U.field(pageMemory, "delayedPageReplacementTracker");

        if (tracker == null)
            throw new IllegalStateException("Delayed replacement is not configured");

        return tracker;
    }

    /**
     * @return all locked pages stripes underlying collectinos
     */
    private List<Collection<FullPageId>> getAllLockedPages() {
        Object tracker = delayedReplacementTracker();

        Object[] stripes = U.field(tracker, "stripes");

        Stream<Collection<FullPageId>> locked = Arrays.asList(stripes).stream().map(stripe ->
            (Collection<FullPageId>)U.field(stripe, "locked"));

        return locked.collect(Collectors.toList());
    }

    /**
     * @param overallSize all regions size
     * @return segments size, cp pool is latest array element.
     */
    private long[] prepareSegmentSizes(long overallSize) {
        int segments = CPUS;
        long[] sizes = new long[segments + 1];

        for (int i = 0; i < sizes.length; i++)
            sizes[i] = overallSize / segments;

        sizes[segments] = overallSize / 100;

        return sizes;
    }
}

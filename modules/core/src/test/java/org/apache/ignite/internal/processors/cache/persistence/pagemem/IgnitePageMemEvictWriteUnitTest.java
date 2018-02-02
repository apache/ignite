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
import java.util.Set;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.mem.DirectMemoryProvider;
import org.apache.ignite.internal.mem.unsafe.UnsafeMemoryProvider;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.PageIdAllocator;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.persistence.DataRegionMetricsImpl;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.IgniteCacheDatabaseSharedManager;
import org.apache.ignite.internal.util.GridMultiCollectionWrapper;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.logger.NullLogger;
import org.junit.Test;
import org.mockito.Mockito;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 *
 */
public class IgnitePageMemEvictWriteUnitTest {

    private static final int CPUS = Runtime.getRuntime().availableProcessors();
    private static final long MB = 1024L * 1024;
    private IgniteLogger log = new NullLogger();
    private PageMemoryImpl pageMemory;

    @Test
    public void testAllocatePages() throws IgniteCheckedException {
        long overallSize = 1 * MB;

        long[] sizes = prepareSegmentSizes(overallSize);

        IgniteConfiguration cfg = new IgniteConfiguration();
        DataStorageConfiguration dsCfg = new DataStorageConfiguration();
        DataRegionConfiguration regCfg = new DataRegionConfiguration();
        regCfg.setPersistenceEnabled(true);
        regCfg.setMaxSize(overallSize);
        dsCfg.setDefaultDataRegionConfiguration(regCfg);
        cfg.setDataStorageConfiguration(dsCfg);

        IgniteCacheDatabaseSharedManager db = mock(GridCacheDatabaseSharedManager.class);
        when(db.checkpointLockIsHeldByThread()).thenReturn(true);

        GridCacheSharedContext sctx = Mockito.mock(GridCacheSharedContext.class);
        when(sctx.pageStore()).thenReturn(new NoOpPageStoreManager());
        when(sctx.wal()).thenReturn(new NoOpWALManager());
        when(sctx.database()).thenReturn(db);
        when(sctx.logger(any(Class.class))).thenReturn(log);

        GridKernalContext kernalCtx = mock(GridKernalContext.class);
        when(kernalCtx.config()).thenReturn(cfg);
        when(sctx.kernalContext()).thenReturn(kernalCtx);

        DataRegionMetricsImpl memMetrics = new DataRegionMetricsImpl(regCfg);
        int pageSize = 4096;
        EvictedPageWriter pageWriter = (FullPageId fullPageId, ByteBuffer byteBuf, int tag)->{
            System.err.println("Evicting " + fullPageId);

            Object tracker = U.field(pageMemory, "delayedPageEvictionTracker");

            Set<FullPageId> locked = U.field(tracker, "locked");

            assert locked.contains(fullPageId);
        };

        DirectMemoryProvider provider = new UnsafeMemoryProvider(log);
        PageMemoryImpl memory
            = new PageMemoryImpl(provider, sizes, sctx, pageSize,
            pageWriter, null, null, memMetrics, null);

        memory.start();

        this.pageMemory = memory;

        long pagesTotal = overallSize / pageSize;
        long markDirty = pagesTotal * 2 / 3;
        for (int i = 0; i < markDirty; i++) {
            long pageId = memory.allocatePage(1, 1, PageIdAllocator.FLAG_DATA);
            long ptr = memory.acquirePage(1, pageId);

            memory.releasePage(1, pageId, ptr);
        }

        GridMultiCollectionWrapper<FullPageId> ids = memory.beginCheckpoint();
        int cpPages = ids.size();
        System.err.println("Started CP with [" + cpPages + "] pages in it, created [" + markDirty + "] pages");

        for (int i = 0; i < cpPages; i++) {
            long pageId = memory.allocatePage(1, 1, PageIdAllocator.FLAG_DATA);
            long ptr = memory.acquirePage(1, pageId);
            memory.releasePage(1, pageId, ptr);
        }

        Object tracker = U.field(pageMemory, "delayedPageEvictionTracker");

        Set<FullPageId> locked = U.field(tracker, "locked");

        assert locked.isEmpty();
    }

    private long[] prepareSegmentSizes(long overallSize) {
        int segments = CPUS;
        long[] sizes = new long[segments + 1];

        for (int i = 0; i < sizes.length; i++)
            sizes[i] = overallSize / segments;

        sizes[segments] = overallSize / 100;
        return sizes;
    }
}

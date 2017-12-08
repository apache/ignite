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
import org.apache.ignite.configuration.MemoryPolicyConfiguration;
import org.apache.ignite.internal.mem.DirectMemoryProvider;
import org.apache.ignite.internal.mem.IgniteOutOfMemoryException;
import org.apache.ignite.internal.mem.unsafe.UnsafeMemoryProvider;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.PageIdAllocator;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.persistence.CheckpointLockStateChecker;
import org.apache.ignite.internal.processors.cache.persistence.IgniteCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.MemoryMetricsImpl;
import org.apache.ignite.internal.util.lang.GridInClosure3X;
import org.apache.ignite.internal.util.typedef.CIX3;
import org.apache.ignite.testframework.junits.GridTestKernalContext;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.logger.GridTestLog4jLogger;

/**
 *
 */
public class PageMemoryImplTest extends GridCommonAbstractTest {
    /** Mb. */
    private static final long MB = 1024 * 1024;

    /** Page size. */
    private static final int PAGE_SIZE = 1024;

    /**
     * @throws Exception if failed.
     */
    public void testThatAllocationTooMuchPagesCauseToOOMException() throws Exception {
        PageMemoryImpl memory = createPageMemory();

        try {
            while (!Thread.currentThread().isInterrupted())
                memory.allocatePage(1, PageIdAllocator.INDEX_PARTITION, PageIdAllocator.FLAG_IDX);
        }
        catch (IgniteOutOfMemoryException ignore) {
            //Success
        }

        assertFalse(memory.safeToUpdate());
    }

    /**
     *
     */
    private PageMemoryImpl createPageMemory() throws Exception {
        long[] sizes = new long[5];

        for (int i = 0; i < sizes.length; i++)
            sizes[i] = 1024 * MB / 4;

        sizes[4] = 10 * MB;

        DirectMemoryProvider provider = new UnsafeMemoryProvider(log);

        GridCacheSharedContext<Object, Object> sharedCtx = new GridCacheSharedContext<>(
            new GridTestKernalContext(new GridTestLog4jLogger()),
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

        PageMemoryImpl mem = new PageMemoryImpl(
            provider,
            sizes,
            sharedCtx,
            PAGE_SIZE,
            new CIX3<FullPageId, ByteBuffer, Integer>() {
                @Override public void applyx(FullPageId fullPageId, ByteBuffer byteBuf, Integer tag) {
                    assert false : "No evictions should happen during the test";
                }
            },
            new GridInClosure3X<Long, FullPageId, PageMemoryEx>() {
                @Override public void applyx(Long page, FullPageId fullId, PageMemoryEx pageMem) {
                }
            }, new CheckpointLockStateChecker() {
                @Override public boolean checkpointLockIsHeldByThread() {
                    return true;
                }
            },
            new MemoryMetricsImpl(new MemoryPolicyConfiguration()),
            false
        );

        mem.start();

        return mem;
    }
}

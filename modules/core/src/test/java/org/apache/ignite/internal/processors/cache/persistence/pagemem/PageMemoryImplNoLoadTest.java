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

import java.io.File;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.internal.mem.DirectMemoryProvider;
import org.apache.ignite.internal.mem.file.MappedFileMemoryProvider;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.pagemem.impl.PageMemoryNoLoadSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.persistence.CheckpointLockStateChecker;
import org.apache.ignite.internal.processors.cache.persistence.DataRegionMetricsImpl;
import org.apache.ignite.internal.processors.cache.persistence.IgniteCacheDatabaseSharedManager;
import org.apache.ignite.internal.util.lang.GridInClosure3X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.GridTestKernalContext;

/**
 *
 */
public class PageMemoryImplNoLoadTest extends PageMemoryNoLoadSelfTest {
    /**
     * @return Page memory implementation.
     */
    @Override protected PageMemory memory() throws Exception {
        File memDir = U.resolveWorkDirectory(U.defaultWorkDirectory(), "pagemem", false);

        long[] sizes = new long[10];

        for (int i = 0; i < sizes.length; i++)
            sizes[i] = 5 * 1024 * 1024;

        DirectMemoryProvider provider = new MappedFileMemoryProvider(log(), memDir);

        GridCacheSharedContext<Object, Object> sharedCtx = new GridCacheSharedContext<>(
            new GridTestKernalContext(log),
            null,
            null,
            null,
            new NoOpPageStoreManager(),
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
            null
        );

        return new PageMemoryImpl(
            provider,
            sizes,
            sharedCtx,
            PAGE_SIZE,
            (fullPageId, byteBuf, tag) -> {
                assert false : "No page replacement (rotation with disk) should happen during the test";
            },
            new GridInClosure3X<Long, FullPageId, PageMemoryEx>() {
                @Override public void applyx(Long page, FullPageId fullId, PageMemoryEx pageMem) {
                }
            },
            new CheckpointLockStateChecker() {
                @Override public boolean checkpointLockIsHeldByThread() {
                    return true;
                }
            },
            new DataRegionMetricsImpl(new DataRegionConfiguration()),
            PageMemoryImpl.ThrottlingPolicy.NONE, null
        );
    }

    /** {@inheritDoc} */
    @Override public void testPageHandleDeallocation() throws Exception {
        // No-op.
    }
}

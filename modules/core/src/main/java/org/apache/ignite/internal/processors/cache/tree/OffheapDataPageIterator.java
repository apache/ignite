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

package org.apache.ignite.internal.processors.cache.tree;

import java.util.function.LongConsumer;
import java.util.function.LongFunction;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.metric.IoStatisticsHolder;
import org.apache.ignite.internal.metric.IoStatisticsHolderNoOp;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.processors.cache.AbstractDataPageIterator;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.IncompleteObject;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRowAdapter;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMemoryEx;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTreeRuntimeException;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO.T_DATA;

/** */
public class OffheapDataPageIterator extends AbstractDataPageIterator {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** */
    private final PageMemory pageMem;

    /** */
    private final int grpId;

    /** */
    private final IoStatisticsHolder statHolder;

    /**
     * @param sctx Shared context.
     * @param pageMem Page memory.
     * @param pages Pages count.
     * @param pageSize
     * @param partId Partition id.
     */
    protected OffheapDataPageIterator(
        GridCacheSharedContext<?, ?> sctx,
        CacheDataRowAdapter.RowData rowData,
        PageMemory pageMem,
        @Nullable CacheGroupContext gctx,
        int pages,
        int pageSize,
        int partId
    ) {
        super(sctx,
            gctx == null ? null : gctx.cacheObjectContext(),
            rowData,
            gctx == null || gctx.storeCacheIdInDataPage(),
            CacheDataRowStore.getSkipVersion(), pages, pageSize, partId);

        // TODO add realPageSize param
        this.pageMem = pageMem;

        grpId = gctx == null ? 0 : gctx.groupId();
        statHolder = gctx == null ? IoStatisticsHolderNoOp.INSTANCE : gctx.statisticsHolderData();

    }

    /** {@inheritDoc}
     * @return*/
    @Override public boolean readHeaderPage(long pageId, LongConsumer reader) throws IgniteCheckedException {
        long page = pageMem.acquirePage(grpId, pageId);

        try {
            long pageAddr = ((PageMemoryEx)pageMem).readLock(page, pageId, true, false);

            try {
                if (PageIO.getType(pageAddr) != T_DATA)
                    return false;

                reader.accept(pageAddr);

                return true;
            }
            finally {
                pageMem.readUnlock(grpId, pageId, page);
            }
        }
        finally {
            pageMem.releasePage(grpId, pageId, page);
        }
    }

    /** {@inheritDoc} */
    @Override public IncompleteObject<?> readFragmentPage(
        long pageId,
        LongFunction<IncompleteObject<?>> reader
    ) throws IgniteCheckedException {
        try {
            final long page = pageMem.acquirePage(grpId, pageId, statHolder);

            try {
                long pageAddr = pageMem.readLock(grpId, pageId, page); // Non-empty data page must not be recycled.

                assert pageAddr != 0L : pageId;

                try {
                    return reader.apply(pageAddr);
                }
                finally {
                    pageMem.readUnlock(grpId, pageId, page);
                }
            }
            finally {
                pageMem.releasePage(grpId, pageId, page);
            }
        }
        catch (RuntimeException | AssertionError e) {
            throw new BPlusTreeRuntimeException(e, grpId, pageId);
        }
    }
}

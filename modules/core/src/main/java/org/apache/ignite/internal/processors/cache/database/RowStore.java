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

package org.apache.ignite.internal.processors.cache.database;

import java.nio.ByteBuffer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.Page;
import org.apache.ignite.internal.pagemem.PageIdAllocator;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.database.freelist.FreeList;
import org.apache.ignite.internal.processors.cache.database.tree.io.DataPageIO;
import org.apache.ignite.internal.processors.cache.database.tree.util.PageHandler;

import static org.apache.ignite.internal.pagemem.PageIdUtils.dwordsOffset;
import static org.apache.ignite.internal.pagemem.PageIdUtils.linkFromDwordOffset;
import static org.apache.ignite.internal.pagemem.PageIdUtils.pageId;
import static org.apache.ignite.internal.processors.cache.database.tree.util.PageHandler.writePage;

/**
 * Data store for H2 rows.
 */
public class RowStore<T extends CacheDataRow> {
    /** */
    protected final FreeList freeList;

    /** */
    protected final PageMemory pageMem;

    /** */
    protected final GridCacheContext<?,?> cctx;

    /** */
    protected final CacheObjectContext coctx;

    /** */
    private volatile long lastDataPageId;

    /** */
    @Deprecated
    private final PageHandler<CacheDataRow> writeRow = new PageHandler<CacheDataRow>() {
        @Override public int run(Page page, ByteBuffer buf, CacheDataRow row, int ignore) throws IgniteCheckedException {
            int entrySize = DataPageIO.getEntrySize(coctx, row.key(), row.value());

            DataPageIO io = DataPageIO.VERSIONS.forPage(buf);

            if (io.isEnoughSpace(buf, entrySize))
                return -1;

            int idx = io.addRow(coctx, buf, row.key(), row.value(), row.version(), entrySize);

            assert idx >= 0: idx;

            row.link(linkFromDwordOffset(page.id(), idx));

            assert row.link() != 0;

            return idx;
        }
    };

    /** */
    @Deprecated
    private final PageHandler<Void> rmvRow = new PageHandler<Void>() {
        @Override public int run(Page page, ByteBuffer buf, Void ignore, int itemId) throws IgniteCheckedException {
            DataPageIO io = DataPageIO.VERSIONS.forPage(buf);

            assert DataPageIO.check(itemId): itemId;

            io.removeRow(buf, (byte)itemId);

            return 0;
        }
    };

    /**
     * @param cctx Cache context.
     */
    public RowStore(GridCacheContext<?,?> cctx, FreeList freeList) {
        assert cctx != null;
        assert freeList != null;

        this.cctx = cctx;
        this.freeList = freeList;

        coctx = cctx.cacheObjectContext();
        pageMem = cctx.shared().database().pageMemory();
    }

    /**
     * @param pageId Page ID.
     * @return Page.
     * @throws IgniteCheckedException If failed.
     */
    protected final Page page(long pageId) throws IgniteCheckedException {
        return pageMem.page(new FullPageId(pageId, cctx.cacheId()));
    }

    /**
     * @param part Partition.
     * @return Allocated page.
     * @throws IgniteCheckedException if failed.
     */
    private Page allocatePage(int part) throws IgniteCheckedException {
        FullPageId fullPageId = pageMem.allocatePage(cctx.cacheId(), part, PageIdAllocator.FLAG_DATA);

        return pageMem.page(fullPageId);
    }

    /**
     * @param link Row link.
     * @throws IgniteCheckedException If failed.
     */
    public void removeRow(long link) throws IgniteCheckedException {
        assert link != 0;

        if (freeList == null) {
            try (Page page = page(pageId(link))) {
                writePage(page, rmvRow, null, dwordsOffset(link), 0);
            }
        }
        else
            freeList.removeRow(link);
    }

    /**
     * @param expLastDataPageId Expected last data page ID.
     * @return Next data page ID.
     */
    private synchronized long nextDataPage(long expLastDataPageId, int partId) throws IgniteCheckedException {
        if (expLastDataPageId != lastDataPageId)
            return lastDataPageId;

        long pageId;

        try (Page page = allocatePage(partId)) {
            pageId = page.id();

            ByteBuffer buf = page.getForInitialWrite();

            DataPageIO.VERSIONS.latest().initNewPage(buf, page.id());
        }

        return lastDataPageId = pageId;
    }

    /**
     * @param row Row.
     */
    public void addRow(CacheDataRow row) throws IgniteCheckedException {
        if (freeList == null)
            writeRowDataOld(row);
        else
            freeList.insertRow(row);
    }

    /**
     * @param row Row.
     * @throws IgniteCheckedException If failed.
     */
    @Deprecated
    private void writeRowDataOld(CacheDataRow row) throws IgniteCheckedException {
        assert row.link() == 0;

        while (row.link() == 0) {
            long pageId = lastDataPageId;

            if (pageId == 0)
                pageId = nextDataPage(0, row.partition());

            try (Page page = page(pageId)) {
                if (writePage(page, writeRow, row, -1, -1) >= 0)
                    return; // Successful write.
            }

            nextDataPage(pageId, row.partition());
        }
    }
}

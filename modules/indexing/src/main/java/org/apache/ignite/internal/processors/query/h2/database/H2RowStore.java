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

package org.apache.ignite.internal.processors.query.h2.database;

import java.nio.ByteBuffer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.Page;
import org.apache.ignite.internal.pagemem.PageIdAllocator;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.query.h2.database.io.BPlusIO;
import org.apache.ignite.internal.processors.query.h2.database.io.DataPageIO;
import org.apache.ignite.internal.processors.query.h2.database.io.H2RowLinkIO;
import org.apache.ignite.internal.processors.query.h2.database.util.PageHandler;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Row;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2RowDescriptor;

import static org.apache.ignite.internal.pagemem.PageIdUtils.dwordsOffset;
import static org.apache.ignite.internal.pagemem.PageIdUtils.linkFromDwordOffset;
import static org.apache.ignite.internal.pagemem.PageIdUtils.pageId;
import static org.apache.ignite.internal.processors.query.h2.database.util.PageHandler.writePage;

/**
 * Data store for H2 rows.
 */
public class H2RowStore implements DataStore<GridH2Row> {
    /** */
    private final PageMemory pageMem;

    /** */
    private final GridH2RowDescriptor rowDesc;

    /** */
    private final GridCacheContext<?,?> cctx;

    /** */
    private final CacheObjectContext coctx;

    /** */
    private volatile long lastDataPageId;

    /** */
    private final PageHandler<GridH2Row> writeRow = new PageHandler<GridH2Row>() {
        @Override public int run(Page page, ByteBuffer buf, GridH2Row row, int ignore) throws IgniteCheckedException {
            DataPageIO io = DataPageIO.VERSIONS.forPage(buf);

            int idx = io.addRow(coctx, buf, row.key, row.val, row.ver);

            if (idx != -1) {
                row.link = linkFromDwordOffset(page.id(), idx);

                assert row.link != 0;
            }

            return idx;
        }
    };

    /**
     * @param pageMem Page memory.
     * @param rowDesc Row descriptor.
     * @param cctx Cache context.
     */
    public H2RowStore(PageMemory pageMem, GridH2RowDescriptor rowDesc, GridCacheContext<?,?> cctx) {
        this.pageMem = pageMem;
        this.rowDesc = rowDesc;
        this.cctx = cctx;
        this.coctx = cctx.cacheObjectContext();
    }

    /** {@inheritDoc} */
    @Override public GridH2Row getRow(BPlusIO<?> io, ByteBuffer buf, int idx) throws IgniteCheckedException {
        long link = ((H2RowLinkIO)io).getLink(buf, idx);

        return getRow(link);
    }

    /**
     * @param pageId Page ID.
     * @return Page.
     * @throws IgniteCheckedException If failed.
     */
    private Page page(long pageId) throws IgniteCheckedException {
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
     * !!! This method must be invoked in read or write lock of referring index page. It is needed to
     * !!! make sure that row at this link will be invisible, when the link will be removed from
     * !!! from all the index pages, so that row can be safely erased from the data page.
     *
     * @param link Link.
     * @return Row.
     */
    private GridH2Row getRow(long link) throws IgniteCheckedException {
        try (Page page = page(pageId(link))) {
            ByteBuffer buf = page.getForRead();

            try {
                GridH2Row existing = rowDesc.cachedRow(link);

                if (existing != null)
                    return existing;

                DataPageIO io = DataPageIO.VERSIONS.forPage(buf);

                int dataOff = io.getDataOffset(buf, dwordsOffset(link));

                buf.position(dataOff);

                // Skip key-value size.
                buf.getShort();

                CacheObject key = coctx.processor().toCacheObject(coctx, buf);
                CacheObject val = coctx.processor().toCacheObject(coctx, buf);

                int topVer = buf.getInt();
                int nodeOrderDrId = buf.getInt();
                long globalTime = buf.getLong();
                long order = buf.getLong();

                GridCacheVersion ver = new GridCacheVersion(topVer, nodeOrderDrId, globalTime, order);

                GridH2Row res;

                try {
                    res = rowDesc.createRow(key, PageIdUtils.partId(link), val, ver, 0);

                    res.link = link;
                }
                catch (IgniteCheckedException e) {
                    throw new IgniteException(e);
                }

                assert res.ver != null;

                rowDesc.cache(res);

                return res;
            }
            finally {
                page.releaseRead();
            }
        }
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
    public void writeRowData(GridH2Row row) throws IgniteCheckedException {
        assert row.link == 0;

        while (row.link == 0) {
            long pageId = lastDataPageId;

            if (pageId == 0)
                pageId = nextDataPage(0, row.partId);

            try (Page page = page(pageId)) {
                if (writePage(page, writeRow, row, -1, -1) >= 0)
                    return; // Successful write.
            }

            nextDataPage(pageId, row.partId);
        }
    }
}

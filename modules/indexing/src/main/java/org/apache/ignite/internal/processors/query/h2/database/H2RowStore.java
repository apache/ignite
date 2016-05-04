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
import org.apache.ignite.internal.pagemem.Page;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.database.RowStore;
import org.apache.ignite.internal.processors.cache.database.freelist.FreeList;
import org.apache.ignite.internal.processors.cache.database.tree.io.DataPageIO;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Row;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2RowDescriptor;

import static org.apache.ignite.internal.pagemem.PageIdUtils.dwordsOffset;
import static org.apache.ignite.internal.pagemem.PageIdUtils.pageId;

/**
 * Data store for H2 rows.
 */
public class H2RowStore extends RowStore<GridH2Row> {
    /** */
    private final GridH2RowDescriptor rowDesc;

    /**
     * @param rowDesc Row descriptor.
     * @param cctx Cache context.
     * @param freeList Free list.
     */
    public H2RowStore(GridH2RowDescriptor rowDesc, GridCacheContext<?,?> cctx, FreeList freeList) {
        super(cctx, freeList);

        this.rowDesc = rowDesc;
    }

    /**
     * !!! This method must be invoked in read or write lock of referring index page. It is needed to
     * !!! make sure that row at this link will be invisible, when the link will be removed from
     * !!! from all the index pages, so that row can be safely erased from the data page.
     *
     * @param link Link.
     * @return Row.
     * @throws IgniteCheckedException If failed.
     */
    public GridH2Row getRow(long link) throws IgniteCheckedException {
        try (Page page = page(pageId(link))) {
            ByteBuffer buf = page.getForRead();

            try {
                DataPageIO io = DataPageIO.VERSIONS.forPage(buf);

                int dataOff = io.getDataOffset(buf, dwordsOffset(link));

                buf.position(dataOff);

                // Skip entry size.
                buf.getShort();

                KeyCacheObject key = coctx.processor().toKeyCacheObject(coctx, buf);
                CacheObject val = coctx.processor().toCacheObject(coctx, buf);

                int topVer = buf.getInt();
                int nodeOrderDrId = buf.getInt();
                long globalTime = buf.getLong();
                long order = buf.getLong();

                GridCacheVersion ver = new GridCacheVersion(topVer, nodeOrderDrId, globalTime, order);

                GridH2Row row;

                try {
                    row = rowDesc.createRow(key, PageIdUtils.partId(link), val, ver, 0);

                    row.link = link;
                }
                catch (IgniteCheckedException e) {
                    throw new IgniteException(e);
                }

                assert row.ver != null;

                return row;
            }
            finally {
                page.releaseRead();
            }
        }
    }
}

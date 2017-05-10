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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.database.CacheDataRowAdapter;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Row;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2RowDescriptor;

/**
 * Data store for H2 rows.
 */
public class H2RowFactory {
    /** */
    private final GridCacheContext<?,?> cctx;

    /** */
    private final GridH2RowDescriptor rowDesc;

    /**
     * @param rowDesc Row descriptor.
     * @param cctx Cache context.
     */
    public H2RowFactory(GridH2RowDescriptor rowDesc, GridCacheContext<?,?> cctx) {
        this.rowDesc = rowDesc;
        this.cctx = cctx;
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
        // TODO Avoid extra garbage generation. In upcoming H2 1.4.193 Row will become an interface,
        // TODO we need to refactor all this to return CacheDataRowAdapter implementing Row here.

        final CacheDataRowAdapter rowBuilder = new CacheDataRowAdapter(link);

        rowBuilder.initFromLink(cctx.group(), CacheDataRowAdapter.RowData.FULL);

        GridH2Row row;

        try {
            row = rowDesc.createRow(rowBuilder.key(),
                PageIdUtils.partId(link), rowBuilder.value(), rowBuilder.version(), rowBuilder.expireTime());

            row.link = link;
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }

        assert row.ver != null;

        return row;
    }
}

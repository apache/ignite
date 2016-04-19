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
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.database.RowStore;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.cache.database.freelist.FreeList;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Row;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2RowDescriptor;

/**
 * Data store for H2 rows.
 */
public class H2RowStore extends RowStore<GridH2Row> {
    /**
     * @param rowDesc Row descriptor.
     * @param cctx Cache context.
     * @param freeList Free list.
     */
    public H2RowStore(GridH2RowDescriptor rowDesc, GridCacheContext<?,?> cctx, FreeList freeList) {
        super(cctx, new H2RowFactory(rowDesc), freeList);
    }
    /**
     *
     */
    static class H2RowFactory implements RowFactory<GridH2Row> {
        /** */
        private final GridH2RowDescriptor rowDesc;

        /**
         * @param rowDesc Row descriptor.
         */
        public H2RowFactory(GridH2RowDescriptor rowDesc) {
            assert rowDesc != null;

            this.rowDesc = rowDesc;
        }

        /** {@inheritDoc} */
        @Override public GridH2Row cachedRow(long link) {
            return rowDesc.cachedRow(link);
        }

        /** {@inheritDoc} */
        @Override public GridH2Row createRow(CacheObject key,
            CacheObject val,
            GridCacheVersion ver,
            long link,
            long expirationTime) throws IgniteCheckedException {
            GridH2Row row = rowDesc.createRow(key, PageIdUtils.partId(link), val, ver, 0);

            row.link = link;

            rowDesc.cache(row);

            return row;
        }
    }
}

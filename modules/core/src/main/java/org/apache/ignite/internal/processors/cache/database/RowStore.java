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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.database.freelist.FreeList;

/**
 * Data store for H2 rows.
 */
public class RowStore {
    /** */
    private final FreeList freeList;

    /** */
    protected final PageMemory pageMem;

    /** */
    protected final GridCacheContext<?,?> cctx;

    /** */
    protected final CacheObjectContext coctx;

    /**
     * @param cctx Cache context.
     * @param freeList Free list.
     */
    public RowStore(GridCacheContext<?,?> cctx, FreeList freeList) {
        assert cctx != null;
        assert freeList != null;

        this.cctx = cctx;
        this.freeList = freeList;

        coctx = cctx.cacheObjectContext();
        pageMem = cctx.memoryPolicy().pageMemory();
    }

    /**
     * @param link Row link.
     * @throws IgniteCheckedException If failed.
     */
    public void removeRow(long link) throws IgniteCheckedException {
        assert link != 0;

        freeList.removeDataRowByLink(link);
    }

    /**
     * @param row Row.
     * @throws IgniteCheckedException If failed.
     */
    public void addRow(CacheDataRow row) throws IgniteCheckedException {
        freeList.insertDataRow(row);
    }

    /**
     * @param link Row link.
     * @param row New row data.
     * @throws IgniteCheckedException If failed.
     * @return {@code True} if was able to update row.
     */
    public boolean updateRow(long link, CacheDataRow row) throws IgniteCheckedException {
        return freeList.updateDataRow(link, row);
    }

    /**
     * @return Free list.
     */
    public FreeList freeList() {
        return freeList;
    }
}

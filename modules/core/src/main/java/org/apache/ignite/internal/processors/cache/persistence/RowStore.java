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

package org.apache.ignite.internal.processors.cache.persistence;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.persistence.freelist.FreeList;
import org.apache.ignite.internal.processors.cache.persistence.tree.util.PageHandler;
import org.apache.ignite.internal.processors.query.GridQueryRowCacheCleaner;

/**
 * Data store for H2 rows.
 */
public class RowStore {
    /** */
    private final FreeList freeList;

    /** */
    private final GridCacheSharedContext ctx;

    /** */
    protected final PageMemory pageMem;

    /** */
    protected final CacheObjectContext coctx;

    /** */
    private final boolean persistenceEnabled;

    /** Row cache cleaner. */
    private GridQueryRowCacheCleaner rowCacheCleaner;

    /**
     * @param grp Cache group.
     * @param freeList Free list.
     */
    public RowStore(CacheGroupContext grp, FreeList freeList) {
        assert grp != null;
        assert freeList != null;

        this.freeList = freeList;

        ctx = grp.shared();
        coctx = grp.cacheObjectContext();
        pageMem = grp.dataRegion().pageMemory();

        persistenceEnabled = grp.dataRegion().config().isPersistenceEnabled();
    }

    /**
     * @param link Row link.
     * @throws IgniteCheckedException If failed.
     */
    public void removeRow(long link) throws IgniteCheckedException {
        assert link != 0;

        if (rowCacheCleaner != null)
            rowCacheCleaner.remove(link);

        if (!persistenceEnabled)
            freeList.removeDataRowByLink(link);
        else {
            ctx.database().checkpointReadLock();

            try {
                freeList.removeDataRowByLink(link);
            }
            finally {
                ctx.database().checkpointReadUnlock();
            }
        }
    }

    /**
     * @param row Row.
     * @throws IgniteCheckedException If failed.
     */
    public void addRow(CacheDataRow row) throws IgniteCheckedException {
        if (!persistenceEnabled)
            freeList.insertDataRow(row);
        else {
            ctx.database().checkpointReadLock();

            try {
                freeList.insertDataRow(row);

                assert row.link() != 0L;
            }
            finally {
                ctx.database().checkpointReadUnlock();
            }
        }
    }

    /**
     * @param link Row link.
     * @param row New row data.
     * @return {@code True} if was able to update row.
     * @throws IgniteCheckedException If failed.
     */
    public boolean updateRow(long link, CacheDataRow row) throws IgniteCheckedException {
        assert !persistenceEnabled || ctx.database().checkpointLockIsHeldByThread();

        if (rowCacheCleaner != null)
            rowCacheCleaner.remove(link);

        return freeList.updateDataRow(link, row);
    }

    /**
     * Run page handler operation over the row.
     *
     * @param link Row link.
     * @param pageHnd Page handler.
     * @param arg Page handler argument.
     * @throws IgniteCheckedException If failed.
     */
    public <S, R> void updateDataRow(long link, PageHandler<S, R> pageHnd, S arg) throws IgniteCheckedException {
        if (!persistenceEnabled)
            freeList.updateDataRow(link, pageHnd, arg);
        else {
            ctx.database().checkpointReadLock();

            try {
                freeList.updateDataRow(link, pageHnd, arg);
            }
            finally {
                ctx.database().checkpointReadUnlock();
            }
        }
    }

    /**
     * @return Free list.
     */
    public FreeList freeList() {
        return freeList;
    }

    /**
     * Inject rows cache cleaner.
     *
     * @param rowCacheCleaner Rows cache cleaner.
     */
    public void setRowCacheCleaner(GridQueryRowCacheCleaner rowCacheCleaner) {
        this.rowCacheCleaner = rowCacheCleaner;
    }
}

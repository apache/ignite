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

import java.util.Collection;
import java.util.function.Supplier;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.DataPageEvictionMode;
import org.apache.ignite.internal.metric.IoStatisticsHolder;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.persistence.freelist.FreeList;
import org.apache.ignite.internal.processors.query.GridQueryRowCacheCleaner;
import org.apache.ignite.internal.util.typedef.internal.U;

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
    private volatile Supplier<GridQueryRowCacheCleaner> rowCacheCleaner = () -> null;

    /** */
    protected final CacheGroupContext grp;

    /**
     * @param grp Cache group.
     * @param freeList Free list.
     */
    public RowStore(CacheGroupContext grp, FreeList freeList) {
        assert grp != null;
        assert freeList != null;

        this.grp = grp;
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
    public void removeRow(long link, IoStatisticsHolder statHolder) throws IgniteCheckedException {
        assert link != 0;

        GridQueryRowCacheCleaner rowCacheCleaner0 = rowCacheCleaner.get();

        if (rowCacheCleaner0 != null)
            rowCacheCleaner0.remove(link);

        if (!persistenceEnabled)
            freeList.removeDataRowByLink(link, statHolder);
        else {
            ctx.database().checkpointReadLock();

            try {
                freeList.removeDataRowByLink(link, statHolder);
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
    public void addRow(CacheDataRow row, IoStatisticsHolder statHolder) throws IgniteCheckedException {
        if (!persistenceEnabled) {
            ctx.database().ensureFreeSpaceForInsert(grp.dataRegion(), row.size());

            freeList.insertDataRow(row, statHolder);
        }
        else {
            ctx.database().checkpointReadLock();

            try {
                freeList.insertDataRow(row, statHolder);

                assert row.link() != 0L;
            }
            finally {
                ctx.database().checkpointReadUnlock();
            }
        }

        assert row.key().partition() == PageIdUtils.partId(row.link()) :
            "Constructed a link with invalid partition ID [partId=" + row.key().partition() +
                ", link=" + U.hexLong(row.link()) + ']';
    }

    /**
     * @param rows Rows.
     * @param statHolder Statistics holder to track IO operations.
     * @throws IgniteCheckedException If failed.
     */
    public void addRows(Collection<? extends CacheDataRow> rows, IoStatisticsHolder statHolder) throws IgniteCheckedException {
        if (!persistenceEnabled && grp.dataRegion().config().getPageEvictionMode() != DataPageEvictionMode.DISABLED) {
            // Size-aware pre-reserve for the rebalance batch (regular single puts get the same guarantee via the
            // per-put reserve in addRow). Reserving "for each row" would collapse to reserving for the largest one:
            // every reserve runs before any insert and only enforces a lower bound on the shared empty-pages counter,
            // so the final guarantee is max(row sizes). A single reserve for the largest row is therefore equivalent
            // and is what is done here.
            //
            // The batch insert path (insertDataRows) mirrors writeSinglePage: it runs its own lazy re-reserve on the
            // trailing fragment when takePage fails, and rethrows IgniteOutOfMemoryException as-is rather than
            // wrapping it into CorruptedFreeListException (see AbstractFreeList). This pre-reserve here is sized for
            // the largest row in the batch and runs before any insert, so it bounds the empty-pages counter up front.
            //
            // The reserve evicts non-blockingly even though the batch path holds no entry locks (so blocking would be
            // deadlock-safe and more effective here): the same reserve path is shared with single-row insertion,
            // which runs under an entry lock and must not block.
            int maxRowSize = 0;

            for (CacheDataRow row : rows) {
                int rowSize = row.size();

                if (rowSize > maxRowSize)
                    maxRowSize = rowSize;
            }

            if (maxRowSize > 0)
                ctx.database().ensureFreeSpaceForInsert(grp.dataRegion(), maxRowSize);
        }

        assert ctx.database().checkpointLockIsHeldByThread();

        freeList.insertDataRows(rows, statHolder);
    }

    /**
     * @param link Row link.
     * @param row New row data.
     * @return {@code True} if was able to update row.
     * @throws IgniteCheckedException If failed.
     */
    public boolean updateRow(long link, CacheDataRow row, IoStatisticsHolder statHolder) throws IgniteCheckedException {
        assert !persistenceEnabled || ctx.database().checkpointLockIsHeldByThread();

        GridQueryRowCacheCleaner rowCacheCleaner0 = rowCacheCleaner.get();

        if (rowCacheCleaner0 != null)
            rowCacheCleaner0.remove(link);

        return freeList.updateDataRow(link, row, statHolder);
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
    public void setRowCacheCleaner(Supplier<GridQueryRowCacheCleaner> rowCacheCleaner) {
        assert rowCacheCleaner != null;

        this.rowCacheCleaner = rowCacheCleaner;
    }
}

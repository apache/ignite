/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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
import org.apache.ignite.internal.stat.IoStatisticsHolder;

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
    public void removeRow(long link, IoStatisticsHolder statHolder) throws IgniteCheckedException {
        assert link != 0;

        if (rowCacheCleaner != null)
            rowCacheCleaner.remove(link);

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
        if (!persistenceEnabled)
            freeList.insertDataRow(row, statHolder);
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
    }

    /**
     * @param link Row link.
     * @param row New row data.
     * @return {@code True} if was able to update row.
     * @throws IgniteCheckedException If failed.
     */
    public boolean updateRow(long link, CacheDataRow row, IoStatisticsHolder statHolder) throws IgniteCheckedException {
        assert !persistenceEnabled || ctx.database().checkpointLockIsHeldByThread();

        if (rowCacheCleaner != null)
            rowCacheCleaner.remove(link);

        return freeList.updateDataRow(link, row, statHolder);
    }

    /**
     * Run page handler operation over the row.
     *
     * @param link Row link.
     * @param pageHnd Page handler.
     * @param arg Page handler argument.
     * @throws IgniteCheckedException If failed.
     */
    public <S, R> void updateDataRow(long link, PageHandler<S, R> pageHnd, S arg,
        IoStatisticsHolder statHolder) throws IgniteCheckedException {
        if (!persistenceEnabled)
            freeList.updateDataRow(link, pageHnd, arg, statHolder);
        else {
            ctx.database().checkpointReadLock();

            try {
                freeList.updateDataRow(link, pageHnd, arg, statHolder);
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

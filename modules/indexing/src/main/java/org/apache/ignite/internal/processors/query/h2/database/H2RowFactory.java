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

package org.apache.ignite.internal.processors.query.h2.database;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRowAdapter;
import org.apache.ignite.internal.processors.cache.tree.mvcc.data.MvccDataRow;
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

        GridH2Row row = rowDesc.createRow(rowBuilder);

        assert row.version() != null;

        return row;
    }

    /**
     * @param link Link.
     * @param mvccCrdVer Mvcc coordinator version.
     * @param mvccCntr Mvcc counter.
     * @param mvccOpCntr Mvcc operation counter.
     * @return Row.
     * @throws IgniteCheckedException If failed.
     */
    public GridH2Row getMvccRow(long link, long mvccCrdVer, long mvccCntr, int mvccOpCntr) throws IgniteCheckedException {
        return rowDesc.createRow(new MvccDataRow(cctx.group(),0, link,
            PageIdUtils.partId(PageIdUtils.pageId(link)),null, mvccCrdVer, mvccCntr, mvccOpCntr));
    }
}

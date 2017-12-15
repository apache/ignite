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
import org.apache.ignite.internal.processors.cache.mvcc.MvccVersion;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusIO;
import org.apache.ignite.internal.processors.query.h2.database.io.H2RowLinkIO;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Row;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2SearchRow;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.spi.indexing.IndexingQueryCacheFilter;

import static org.apache.ignite.internal.processors.cache.mvcc.MvccProcessor.assertMvccVersionValid;
import static org.apache.ignite.internal.processors.cache.mvcc.MvccProcessor.unmaskCoordinatorVersion;

/**
 *
 */
public class H2TreeFilterClosure implements H2Tree.TreeRowClosure<GridH2SearchRow, GridH2Row> {
    /** */
    private final MvccVersion mvccVer;
    /** */
    private final IndexingQueryCacheFilter filter;

    /**
     * @param filter Cache filter.
     * @param mvccVer Mvcc version.
     */
    public H2TreeFilterClosure(IndexingQueryCacheFilter filter, MvccVersion mvccVer) {
        assert filter != null || mvccVer != null;

        this.filter = filter;
        this.mvccVer = mvccVer;
    }

    /** {@inheritDoc} */
    @Override public boolean apply(BPlusTree<GridH2SearchRow, GridH2Row> tree, BPlusIO<GridH2SearchRow> io,
        long pageAddr, int idx)  throws IgniteCheckedException {

        return (filter  == null || applyFilter((H2RowLinkIO)io, pageAddr, idx))
            && (mvccVer == null || applyMvcc((H2RowLinkIO)io, pageAddr, idx));
    }

    /**
     * @param io Row IO.
     * @param pageAddr Page address.
     * @param idx Item index.
     * @return {@code True} if row passes the filter.
     */
    private boolean applyFilter(H2RowLinkIO io, long pageAddr, int idx) {
        assert filter != null;

        return filter.applyPartition(PageIdUtils.partId(PageIdUtils.pageId(io.getLink(pageAddr, idx))));
    }

    /**
     * @param io Row IO.
     * @param pageAddr Page address.
     * @param idx Item index.
     * @return {@code True} if row passes the filter.
     */
    private boolean applyMvcc(H2RowLinkIO io, long pageAddr, int idx) {
        assert io.storeMvccInfo() : io;

        long rowCrdVer = io.getMvccCoordinatorVersion(pageAddr, idx);

        assert unmaskCoordinatorVersion(rowCrdVer) == rowCrdVer : rowCrdVer;
        assert rowCrdVer > 0 : rowCrdVer;

        int cmp = Long.compare(mvccVer.coordinatorVersion(), rowCrdVer);

        if (cmp == 0) {
            long rowCntr = io.getMvccCounter(pageAddr, idx);

            cmp = Long.compare(mvccVer.counter(), rowCntr);

            return cmp >= 0 &&
                !newVersionAvailable(io, pageAddr, idx) &&
                !mvccVer.activeTransactions().contains(rowCntr);
        }
        else
            return cmp > 0;
    }

    /**
     * @param rowIo Row IO.
     * @param pageAddr Page address.
     * @param idx Item index.
     * @return {@code True}
     */
    private boolean newVersionAvailable(H2RowLinkIO rowIo, long pageAddr, int idx) {
        long newCrdVer = rowIo.getNewMvccCoordinatorVersion(pageAddr, idx);

        if (newCrdVer == 0)
            return false;

        int cmp = Long.compare(mvccVer.coordinatorVersion(), newCrdVer);

        if (cmp == 0) {
            long newCntr = rowIo.getNewMvccCounter(pageAddr, idx);

            assert assertMvccVersionValid(newCrdVer, newCntr);

            return newCntr <= mvccVer.counter() && !mvccVer.activeTransactions().contains(newCntr);
        }
        else
            return cmp < 0;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(H2TreeFilterClosure.class, this);
    }
}

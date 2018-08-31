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
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.mvcc.MvccSnapshot;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusIO;
import org.apache.ignite.internal.processors.query.h2.database.io.H2RowLinkIO;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Row;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2SearchRow;
import org.apache.ignite.internal.transactions.IgniteTxMvccVersionCheckedException;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.spi.indexing.IndexingQueryCacheFilter;

import static org.apache.ignite.internal.pagemem.PageIdUtils.pageId;
import static org.apache.ignite.internal.processors.cache.mvcc.MvccUtils.isVisible;
import static org.apache.ignite.internal.processors.cache.mvcc.MvccUtils.mvccVersionIsValid;

/**
 *
 */
public class H2TreeFilterClosure implements H2Tree.TreeRowClosure<GridH2SearchRow, GridH2Row> {
    /** */
    private final MvccSnapshot mvccSnapshot;

    /** */
    private final IndexingQueryCacheFilter filter;

    /** */
    private final GridCacheContext cctx;

    /**
     * @param filter Cache filter.
     * @param mvccSnapshot MVCC snapshot.
     * @param cctx Cache context.
     */
    public H2TreeFilterClosure(IndexingQueryCacheFilter filter, MvccSnapshot mvccSnapshot, GridCacheContext cctx) {
        assert (filter != null || mvccSnapshot != null) && cctx != null ;

        this.filter = filter;
        this.mvccSnapshot = mvccSnapshot;
        this.cctx = cctx;
    }

    /** {@inheritDoc} */
    @Override public boolean apply(BPlusTree<GridH2SearchRow, GridH2Row> tree, BPlusIO<GridH2SearchRow> io,
        long pageAddr, int idx)  throws IgniteCheckedException {
        return (filter  == null || applyFilter((H2RowLinkIO)io, pageAddr, idx))
            && (mvccSnapshot == null || applyMvcc((H2RowLinkIO)io, pageAddr, idx));
    }

    /**
     * @param io Row IO.
     * @param pageAddr Page address.
     * @param idx Item index.
     * @return {@code True} if row passes the filter.
     */
    private boolean applyFilter(H2RowLinkIO io, long pageAddr, int idx) {
        assert filter != null;

        return filter.applyPartition(PageIdUtils.partId(pageId(io.getLink(pageAddr, idx))));
    }

    /**
     * @param io Row IO.
     * @param pageAddr Page address.
     * @param idx Item index.
     * @return {@code True} if row passes the filter.
     */
    private boolean applyMvcc(H2RowLinkIO io, long pageAddr, int idx) throws IgniteCheckedException {
        assert io.storeMvccInfo() : io;

        long rowCrdVer = io.getMvccCoordinatorVersion(pageAddr, idx);
        long rowCntr = io.getMvccCounter(pageAddr, idx);
        int rowOpCntr = io.getMvccOperationCounter(pageAddr, idx);

        assert mvccVersionIsValid(rowCrdVer, rowCntr, rowOpCntr);

        try {
            return isVisible(cctx, mvccSnapshot, rowCrdVer, rowCntr, rowOpCntr, io.getLink(pageAddr, idx));
        }
        catch (IgniteTxMvccVersionCheckedException ignored) {
            return false; // The row is going to be removed.
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(H2TreeFilterClosure.class, this);
    }
}

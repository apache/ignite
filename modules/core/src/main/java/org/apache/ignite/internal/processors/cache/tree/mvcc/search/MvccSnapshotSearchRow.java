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
  See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.tree.mvcc.search;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.mvcc.MvccSnapshot;
import org.apache.ignite.internal.processors.cache.mvcc.MvccUtils;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRowAdapter;
import org.apache.ignite.internal.processors.cache.persistence.CacheSearchRow;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusIO;
import org.apache.ignite.internal.processors.cache.tree.RowLinkIO;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.mvcc.MvccUtils.MVCC_INVISIBLE;
import static org.apache.ignite.internal.processors.cache.mvcc.MvccUtils.MVCC_VISIBLE;
import static org.apache.ignite.internal.processors.cache.mvcc.MvccUtils.MVCC_VISIBLE_REMOVED;

/**
 * Search row which returns the first row visible for the given snapshot. Usage:
 * - set this row as the upper bound
 * - pass the same row as search closure.
 */
public class MvccSnapshotSearchRow extends MvccSearchRow implements MvccTreeClosure {
    /** */
    private final GridCacheContext cctx;

    /** Resulting row. */
    private CacheDataRow res;

    /** */
    private MvccSnapshot snapshot;

    /**
     * Constructor.
     *
     * @param cctx
     * @param key Key.
     * @param snapshot Snapshot.
     */
    public MvccSnapshotSearchRow(GridCacheContext cctx, KeyCacheObject key,
        MvccSnapshot snapshot) {
        super(cctx.cacheId(), key, snapshot.coordinatorVersion(), snapshot.counter(), Integer.MAX_VALUE);

        this.cctx = cctx;

        this.snapshot = snapshot;
    }

    /**
     * @return Found row.
     */
    @Nullable public CacheDataRow row() {
        return res;
    }

    /** {@inheritDoc} */
    @Override public boolean apply(BPlusTree<CacheSearchRow, CacheDataRow> tree, BPlusIO<CacheSearchRow> io,
        long pageAddr, int idx) throws IgniteCheckedException {
        RowLinkIO rowIo = (RowLinkIO)io;

        long rowCrdVer = rowIo.getMvccCoordinatorVersion(pageAddr, idx);
        long rowCntr = rowIo.getMvccCounter(pageAddr, idx);
        int rowOpCntr = rowIo.getMvccOperationCounter(pageAddr, idx);

        assert MvccUtils.mvccVersionIsValid(rowCrdVer, rowCntr, rowOpCntr);

        if (MvccUtils.isVisible(cctx, snapshot, rowCrdVer, rowCntr, rowOpCntr, false)) {
            int state = MvccUtils.getVisibleState(cctx, rowIo.getLink(pageAddr, idx), snapshot);

            if (state == MVCC_INVISIBLE)
                return true;

            if (state == MVCC_VISIBLE_REMOVED)
                res = null;
            else {
                assert state == MVCC_VISIBLE;

                res = tree.getRow(io, pageAddr, idx, CacheDataRowAdapter.RowData.NO_KEY);

                res.key(key());
            }

            return false; // Stop search.
        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(MvccSnapshotSearchRow.class, this);
    }
}

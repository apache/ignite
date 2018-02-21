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
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.mvcc.MvccLongList;
import org.apache.ignite.internal.processors.cache.mvcc.MvccSnapshot;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRowAdapter;
import org.apache.ignite.internal.processors.cache.persistence.CacheSearchRow;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusIO;
import org.apache.ignite.internal.processors.cache.tree.RowLinkIO;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.mvcc.MvccProcessor.unmaskCoordinatorVersion;
import static org.apache.ignite.internal.processors.cache.mvcc.MvccProcessor.versionForRemovedValue;

/**
 * Search row which returns the first row visible for the given snapshot. Usage:
 * - set this row as the upper bound
 * - pass the same row as search closure.
 */
public class MvccSnapshotSearchRow extends MvccSearchRow implements MvccTreeClosure {
    /** Active transactions. */
    private final MvccLongList activeTxs;

    /** Resulting row. */
    private CacheDataRow resRow;

    /**
     * Constructor.
     *
     * @param cacheId Cache ID.
     * @param key Key.
     * @param snapshot Snapshot.
     */
    public MvccSnapshotSearchRow(int cacheId, KeyCacheObject key, MvccSnapshot snapshot) {
        super(cacheId, key, snapshot.coordinatorVersion(), snapshot.counter());

        this.activeTxs = snapshot.activeTransactions();
    }

    /**
     * @return Found row.
     */
    @Nullable public CacheDataRow row() {
        return resRow;
    }

    /** {@inheritDoc} */
    @Override public boolean apply(BPlusTree<CacheSearchRow, CacheDataRow> tree, BPlusIO<CacheSearchRow> io,
        long pageAddr, int idx) throws IgniteCheckedException {
        boolean visible = true;

        RowLinkIO rowIo = (RowLinkIO)io;

        long rowCrdVerMasked = rowIo.getMvccCoordinatorVersion(pageAddr, idx);

        if (activeTxs != null && activeTxs.size() > 0) {
            long rowCrdVer = unmaskCoordinatorVersion(rowCrdVerMasked);

            // TODO: What is the purpose of this check?
            if (rowCrdVer == crdVer)
                visible = !activeTxs.contains(rowIo.getMvccCounter(pageAddr, idx));
        }

        if (visible) {
            if (versionForRemovedValue(rowCrdVerMasked))
                resRow = null;
            else
                resRow = tree.getRow(io, pageAddr, idx, CacheDataRowAdapter.RowData.NO_KEY);

            return false; // Stop search.
        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(MvccSnapshotSearchRow.class, this);
    }
}

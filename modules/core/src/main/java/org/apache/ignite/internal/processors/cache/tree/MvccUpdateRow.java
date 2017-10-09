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

package org.apache.ignite.internal.processors.cache.tree;

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.mvcc.MvccCoordinatorVersion;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.persistence.CacheSearchRow;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusIO;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.GridLongList;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public class MvccUpdateRow extends DataRow implements BPlusTree.TreeRowClosure<CacheSearchRow, CacheDataRow> {
    /** */
    private Boolean hasPrev;

    /** */
    private boolean canCleanup;

    /** */
    private GridLongList activeTxs;

    /** */
    private List<CacheSearchRow> cleanupRows;

    /** */
    private final MvccCoordinatorVersion mvccVer;

    /**
     * @param key Key.
     * @param val Value.
     * @param ver Version.
     * @param mvccVer Mvcc version.
     * @param part Partition.
     * @param cacheId Cache ID.
     */
    public MvccUpdateRow(
        KeyCacheObject key,
        CacheObject val,
        GridCacheVersion ver,
        MvccCoordinatorVersion mvccVer,
        int part,
        int cacheId) {
        super(key, val, ver, part, 0L, cacheId);

        this.mvccVer = mvccVer;
    }

    /**
     * @return {@code True} if previous value was non-null.
     */
    public boolean previousNotNull() {
        return hasPrev != null && hasPrev;
    }

    /**
     * @return Active transactions to wait for.
     */
    @Nullable public GridLongList activeTransactions() {
        return activeTxs;
    }

    /**
     * @return Rows which are safe to cleanup.
     */
    public List<CacheSearchRow> cleanupRows() {
        return cleanupRows;
    }

    /** {@inheritDoc} */
    @Override public boolean apply(BPlusTree<CacheSearchRow, CacheDataRow> tree,
        BPlusIO<CacheSearchRow> io,
        long pageAddr,
        int idx)
        throws IgniteCheckedException
    {
        RowLinkIO rowIo = (RowLinkIO)io;

        // All previous version should be less then new one.
        assert mvccVer.coordinatorVersion() >= rowIo.getMvccCoordinatorVersion(pageAddr, idx);
        assert mvccVer.coordinatorVersion() > rowIo.getMvccCoordinatorVersion(pageAddr, idx) || mvccVer.counter() > rowIo.getMvccCounter(pageAddr, idx);

        boolean checkActive = mvccVer.activeTransactions().size() > 0;

        boolean txActive = false;

        // Suppose transactions on previous coordinator versions are done.
        if (checkActive && mvccVer.coordinatorVersion() == rowIo.getMvccCoordinatorVersion(pageAddr, idx)) {
            long rowMvccCntr = rowIo.getMvccCounter(pageAddr, idx);

            if (mvccVer.activeTransactions().contains(rowMvccCntr)) {
                txActive = true;

                if (activeTxs == null)
                    activeTxs = new GridLongList();

                activeTxs.add(rowMvccCntr);
            }
        }

        if (hasPrev == null)
            hasPrev = Boolean.TRUE; // TODO IGNITE-3478 support removes.

        if (!txActive) {
            assert Long.compare(mvccVer.coordinatorVersion(), rowIo.getMvccCoordinatorVersion(pageAddr, idx)) >= 0;

            int cmp;

            if (mvccVer.coordinatorVersion() == rowIo.getMvccCoordinatorVersion(pageAddr, idx))
                cmp = Long.compare(mvccVer.cleanupVersion(), rowIo.getMvccCounter(pageAddr, idx));
            else
                cmp = 1;

            if (cmp >= 0) {
                // Do not cleanup oldest version.
                if (canCleanup) {
                    CacheSearchRow row = io.getLookupRow(tree, pageAddr, idx);

                    assert row.link() != 0 && row.mvccCoordinatorVersion() > 0 : row;

                    // Should not be possible to cleanup active tx.
                    assert row.mvccCoordinatorVersion() != mvccVer.coordinatorVersion()
                        || !mvccVer.activeTransactions().contains(row.mvccCounter());

                    if (cleanupRows == null)
                        cleanupRows = new ArrayList<>();

                    cleanupRows.add(row);
                }
                else
                    canCleanup = true;
            }
        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public long mvccCoordinatorVersion() {
        return mvccVer.coordinatorVersion();
    }

    /** {@inheritDoc} */
    @Override public long mvccCounter() {
        return mvccVer.counter();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(MvccUpdateRow.class, this, "super", super.toString());
    }
}

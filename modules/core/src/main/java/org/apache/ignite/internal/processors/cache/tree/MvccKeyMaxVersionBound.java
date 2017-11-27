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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRowAdapter;
import org.apache.ignite.internal.processors.cache.persistence.CacheSearchRow;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusIO;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.mvcc.CacheCoordinatorsProcessor.versionForRemovedValue;

/**
 *
 */
public class MvccKeyMaxVersionBound extends SearchRow implements BPlusTree.TreeRowClosure<CacheSearchRow, CacheDataRow> {
    /** */
    private CacheDataRow resRow;

    /**
     * @param cacheId Cache ID.
     * @param key Key.
     */
    public MvccKeyMaxVersionBound(int cacheId, KeyCacheObject key) {
        super(cacheId, key);
    }

    /**
     * @return Found row.
     */
    @Nullable public CacheDataRow row() {
        return resRow;
    }

    /** {@inheritDoc} */
    @Override public boolean apply(BPlusTree<CacheSearchRow, CacheDataRow> tree, BPlusIO<CacheSearchRow> io,
        long pageAddr,
        int idx)
        throws IgniteCheckedException
    {
        RowLinkIO rowIo = (RowLinkIO)io;

        if (versionForRemovedValue(rowIo.getMvccCoordinatorVersion(pageAddr, idx)))
            resRow = null;
        else
            resRow = ((CacheDataTree)tree).getRow(io, pageAddr, idx, CacheDataRowAdapter.RowData.NO_KEY);

        return false;  // Stop search.
    }

    /** {@inheritDoc} */
    @Override public long mvccCoordinatorVersion() {
        return Long.MAX_VALUE;
    }

    /** {@inheritDoc} */
    @Override public long mvccCounter() {
        return Long.MAX_VALUE;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(MvccKeyMaxVersionBound.class, this);
    }
}

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

package org.apache.ignite.internal.processors.cache.tree.mvcc.search;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.mvcc.MvccVersion;
import org.apache.ignite.internal.processors.cache.mvcc.MvccVersionImpl;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.persistence.CacheSearchRow;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusIO;
import org.apache.ignite.internal.processors.cache.tree.RowLinkIO;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.mvcc.MvccUtils.getNewVersion;

/**
 * Closure which returns version of the very first encountered row.
 */
public class MvccFirstRowVersionTreeClosure implements MvccTreeClosure {
    /** */
    private final GridCacheContext cctx;

    /** Maximum MVCC version found for the row. */
    private MvccVersion res;

    /**
     * @param cctx Cache context.
     */
    public MvccFirstRowVersionTreeClosure(GridCacheContext cctx) {
        this.cctx = cctx;
    }

    /**
     * @return Maximum MVCC version found for the row.
     */
    @Nullable public MvccVersion maxMvccVersion() {
        return res;
    }

    /** {@inheritDoc} */
    @Override public boolean apply(BPlusTree<CacheSearchRow, CacheDataRow> tree, BPlusIO<CacheSearchRow> io,
        long pageAddr, int idx) throws IgniteCheckedException {
        RowLinkIO rowIo = (RowLinkIO)io;

        MvccVersion newVer = getNewVersion(cctx, rowIo.getLink(pageAddr, idx));

        if (newVer != null)
            res = newVer;
        else
            res = new MvccVersionImpl(rowIo.getMvccCoordinatorVersion(pageAddr, idx), rowIo.getMvccCounter(pageAddr, idx));

        return false;  // Stop search.
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(MvccFirstRowVersionTreeClosure.class, this);
    }
}

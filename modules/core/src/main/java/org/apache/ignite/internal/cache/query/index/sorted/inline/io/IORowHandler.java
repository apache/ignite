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

package org.apache.ignite.internal.cache.query.index.sorted.inline.io;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.cache.query.index.sorted.IndexRow;
import org.apache.ignite.internal.cache.query.index.sorted.IndexRowImpl;
import org.apache.ignite.internal.cache.query.index.sorted.ThreadLocalRowHandlerHolder;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndexTree;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.PageUtils;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.mvcc.MvccUtils;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRowAdapter;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusIO;
import org.apache.ignite.internal.processors.cache.tree.mvcc.data.MvccDataRow;

/**
 * Class provide a common logic for looking up and storing an index row.
 */
class IORowHandler {
    /** */
    static <T extends BPlusIO & InlineIO> IndexRow get(T delegate, BPlusTree<IndexRow, ?> tree, long pageAddr, int idx)
        throws IgniteCheckedException {

        long link = PageUtils.getLong(pageAddr, delegate.offset(idx) + delegate.inlineSize());

        assert link != 0;

        if (delegate.storeMvccInfo())
            return mvccIndexRow(delegate, link, (InlineIndexTree) tree, pageAddr, idx);

        return indexRow(link, (InlineIndexTree) tree);
    }

    /** */
    static void store(long pageAddr, int off, IndexRow row, int inlineSize, boolean storeMvccInfo) {
        // Meta stores link and MVCC data for cache row.
        int metaOff = off + inlineSize;

        // Write link after all inlined idx keys.
        PageUtils.putLong(pageAddr, metaOff, row.link());

        if (storeMvccInfo) {
            long mvccCrdVer = row.mvccCoordinatorVersion();
            long mvccCntr = row.mvccCounter();
            int mvccOpCntr = row.mvccOperationCounter();

            assert MvccUtils.mvccVersionIsValid(mvccCrdVer, mvccCntr, mvccOpCntr);

            PageUtils.putLong(pageAddr, metaOff + 8, mvccCrdVer);
            PageUtils.putLong(pageAddr, metaOff + 16, mvccCntr);
            PageUtils.putInt(pageAddr, metaOff + 24, mvccOpCntr);
        }
    }

    /**
     * @param dstPageAddr Destination page address.
     * @param dstOff Destination page offset.
     * @param srcIo Source IO.
     * @param srcPageAddr Source page address.
     * @param srcIdx Source index.
     * @param storeMvcc {@code True} to store mvcc data.
     */
    static void store(long dstPageAddr, int dstOff, BPlusIO<IndexRow> srcIo, long srcPageAddr, int srcIdx, boolean storeMvcc)
    {
        InlineIO rowIo = (InlineIO) srcIo;

        long link = rowIo.link(srcPageAddr, srcIdx);

        PageUtils.putLong(dstPageAddr, dstOff, link);

        if (storeMvcc) {
            long mvccCrdVer = rowIo.mvccCoordinatorVersion(srcPageAddr, srcIdx);
            long mvccCntr = rowIo.mvccCounter(srcPageAddr, srcIdx);
            int mvccOpCntr = rowIo.mvccOperationCounter(srcPageAddr, srcIdx);

            assert MvccUtils.mvccVersionIsValid(mvccCrdVer, mvccCntr, mvccOpCntr);

            PageUtils.putLong(dstPageAddr, dstOff + 8, mvccCrdVer);
            PageUtils.putLong(dstPageAddr, dstOff + 16, mvccCntr);
            PageUtils.putInt(dstPageAddr, dstOff + 24, mvccOpCntr);
        }
    }

    /** */
    private static IndexRow indexRow(long link, InlineIndexTree tree) throws IgniteCheckedException {
        IndexRowImpl cachedRow = tree.getCachedIndexRow(link);

        if (cachedRow != null)
            return cachedRow;

        CacheDataRowAdapter row = new CacheDataRowAdapter(link);

        CacheGroupContext ctx = tree.cacheContext().group();

        row.initFromLink(ctx, CacheDataRowAdapter.RowData.FULL, true);

        IndexRowImpl r = new IndexRowImpl(ThreadLocalRowHandlerHolder.rowHandler(), row);

        tree.cacheIndexRow(r);

        return r;
    }

    /** */
    private static <T extends BPlusIO & InlineIO> IndexRow mvccIndexRow(T delegate, long link, InlineIndexTree tree, long pageAddr, int idx) throws IgniteCheckedException {
        IndexRowImpl cachedRow = tree.getCachedIndexRow(link);

        if (cachedRow != null)
            return cachedRow;

        long mvccCrdVer = delegate.mvccCoordinatorVersion(pageAddr, idx);
        long mvccCntr = delegate.mvccCounter(pageAddr, idx);
        int mvccOpCntr = delegate.mvccOperationCounter(pageAddr, idx);

        int partId = PageIdUtils.partId(PageIdUtils.pageId(link));

        CacheGroupContext ctx = tree.cacheContext().group();

        MvccDataRow row = new MvccDataRow(
            ctx,
            0,
            link,
            partId,
            null,
            mvccCrdVer,
            mvccCntr,
            mvccOpCntr,
            true
        );

        IndexRowImpl r = new IndexRowImpl(ThreadLocalRowHandlerHolder.rowHandler(), row);

        tree.cacheIndexRow(r);

        return r;
    }
}

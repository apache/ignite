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
import org.apache.ignite.internal.pagemem.PageUtils;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRowAdapter;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusLeafIO;

/**
 * Leaf page to store index rows.
 */
public abstract class AbstractLeafIO extends BPlusLeafIO<IndexRow> implements InlineIO {
    /**
     * @param type Page type.
     * @param ver Page format version.
     * @param itemSize Single item size on page.
     */
    AbstractLeafIO(int type, int ver, int itemSize) {
        super(type, ver, itemSize);
    }

    /** {@inheritDoc} */
    @Override public void storeByOffset(long pageAddr, int off, IndexRow row) {
        assert row.link() != 0;

        PageUtils.putLong(pageAddr, off, row.link());
    }

    /** {@inheritDoc} */
    @Override public IndexRow getLookupRow(BPlusTree<IndexRow, ?> tree, long pageAddr, int idx)
        throws IgniteCheckedException {
        long link = link(pageAddr, idx);

        assert link != 0;

        CacheDataRowAdapter row = new CacheDataRowAdapter(link);

        CacheGroupContext ctx = ((InlineIndexTree) tree).cacheContext().group();

        row.initFromLink(ctx, CacheDataRowAdapter.RowData.FULL, true);

        return new IndexRowImpl(ThreadLocalRowHandlerHolder.rowHandler(), row);
    }

    /** {@inheritDoc} */
    @Override public void store(long dstPageAddr, int dstIdx, BPlusIO<IndexRow> srcIo, long srcPageAddr, int srcIdx) {
        long link = ((InlineIO) srcIo).link(srcPageAddr, srcIdx);

        PageUtils.putLong(dstPageAddr, offset(dstIdx), link);
    }

    /** {@inheritDoc} */
    @Override public long link(long pageAddr, int idx) {
        return PageUtils.getLong(pageAddr, offset(idx));
    }

    /** {@inheritDoc} */
    @Override public int inlineSize() {
        return 0;
    }
}

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
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndexTree;
import org.apache.ignite.internal.pagemem.PageUtils;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRowAdapter;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusInnerIO;

/**
 * Inner page to store index rows.
 */
public abstract class AbstractInnerIO extends BPlusInnerIO<IndexSearchRow> implements InlineIO {
    /**
     * @param type Page type.
     * @param ver Page format version.
     * @param itemSize Single item size on page.
     */
    AbstractInnerIO(int type, int ver, int itemSize) {
        super(type, ver, true, itemSize);
    }

    /** {@inheritDoc} */
    @Override public void storeByOffset(long pageAddr, int off, IndexSearchRow row) {
        PageUtils.putLong(pageAddr, off, row.getLink());
    }

    /** {@inheritDoc} */
    @Override public IndexSearchRow getLookupRow(BPlusTree<IndexSearchRow, ?> tree, long pageAddr, int idx)
        throws IgniteCheckedException {
        long link = getLink(pageAddr, idx);

        assert link != 0;

        CacheDataRowAdapter row = new CacheDataRowAdapter(link);

        CacheGroupContext ctx = ((InlineIndexTree) tree).getContext();

        row.initFromLink(ctx, CacheDataRowAdapter.RowData.FULL, true);

        return new IndexRowImpl(ThreadLocalSchemaHolder.getSchema(), row);
    }

    /** {@inheritDoc} */
    @Override public void store(long dstPageAddr, int dstIdx, BPlusIO<IndexSearchRow> srcIo, long srcPageAddr, int srcIdx) {
        long link = getLink(srcPageAddr, srcIdx);

        PageUtils.putLong(dstPageAddr, offset(dstIdx), link);
    }

    /** {@inheritDoc} */
    @Override public long getLink(long pageAddr, int idx) {
        return PageUtils.getLong(pageAddr, offset(idx));
    }
}

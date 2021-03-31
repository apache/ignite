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
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndexTree;
import org.apache.ignite.internal.pagemem.PageUtils;
import org.apache.ignite.internal.processors.cache.mvcc.MvccUtils;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusInnerIO;

/**
 * Inner page to store index rows.
 */
public abstract class AbstractInnerIO extends BPlusInnerIO<IndexRow> implements InlineIO {
    /**
     * @param type Page type.
     * @param ver Page format version.
     * @param itemSize Single item size on page.
     */
    AbstractInnerIO(int type, int ver, int itemSize) {
        super(type, ver, true, itemSize);
    }

    /** {@inheritDoc} */
    @Override public void storeByOffset(long pageAddr, int off, IndexRow row) {
        assert row.link() != 0;

        // Write link after all inlined idx keys.
        PageUtils.putLong(pageAddr, off, row.link());

        if (storeMvccInfo()) {
            long mvccCrdVer = row.mvccCoordinatorVersion();
            long mvccCntr = row.mvccCounter();
            int mvccOpCntr = row.mvccOperationCounter();

            assert MvccUtils.mvccVersionIsValid(mvccCrdVer, mvccCntr, mvccOpCntr);

            PageUtils.putLong(pageAddr, off + 8, mvccCrdVer);
            PageUtils.putLong(pageAddr, off + 16, mvccCntr);
            PageUtils.putInt(pageAddr, off + 24, mvccOpCntr);
        }
    }

    /** {@inheritDoc} */
    @Override public IndexRow getLookupRow(BPlusTree<IndexRow, ?> tree, long pageAddr, int idx)
        throws IgniteCheckedException {

        long link = PageUtils.getLong(pageAddr, offset(idx));

        assert link != 0;

        if (storeMvccInfo())
            return IORowHandler.mvccIndexRow(this, link, (InlineIndexTree) tree, pageAddr, idx);

        return IORowHandler.indexRow(link, (InlineIndexTree) tree);
    }

    /** {@inheritDoc} */
    @Override public void store(long dstPageAddr, int dstIdx, BPlusIO<IndexRow> srcIo, long srcPageAddr, int srcIdx) {
        InlineIO rowIo = (InlineIO) srcIo;

        int dstOff = offset(dstIdx);

        long link = rowIo.link(srcPageAddr, srcIdx);

        PageUtils.putLong(dstPageAddr, dstOff, link);

        if (storeMvccInfo()) {
            long mvccCrdVer = rowIo.mvccCoordinatorVersion(srcPageAddr, srcIdx);
            long mvccCntr = rowIo.mvccCounter(srcPageAddr, srcIdx);
            int mvccOpCntr = rowIo.mvccOperationCounter(srcPageAddr, srcIdx);

            assert MvccUtils.mvccVersionIsValid(mvccCrdVer, mvccCntr, mvccOpCntr);

            PageUtils.putLong(dstPageAddr, dstOff + 8, mvccCrdVer);
            PageUtils.putLong(dstPageAddr, dstOff + 16, mvccCntr);
            PageUtils.putInt(dstPageAddr, dstOff + 24, mvccOpCntr);
        }
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

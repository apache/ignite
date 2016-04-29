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

package org.apache.ignite.internal.processors.cache.database.freelist.io;

import java.nio.ByteBuffer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.PageIdAllocator;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.processors.cache.database.freelist.FreeItem;
import org.apache.ignite.internal.processors.cache.database.freelist.FreeTree;
import org.apache.ignite.internal.processors.cache.database.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.database.tree.io.BPlusIO;
import org.apache.ignite.internal.processors.cache.database.tree.io.BPlusLeafIO;
import org.apache.ignite.internal.processors.cache.database.tree.io.IOVersions;

/**
 * Routines for free list leaf pages.
 */
public class FreeLeafIO extends BPlusLeafIO<FreeItem> implements FreeIO {
    /** */
    public static final IOVersions<FreeLeafIO> VERSIONS = new IOVersions<>(
        new FreeLeafIO(1)
    );

    /**
     * @param ver Page format version.
     */
    protected FreeLeafIO(int ver) {
        super(T_FREE_LEAF, ver, 6); // freeSpace(2) + pageIndex(4)
    }

    /** {@inheritDoc} */
    @Override public void store(ByteBuffer buf, int idx, FreeItem row) {
        int off = offset(idx);

        buf.putShort(off, row.freeSpace());
        buf.putInt(off + 2, PageIdUtils.pageIndex(row.pageId()));
    }

    /** {@inheritDoc} */
    @Override public void store(ByteBuffer dst, int dstIdx, BPlusIO<FreeItem> srcIo, ByteBuffer src, int srcIdx)
        throws IgniteCheckedException {
        assert srcIo == this: srcIo;

        int off = offset(dstIdx);

        dst.putShort(off, getFreeSpace(src, srcIdx));
        dst.putInt(off + 2, getPageIndex(src, srcIdx));
    }

    /** {@inheritDoc} */
    @Override public short getFreeSpace(ByteBuffer buf, int idx) {
        int off = offset(idx);

        return buf.getShort(off);
    }

    /** {@inheritDoc} */
    @Override public int getPageIndex(ByteBuffer buf, int idx) {
        int off = offset(idx);

        return buf.getInt(off + 2);
    }

    /** {@inheritDoc} */
    @Override public FreeItem getLookupRow(BPlusTree<FreeItem, ?> tree, ByteBuffer buf, int idx) {
        int off = offset(idx);

        short freeSpace = buf.getShort(off);
        int pageIdx = buf.getInt(off + 2);

        long pageId = PageIdUtils.pageId(((FreeTree)tree).getPartId(), PageIdAllocator.FLAG_DATA, pageIdx);

        return new FreeItem(freeSpace, pageId, tree.getCacheId());
    }
}

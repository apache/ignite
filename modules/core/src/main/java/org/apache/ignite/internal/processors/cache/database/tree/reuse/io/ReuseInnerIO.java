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

package org.apache.ignite.internal.processors.cache.database.tree.reuse.io;

import java.nio.ByteBuffer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.processors.cache.database.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.database.tree.io.BPlusIO;
import org.apache.ignite.internal.processors.cache.database.tree.io.BPlusInnerIO;
import org.apache.ignite.internal.processors.cache.database.tree.io.IOVersions;

/**
 * Reuse list inner page IO routines.
 */
public final class ReuseInnerIO extends BPlusInnerIO<Number> {
    /** */
    public static final IOVersions<ReuseInnerIO> VERSIONS = new IOVersions<>(
        new ReuseInnerIO(1)
    );

    /**
     * @param ver Page format version.
     */
    protected ReuseInnerIO(int ver) {
        super(T_REUSE_INNER, ver, false, 4);
    }

    /** {@inheritDoc} */
    @Override public void store(ByteBuffer dst, int dstIdx, BPlusIO<Number> srcIo, ByteBuffer src, int srcIdx) {
        int pageIdx = srcIo.isLeaf() ?
            PageIdUtils.pageIndex(((ReuseLeafIO)srcIo).getPageId(src, srcIdx)) :
            ((ReuseInnerIO)srcIo).getPageIndex(src, srcIdx);

        store(dst, dstIdx, pageIdx);
    }

    /**
     * @param buf Buffer.
     * @param idx Item index.
     * @return Page number.
     */
    public int getPageIndex(ByteBuffer buf, int idx) {
        return buf.getInt(offset(idx));
    }

    /**
     * @param buf Buffer.
     * @param idx Index.
     * @param pageIdx Page index.
     */
    private void store(ByteBuffer buf, int idx, int pageIdx) {
        buf.putInt(offset(idx), pageIdx);
    }

    /** {@inheritDoc} */
    @Override public void store(ByteBuffer buf, int idx, Number row) {
        store(buf, idx, row.getClass() == Integer.class ? row.intValue() :
            PageIdUtils.pageIndex(row.longValue()));
    }

    /** {@inheritDoc} */
    @Override public Number getLookupRow(BPlusTree<Number,?> tree, ByteBuffer buf, int idx)
        throws IgniteCheckedException {
        return getPageIndex(buf, idx);
    }
}

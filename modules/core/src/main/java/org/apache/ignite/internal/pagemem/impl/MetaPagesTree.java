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

package org.apache.ignite.internal.pagemem.impl;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.PageIdAllocator;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.processors.cache.database.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.database.tree.io.BPlusIO;
import org.apache.ignite.internal.processors.cache.database.tree.io.BPlusInnerIO;
import org.apache.ignite.internal.processors.cache.database.tree.io.BPlusLeafIO;
import org.apache.ignite.internal.processors.cache.database.tree.io.IOVersions;

import java.nio.ByteBuffer;

/**
 * Keeps IDs of allocated metadata pages.
 */
public class MetaPagesTree extends BPlusTree<MetaTreeItem, MetaTreeItem> {
    /**
     * @param pageMem Page memory instance.
     * @param metaPageId Metadata page ID.
     * @param initNew {@code True} if it's just allocated.
     * @throws IgniteCheckedException
     */
    public MetaPagesTree(
        final PageMemory pageMem,
        final long metaPageId,
        final boolean initNew) throws IgniteCheckedException {
        super(treeName("meta", "Meta"), 0, pageMem, new FullPageId(metaPageId, 0),
            null, MetaInnerIO.VERSIONS, MetaLeafIO.VERSIONS);

        if (initNew)
            initNew();
    }

    /** {@inheritDoc} */
    @Override protected int compare(final BPlusIO<MetaTreeItem> io, final ByteBuffer buf,
        final int idx, final MetaTreeItem row) throws IgniteCheckedException {
        final int off = ((MetaIO) io).getOffset(idx);

        return Integer.compare(buf.getInt(off), row.cacheId());
    }

    /** {@inheritDoc} */
    @Override protected MetaTreeItem getRow(final BPlusIO<MetaTreeItem> io, final ByteBuffer buf,
        final int idx) throws IgniteCheckedException {
        return readRow(buf, ((MetaIO) io).getOffset(idx));
    }

    /** {@inheritDoc} */
    @Override protected long allocatePage0() throws IgniteCheckedException {
        return pageMem.allocatePage(0, 0, PageIdAllocator.FLAG_META);
    }

    /**
     *
     */
    private interface MetaIO {
        /**
         * @param idx Index.
         * @return Offset.
         */
        int getOffset(int idx);
    }

    /**
     * Inner IO.
     */
    private static class MetaInnerIO extends BPlusInnerIO<MetaTreeItem> implements MetaIO {
        /** */
        static final IOVersions<MetaInnerIO> VERSIONS = new IOVersions<>(
            new MetaInnerIO(1)
        );

        /**
         * @param ver Version.
         */
        public MetaInnerIO(final int ver) {
            super(T_META_INNER, ver, false, MetaTreeItem.ITEM_SIZE);
        }

        /** {@inheritDoc} */
        @Override public void store(final ByteBuffer buf, final int idx,
            final MetaTreeItem row) throws IgniteCheckedException {
            storeRow(buf, offset(idx), row);
        }

        /** {@inheritDoc} */
        @Override public void store(
            final ByteBuffer dst,
            final int dstIdx,
            final BPlusIO<MetaTreeItem> srcIo,
            final ByteBuffer src,
            final int srcIdx) throws IgniteCheckedException {
            storeRow(dst, offset(dstIdx), src, ((MetaIO)srcIo).getOffset(srcIdx));
        }

        /** {@inheritDoc} */
        @Override public MetaTreeItem getLookupRow(
            final BPlusTree<MetaTreeItem, ?> tree,
            final ByteBuffer buf,
            final int idx) throws IgniteCheckedException {
            return readRow(buf, offset(idx));
        }

        /** {@inheritDoc} */
        @Override public int getOffset(int idx) {
            return offset(idx);
        }
    }

    /**
     * Leaf IO.
     */
    private static class MetaLeafIO extends BPlusLeafIO<MetaTreeItem> implements MetaIO {
        /** */
        static final IOVersions<MetaLeafIO> VERSIONS = new IOVersions<>(
            new MetaLeafIO(1)
        );

        /**
         * @param ver Version.
         */
        public MetaLeafIO(final int ver) {
            super(T_META_LEAF, ver, MetaTreeItem.ITEM_SIZE);
        }

        /** {@inheritDoc} */
        @Override public void store(final ByteBuffer buf, final int idx,
            final MetaTreeItem row) throws IgniteCheckedException {
            storeRow(buf, offset(idx), row);
        }

        /** {@inheritDoc} */
        @Override public void store(
            final ByteBuffer dst,
            final int dstIdx,
            final BPlusIO<MetaTreeItem> srcIo,
            final ByteBuffer src,
            final int srcIdx) throws IgniteCheckedException {
            storeRow(dst, offset(dstIdx), src, ((MetaIO)srcIo).getOffset(srcIdx));
        }

        /** {@inheritDoc} */
        @Override public MetaTreeItem getLookupRow(final BPlusTree<MetaTreeItem, ?> tree, final ByteBuffer buf,
            final int idx) throws IgniteCheckedException {
            return readRow(buf, offset(idx));
        }

        /** {@inheritDoc} */
        @Override public int getOffset(int idx) {
            return offset(idx);
        }
    }

    /**
     * @param buf Buffer.
     * @param off Offset.
     * @param row Row to store.
     */
    private static void storeRow(
        final ByteBuffer buf,
        final int off,
        final MetaTreeItem row
    ) {
        buf.putInt(off, row.cacheId());
        buf.putLong(off + 4, row.pageId());
    }

    /**
     * @param dst Destination buffer.
     * @param dstOff Destination offset.
     * @param src Source buffer.
     * @param srcOff Source offset.
     */
    private static void storeRow(
        final ByteBuffer dst,
        final int dstOff,
        final ByteBuffer src,
        final int srcOff
    ) {
        dst.putInt(dstOff, src.getInt(srcOff));
        dst.putLong(dstOff + 4, src.getLong(srcOff + 4));
    }

    /**
     * @param buf Buffer.
     * @param off Offset.
     * @return Row item.
     */
    private static MetaTreeItem readRow(final ByteBuffer buf, final int off) {
        return new MetaTreeItem(buf.getInt(off), buf.getLong(off + 4));
    }

}

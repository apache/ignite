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

package org.apache.ignite.internal.processors.cache.database;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.processors.cache.database.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.database.tree.io.BPlusIO;
import org.apache.ignite.internal.processors.cache.database.tree.io.BPlusInnerIO;
import org.apache.ignite.internal.processors.cache.database.tree.io.BPlusLeafIO;
import org.apache.ignite.internal.processors.cache.database.tree.io.IOVersions;
import org.apache.ignite.internal.processors.cache.database.tree.reuse.ReuseList;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Metadata storage.
 */
public class MetadataStorage implements MetaStore {
    /** Max index name length (bytes num) */
    public static final int MAX_IDX_NAME_LEN = 768;

    /** Bytes in byte. */
    private static final int BYTE_LEN = 1;

    /** Page memory. */
    private final PageMemory pageMem;

    /** Index tree. */
    private final MetaTree metaTree;

    /** Meta page reuse tree. */
    private final ReuseList reuseList;

    /** Cache ID. */
    private final int cacheId;

    /** */
    private final int allocPartId;

    /** */
    private final byte allocSpace;

    /**
     * @param pageMem Page memory.
     * @param wal Write ahead log manager.
     */
    public MetadataStorage(
        final PageMemory pageMem,
        final IgniteWriteAheadLogManager wal,
        final AtomicLong globalRmvId,
        final int cacheId,
        final int allocPartId,
        final byte allocSpace,
        final ReuseList reuseList,
        final long rootPageId,
        final boolean initNew
    ) {
        try {
            this.pageMem = pageMem;
            this.cacheId = cacheId;
            this.allocPartId = allocPartId;
            this.allocSpace = allocSpace;
            this.reuseList = reuseList;

            metaTree = new MetaTree(cacheId, allocPartId, allocSpace, pageMem, wal, globalRmvId, rootPageId,
                reuseList, MetaStoreInnerIO.VERSIONS, MetaStoreLeafIO.VERSIONS, initNew);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public RootPage getOrAllocateForTree(final String idxName) throws IgniteCheckedException {
        final MetaTree tree = metaTree;

        synchronized (this) {
            byte[] idxNameBytes = idxName.getBytes(StandardCharsets.UTF_8);

            if (idxNameBytes.length > MAX_IDX_NAME_LEN)
                throw new IllegalArgumentException("Too long encoded indexName [maxAllowed=" + MAX_IDX_NAME_LEN +
                    ", currentLength=" + idxNameBytes.length + ", name=" + idxName + "]");

            final IndexItem row = tree.findOne(new IndexItem(idxNameBytes, 0));

            if (row == null) {
                long pageId = 0;

                if (reuseList != null)
                    pageId = reuseList.takeRecycledPage();

                pageId = pageId == 0 ? pageMem.allocatePage(cacheId, allocPartId, allocSpace) : pageId;

                tree.put(new IndexItem(idxNameBytes, pageId));

                return new RootPage(new FullPageId(pageId, cacheId), true);
            }
            else {
                final FullPageId pageId = new FullPageId(row.pageId, cacheId);

                return new RootPage(pageId, false);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public RootPage dropRootPage(final String idxName)
        throws IgniteCheckedException {
        byte[] idxNameBytes = idxName.getBytes(StandardCharsets.UTF_8);

        final IndexItem row = metaTree.remove(new IndexItem(idxNameBytes, 0));

        if (row != null) {
            if (reuseList == null)
                pageMem.freePage(cacheId, row.pageId);
        }

        return row != null ? new RootPage(new FullPageId(row.pageId, cacheId), false) : null;
    }

    /** {@inheritDoc} */
    @Override public void destroy() throws IgniteCheckedException {
        metaTree.destroy();
    }

    /**
     *
     */
    private static class MetaTree extends BPlusTree<IndexItem, IndexItem> {
        /** */
        private final int allocPartId;

        /** */
        private final byte allocSpace;

        /**
         * @param pageMem Page memory.
         * @param metaPageId Meta page ID.
         * @param reuseList Reuse list.
         * @param innerIos Inner IOs.
         * @param leafIos Leaf IOs.
         * @throws IgniteCheckedException If failed.
         */
        private MetaTree(
            final int cacheId,
            final int allocPartId,
            final byte allocSpace,
            final PageMemory pageMem,
            final IgniteWriteAheadLogManager wal,
            final AtomicLong globalRmvId,
            final long metaPageId,
            final ReuseList reuseList,
            final IOVersions<? extends BPlusInnerIO<IndexItem>> innerIos,
            final IOVersions<? extends BPlusLeafIO<IndexItem>> leafIos,
            final boolean initNew
        ) throws IgniteCheckedException {
            super(treeName("meta", "Meta"), cacheId, pageMem, wal, globalRmvId, metaPageId, reuseList, innerIos, leafIos);

            this.allocPartId = allocPartId;
            this.allocSpace = allocSpace;

            if (initNew)
                initNew();
        }

        /** {@inheritDoc} */
        @Override protected long allocatePageNoReuse() throws IgniteCheckedException {
            return pageMem.allocatePage(getCacheId(), allocPartId, allocSpace);
        }

        /** {@inheritDoc} */
        @Override protected int compare(final BPlusIO<IndexItem> io, final ByteBuffer buf, final int idx,
            final IndexItem row) throws IgniteCheckedException {
            final int off = ((IndexIO)io).getOffset(idx);

            int shift = 0;

            // Compare index names.
            final byte len = buf.get(off + shift);

            shift += BYTE_LEN;

            for (int i = 0; i < len && i < row.idxName.length; i++) {
                final int cmp = Byte.compare(buf.get(off + i + shift), row.idxName[i]);

                if (cmp != 0)
                    return cmp;
            }

            return Integer.compare(len, row.idxName.length);
        }

        /** {@inheritDoc} */
        @Override protected IndexItem getRow(final BPlusIO<IndexItem> io, final ByteBuffer buf,
            final int idx) throws IgniteCheckedException {
            return readRow(buf, ((IndexIO)io).getOffset(idx));
        }
    }

    /**
     *
     */
    private static class IndexItem {
        /** */
        private byte[] idxName;

        /** */
        private long pageId;

        /**
         * @param idxName Index name.
         * @param pageId Page ID.
         */
        private IndexItem(final byte[] idxName, final long pageId) {
            this.idxName = idxName;
            this.pageId = pageId;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "I [idxName=" + new String(idxName) + ", pageId=" + U.hexLong(pageId) + ']';
        }
    }

    /**
     * Store row to buffer.
     *
     * @param buf Buffer.
     * @param off Offset in buf.
     * @param row Row to store.
     */
    private static void storeRow(
        final ByteBuffer buf,
        final int off,
        final IndexItem row
    ) {
        int origPos = buf.position();

        try {
            buf.position(off);

            // Index name length.
            buf.put((byte)row.idxName.length);

            // Index name.
            buf.put(row.idxName);

            // Page ID.
            buf.putLong(row.pageId);
        }
        finally {
            buf.position(origPos);
        }
    }

    /**
     * Copy row data.
     *
     * @param dst Destination buffer.
     * @param dstOff Destination buf offset.
     * @param src Source buffer.
     * @param srcOff Src buf offset.
     */
    private static void storeRow(
        final ByteBuffer dst,
        final int dstOff,
        final ByteBuffer src,
        final int srcOff
    ) {
        int srcOrigPos = src.position();
        int dstOrigPos = dst.position();

        try {
            src.position(srcOff);
            dst.position(dstOff);

            // Index name length.
            final byte len = src.get();

            dst.put(len);

            int lim = src.limit();

            src.limit(src.position() + len);

            // Index name.
            dst.put(src);

            src.limit(lim);

            // Page ID.
            dst.putLong(src.getLong());
        }
        finally {
            src.position(srcOrigPos);
            dst.position(dstOrigPos);
        }
    }

    /**
     * Read row from buffer.
     *
     * @param buf Buffer to read.
     * @param off Offset in buf.
     * @return Read row.
     */
    private static IndexItem readRow(final ByteBuffer buf, final int off) {
        int origOff = buf.position();

        try {
            buf.position(off);

            // Index name length.
            final int len = buf.get() & 0xFF;

            // Index name.
            final byte[] idxName = new byte[len];

            buf.get(idxName);

            // Page ID.
            final long pageId = buf.getLong();

            return new IndexItem(idxName, pageId);
        }
        finally {
            buf.position(origOff);
        }
    }

    /**
     *
     */
    private interface IndexIO {
        /**
         * @param idx Index.
         * @return Offset in buffer according to {@code idx}.
         */
        int getOffset(int idx);
    }

    /**
     *
     */
    public static final class MetaStoreInnerIO extends BPlusInnerIO<IndexItem> implements IndexIO {
        /** */
        public static final IOVersions<MetaStoreInnerIO> VERSIONS = new IOVersions<>(
            new MetaStoreInnerIO(1)
        );

        /**
         * @param ver Version.
         */
        private MetaStoreInnerIO(final int ver) {
            // name bytes and 1 byte for length, 8 bytes pageId
            super(T_METASTORE_INNER, ver, false, MAX_IDX_NAME_LEN + 1 + 8);
        }

        /** {@inheritDoc} */
        @Override public void storeByOffset(ByteBuffer buf, int off, IndexItem row) throws IgniteCheckedException {
            storeRow(buf, off, row);
        }

        /** {@inheritDoc} */
        @Override public void store(final ByteBuffer dst, final int dstIdx, final BPlusIO<IndexItem> srcIo,
            final ByteBuffer src,
            final int srcIdx) throws IgniteCheckedException {
            storeRow(dst, offset(dstIdx), src, ((IndexIO)srcIo).getOffset(srcIdx));
        }

        /** {@inheritDoc} */
        @Override public IndexItem getLookupRow(final BPlusTree<IndexItem, ?> tree, final ByteBuffer buf,
            final int idx) throws IgniteCheckedException {
            return readRow(buf, offset(idx));
        }

        /** {@inheritDoc} */
        @Override public int getOffset(final int idx) {
            return offset(idx);
        }
    }

    /**
     *
     */
    public static final class MetaStoreLeafIO extends BPlusLeafIO<IndexItem> implements IndexIO {
        /** */
        public static final IOVersions<MetaStoreLeafIO> VERSIONS = new IOVersions<>(
            new MetaStoreLeafIO(1)
        );

        /**
         * @param ver Version.
         */
        private MetaStoreLeafIO(final int ver) {
            // 4 byte cache ID, UTF-16 symbols and 1 byte for length, 8 bytes pageId
            super(T_METASTORE_LEAF, ver, MAX_IDX_NAME_LEN + 1 + 8);
        }

        /** {@inheritDoc} */
        @Override public void storeByOffset(ByteBuffer buf, int off, IndexItem row) throws IgniteCheckedException {
            storeRow(buf, off, row);
        }

        /** {@inheritDoc} */
        @Override public void store(final ByteBuffer dst, final int dstIdx, final BPlusIO<IndexItem> srcIo,
            final ByteBuffer src,
            final int srcIdx) throws IgniteCheckedException {
            storeRow(dst, offset(dstIdx), src, ((IndexIO)srcIo).getOffset(srcIdx));
        }

        /** {@inheritDoc} */
        @Override public IndexItem getLookupRow(final BPlusTree<IndexItem, ?> tree, final ByteBuffer buf,
            final int idx) throws IgniteCheckedException {
            return readRow(buf, offset(idx));
        }

        /** {@inheritDoc} */
        @Override public int getOffset(final int idx) {
            return offset(idx);
        }
    }
}

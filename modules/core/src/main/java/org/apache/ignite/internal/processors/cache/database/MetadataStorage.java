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
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.Page;
import org.apache.ignite.internal.pagemem.PageIdAllocator;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.processors.cache.database.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.database.tree.io.BPlusIO;
import org.apache.ignite.internal.processors.cache.database.tree.io.BPlusInnerIO;
import org.apache.ignite.internal.processors.cache.database.tree.io.BPlusLeafIO;
import org.apache.ignite.internal.processors.cache.database.tree.io.IOVersions;
import org.apache.ignite.internal.processors.cache.database.tree.reuse.ReuseList;

/**
 * Metadata storage.
 */
public class MetadataStorage implements MetaStore {
    /** Max index name length (symbols num) */
    public static final int MAX_IDX_NAME_LEN = 64;

    /** Bytes in byte. */
    private static final int BYTE_LEN = 1;

    /** Bytes in int. */
    private static final int INT_LEN = 4;

    /** Bytes in long. */
    private static final int LONG_LEN = 8;

    /** Constant that's used for tree root page allocation. */
    private static final int TREE_CACHE_ID = Integer.MAX_VALUE; // TODO drop this

    /** Page memory. */
    private final PageMemory pageMem;

    /** Increments on each root page allocation. */
    private final AtomicLong rootId = new AtomicLong(0); // TODO drop this

    /** Index tree. */
    private volatile MetaTree metaTree;

    /**
     * @param pageMem Page memory.
     */
    public MetadataStorage(PageMemory pageMem) {
        this.pageMem = pageMem;
    }

    /** {@inheritDoc} */
    @Override public RootPage getOrAllocateForTree(final int cacheId, final String idxName, final boolean idx)
        throws IgniteCheckedException {
        // TODO drop cacheId from signature - must be one metastore per cache and it is bad idea to store more data than needed
        // TODO drop idx from signature - never used

        if (idxName.length() > MAX_IDX_NAME_LEN) // TODO useless check because we actually store in UTF-8
            throw new IllegalArgumentException("Too long indexName [max allowed length=" + MAX_IDX_NAME_LEN +
                ", current length=" + idxName.length() + "]");

        final MetaTree tree = metaTree();

        synchronized (this) {
            byte[] idxNameBytes = idxName.getBytes(StandardCharsets.UTF_8); // TODO either store UTF-16 or calculate for UTF-8

            final IndexItem row = tree.findOne(new IndexItem(idxNameBytes, 0, cacheId, 0));

            if (row == null) {
                final FullPageId idxRoot = pageMem.allocatePage(cacheId, 0, PageIdAllocator.FLAG_META);

                final long rootId = this.rootId.getAndIncrement();

                tree.put(new IndexItem(idxNameBytes, idxRoot.pageId(), cacheId, rootId));

                return new RootPage(idxRoot, true, rootId);
            }
            else {
                final FullPageId pageId = new FullPageId(row.pageId, cacheId);

                return new RootPage(pageId, false, rootId.get());
            }
        }
    }

    /** {@inheritDoc} */
    @Override public long dropRootPage(final int cacheId, final String idxName)
        throws IgniteCheckedException {
        final MetaTree tree = metaTree();

        byte[] idxNameBytes = idxName.getBytes(StandardCharsets.UTF_8); // TODO either store UTF-16 or calculate for UTF-8

        final IndexItem row;

        synchronized (this) { // TODO don't think this synchronisation makes any sense
            row = tree.remove(new IndexItem(idxNameBytes, 0, cacheId, 0));
        }

        if (row != null) // TODO looks like a bug, are we allowed to call freePage?
            pageMem.freePage(new FullPageId(row.pageId, cacheId));

        // TODO Looks like this rootId is useless, and it is better to just return dropped root page ID or true/false.
        return row != null ? row.rootId : -1;
    }

    /**
     * @param cacheId Cache ID to get meta page for.
     * @return Meta page.
     */
    private RootPage metaPageTree(int cacheId) throws IgniteCheckedException {
        // TODO this complex stuff must go away after metastore will be made one per cache
        // TODO and we must make pageMem.metaPage to be a MetaTree root.

        Page meta = pageMem.metaPage();

        try {
            boolean written = false;

            ByteBuffer buf = meta.getForWrite();

            try {
                int cnt = buf.getShort() & 0xFFFF;
                int cnt0 = cnt;

                int writePos = 0;

                while (cnt0 > 0) {
                    int readId = buf.getInt();
                    long pageId = buf.getLong();

                    if (readId != 0) {
                        if (readId == cacheId)
                            return new RootPage(new FullPageId(pageId, cacheId), false, 0);

                        cnt0--;
                    }
                    else
                        writePos = buf.position() - 12;
                }

                if (writePos != 0)
                    buf.position(writePos);

                FullPageId fullId = pageMem.allocatePage(cacheId, 0, PageIdAllocator.FLAG_META);

                assert !fullId.equals(meta.fullId()) : "Duplicate page allocated " +
                    "[metaId=" + meta.fullId() + ", allocated=" + fullId + ']';

                buf.putInt(cacheId);
                buf.putLong(fullId.pageId());

                written = true;

                buf.putShort(0, (short)(cnt + 1));

                return new RootPage(fullId, true, 0);
            }
            finally {
                meta.releaseWrite(written);
            }
        }
        finally {
            pageMem.releasePage(meta);
        }
    }

    /**
     *
     */
    private static class MetaTree extends BPlusTree<IndexItem, IndexItem> {
        /**
         * @param cacheId Cache ID.
         * @param pageMem Page memory.
         * @param metaPageId Meta page ID.
         * @param reuseList Reuse list.
         * @param innerIos Inner IOs.
         * @param leafIos Leaf IOs.
         * @throws IgniteCheckedException
         */
        private MetaTree(final int cacheId, final PageMemory pageMem, final FullPageId metaPageId,
            final ReuseList reuseList,
            final IOVersions<? extends BPlusInnerIO<IndexItem>> innerIos,
            final IOVersions<? extends BPlusLeafIO<IndexItem>> leafIos, final boolean initNew)
            throws IgniteCheckedException {
            super(treeName("meta", cacheId, "Meta"), cacheId, pageMem, metaPageId, reuseList, innerIos, leafIos);

            if (initNew)
                initNew();
        }

        /** {@inheritDoc} */
        @Override protected long allocatePage0() throws IgniteCheckedException {
            return pageMem.allocatePage(getCacheId(), 0, PageIdAllocator.FLAG_META).pageId();
        }

        /** {@inheritDoc} */
        @Override protected int compare(final BPlusIO<IndexItem> io, final ByteBuffer buf, final int idx,
            final IndexItem row) throws IgniteCheckedException {

            // Compare cache IDs
            final int off = ((IndexIO) io).getOffset(idx);

            final int cacheId = buf.getInt(off);

            final int cacheIdCmp = Integer.compare(cacheId, row.cacheId);

            if (cacheIdCmp != 0)
                return cacheIdCmp;

            int shift = 0;

            shift += INT_LEN;

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
        private int cacheId;

        /** */
        private long pageId;

        /** */
        private long rootId;

        /**
         * @param idxName Index name.
         * @param pageId Page ID.
         */
        private IndexItem(final byte[] idxName, final long pageId, final int cacheId, final long rootId) {
            this.idxName = idxName;
            this.pageId = pageId;
            this.cacheId = cacheId;
            this.rootId = rootId;
        }
    }

    /**
     * Store row to buffer.
     *
     * @param buf Buffer.
     * @param off Offset in buf.
     * @param row Row to store.
     */
    private static void storeRow(final ByteBuffer buf, final int off,
        final IndexItem row) {

        // Cache ID.
        buf.putInt(off, row.cacheId);

        int shift = 0;

        shift += INT_LEN;

        // Index name length.
        final int len = row.idxName.length;

        buf.put(off + shift, (byte) len);

        shift += BYTE_LEN;

        // Index name.
        for (int i = 0; i < len; i++)
            buf.put(off + i + shift, row.idxName[i]);

        shift += len;

        // Page ID.
        buf.putLong(off + shift, row.pageId);

        shift += LONG_LEN;

        // Root ID.
        buf.putLong(off + shift, row.rootId);
    }

    /**
     * Copy row data.
     *
     * @param dst Destination buffer.
     * @param dstOff Destination buf offset.
     * @param src Source buffer.
     * @param srcOff Src buf offset.
     */
    private static void storeRow(final ByteBuffer dst, final int dstOff,
        final ByteBuffer src,
        final int srcOff) {

        // Cache ID.
        dst.putInt(dstOff, src.getInt(srcOff));

        int shift = 0; // TODO drop shift and use buf.position(off)

        shift += INT_LEN;

        // Index name length.
        final byte len = src.get(srcOff + shift);

        dst.put(dstOff + shift, len);

        shift += BYTE_LEN;

        // Index name.
        for (int i = 0; i < len; i++) // TODO byte by byte write???
            dst.put(dstOff + i + shift, src.get(srcOff + i + shift));

        shift += len;

        // Page ID.
        dst.putLong(dstOff + shift, src.getLong(srcOff + shift));

        shift += LONG_LEN;

        // Root ID.
        dst.putLong(dstOff + shift, src.getLong(srcOff + shift));
    }

    /**
     * Read row from buffer.
     *
     * @param buf Buffer to read.
     * @param off Offset in buf.
     * @return Read row.
     */
    private static IndexItem readRow(final ByteBuffer buf, final int off) {
        int shift = 0; // TODO drop shift and use buf.position(off)

        // Cache ID.
        final int cacheId = buf.getInt(off);

        shift += INT_LEN;

        // Index name length.
        final byte len = buf.get(off + shift);

        shift += BYTE_LEN;

        // Index name.
        final byte[] idxName = new byte[len];

        for (int i = 0; i < len; i++)
            idxName[i] = buf.get(off + i + shift); // TODO byte by byte read???

        shift += len;

        // Page ID.
        final long pageId = buf.getLong(off + shift);

        shift += LONG_LEN;

        // Root ID.
        final long rootId = buf.getLong(off + shift);

        return new IndexItem(idxName, pageId, cacheId, rootId);
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
    private static class IndexInnerIO extends BPlusInnerIO<IndexItem> implements IndexIO {
        /** */
        static final IOVersions<IndexInnerIO> VERSIONS = new IOVersions<>(
            new IndexInnerIO(1)
        );

        /**
         * @param ver Version.
         */
        private IndexInnerIO(final int ver) {
            // TODO fix calculation for UTF-8 or actually store UTF-16
            // 4 byte cache ID, UTF-16 symbols and 1 byte for length, 8 bytes pageId, 8 bytes root ID
            super(T_INDEX_INNER, ver, false, 4 + MAX_IDX_NAME_LEN * 2 + 1 + 8 + 8);
        }

        /** {@inheritDoc} */
        @Override public void store(final ByteBuffer buf, final int idx,
            final IndexItem row) throws IgniteCheckedException {
            storeRow(buf, offset(idx), row);
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
    private static class IndexLeafIO extends BPlusLeafIO<IndexItem> implements IndexIO {
        /** */
        static final IOVersions<IndexLeafIO> VERSIONS = new IOVersions<>(
            new IndexLeafIO(1)
        );

        /**
         * @param ver Version.
         */
        private IndexLeafIO(final int ver) {
            // TODO fix calculation for UTF-8 or actually store UTF-16
            // 4 byte cache ID, UTF-16 symbols and 1 byte for length, 8 bytes pageId, 8 bytes root ID
            super(T_INDEX_LEAF, ver, 4 + MAX_IDX_NAME_LEN * 2 + 1 + 8 + 8);
        }

        /** {@inheritDoc} */
        @Override public void store(final ByteBuffer buf, final int idx,
            final IndexItem row) throws IgniteCheckedException {
            storeRow(buf, offset(idx), row);
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
     * Get or allocate tree for metadata.
     *
     * @return Tree instance.
     * @throws IgniteCheckedException
     */
    private MetaTree metaTree() throws IgniteCheckedException {
        if (metaTree == null) { // TODO do we really need this lazy initialization?
            synchronized (this) {
                if (metaTree == null) {
                    final RootPage rootPage = metaPageTree(TREE_CACHE_ID);

                    metaTree = new MetaTree(TREE_CACHE_ID, pageMem, rootPage.pageId(), null, IndexInnerIO.VERSIONS,
                        IndexLeafIO.VERSIONS, rootPage.isAllocated());
                }
            }
        }

        return metaTree;
    }
}

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
import java.util.concurrent.ConcurrentMap;
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
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.jsr166.ConcurrentHashMap8;

/**
 * Metadata storage.
 */
public class MetadataStorage implements MetaStore {
    /** */
    public static final int MAX_IDX_NAME_LEN = 30;

    /** */
    private PageMemory pageMem;

    /** */
    private final AtomicLong rootId = new AtomicLong(0);

    /** */
    private final ConcurrentMap<Integer, GridFutureAdapter<MetaTree>> trees = new ConcurrentHashMap8<>();

    /**
     * @param pageMem Page memory.
     */
    public MetadataStorage(PageMemory pageMem) {
        this.pageMem = pageMem;
    }

    /** {@inheritDoc} */
    @Override public RootPage getOrAllocateForTree(final int cacheId, final String idxName, final boolean idx)
        throws IgniteCheckedException {
        final MetaTree tree = metaTree(cacheId);

        synchronized (this) {
            byte[] idxNameBytes = idxName.getBytes(StandardCharsets.UTF_8);

            final IndexItem row = tree.findOne(new IndexItem(idxNameBytes, 0));

            if (row == null) {
                final FullPageId idxRoot = pageMem.allocatePage(cacheId, 0, idx
                    ? PageIdAllocator.FLAG_IDX : PageIdAllocator.FLAG_META);

                tree.put(new IndexItem(idxNameBytes, idxRoot.pageId()));

                return new RootPage(idxRoot, true, rootId.getAndIncrement());
            }
            else {
                final FullPageId pageId = new FullPageId(row.pageId, cacheId);

                return new RootPage(pageId, false, rootId.get());
            }
        }
    }

    /** {@inheritDoc} */
    @Override public boolean dropRootPage(final int cacheId, final String idxName)
        throws IgniteCheckedException {
        final MetaTree tree = metaTree(cacheId);

        byte[] idxNameBytes = idxName.getBytes(StandardCharsets.UTF_8);

        final IndexItem row = tree.remove(new IndexItem(idxNameBytes, 0));

        if (row != null)
            pageMem.freePage(new FullPageId(row.pageId, cacheId));

        return row != null;
    }

    /**
     * @param cacheId Cache ID to get meta page for.
     * @return Meta page.
     */
    private RootPage metaPageTree(int cacheId) throws IgniteCheckedException {
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
                meta.incrementVersion();

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
        public MetaTree(final int cacheId, final PageMemory pageMem, final FullPageId metaPageId,
            final ReuseList reuseList,
            final IOVersions<? extends BPlusInnerIO<IndexItem>> innerIos,
            final IOVersions<? extends BPlusLeafIO<IndexItem>> leafIos, final boolean initNew)
            throws IgniteCheckedException {
            super(cacheId, pageMem, metaPageId, reuseList, innerIos, leafIos);

            if (initNew)
                initNew();
        }

        /** {@inheritDoc} */
        @Override protected int compare(final BPlusIO<IndexItem> io, final ByteBuffer buf, final int idx,
            final IndexItem row) throws IgniteCheckedException {
            // compare index names
            final int off = ((IndexIO) io).getOffset(idx);

            final byte len = buf.get(off);

            // TODO actually not correct UTF string comparison
            for (int i = 0; i < len && i < row.idxName.length; i++) {
                final int cmp = Byte.compare(buf.get(off + i + 1), row.idxName[i]);

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
        byte[] idxName;

        /** */
        long pageId;

        /**
         * @param idxName Index name.
         * @param pageId Page ID.
         */
        public IndexItem(final byte[] idxName, final long pageId) {
            this.idxName = idxName;
            this.pageId = pageId;
        }
    }

    /**
     * Store row to buffer.
     *
     * @param buf Buffer.
     * @param off Offset in buf.
     * @param row Row to store.
     * @throws IgniteCheckedException
     */
    private static void storeRow(final ByteBuffer buf, final int off,
        final IndexItem row) throws IgniteCheckedException {
        final int len = row.idxName.length;

        buf.put(off, (byte) len);

        for (int i = 0; i < len; i++)
            buf.put(off + i + 1, row.idxName[i]);

        buf.putLong(off + len + 1, row.pageId);
    }

    /**
     * Copy row data.
     *
     * @param dst Destination buffer.
     * @param dstOff Destination buf offset.
     * @param src Source buffer.
     * @param srcOff Src buf offset.
     * @throws IgniteCheckedException
     */
    private static void storeRow(final ByteBuffer dst, final int dstOff,
        final ByteBuffer src,
        final int srcOff) throws IgniteCheckedException {

        final byte len = src.get(srcOff);

        dst.put(dstOff, len);

        for (int i = 0; i < len; i++)
            dst.put(dstOff + i + 1, src.get(srcOff + i + 1));

        dst.putLong(dstOff + len + 1, src.getLong(srcOff + len + 1));
    }

    /**
     * Read row from buffer.
     *
     * @param buf Buffer to read.
     * @param off Offset in buf.
     * @return Read row.
     * @throws IgniteCheckedException
     */
    private static IndexItem readRow(final ByteBuffer buf, final int off) throws IgniteCheckedException {
        final byte len = buf.get(off);

        final byte[] idxName = new byte[len];

        for (int i = 0; i < len; i++)
            idxName[i] = buf.get(off + i + 1);

        final long pageId = buf.getLong(off + len + 1);

        return new IndexItem(idxName, pageId);
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
        public IndexInnerIO(final int ver) {
            super(T_INDEX_INNER, ver, false, MAX_IDX_NAME_LEN * 2 + 1 + 8); // UTF-16 symbols and 1 byte for length, 8 bytes pageId
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
        public IndexLeafIO(final int ver) {
            super(T_INDEX_LEAF, ver, MAX_IDX_NAME_LEN * 2 + 1 + 8); // UTF-16 symbols, 1 byte for length, 8 bytes pageId
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
     * @param cacheId Cache ID.
     * @return Tree instance.
     * @throws IgniteCheckedException
     */
    private MetaTree metaTree(final int cacheId) throws IgniteCheckedException {
        GridFutureAdapter<MetaTree> fut = trees.get(cacheId);

        if (fut == null) {
            fut = new GridFutureAdapter<>();

            final GridFutureAdapter<MetaTree> cur = trees.putIfAbsent(cacheId, fut);

            if (cur != null)
                fut = cur;
            else {
                final RootPage rootPage = metaPageTree(cacheId);

                fut.onDone(new MetaTree(cacheId, pageMem, rootPage.pageId(), null, IndexInnerIO.VERSIONS,
                    IndexLeafIO.VERSIONS, rootPage.isAllocated()));
            }
        }

        return fut.get();
    }
}

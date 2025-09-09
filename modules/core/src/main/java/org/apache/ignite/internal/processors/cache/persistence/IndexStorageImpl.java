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

package org.apache.ignite.internal.processors.cache.persistence;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.PageIdAllocator;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.pagemem.PageUtils;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.PageLockTrackerManager;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusInnerIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusLeafIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.IOVersions;
import org.apache.ignite.internal.processors.cache.persistence.tree.reuse.ReuseList;
import org.apache.ignite.internal.processors.cache.persistence.tree.util.PageHandler;
import org.apache.ignite.internal.processors.failure.FailureProcessor;
import org.apache.ignite.internal.util.lang.GridCursor;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Metadata storage.
 */
public class IndexStorageImpl implements IndexStorage {
    /** Max index name length (bytes num) */
    public static final int MAX_IDX_NAME_LEN = 255;

    /** Reserved size for index name. Needed for backward compatibility. */
    public static final int RESERVED_IDX_NAME_LEN = 768;

    /** Bytes in byte. */
    private static final int BYTE_LEN = 1;

    /** Page memory. */
    private final PageMemory pageMem;

    /** Index tree. */
    private final MetaTree metaTree;

    /** Meta page reuse tree. */
    private final ReuseList reuseList;

    /** Cache group ID. */
    private final int grpId;

    /** Whether group is shared. */
    private final boolean grpShared;

    /** */
    private final int allocPartId;

    /** */
    private final byte allocSpace;

    /**
     * @param treeName Tree name.
     * @param pageMem Page memory.
     * @param wal Write ahead log manager.
     * @param globalRmvId Global remove id.
     * @param grpId Group id.
     * @param grpShared Group shared.
     * @param allocPartId Alloc partition id.
     * @param allocSpace Alloc space.
     * @param reuseList Reuse list.
     * @param rootPageId Root page id.
     * @param initNew If index tree should be (re)created.
     * @param pageLockTrackerManager Page lock tracker manager.
     */
    public IndexStorageImpl(
        String treeName,
        PageMemory pageMem,
        @Nullable IgniteWriteAheadLogManager wal,
        AtomicLong globalRmvId,
        int grpId,
        boolean grpShared,
        int allocPartId,
        byte allocSpace,
        ReuseList reuseList,
        long rootPageId,
        boolean initNew,
        @Nullable FailureProcessor failureProcessor,
        PageLockTrackerManager pageLockTrackerManager
    ) {
        try {
            this.pageMem = pageMem;
            this.grpId = grpId;
            this.grpShared = grpShared;
            this.allocPartId = allocPartId;
            this.allocSpace = allocSpace;
            this.reuseList = reuseList;

            metaTree = new MetaTree(
                treeName,
                grpId,
                allocPartId,
                allocSpace,
                pageMem,
                wal,
                globalRmvId,
                rootPageId,
                reuseList,
                MetaStoreInnerIO.VERSIONS,
                MetaStoreLeafIO.VERSIONS,
                initNew,
                failureProcessor,
                pageLockTrackerManager
            );
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public RootPage allocateCacheIndex(Integer cacheId, String idxName, int segment)
        throws IgniteCheckedException {
        String maskedIdxName = maskCacheIndexName(cacheId, idxName, segment);

        return allocateIndex(maskedIdxName);
    }

    /** {@inheritDoc} */
    @Override public RootPage allocateIndex(String idxName) throws IgniteCheckedException {
        byte[] idxNameBytes = idxName.getBytes(UTF_8);

        checkIndexName(idxNameBytes);

        synchronized (this) {
            final MetaTree tree = metaTree;

            IndexItem row = tree.findOne(new IndexItem(idxNameBytes, 0));

            if (row == null) {
                long pageId = 0;

                if (reuseList != null)
                    pageId = reuseList.takeRecycledPage();

                pageId = pageId == 0 ? pageMem.allocatePage(grpId, allocPartId, allocSpace) : pageId;

                tree.put(new IndexItem(idxNameBytes, pageId));

                return new RootPage(new FullPageId(pageId, grpId), true);
            }
            else {
                FullPageId pageId = new FullPageId(row.pageId, grpId);

                return new RootPage(pageId, false);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public @Nullable RootPage findCacheIndex(
        Integer cacheId,
        String idxName,
        int segment
    ) throws IgniteCheckedException {
        idxName = maskCacheIndexName(cacheId, idxName, segment);

        byte[] idxNameBytes = idxName.getBytes(UTF_8);

        IndexItem row = metaTree.findOne(new IndexItem(idxNameBytes, 0));

        return row != null ? new RootPage(new FullPageId(row.pageId, grpId), false) : null;
    }

    /** {@inheritDoc} */
    @Override public @Nullable RootPage dropCacheIndex(
        Integer cacheId,
        String idxName,
        int segment
    ) throws IgniteCheckedException {
        String maskedIdxName = maskCacheIndexName(cacheId, idxName, segment);

        return dropIndex(maskedIdxName);
    }

    /** {@inheritDoc} */
    @Override public @Nullable RootPage dropIndex(String idxName) throws IgniteCheckedException {
        byte[] idxNameBytes = idxName.getBytes(UTF_8);

        IndexItem row = metaTree.remove(new IndexItem(idxNameBytes, 0));

        if (row != null) {
            if (reuseList == null)
                pageMem.freePage(grpId, row.pageId);
        }

        return row != null ? new RootPage(new FullPageId(row.pageId, grpId), false) : null;
    }

    /** {@inheritDoc} */
    @Override public @Nullable RootPage renameCacheIndex(
        Integer cacheId,
        String oldIdxName,
        String newIdxName,
        int segment
    ) throws IgniteCheckedException {
        byte[] oldIdxNameBytes = maskCacheIndexName(cacheId, oldIdxName, segment).getBytes(UTF_8);
        byte[] newIdxNameBytes = maskCacheIndexName(cacheId, newIdxName, segment).getBytes(UTF_8);

        checkIndexName(newIdxNameBytes);

        IndexItem rmv = metaTree.remove(new IndexItem(oldIdxNameBytes, 0));

        if (rmv != null) {
            metaTree.putx(new IndexItem(newIdxNameBytes, rmv.pageId));

            return new RootPage(new FullPageId(rmv.pageId, grpId), false);
        }
        else
            return null;
    }

    /** {@inheritDoc} */
    @Override public void destroy() throws IgniteCheckedException {
        metaTree.destroy();
    }

    /** {@inheritDoc} */
    @Override public Collection<String> getIndexNames() throws IgniteCheckedException {
        GridCursor<IndexItem> cursor = metaTree.find(null, null);

        ArrayList<String> names = new ArrayList<>((int)metaTree.size());

        while (cursor.next()) {
            IndexItem item = cursor.get();

            if (item != null)
                names.add(new String(item.idxName));
        }

        return names;
    }

    /** {@inheritDoc} */
    @Override public boolean nameIsAssosiatedWithCache(String idxName, int cacheId) {
        return !grpShared || idxName.startsWith(Integer.toString(cacheId));
    }

    /**
     * Mask cache index name.
     *
     * @param idxName Index name.
     * @return Masked name.
     */
    private String maskCacheIndexName(Integer cacheId, String idxName, int segment) {
        return (grpShared ? cacheId + "_" : "") + idxName + "%" + segment;
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
            String treeName,
            int cacheId,
            int allocPartId,
            byte allocSpace,
            PageMemory pageMem,
            IgniteWriteAheadLogManager wal,
            AtomicLong globalRmvId,
            long metaPageId,
            ReuseList reuseList,
            IOVersions<? extends BPlusInnerIO<IndexItem>> innerIos,
            IOVersions<? extends BPlusLeafIO<IndexItem>> leafIos,
            boolean initNew,
            @Nullable FailureProcessor failureProcessor,
            PageLockTrackerManager pageLockTrackerManager
        ) throws IgniteCheckedException {
            super(
                treeName,
                cacheId,
                null,
                pageMem,
                wal,
                globalRmvId,
                metaPageId,
                reuseList,
                innerIos,
                leafIos,
                PageIdAllocator.FLAG_IDX,
                failureProcessor,
                pageLockTrackerManager
            );

            this.allocPartId = allocPartId;
            this.allocSpace = allocSpace;

            initTree(initNew);
        }

        /** {@inheritDoc} */
        @Override protected long allocatePageNoReuse() throws IgniteCheckedException {
            return pageMem.allocatePage(groupId(), allocPartId, allocSpace);
        }

        /** {@inheritDoc} */
        @Override protected int compare(BPlusIO<IndexItem> io, long pageAddr, int idx,
            IndexItem row) throws IgniteCheckedException {
            int off = ((IndexIO)io).getOffset(pageAddr, idx);

            int shift = 0;

            // Compare index names.
            int len = PageUtils.getUnsignedByte(pageAddr, off + shift);

            shift += BYTE_LEN;

            for (int i = 0; i < len && i < row.idxName.length; i++) {
                int cmp = Byte.compare(PageUtils.getByte(pageAddr, off + i + shift), row.idxName[i]);

                if (cmp != 0)
                    return cmp;
            }

            return Integer.compare(len, row.idxName.length);
        }

        /** {@inheritDoc} */
        @Override public IndexItem getRow(BPlusIO<IndexItem> io, long pageAddr,
            int idx, Object ignore) throws IgniteCheckedException {
            return readRow(pageAddr, ((IndexIO)io).getOffset(pageAddr, idx));
        }
    }

    /**
     *
     */
    public static class IndexItem {
        /** */
        private final byte[] idxName;

        /** */
        private final long pageId;

        /**
         * @param idxName Index name.
         * @param pageId Page ID.
         */
        private IndexItem(byte[] idxName, long pageId) {
            this.idxName = idxName;
            this.pageId = pageId;
        }

        /** */
        public long pageId() {
            return pageId;
        }

        /** */
        public String nameString() {
            return new String(idxName);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "I [idxName=" + nameString() + ", pageId=" + U.hexLong(pageId) + ']';
        }
    }

    /**
     * Store row to buffer.
     *
     * @param pageAddr Page address.
     * @param off Offset in buf.
     * @param row Row to store.
     */
    private static void storeRow(
        long pageAddr,
        int off,
        IndexItem row
    ) {
        // Index name length.
        PageUtils.putUnsignedByte(pageAddr, off, row.idxName.length);
        off++;

        // Index name.
        PageUtils.putBytes(pageAddr, off, row.idxName);
        off += row.idxName.length;

        // Page ID.
        PageUtils.putLong(pageAddr, off, row.pageId);
    }

    /**
     * Copy row data.
     *
     * @param dstPageAddr Destination page address.
     * @param dstOff Destination buf offset.
     * @param srcPageAddr Source page address.
     * @param srcOff Src buf offset.
     */
    private static void storeRow(
        long dstPageAddr,
        int dstOff,
        long srcPageAddr,
        int srcOff
    ) {
        // Index name length.
        int len = PageUtils.getUnsignedByte(srcPageAddr, srcOff);
        srcOff++;

        PageUtils.putUnsignedByte(dstPageAddr, dstOff, len);
        dstOff++;

        PageHandler.copyMemory(srcPageAddr, srcOff, dstPageAddr, dstOff, len);
        srcOff += len;
        dstOff += len;

        // Page ID.
        PageUtils.putLong(dstPageAddr, dstOff, PageUtils.getLong(srcPageAddr, srcOff));
    }

    /**
     * Read row from buffer.
     *
     * @param pageAddr Page address.
     * @param off Offset.
     * @return Read row.
     */
    private static IndexItem readRow(long pageAddr, int off) {
        // Index name length.
        int len = PageUtils.getUnsignedByte(pageAddr, off) & 0xFF;
        off++;

        // Index name.
        byte[] idxName = PageUtils.getBytes(pageAddr, off, len);
        off += len;

        // Page ID.
        long pageId = PageUtils.getLong(pageAddr, off);

        return new IndexItem(idxName, pageId);
    }

    /**
     *
     */
    private interface IndexIO {
        /**
         * @param pageAddr Page address.
         * @param idx Index.
         * @return Offset in buffer according to {@code idx}.
         */
        int getOffset(long pageAddr, int idx);
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
        private MetaStoreInnerIO(int ver) {
            // name bytes and 1 byte for length, 8 bytes pageId
            super(T_METASTORE_INNER, ver, false, RESERVED_IDX_NAME_LEN + 1 + 8);
        }

        /** {@inheritDoc} */
        @Override public void storeByOffset(long pageAddr, int off, IndexItem row) throws IgniteCheckedException {
            storeRow(pageAddr, off, row);
        }

        /** {@inheritDoc} */
        @Override public void store(long dstPageAddr, int dstIdx, BPlusIO<IndexItem> srcIo,
            long srcPageAddr,
            int srcIdx) throws IgniteCheckedException {
            storeRow(dstPageAddr, offset(dstIdx), srcPageAddr, ((IndexIO)srcIo).getOffset(srcPageAddr, srcIdx));
        }

        /** {@inheritDoc} */
        @Override public IndexItem getLookupRow(BPlusTree<IndexItem, ?> tree, long pageAddr,
            int idx) throws IgniteCheckedException {
            return readRow(pageAddr, offset(idx));
        }

        /** {@inheritDoc} */
        @Override public int getOffset(long pageAddr, int idx) {
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
        private MetaStoreLeafIO(int ver) {
            // 4 byte cache ID, UTF-16 symbols and 1 byte for length, 8 bytes pageId
            super(T_METASTORE_LEAF, ver, RESERVED_IDX_NAME_LEN + 1 + 8);
        }

        /** {@inheritDoc} */
        @Override public void storeByOffset(long buf, int off, IndexItem row) throws IgniteCheckedException {
            assertPageType(buf);

            storeRow(buf, off, row);
        }

        /** {@inheritDoc} */
        @Override public void store(long dstPageAddr,
            int dstIdx,
            BPlusIO<IndexItem> srcIo,
            long srcPageAddr,
            int srcIdx) throws IgniteCheckedException {
            assertPageType(dstPageAddr);

            storeRow(dstPageAddr, offset(dstIdx), srcPageAddr, ((IndexIO)srcIo).getOffset(srcPageAddr, srcIdx));
        }

        /** {@inheritDoc} */
        @Override public IndexItem getLookupRow(BPlusTree<IndexItem, ?> tree,
            long pageAddr,
            int idx) throws IgniteCheckedException {
            return readRow(pageAddr, offset(idx));
        }

        /** {@inheritDoc} */
        @Override public int getOffset(long pageAddr, int idx) {
            return offset(idx);
        }
    }

    /**
     * Checking the name of the index.
     *
     * @param idxName Index name bytes.
     */
    public static void checkIndexName(byte[] idxName) {
        if (idxName.length > MAX_IDX_NAME_LEN) {
            throw new IllegalArgumentException("Too long encoded indexName [maxAllowed=" + MAX_IDX_NAME_LEN +
                ", currentLength=" + idxName.length + ", name=" + idxName + "]");
        }
    }
}

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

package org.apache.ignite.internal.processors.cache.persistence.metastorage;

import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.pagemem.PageUtils;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusInnerIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusLeafIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.IOVersions;
import org.apache.ignite.internal.processors.cache.persistence.tree.reuse.ReuseList;
import org.apache.ignite.internal.processors.failure.FailureProcessor;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public class MetastorageTree extends BPlusTree<MetastorageSearchRow, MetastorageDataRow> {
    /** Max key length (bytes num) */
    public static final int MAX_KEY_LEN = 64;

    /** */
    private MetastorageRowStore rowStore;

    /**
     * @param pageMem Page memory instance.
     * @param wal WAL manager.
     * @param globalRmvId Global remove ID.
     * @param metaPageId Meta page ID.
     * @param reuseList Reuse list.
     * @param rowStore Row store.
     * @param initNew Init new flag, if {@code true}, then new tree will be allocated.
     * @param failureProcessor To call if the tree is corrupted.
     * @throws IgniteCheckedException If failed to initialize.
     */
    public MetastorageTree(int cacheId,
        PageMemory pageMem,
        IgniteWriteAheadLogManager wal,
        AtomicLong globalRmvId,
        ReuseList reuseList,
        MetastorageRowStore rowStore,
        long metaPageId,
        boolean initNew,
        @Nullable FailureProcessor failureProcessor) throws IgniteCheckedException {
        super("Metastorage", cacheId, pageMem, wal,
            globalRmvId, metaPageId, reuseList, MetastorageInnerIO.VERSIONS, MetastoreLeafIO.VERSIONS, failureProcessor);

        this.rowStore = rowStore;

        initTree(initNew);
    }

    /** {@inheritDoc} */
    @Override protected int compare(BPlusIO<MetastorageSearchRow> io, long pageAddr, int idx,
        MetastorageSearchRow row) {

        String key = ((DataLinkIO)io).getKey(pageAddr, idx);

        return key.compareTo(row.key());
    }

    /** {@inheritDoc} */
    @Override public MetastorageDataRow getRow(BPlusIO<MetastorageSearchRow> io, long pageAddr, int idx,
        Object x) throws IgniteCheckedException {
        long link = ((DataLinkIO)io).getLink(pageAddr, idx);
        String key = ((DataLinkIO)io).getKey(pageAddr, idx);

        return rowStore.dataRow(key, link);
    }

    /**
     * @return RowStore.
     */
    public MetastorageRowStore rowStore() {
        return rowStore;
    }

    /**
     *
     */
    private interface DataLinkIO {
        /**
         * @param pageAddr Page address.
         * @param idx Index.
         * @return Row link.
         */
        public long getLink(long pageAddr, int idx);

        /**
         * @param pageAddr Page address.
         * @param idx Index.
         * @return Key size in bytes.
         */
        public short getKeySize(long pageAddr, int idx);

        /**
         * @param pageAddr Page address.
         * @param idx Index.
         * @return Key.
         */
        public String getKey(long pageAddr, int idx);
    }

    /**
     *
     */
    public static class MetastorageInnerIO extends BPlusInnerIO<MetastorageSearchRow> implements DataLinkIO {
        /** */
        public static final IOVersions<MetastorageInnerIO> VERSIONS = new IOVersions<>(
            new MetastorageInnerIO(1)
        );

        /**
         * @param ver Page format version.
         */
        MetastorageInnerIO(int ver) {
            super(T_DATA_REF_METASTORAGE_INNER, ver, true, 10 + MAX_KEY_LEN);
        }

        /** {@inheritDoc} */
        @Override public void storeByOffset(long pageAddr, int off,
            MetastorageSearchRow row) {
            assert row.link() != 0;

            PageUtils.putLong(pageAddr, off, row.link());

            byte[] bytes = row.key().getBytes();
            assert bytes.length <= MAX_KEY_LEN;

            PageUtils.putShort(pageAddr, off + 8, (short)bytes.length);
            PageUtils.putBytes(pageAddr, off + 10, bytes);
        }

        /** {@inheritDoc} */
        @Override public void store(long dstPageAddr, int dstIdx, BPlusIO<MetastorageSearchRow> srcIo, long srcPageAddr,
            int srcIdx) {
            int srcOff = srcIo.offset(srcIdx);
            int dstOff = offset(dstIdx);

            long link = ((DataLinkIO)srcIo).getLink(srcPageAddr, srcIdx);
            short len = ((DataLinkIO)srcIo).getKeySize(srcPageAddr, srcIdx);

            byte[] payload = PageUtils.getBytes(srcPageAddr, srcOff + 10, len);

            PageUtils.putLong(dstPageAddr, dstOff, link);
            PageUtils.putShort(dstPageAddr, dstOff + 8, len);
            PageUtils.putBytes(dstPageAddr, dstOff + 10, payload);
        }

        /** {@inheritDoc} */
        @Override public MetastorageSearchRow getLookupRow(BPlusTree<MetastorageSearchRow, ?> tree, long pageAddr,
            int idx) {
            long link = getLink(pageAddr, idx);
            String key = getKey(pageAddr, idx);

            return new MetsatorageSearchRowImpl(key, link);
        }

        /** {@inheritDoc} */
        @Override public long getLink(long pageAddr, int idx) {
            assert idx < getCount(pageAddr) : idx;

            return PageUtils.getLong(pageAddr, offset(idx));
        }

        /** {@inheritDoc} */
        @Override public short getKeySize(long pageAddr, int idx) {
            return PageUtils.getShort(pageAddr, offset(idx) + 8);
        }

        /** {@inheritDoc} */
        @Override public String getKey(long pageAddr, int idx) {
            int len = PageUtils.getShort(pageAddr, offset(idx) + 8);
            byte[] bytes = PageUtils.getBytes(pageAddr, offset(idx) + 10, len);
            return new String(bytes);
        }
    }

    /**
     *
     */
    public static class MetastoreLeafIO extends BPlusLeafIO<MetastorageSearchRow> implements DataLinkIO {
        /** */
        public static final IOVersions<MetastoreLeafIO> VERSIONS = new IOVersions<>(
            new MetastoreLeafIO(1)
        );

        /**
         * @param ver Page format version.
         */
        MetastoreLeafIO(int ver) {
            super(T_DATA_REF_METASTORAGE_LEAF, ver, 10 + MAX_KEY_LEN);
        }

        /** {@inheritDoc} */
        @Override public void storeByOffset(long pageAddr, int off,
            MetastorageSearchRow row) {
            assert row.link() != 0;

            PageUtils.putLong(pageAddr, off, row.link());
            byte[] bytes = row.key().getBytes();

            assert bytes.length <= MAX_KEY_LEN;

            PageUtils.putShort(pageAddr, off + 8, (short)bytes.length);
            PageUtils.putBytes(pageAddr, off + 10, bytes);
        }

        /** {@inheritDoc} */
        @Override public void store(long dstPageAddr, int dstIdx, BPlusIO<MetastorageSearchRow> srcIo, long srcPageAddr,
            int srcIdx) {
            int srcOff = srcIo.offset(srcIdx);
            int dstOff = offset(dstIdx);

            long link = ((DataLinkIO)srcIo).getLink(srcPageAddr, srcIdx);
            short len = ((DataLinkIO)srcIo).getKeySize(srcPageAddr, srcIdx);

            byte[] payload = PageUtils.getBytes(srcPageAddr, srcOff + 10, len);

            PageUtils.putLong(dstPageAddr, dstOff, link);
            PageUtils.putShort(dstPageAddr, dstOff + 8, len);
            PageUtils.putBytes(dstPageAddr, dstOff + 10, payload);
        }

        /** {@inheritDoc} */
        @Override public MetastorageSearchRow getLookupRow(BPlusTree<MetastorageSearchRow, ?> tree, long pageAddr,
            int idx) {
            long link = getLink(pageAddr, idx);
            String key = getKey(pageAddr, idx);

            return new MetsatorageSearchRowImpl(key, link);
        }

        /** {@inheritDoc} */
        @Override public long getLink(long pageAddr, int idx) {
            assert idx < getCount(pageAddr) : idx;

            return PageUtils.getLong(pageAddr, offset(idx));
        }

        /** {@inheritDoc} */
        @Override public short getKeySize(long pageAddr, int idx) {
            return PageUtils.getShort(pageAddr, offset(idx) + 8);
        }

        /** {@inheritDoc} */
        @Override public String getKey(long pageAddr, int idx) {
            int len = PageUtils.getShort(pageAddr, offset(idx) + 8);
            byte[] bytes = PageUtils.getBytes(pageAddr, offset(idx) + 10, len);
            return new String(bytes);
        }
    }
}

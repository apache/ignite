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

package org.apache.ignite.internal.processors.cache.tree;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.PageUtils;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRowAdapter;
import org.apache.ignite.internal.processors.cache.persistence.CacheSearchRow;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.DataPageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.DataPagePayload;
import org.apache.ignite.internal.processors.cache.persistence.tree.reuse.ReuseList;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.typedef.internal.CU;

import static org.apache.ignite.internal.pagemem.PageIdUtils.itemId;
import static org.apache.ignite.internal.pagemem.PageIdUtils.pageId;

/**
 *
 */
public class CacheDataTree extends BPlusTree<CacheSearchRow, CacheDataRow> {
    /** */
    private final CacheDataRowStore rowStore;

    /** */
    private final CacheGroupContext grp;

    /**
     * @param grp Ccahe group.
     * @param name Tree name.
     * @param reuseList Reuse list.
     * @param rowStore Row store.
     * @param metaPageId Meta page ID.
     * @param initNew Initialize new index.
     * @throws IgniteCheckedException If failed.
     */
    public CacheDataTree(
        CacheGroupContext grp,
        String name,
        ReuseList reuseList,
        CacheDataRowStore rowStore,
        long metaPageId,
        boolean initNew
    ) throws IgniteCheckedException {
        super(name,
            grp.groupId(),
            grp.dataRegion().pageMemory(),
            grp.dataRegion().config().isPersistenceEnabled() ? grp.shared().wal() : null,
            grp.offheap().globalRemoveId(),
            metaPageId,
            reuseList,
            grp.sharedGroup() ? CacheIdAwareDataInnerIO.VERSIONS : DataInnerIO.VERSIONS,
            grp.sharedGroup() ? CacheIdAwareDataLeafIO.VERSIONS : DataLeafIO.VERSIONS,
            grp.shared().kernalContext().failure());

        assert rowStore != null;

        this.rowStore = rowStore;
        this.grp = grp;

        assert !grp.dataRegion().config().isPersistenceEnabled() || grp.shared().database().checkpointLockIsHeldByThread();

        initTree(initNew);
    }

    /**
     * @return Row store.
     */
    CacheDataRowStore rowStore() {
        return rowStore;
    }

    /** {@inheritDoc} */
    @Override protected int compare(BPlusIO<CacheSearchRow> iox, long pageAddr, int idx, CacheSearchRow row)
        throws IgniteCheckedException {
        RowLinkIO io = (RowLinkIO)iox;

        int cmp;

        if (grp.sharedGroup()) {
            assert row.cacheId() != CU.UNDEFINED_CACHE_ID : "Cache ID is not provided: " + row;

            int cacheId = io.getCacheId(pageAddr, idx);

            assert cacheId != CU.UNDEFINED_CACHE_ID : "Cache ID is not stored";

            cmp = Integer.compare(cacheId, row.cacheId());

            if (cmp != 0)
                return cmp;

            if (row.key() == null) {
                assert row.getClass() == SearchRow.class : row;

                // A search row with a cache ID only is used as a cache bound.
                // The found position will be shifted until the exact cache bound is found;
                // See for details:
                // o.a.i.i.p.c.database.tree.BPlusTree.ForwardCursor.findLowerBound()
                // o.a.i.i.p.c.database.tree.BPlusTree.ForwardCursor.findUpperBound()
                return cmp;
            }
        }

        cmp = Integer.compare(io.getHash(pageAddr, idx), row.hash());

        if (cmp != 0)
            return cmp;

        long link = io.getLink(pageAddr, idx);

        assert row.key() != null : row;

        return compareKeys(row.key(), link);
    }

    /** {@inheritDoc} */
    @Override protected CacheDataRow getRow(BPlusIO<CacheSearchRow> io, long pageAddr, int idx, Object flags)
        throws IgniteCheckedException {
        long link = ((RowLinkIO)io).getLink(pageAddr, idx);
        int hash = ((RowLinkIO)io).getHash(pageAddr, idx);
        int cacheId = ((RowLinkIO)io).getCacheId(pageAddr, idx);

        CacheDataRowAdapter.RowData x = flags != null ?
            (CacheDataRowAdapter.RowData)flags :
            CacheDataRowAdapter.RowData.FULL;

        return rowStore.dataRow(cacheId, hash, link, x);
    }

    /**
     * @param key Key.
     * @param link Link.
     * @return Compare result.
     * @throws IgniteCheckedException If failed.
     */
    private int compareKeys(KeyCacheObject key, final long link) throws IgniteCheckedException {
        byte[] bytes = key.valueBytes(grp.cacheObjectContext());

        final long pageId = pageId(link);
        final long page = acquirePage(pageId);

        try {
            long pageAddr = readLock(pageId, page); // Non-empty data page must not be recycled.

            assert pageAddr != 0L : link;

            try {
                DataPageIO io = DataPageIO.VERSIONS.forPage(pageAddr);

                DataPagePayload data = io.readPayload(pageAddr,
                    itemId(link),
                    pageSize());

                if (data.nextLink() == 0) {
                    long addr = pageAddr + data.offset();

                    if (grp.storeCacheIdInDataPage())
                        addr += 4; // Skip cache id.

                    final int len = PageUtils.getInt(addr, 0);

                    int lenCmp = Integer.compare(len, bytes.length);

                    if (lenCmp != 0)
                        return lenCmp;

                    addr += 5; // Skip length and type byte.

                    final int words = len / 8;

                    for (int i = 0; i < words; i++) {
                        int off = i * 8;

                        long b1 = PageUtils.getLong(addr, off);
                        long b2 = GridUnsafe.getLong(bytes, GridUnsafe.BYTE_ARR_OFF + off);

                        int cmp = Long.compare(b1, b2);

                        if (cmp != 0)
                            return cmp;
                    }

                    for (int i = words * 8; i < len; i++) {
                        byte b1 = PageUtils.getByte(addr, i);
                        byte b2 = bytes[i];

                        if (b1 != b2)
                            return b1 > b2 ? 1 : -1;
                    }

                    return 0;
                }
            }
            finally {
                readUnlock(pageId, page, pageAddr);
            }
        }
        finally {
            releasePage(pageId, page);
        }

        // TODO GG-11768.
        CacheDataRowAdapter other = new CacheDataRowAdapter(link);
        other.initFromLink(grp, CacheDataRowAdapter.RowData.KEY_ONLY);

        byte[] bytes1 = other.key().valueBytes(grp.cacheObjectContext());
        byte[] bytes2 = key.valueBytes(grp.cacheObjectContext());

        int lenCmp = Integer.compare(bytes1.length, bytes2.length);

        if (lenCmp != 0)
            return lenCmp;

        final int len = bytes1.length;
        final int words = len / 8;

        for (int i = 0; i < words; i++) {
            int off = GridUnsafe.BYTE_ARR_INT_OFF + i * 8;

            long b1 = GridUnsafe.getLong(bytes1, off);
            long b2 = GridUnsafe.getLong(bytes2, off);

            int cmp = Long.compare(b1, b2);

            if (cmp != 0)
                return cmp;
        }

        for (int i = words * 8; i < len; i++) {
            byte b1 = bytes1[i];
            byte b2 = bytes2[i];

            if (b1 != b2)
                return b1 > b2 ? 1 : -1;
        }

        return 0;
    }
}

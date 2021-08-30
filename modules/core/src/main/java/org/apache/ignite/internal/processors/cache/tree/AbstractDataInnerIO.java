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
import org.apache.ignite.internal.processors.cache.mvcc.MvccUtils;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRowAdapter;
import org.apache.ignite.internal.processors.cache.persistence.CacheSearchRow;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusInnerIO;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.lang.IgniteInClosure;

import static org.apache.ignite.internal.processors.cache.mvcc.MvccUtils.mvccVersionIsValid;

/**
 *
 */
public abstract class AbstractDataInnerIO extends BPlusInnerIO<CacheSearchRow> implements RowLinkIO {
    /**
     * @param type Page type.
     * @param ver Page format version.
     * @param canGetRow If we can get full row from this page.
     * @param itemSize Single item size on page.
     */
    protected AbstractDataInnerIO(int type, int ver, boolean canGetRow, int itemSize) {
        super(type, ver, canGetRow, itemSize);
    }

    /** {@inheritDoc} */
    @Override public final void storeByOffset(long pageAddr, int off, CacheSearchRow row) {
        assert row.link() != 0;

        PageUtils.putLong(pageAddr, off, row.link());
        off += 8;

        PageUtils.putInt(pageAddr, off, row.hash());
        off += 4;

        if (storeCacheId()) {
            assert row.cacheId() != CU.UNDEFINED_CACHE_ID : row;

            PageUtils.putInt(pageAddr, off, row.cacheId());
            off += 4;
        }

        if (storeMvccVersion()) {
            long mvccCrd = row.mvccCoordinatorVersion();
            long mvccCntr = row.mvccCounter();
            int opCntr = row.mvccOperationCounter();

            assert mvccVersionIsValid(mvccCrd, mvccCntr, opCntr);

            PageUtils.putLong(pageAddr, off, mvccCrd);
            off += 8;

            PageUtils.putLong(pageAddr, off, mvccCntr);
            off += 8;

            PageUtils.putInt(pageAddr, off, opCntr);
        }
    }

    /** {@inheritDoc} */
    @Override public final CacheSearchRow getLookupRow(BPlusTree<CacheSearchRow, ?> tree, long pageAddr, int idx)
        throws IgniteCheckedException {
        long link = getLink(pageAddr, idx);
        int hash = getHash(pageAddr, idx);

        int cacheId = storeCacheId() ? getCacheId(pageAddr, idx) : CU.UNDEFINED_CACHE_ID;

        if (storeMvccVersion()) {
            long mvccCrd = getMvccCoordinatorVersion(pageAddr, idx);
            long mvccCntr = getMvccCounter(pageAddr, idx);
            int opCntr = getMvccOperationCounter(pageAddr, idx);

            assert mvccVersionIsValid(mvccCrd, mvccCntr, opCntr);

            return ((CacheDataTree)tree).rowStore().mvccRow(cacheId,
                hash,
                link,
                CacheDataRowAdapter.RowData.KEY_ONLY,
                mvccCrd,
                mvccCntr,
                opCntr);
        }

        return ((CacheDataTree)tree).rowStore().keySearchRow(cacheId, hash, link);
    }

    /** {@inheritDoc} */
    @Override public final void store(long dstPageAddr,
        int dstIdx,
        BPlusIO<CacheSearchRow> srcIo,
        long srcPageAddr,
        int srcIdx)
    {
        RowLinkIO rowIo = ((RowLinkIO)srcIo);

        long link = rowIo.getLink(srcPageAddr, srcIdx);
        int hash = rowIo.getHash(srcPageAddr, srcIdx);

        int off = offset(dstIdx);

        PageUtils.putLong(dstPageAddr, off, link);
        off += 8;

        PageUtils.putInt(dstPageAddr, off, hash);
        off += 4;

        if (storeCacheId()) {
            int cacheId = rowIo.getCacheId(srcPageAddr, srcIdx);

            assert cacheId != CU.UNDEFINED_CACHE_ID;

            PageUtils.putInt(dstPageAddr, off, cacheId);
            off += 4;
        }

        if (storeMvccVersion()) {
            long mvccCrd = rowIo.getMvccCoordinatorVersion(srcPageAddr, srcIdx);
            long mvccCntr = rowIo.getMvccCounter(srcPageAddr, srcIdx);
            int opCntr = rowIo.getMvccOperationCounter(srcPageAddr, srcIdx);

            assert MvccUtils.mvccVersionIsValid(mvccCrd, mvccCntr, opCntr);

            PageUtils.putLong(dstPageAddr, off, mvccCrd);
            off += 8;

            PageUtils.putLong(dstPageAddr, off, mvccCntr);
            off += 8;

            PageUtils.putInt(dstPageAddr, off, opCntr);
        }
    }

    /** {@inheritDoc} */
    @Override public final long getLink(long pageAddr, int idx) {
        assert idx < getCount(pageAddr) : idx;

        return PageUtils.getLong(pageAddr, offset(idx));
    }

    /** {@inheritDoc} */
    @Override public final int getHash(long pageAddr, int idx) {
        return PageUtils.getInt(pageAddr, offset(idx) + 8);
    }

    /** {@inheritDoc} */
    @Override public void visit(long pageAddr, IgniteInClosure<CacheSearchRow> c) {
        int cnt = getCount(pageAddr);

        for (int i = 0; i < cnt; i++)
            c.apply(new CacheDataRowAdapter(getLink(pageAddr, i)));
    }

    /**
     * @return {@code True} if cache ID has to be stored.
     */
    protected boolean storeCacheId() {
        return false;
    }

    /**
     * @return {@code True} if mvcc version has to be stored.
     */
    protected boolean storeMvccVersion() {
        return false;
    }
}

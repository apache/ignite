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
import org.apache.ignite.internal.processors.cache.mvcc.MvccProcessor;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRowAdapter;
import org.apache.ignite.internal.processors.cache.persistence.CacheSearchRow;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusLeafIO;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.lang.IgniteInClosure;

import static org.apache.ignite.internal.processors.cache.mvcc.MvccProcessor.MVCC_COUNTER_NA;
import static org.apache.ignite.internal.processors.cache.mvcc.MvccUtils.assertMvccVersionValid;

/**
 *
 */
public abstract class AbstractDataLeafIO extends BPlusLeafIO<CacheSearchRow> implements RowLinkIO {
    /**
     * @param type Page type.
     * @param ver Page format version.
     * @param itemSize Single item size on page.
     */
    public AbstractDataLeafIO(int type, int ver, int itemSize) {
        super(type, ver, itemSize);
    }

    /** {@inheritDoc} */
    @Override public void storeByOffset(long pageAddr, int off, CacheSearchRow row) {
        assert row.link() != 0;

        PageUtils.putLong(pageAddr, off, row.link());
        off += 8;

        PageUtils.putInt(pageAddr, off, row.hash());
        off += 4;

        if (storeCacheId()) {
            assert row.cacheId() != CU.UNDEFINED_CACHE_ID;

            PageUtils.putInt(pageAddr, off, row.cacheId());
            off += 4;
        }

        if (storeMvccVersion()) {
            long mvccCrdVer = row.mvccCoordinatorVersion();
            long mvccCntr = row.mvccCounter();

            assert assertMvccVersionValid(mvccCrdVer, mvccCntr);

            PageUtils.putLong(pageAddr, off, mvccCrdVer);
            off += 8;

            PageUtils.putLong(pageAddr, off, mvccCntr);
            off += 8;

            PageUtils.putLong(pageAddr, off, mvccCrdVer);
            off += 8;

            PageUtils.putLong(pageAddr, off, mvccCntr);
        }
    }

    /** {@inheritDoc} */
    @Override public void store(long dstPageAddr, int dstIdx, BPlusIO<CacheSearchRow> srcIo, long srcPageAddr,
        int srcIdx) {
        RowLinkIO rowIo = (RowLinkIO) srcIo;

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
            long mvccUpdateTopVer = rowIo.getMvccCoordinatorVersion(srcPageAddr, srcIdx);
            long mvccUpdateCntr = rowIo.getMvccCounter(srcPageAddr, srcIdx);

            assert mvccUpdateTopVer > 0 : mvccUpdateCntr;
            assert mvccUpdateCntr != MVCC_COUNTER_NA;

            long lockCrdVer = rowIo.getMvccLockCoordinatorVersion(srcPageAddr, srcIdx);
            long lockCntr = rowIo.getMvccLockCounter(srcPageAddr, srcIdx);

            assert lockCrdVer >= 0;
            assert lockCntr >= 0;

            PageUtils.putLong(dstPageAddr, off, mvccUpdateTopVer);
            off += 8;

            PageUtils.putLong(dstPageAddr, off, mvccUpdateCntr);
            off += 8;

            PageUtils.putLong(dstPageAddr, off, lockCrdVer);
            off += 8;

            PageUtils.putLong(dstPageAddr, off, lockCntr);
        }
    }

    /** {@inheritDoc} */
    @Override public final CacheSearchRow getLookupRow(BPlusTree<CacheSearchRow, ?> tree, long pageAddr, int idx)
        throws IgniteCheckedException {
        long link = getLink(pageAddr, idx);
        int hash = getHash(pageAddr, idx);
        int cacheId = getCacheId(pageAddr, idx);

        if (storeMvccVersion()) {
            long mvccTopVer = getMvccCoordinatorVersion(pageAddr, idx);
            long mvccCntr = getMvccCounter(pageAddr, idx);

            return ((CacheDataTree)tree).rowStore().mvccRow(cacheId,
                hash,
                link,
                CacheDataRowAdapter.RowData.KEY_ONLY,
                mvccTopVer,
                mvccCntr);
        }

        return ((CacheDataTree)tree).rowStore().keySearchRow(cacheId, hash, link);
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

    /** {@inheritDoc} */
    @Override public long getMvccLockCoordinatorVersion(long pageAddr, int idx) {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public long getMvccLockCounter(long pageAddr, int idx) {
        return MvccProcessor.MVCC_COUNTER_NA;
    }

    /** {@inheritDoc} */
    @Override public void setMvccLockCoordinatorVersion(long pageAddr, int idx, long lockCrd) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public void setMvccLockCounter(long pageAddr, int idx, long lockCntr) {
        throw new UnsupportedOperationException();
    }

    /**
     * @return {@code True} if cache ID has to be stored.
     */
    protected abstract boolean storeCacheId();

    /**
     * @return {@code True} if mvcc version has to be stored.
     */
    protected abstract boolean storeMvccVersion();
}

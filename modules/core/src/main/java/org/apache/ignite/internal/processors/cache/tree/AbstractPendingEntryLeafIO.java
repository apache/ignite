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
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusLeafIO;
import org.apache.ignite.internal.util.typedef.internal.CU;

/**
 *
 */
public abstract class AbstractPendingEntryLeafIO extends BPlusLeafIO<PendingRow> implements PendingRowIO {
    /**
     * @param type Page type.
     * @param ver Page format version.
     * @param itemSize Single item size on page.
     */
    AbstractPendingEntryLeafIO(int type, int ver, int itemSize) {
        super(type, ver, itemSize);
    }

    /** {@inheritDoc} */
    @Override public void storeByOffset(long pageAddr, int off, PendingRow row) throws IgniteCheckedException {
        assert row.link != 0;
        assert row.expireTime != 0;

        PageUtils.putLong(pageAddr, off, row.expireTime);
        PageUtils.putLong(pageAddr, off + 8, row.link);

        if (storeCacheId()) {
            assert row.cacheId != CU.UNDEFINED_CACHE_ID;

            PageUtils.putInt(pageAddr, off + 16, row.cacheId);
        }
    }

    /** {@inheritDoc} */
    @Override public void store(long dstPageAddr,
        int dstIdx,
        BPlusIO<PendingRow> srcIo,
        long srcPageAddr,
        int srcIdx) throws IgniteCheckedException {
        int dstOff = offset(dstIdx);

        long link = ((PendingRowIO)srcIo).getLink(srcPageAddr, srcIdx);
        long expireTime = ((PendingRowIO)srcIo).getExpireTime(srcPageAddr, srcIdx);

        PageUtils.putLong(dstPageAddr, dstOff, expireTime);
        PageUtils.putLong(dstPageAddr, dstOff + 8, link);

        if (storeCacheId()) {
            int cacheId = ((PendingRowIO)srcIo).getCacheId(srcPageAddr, srcIdx);

            assert cacheId != CU.UNDEFINED_CACHE_ID;

            PageUtils.putInt(dstPageAddr, dstOff + 16, cacheId);
        }
    }

    /** {@inheritDoc} */
    @Override public PendingRow getLookupRow(BPlusTree<PendingRow, ?> tree, long pageAddr, int idx)
        throws IgniteCheckedException {
        return new PendingRow(getCacheId(pageAddr, idx), getExpireTime(pageAddr, idx), getLink(pageAddr, idx));
    }

    /** {@inheritDoc} */
    @Override public long getExpireTime(long pageAddr, int idx) {
        return PageUtils.getLong(pageAddr, offset(idx));
    }

    /** {@inheritDoc} */
    @Override public long getLink(long pageAddr, int idx) {
        return PageUtils.getLong(pageAddr, offset(idx) + 8);
    }

    /**
     * @return {@code True} if cache ID has to be stored.
     */
    protected abstract boolean storeCacheId();
}

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
 *
 */

package org.apache.ignite.internal.processors.cache.persistence.tree.io;

import java.util.Map;
import org.apache.ignite.internal.pagemem.PageUtils;

/**
 *
 */
public class PagePartitionCountersIO extends PageIO {
    /** */
    private static final int CNT_OFF = COMMON_HEADER_END;

    /** */
    private static final int LAST_FLAG_OFF = CNT_OFF + 2;

    /** */
    private static final int NEXT_COUNTERS_PAGE_OFF = LAST_FLAG_OFF + 1;

    /** */
    private static final int ITEMS_OFF = NEXT_COUNTERS_PAGE_OFF + 8;

    /** */
    private static final int ITEM_SIZE = 12;

    /** */
    private static final byte LAST_FLAG = 0b1;

    /** */
    public static final IOVersions<PagePartitionCountersIO> VERSIONS = new IOVersions<>(
        new PagePartitionCountersIO(1)
    );

    /**
     * @param ver Page format version.
     */
    public PagePartitionCountersIO(int ver) {
        super(T_PART_CNTRS, ver);
    }

    /** {@inheritDoc} */
    @Override public void initNewPage(long pageAddr, long pageId, int pageSize) {
        super.initNewPage(pageAddr, pageId, pageSize);

        setCount(pageAddr, 0);
        setNextCountersPageId(pageAddr, 0);
    }

    /**
     * @param pageAddr Page address.
     * @return Next counters page ID or {@code 0} if it does not exist.
     */
    public long getNextCountersPageId(long pageAddr) {
        return PageUtils.getLong(pageAddr, NEXT_COUNTERS_PAGE_OFF);
    }

    /**
     * @param pageAddr Page address.
     * @param partMetaPageId Next counters page ID.
     */
    public void setNextCountersPageId(long pageAddr, long partMetaPageId) {
        PageUtils.putLong(pageAddr, NEXT_COUNTERS_PAGE_OFF, partMetaPageId);
    }

    /**
     * @param pageSize Page size.
     * @param pageAddr Page address.
     * @param cacheSizes Serialized cache size items (pairs of cache ID and its size).
     * @return Number of written pairs.
     */
    public int writeCacheSizes(int pageSize, long pageAddr, byte[] cacheSizes, int itemsOff) {
        assert cacheSizes != null;
        assert cacheSizes.length % ITEM_SIZE == 0 : cacheSizes.length;

        int cap = getCapacity(pageSize);

        int items = (cacheSizes.length / ITEM_SIZE) - itemsOff;
        int write = Math.min(cap, items);

        if (write > 0)
            // This can happen in case there are no items in a given partition for all caches in the group.
            PageUtils.putBytes(pageAddr, ITEMS_OFF, cacheSizes, itemsOff * ITEM_SIZE, write * ITEM_SIZE);

        setCount(pageAddr, write);

        setLastFlag(pageAddr, write == items);

        return write;
    }

    /**
     * @param pageAddr Page address.
     * @param res Result map of cache sizes.
     * @return {@code True} if the map was fully read.
     */
    public boolean readCacheSizes(long pageAddr, Map<Integer, Long> res) {
        int cnt = getCount(pageAddr);

        assert cnt >= 0 && cnt <= Short.MAX_VALUE : cnt;

        if (cnt == 0)
            return true;

        int off = ITEMS_OFF;

        for (int i = 0; i < cnt; i++) {
            int cacheId = PageUtils.getInt(pageAddr, off);
            off += 4;

            assert cacheId != 0;

            long cacheSize = PageUtils.getLong(pageAddr, off);
            off += 8;

            assert cacheSize > 0 : cacheSize;

            Long old = res.put(cacheId, cacheSize);

            assert old == null;
        }

        return getLastFlag(pageAddr);
    }

    private boolean getLastFlag(long pageAddr) {
        return PageUtils.getByte(pageAddr, LAST_FLAG_OFF) == LAST_FLAG;
    }

    private void setLastFlag(long pageAddr, boolean last) {
        PageUtils.putByte(pageAddr, LAST_FLAG_OFF, last ? LAST_FLAG : ~LAST_FLAG);
    }

    /**
     * @param pageAddr Page address.
     * @return Stored items count.
     */
    private int getCount(long pageAddr) {
        return PageUtils.getShort(pageAddr, CNT_OFF);
    }

    /**
     * @param pageAddr Page address.
     * @param cnt Stored items count.
     */
    private void setCount(long pageAddr, int cnt) {
        assert cnt >= 0 && cnt <= Short.MAX_VALUE : cnt;

        PageUtils.putShort(pageAddr, CNT_OFF, (short)cnt);
    }

    /**
     * @param pageSize Page size.
     * @return Maximum number of items which can be stored in buffer.
     */
    private int getCapacity(int pageSize) {
        return (pageSize - ITEMS_OFF) / ITEM_SIZE;
    }
}

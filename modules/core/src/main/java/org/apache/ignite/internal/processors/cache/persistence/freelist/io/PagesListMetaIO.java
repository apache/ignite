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

package org.apache.ignite.internal.processors.cache.persistence.freelist.io;

import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.PageUtils;
import org.apache.ignite.internal.processors.cache.persistence.freelist.PagesList;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.IOVersions;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.util.GridLongList;
import org.apache.ignite.internal.util.GridStringBuilder;

/**
 *
 */
public class PagesListMetaIO extends PageIO {
    /** */
    private static final int CNT_OFF = COMMON_HEADER_END;

    /** */
    private static final int NEXT_META_PAGE_OFF = CNT_OFF + 2;

    /** */
    private static final int ITEMS_OFF = NEXT_META_PAGE_OFF + 8;

    /** */
    private static final int ITEM_SIZE = 10;

    /** */
    public static final IOVersions<PagesListMetaIO> VERSIONS = new IOVersions<>(
        new PagesListMetaIO(1)
    );

    /**
     * @param ver  Page format version.
     */
    private PagesListMetaIO(int ver) {
        super(T_PAGE_LIST_META, ver);
    }

    /** {@inheritDoc} */
    @Override public void initNewPage(long pageAddr, long pageId, int pageSize) {
        super.initNewPage(pageAddr, pageId, pageSize);

        setCount(pageAddr, 0);
        setNextMetaPageId(pageAddr, 0L);
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
     * @param pageAddr Page address.
     * @return Next meta page ID.
     */
    public long getNextMetaPageId(long pageAddr) {
        return PageUtils.getLong(pageAddr, NEXT_META_PAGE_OFF);
    }

    /**
     * @param pageAddr Page address.
     * @param metaPageId Next meta page ID.
     */
    public void setNextMetaPageId(long pageAddr, long metaPageId) {
        PageUtils.putLong(pageAddr, NEXT_META_PAGE_OFF, metaPageId);
    }

    /**
     * @param pageAddr Page address.
     */
    public void resetCount(long pageAddr) {
        setCount(pageAddr, 0);
    }

    /**
     * @param pageSize Page size.
     * @param pageAddr Page address.
     * @param bucket Bucket number.
     * @param tails Tails.
     * @param tailsOff Tails offset.
     * @return Number of items written.
     */
    public int addTails(int pageSize, long pageAddr, int bucket, PagesList.Stripe[] tails, int tailsOff) {
        assert bucket >= 0 && bucket <= Short.MAX_VALUE : bucket;

        int cnt = getCount(pageAddr);
        int cap = getCapacity(pageSize, pageAddr);

        if (cnt == cap)
            return 0;

        int off = offset(cnt);

        int write = Math.min(cap - cnt, tails.length - tailsOff);

        for (int i = 0; i < write; i++) {
            PageUtils.putShort(pageAddr, off, (short)bucket);
            PageUtils.putLong(pageAddr, off + 2, tails[tailsOff].tailId);

            tailsOff++;

            off += ITEM_SIZE;
        }

        setCount(pageAddr, cnt + write);

        return write;
    }

    /**
     * @param pageAddr Page address.
     * @param res Results map.
     */
    public void getBucketsData(long pageAddr, Map<Integer, GridLongList> res) {
        int cnt = getCount(pageAddr);

        assert cnt >= 0 && cnt <= Short.MAX_VALUE : cnt;

        if (cnt == 0)
            return;

        int off = offset(0);

        for (int i = 0; i < cnt; i++) {
            int bucket = (int)PageUtils.getShort(pageAddr, off);
            assert bucket >= 0 && bucket <= Short.MAX_VALUE : bucket;

            long tailId = PageUtils.getLong(pageAddr, off + 2);
            assert tailId != 0;

            GridLongList list = res.get(bucket);

            if (list == null)
                res.put(bucket, list = new GridLongList());

            list.add(tailId);

            off += ITEM_SIZE;
        }
    }

    /**
     * @param pageAddr Page address.
     * @return Maximum number of items which can be stored in buffer.
     */
    private int getCapacity(int pageSize, long pageAddr) {
        return (pageSize - ITEMS_OFF) / ITEM_SIZE;
    }

    /**
     * @param idx Item index.
     * @return Item offset.
     */
    private int offset(int idx) {
        return ITEMS_OFF + ITEM_SIZE * idx;
    }

    /** {@inheritDoc} */
    @Override protected void printPage(long addr, int pageSize, GridStringBuilder sb) throws IgniteCheckedException {
        int cnt = getCount(addr);

        sb.a("PagesListMeta [\n\tnextMetaPageId=").appendHex(getNextMetaPageId(addr))
            .a(",\n\tcount=").a(cnt)
            .a(",\n\tbucketData={");

        Map<Integer, GridLongList> bucketsData = new HashMap<>(cnt);

        getBucketsData(addr, bucketsData);

        for (Map.Entry<Integer, GridLongList> e : bucketsData.entrySet())
            sb.a("\n\t\tbucket=").a(e.getKey()).a(", list=").a(e.getValue());

        sb.a("\n\t}\n]");
    }
}

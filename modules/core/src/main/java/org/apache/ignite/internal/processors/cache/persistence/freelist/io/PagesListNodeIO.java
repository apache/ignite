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

import java.nio.ByteBuffer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.PageUtils;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.CompactablePageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.IOVersions;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.util.GridStringBuilder;
import org.apache.ignite.internal.util.GridUnsafe;

import static org.apache.ignite.internal.processors.cache.persistence.tree.util.PageHandler.copyMemory;

/**
 * TODO optimize: now we have slow {@link #removePage(long, long)}
 */
public class PagesListNodeIO extends PageIO implements CompactablePageIO {
    /** */
    public static final IOVersions<PagesListNodeIO> VERSIONS = new IOVersions<>(
        new PagesListNodeIO(1)
    );

    /** */
    private static final int PREV_PAGE_ID_OFF = COMMON_HEADER_END;

    /** */
    private static final int NEXT_PAGE_ID_OFF = PREV_PAGE_ID_OFF + 8;

    /** */
    private static final int CNT_OFF = NEXT_PAGE_ID_OFF + 8;

    /** */
    private static final int PAGE_IDS_OFF = CNT_OFF + 2;

    /**
     * @param ver  Page format version.
     */
    protected PagesListNodeIO(int ver) {
        super(T_PAGE_LIST_NODE, ver);
    }

    /** {@inheritDoc} */
    @Override public void initNewPage(long pageAddr, long pageId, int pageSize) {
        super.initNewPage(pageAddr, pageId, pageSize);

        setEmpty(pageAddr);

        setPreviousId(pageAddr, 0L);
        setNextId(pageAddr, 0L);
    }

    /**
     * @param pageAddr Page address.
     */
    private void setEmpty(long pageAddr) {
        setCount(pageAddr, 0);
    }

    /**
     * @param pageAddr Page address.
     * @return Next page ID.
     */
    public long getNextId(long pageAddr) {
        return PageUtils.getLong(pageAddr, NEXT_PAGE_ID_OFF);
    }

    /**
     * @param pageAddr Page address.
     * @param nextId Next page ID.
     */
    public void setNextId(long pageAddr, long nextId) {
        PageUtils.putLong(pageAddr, NEXT_PAGE_ID_OFF, nextId);
    }

    /**
     * @param pageAddr Page address.
     * @return Previous page ID.
     */
    public long getPreviousId(long pageAddr) {
        return PageUtils.getLong(pageAddr, PREV_PAGE_ID_OFF);
    }

    /**
     * @param pageAddr Page address.
     * @param prevId Previous  page ID.
     */
    public void setPreviousId(long pageAddr, long prevId) {
        PageUtils.putLong(pageAddr, PREV_PAGE_ID_OFF, prevId);
    }

    /**
     * Gets total count of entries in this page. Does not change the buffer state.
     *
     * @param pageAddr Page address to get count from.
     * @return Total number of entries.
     */
    public int getCount(long pageAddr) {
        return PageUtils.getShort(pageAddr, CNT_OFF);
    }

    /**
     * Sets total count of entries in this page. Does not change the buffer state.
     *
     * @param pageAddr Page address to write to.
     * @param cnt Count.
     */
    private void setCount(long pageAddr, int cnt) {
        assert cnt >= 0 && cnt <= Short.MAX_VALUE : cnt;

        PageUtils.putShort(pageAddr, CNT_OFF, (short)cnt);
    }

    /**
     * Gets capacity of this page in items.
     *
     * @param pageSize Page size.
     * @return Capacity of this page in items.
     */
    private int getCapacity(int pageSize) {
        return (pageSize - PAGE_IDS_OFF) >>> 3; // /8
    }

    /**
     * @param idx Item index.
     * @return Item offset.
     */
    private int offset(int idx) {
        return PAGE_IDS_OFF + 8 * idx;
    }

    /**
     * @param pageAddr Page address.
     * @param idx Item index.
     * @return Item at the given index.
     */
    private long getAt(long pageAddr, int idx) {
        return PageUtils.getLong(pageAddr, offset(idx));
    }

    /**
     * @param pageAddr Page address.
     * @param idx Item index.
     * @param pageId Item value to write.
     */
    private void setAt(long pageAddr, int idx, long pageId) {
        PageUtils.putLong(pageAddr, offset(idx), pageId);
    }

    /**
     * Adds page to the end of pages list.
     *
     * @param pageAddr Page address.
     * @param pageId Page ID.
     * @param pageSize Page size.
     * @return Total number of items in this page.
     */
    public int addPage(long pageAddr, long pageId, int pageSize) {
        int cnt = getCount(pageAddr);

        if (cnt == getCapacity(pageSize))
            return -1;

        setAt(pageAddr, cnt, pageId);
        setCount(pageAddr, cnt + 1);

        return cnt;
    }

    /**
     * Removes any page from the pages list.
     *
     * @param pageAddr Page address.
     * @return Removed page ID.
     */
    public long takeAnyPage(long pageAddr) {
        int cnt = getCount(pageAddr);

        if (cnt == 0)
            return 0L;

        setCount(pageAddr, --cnt);

        return getAt(pageAddr, cnt);
    }

    /**
     * Removes the given page ID from the pages list.
     *
     * @param pageAddr Page address.
     * @param dataPageId Page ID to remove.
     * @return {@code true} if page was in the list and was removed, {@code false} otherwise.
     */
    public boolean removePage(long pageAddr, long dataPageId) {
        assert dataPageId != 0;

        int cnt = getCount(pageAddr);

        for (int i = 0; i < cnt; i++) {
            if (PageIdUtils.maskPartitionId(getAt(pageAddr, i)) == PageIdUtils.maskPartitionId(dataPageId)) {
                if (i != cnt - 1)
                    copyMemory(pageAddr, offset(i + 1), pageAddr, offset(i), 8 * (cnt - i - 1));

                setCount(pageAddr, cnt - 1);

                return true;
            }
        }

        return false;
    }

    /**
     * @param pageAddr Page address.
     * @return {@code True} if there are no items in this page.
     */
    public boolean isEmpty(long pageAddr) {
        return getCount(pageAddr) == 0;
    }

    /** {@inheritDoc} */
    @Override public void compactPage(ByteBuffer page, ByteBuffer out, int pageSize) {
        copyPage(page, out, pageSize);

        long pageAddr = GridUnsafe.bufferAddress(out);

        // Just drop all the extra garbage at the end.
        out.limit(offset(getCount(pageAddr)));
    }

    /** {@inheritDoc} */
    @Override public void restorePage(ByteBuffer compactPage, int pageSize) {
        assert compactPage.isDirect();
        assert compactPage.position() == 0;
        assert compactPage.limit() <= pageSize;

        compactPage.limit(pageSize); // Just add garbage to the end.
    }

    /** {@inheritDoc} */
    @Override protected void printPage(long addr, int pageSize, GridStringBuilder sb) throws IgniteCheckedException {
        sb.a("PagesListNode [\n\tpreviousPageId=").appendHex(getPreviousId(addr))
            .a(",\n\tnextPageId=").appendHex(getNextId(addr))
            .a(",\n\tcount=").a(getCount(addr))
            .a(",\n\tpages={");

        for (int i = 0; i < getCount(addr); i++)
            sb.a("\n\t\t").a(getAt(addr, i));

        sb.a("\n\t}\n]");
    }
}

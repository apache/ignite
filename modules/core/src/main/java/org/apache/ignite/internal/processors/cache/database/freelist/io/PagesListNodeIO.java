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

package org.apache.ignite.internal.processors.cache.database.freelist.io;

import java.nio.ByteBuffer;
import org.apache.ignite.internal.processors.cache.database.tree.io.IOVersions;
import org.apache.ignite.internal.processors.cache.database.tree.io.PageIO;

import static org.apache.ignite.internal.processors.cache.database.tree.util.PageHandler.copyMemory;

/**
 * TODO optimize: now we have slow {@link #removePage(ByteBuffer, long)}
 */
public class PagesListNodeIO extends PageIO {
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
    @Override public void initNewPage(ByteBuffer buf, long pageId) {
        super.initNewPage(buf, pageId);

        setEmpty(buf);

        setPreviousId(buf, 0L);
        setNextId(buf, 0L);
    }

    /**
     * @param buf Buffer.
     */
    private void setEmpty(ByteBuffer buf) {
        setCount(buf, 0);
    }

    /**
     * @param buf Buffer.
     * @return Next page ID.
     */
    public long getNextId(ByteBuffer buf) {
        return buf.getLong(NEXT_PAGE_ID_OFF);
    }

    /**
     * @param buf Buffer.
     * @param nextId Next page ID.
     */
    public void setNextId(ByteBuffer buf, long nextId) {
        buf.putLong(NEXT_PAGE_ID_OFF, nextId);
    }

    /**
     * @param buf Buffer.
     * @return Previous page ID.
     */
    public long getPreviousId(ByteBuffer buf) {
        return buf.getLong(PREV_PAGE_ID_OFF);
    }

    /**
     * @param buf Page buffer.
     * @param prevId Previous  page ID.
     */
    public void setPreviousId(ByteBuffer buf, long prevId) {
        buf.putLong(PREV_PAGE_ID_OFF, prevId);
    }

    /**
     * Gets total count of entries in this page. Does not change the buffer state.
     *
     * @param buf Page buffer to get count from.
     * @return Total number of entries.
     */
    public int getCount(ByteBuffer buf) {
        return buf.getShort(CNT_OFF);
    }

    /**
     * Sets total count of entries in this page. Does not change the buffer state.
     *
     * @param buf Page buffer to write to.
     * @param cnt Count.
     */
    private void setCount(ByteBuffer buf, int cnt) {
        assert cnt >= 0 && cnt <= Short.MAX_VALUE : cnt;

        buf.putShort(CNT_OFF, (short)cnt);
    }

    /**
     * Gets capacity of this page in items.
     *
     * @param buf Page buffer to get capacity.
     * @return Capacity of this page in items.
     */
    private int getCapacity(ByteBuffer buf) {
        return (buf.capacity() - PAGE_IDS_OFF) >>> 3; // /8
    }

    /**
     * @param idx Item index.
     * @return Item offset.
     */
    private int offset(int idx) {
        return PAGE_IDS_OFF + 8 * idx;
    }

    /**
     * @param buf Page buffer.
     * @param idx Item index.
     * @return Item at the given index.
     */
    private long getAt(ByteBuffer buf, int idx) {
        return buf.getLong(offset(idx));
    }

    /**
     * @param buf Buffer.
     * @param idx Item index.
     * @param pageId Item value to write.
     */
    private void setAt(ByteBuffer buf, int idx, long pageId) {
        buf.putLong(offset(idx), pageId);
    }

    /**
     * Adds page to the end of pages list.
     *
     * @param buf Page buffer.
     * @param pageId Page ID.
     * @return Total number of items in this page.
     */
    public int addPage(ByteBuffer buf, long pageId) {
        int cnt = getCount(buf);

        if (cnt == getCapacity(buf))
            return -1;

        setAt(buf, cnt, pageId);
        setCount(buf, cnt + 1);

        return cnt;
    }

    /**
     * Removes any page from the pages list.
     *
     * @param buf Page buffer.
     * @return Removed page ID.
     */
    public long takeAnyPage(ByteBuffer buf) {
        int cnt = getCount(buf);

        if (cnt == 0)
            return 0L;

        setCount(buf, --cnt);

        return getAt(buf, cnt);
    }

    /**
     * Removes the given page ID from the pages list.
     *
     * @param buf Page buffer.
     * @param dataPageId Page ID to remove.
     * @return {@code true} if page was in the list and was removed, {@code false} otherwise.
     */
    public boolean removePage(ByteBuffer buf, long dataPageId) {
        assert dataPageId != 0;

        int cnt = getCount(buf);

        for (int i = 0; i < cnt; i++) {
            if (getAt(buf, i) == dataPageId) {
                if (i != cnt - 1)
                    copyMemory(buf, buf, offset(i + 1), offset(i), 8 * (cnt - i - 1));

                setCount(buf, cnt - 1);

                return true;
            }
        }

        return false;
    }

    /**
     * @param buf Page buffer.
     * @return {@code true} if there are no items in this page.
     */
    public boolean isEmpty(ByteBuffer buf) {
        return getCount(buf) == 0;
    }
}

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
import java.util.Map;
import org.apache.ignite.internal.processors.cache.database.tree.io.IOVersions;
import org.apache.ignite.internal.processors.cache.database.tree.io.PageIO;
import org.apache.ignite.internal.util.GridLongList;

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
    private static final int ITEM_SIZE = 18;

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
    @Override public void initNewPage(ByteBuffer buf, long pageId) {
        super.initNewPage(buf, pageId);

        setCount(buf, 0);
        setNextMetaPageId(buf, 0L);
    }

    /**
     * @param buf Buffer.
     * @return Stored items count.
     */
    private int getCount(ByteBuffer buf) {
        return buf.getShort(CNT_OFF);
    }

    /**
     * @param buf Buffer,
     * @param cnt Stored items count.
     */
    private void setCount(ByteBuffer buf, int cnt) {
        assert cnt >= 0 && cnt <= Short.MAX_VALUE : cnt;

        buf.putShort(CNT_OFF, (short)cnt);
    }

    /**
     * @param buf Buffer.
     * @return Next meta page ID.
     */
    public long getNextMetaPageId(ByteBuffer buf) {
        return buf.getLong(NEXT_META_PAGE_OFF);
    }

    /**
     * @param buf Buffer.
     * @param metaPageId Next meta page ID.
     */
    public void setNextMetaPageId(ByteBuffer buf, long metaPageId) {
        buf.putLong(NEXT_META_PAGE_OFF, metaPageId);
    }

    /**
     * @param buf Buffer.
     */
    public void resetCount(ByteBuffer buf) {
        setCount(buf, 0);
    }

    /**
     * @param buf Buffer.
     * @param bucket Bucket number.
     * @param tailId Tail page ID.
     * @param headId Head page ID.
     * @return {@code True} if bucket information was stored.
     */
    public boolean addListHead(ByteBuffer buf, int bucket, long tailId, long headId) {
        assert bucket >= 0 && bucket <= Short.MAX_VALUE : bucket;

        int cnt = getCount(buf);

        if (cnt == getCapacity(buf))
            return false;

        int off = offset(cnt);

        buf.putShort(off, (short)bucket);
        buf.putLong(off + 2, tailId);
        buf.putLong(off + 10, headId);

        setCount(buf, cnt + 1);

        return true;
    }

    /**
     * @param buf Buffer.
     * @param res Results map.
     */
    public void getBucketsData(ByteBuffer buf, Map<Integer, GridLongList> res) {
        int cnt = getCount(buf);

        assert cnt >= 0 && cnt <= Short.MAX_VALUE : cnt;

        if (cnt == 0)
            return;

        int off = offset(0);

        for (int i = 0; i < cnt; i++) {
            Integer bucket = (int)buf.getShort(off);
            assert bucket >= 0 && bucket <= Short.MAX_VALUE : bucket;

            long tailId = buf.getLong(off + 2);
            assert tailId != 0;

            long headId = buf.getLong(off + 10);
            assert headId != 0;

            GridLongList list = res.get(bucket);

            if (list == null)
                res.put(bucket, list = new GridLongList());

            list.add(tailId);
            list.add(headId);

            off += ITEM_SIZE;
        }
    }

    /**
     * @param buf Buffer.
     * @return Maximum number of items which can be stored in buffer.
     */
    private int getCapacity(ByteBuffer buf) {
        return (buf.capacity() - NEXT_META_PAGE_OFF) / ITEM_SIZE;
    }

    /**
     * @param idx Item index.
     * @return Item offset.
     */
    private int offset(int idx) {
        return ITEMS_OFF + ITEM_SIZE * idx;
    }
}

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

package org.apache.ignite.internal.processors.cache.database.tree.io;

import java.nio.ByteBuffer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;

/**
 * Data pages IO.
 */
public class DataPageIO extends PageIO {
    /** */
    public static final IOVersions<DataPageIO> VERSIONS = new IOVersions<>(
        new DataPageIO(1)
    );

    /** */
    private static final int OCCUPIED_SIZE_OFF = COMMON_HEADER_END;

    /** */
    private static final int ALL_CNT_OFF = OCCUPIED_SIZE_OFF + 2;

    /** */
    private static final int LIVE_CNT_OFF = ALL_CNT_OFF + 2;

    /** */
    private static final int ITEMS_OFF = LIVE_CNT_OFF + 2;

    /** */
    private static final int ITEM_SIZE = 2;

    /**
     * @param ver Page format version.
     */
    protected DataPageIO(int ver) {
        super(ver);
    }

    /** {@inheritDoc} */
    @Override public int getType() {
        return T_DATA;
    }

    /** {@inheritDoc} */
    @Override public void initNewPage(ByteBuffer buf, long pageId) {
        super.initNewPage(buf, pageId);

        setAllCount(buf, 0);
        setLiveCount(buf, 0);
        setOccupiedSize(buf, 0);
    }

    public int getOccupiedSize(ByteBuffer buf) {
        return buf.getShort(OCCUPIED_SIZE_OFF) & 0xFFFF;
    }

    public void setOccupiedSize(ByteBuffer buf, int size) {
        buf.putShort(OCCUPIED_SIZE_OFF, (short)size);

        assert getOccupiedSize(buf) == size;
    }

    public int getAllCount(ByteBuffer buf) {
        return buf.getShort(ALL_CNT_OFF) & 0xFFFF;
    }

    public void setAllCount(ByteBuffer buf, int cnt) {
        buf.putShort(ALL_CNT_OFF, (short)cnt);

        assert cnt == getAllCount(buf);
    }

    public int getLiveCount(ByteBuffer buf) {
        return buf.getShort(LIVE_CNT_OFF) & 0xFFFF;
    }

    public void setLiveCount(ByteBuffer buf, int cnt) {
        buf.putShort(LIVE_CNT_OFF, (short)cnt);

        assert cnt == getLiveCount(buf);
    }

    public boolean canAddEntry(ByteBuffer buf, int entrySize) {
        int free = buf.capacity() - ITEMS_OFF - getOccupiedSize(buf);

        if (free < entrySize)
            return false;

        free -= (getAllCount(buf) - getLiveCount(buf)) * ITEM_SIZE;

        return free >= entrySize;
    }

    /**
     * @param keySize Key size.
     * @param valSize Value size.
     * @return Entry size including item.
     */
    private static int entrySize(int keySize, int valSize) {
        return ITEM_SIZE + 2/*key+val len*/ + keySize + valSize + 24/*ver*/;
    }

    /**
     * @param idx Index of item.
     * @return Offset in bytes.
     */
    private static int offset(int idx) {
        return ITEMS_OFF + idx * ITEM_SIZE;
    }

    /**
     * @param buf Buffer.
     * @param idx Index of item.
     * @return Data offset in bytes.
     */
    public int getDataOffset(ByteBuffer buf, int idx) {
        return buf.getShort(offset(idx)) & 0xFFFF;
    }

    /**
     * @param buf Buffer.
     * @param idx Index of item.
     * @param dataOff Data offset in bytes.
     */
    private void setDataOffset(ByteBuffer buf, int idx, int dataOff) {
        buf.putShort(offset(idx), (short)dataOff);

        assert dataOff == getDataOffset(buf, idx);
    }

    /**
     * Make a window for data entry.
     *
     * @param buf Buffer.
     * @param idx Index of the new item.
     * @param allCnt All count.
     * @param entrySize Entry size.
     * @return Data offset for the new entry.
     */
    public int makeWindow(ByteBuffer buf, int idx, int allCnt, int entrySize) {
        if (idx == allCnt) { // Adding to the end of items.
            int off = offset(idx);
            int lastDataOff = allCnt == 0 ? buf.capacity() : getDataOffset(buf, allCnt - 1);

            if (lastDataOff - off < entrySize) // TODO try to defragment
                return -1;

            return lastDataOff - entrySize + ITEM_SIZE;
        }
        else {
            //TODO defragment page with respect to idx and entrySize (if idx is not last, the window must be not first)
            throw new UnsupportedOperationException();
        }
    }

    public int addRow(
        CacheObjectContext coctx,
        ByteBuffer buf,
        CacheObject key,
        CacheObject val,
        GridCacheVersion ver) throws IgniteCheckedException
    {
        int keyLen = key.valueBytesLength(coctx);
        int valLen = val.valueBytesLength(coctx);
        int entrySize = entrySize(keyLen, valLen);

        if (entrySize >= buf.capacity() - ITEMS_OFF)
            throw new IgniteException("Too big entry: " + keyLen + " " + valLen);

        if (!canAddEntry(buf, entrySize))
            return -1;

        int liveCnt = getLiveCount(buf);
        int allCnt = getAllCount(buf);
        int idx = 0;

        if (allCnt == liveCnt)
            idx = allCnt; // Allocate new idx at allCnt if all are alive.
        else {
            // Lookup for a free parking lot.
            while (idx < allCnt) {
                if (getDataOffset(buf, idx) == 0)
                    break;

                idx++;
            }
        }

        int dataOff = makeWindow(buf, idx, allCnt, entrySize);

        if (dataOff == -1)
            return -1;

        // Write data.
        writeRowDataInPlace(coctx, buf, dataOff, keyLen + valLen, key, val, ver);
        // Write item.
        setDataOffset(buf, idx, dataOff);

        // Update header.
        setOccupiedSize(buf, getOccupiedSize(buf) + entrySize);
        setAllCount(buf, allCnt + 1);
        setLiveCount(buf, liveCnt + 1);

        return idx;
    }

    /**
     * @param buf Buffer.
     * @param dataOff Data offset.
     * @param key Key.
     * @param val Value.
     * @param ver Version.
     */
    public void writeRowDataInPlace(
        CacheObjectContext coctx,
        ByteBuffer buf,
        int dataOff,
        int keyValLen,
        CacheObject key,
        CacheObject val,
        GridCacheVersion ver
    ) throws IgniteCheckedException {
        try {
            buf.position(dataOff);

            buf.putShort((short)keyValLen);

            boolean written = key.putValue(buf, coctx);

            assert written;

            written = val.putValue(buf, coctx);

            assert written;

            buf.putInt(ver.topologyVersion());
            buf.putInt(ver.nodeOrderAndDrIdRaw());
            buf.putLong(ver.globalTime());
            buf.putLong(ver.order());
        }
        finally {
            buf.position(0);
        }
    }

    public int getKeyValueSize(ByteBuffer buf, int idx) {
        return buf.getShort(getDataOffset(buf, idx)) & 0xFFFF;
    }
}

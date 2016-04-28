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
import java.util.Arrays;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.database.tree.util.PageHandler;
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
    private static final int FREE_SPACE_OFF = COMMON_HEADER_END;

    /** */
    private static final int DIRECT_CNT_OFF = FREE_SPACE_OFF + 2;

    /** */
    private static final int INDIRECT_CNT_OFF = DIRECT_CNT_OFF + 1;

    /** */
    private static final int FIRST_ENTRY_OFF = INDIRECT_CNT_OFF + 1;

    /** */
    private static final int ITEMS_OFF = FIRST_ENTRY_OFF + 2;

    /** */
    private static final int ITEM_SIZE = 2;

    /** */
    private static final int KV_LEN_SIZE = 2; // TODO entry will span multiple pages??

    /** */
    private static final int VER_SIZE = 24;

    /**
     * @param ver Page format version.
     */
    protected DataPageIO(int ver) {
        super(T_DATA, ver);
    }

    /** {@inheritDoc} */
    @Override public void initNewPage(ByteBuffer buf, long pageId) {
        super.initNewPage(buf, pageId);

        setEmptyPage(buf);
    }

    /**
     * @param buf Buffer.
     */
    private void setEmptyPage(ByteBuffer buf) {
        setDirectCount(buf, 0);
        setIndirectCount(buf, 0);
        setFreeSpace(buf, buf.capacity() - ITEMS_OFF);
        setFirstEntryOffset(buf, buf.capacity());
    }

    /**
     * @param coctx Cache object context.
     * @param key Key.
     * @param val Value.
     * @return Entry size on page.
     * @throws IgniteCheckedException If failed.
     */
    public static int getEntrySize(CacheObjectContext coctx, CacheObject key, CacheObject val)
        throws IgniteCheckedException {
        int keyLen = key.valueBytesLength(coctx);
        int valLen = val.valueBytesLength(coctx);

        return getEntrySize(keyLen, valLen);
    }

    /**
     * @param keySize Key size.
     * @param valSize Value size.
     * @return Entry size including item.
     */
    private static int getEntrySize(int keySize, int valSize) {
        return ITEM_SIZE + KV_LEN_SIZE + keySize + valSize + VER_SIZE;
    }

    /**
     * @param buf Buffer.
     * @param dataOff Data offset.
     * @param withItem Return entry size including item size.
     * @return Entry size.
     */
    private int getEntrySize(ByteBuffer buf, int dataOff, boolean withItem) {
        int res = buf.getShort(dataOff) & 0xFFFF;

        if (!withItem)
            res -= ITEM_SIZE;

        return res;
    }

    /**
     * @param buf Buffer.
     * @param dataOff Entry data offset.
     */
    private void setFirstEntryOffset(ByteBuffer buf, int dataOff) {
        assert dataOff >= ITEMS_OFF + ITEM_SIZE && dataOff <= buf.capacity(): dataOff;

        buf.putShort(FIRST_ENTRY_OFF, (short)dataOff);
    }

    /**
     * @param buf Buffer.
     * @return Entry data offset.
     */
    private int getFirstEntryOffset(ByteBuffer buf) {
        return buf.getShort(FIRST_ENTRY_OFF) & 0xFFFF;
    }

    /**
     * @param buf Buffer.
     * @param freeSpace Free space.
     */
    private void setFreeSpace(ByteBuffer buf, int freeSpace) {
        assert freeSpace >= 0 && freeSpace <= Short.MAX_VALUE;

        buf.putShort(FREE_SPACE_OFF, (short)freeSpace);
    }

    /**
     * @param buf Buffer.
     * @return Free space.
     */
    public int getFreeSpace(ByteBuffer buf) {
        return buf.getShort(FREE_SPACE_OFF);
    }

    /**
     * @param buf Buffer.
     * @param cnt Direct count.
     */
    private void setDirectCount(ByteBuffer buf, int cnt) {
        assert check(cnt): cnt;

        buf.put(DIRECT_CNT_OFF, (byte)cnt);
    }

    /**
     * @param buf Buffer.
     * @return Direct count.
     */
    private int getDirectCount(ByteBuffer buf) {
        return buf.get(DIRECT_CNT_OFF) & 0xFF;
    }

    /**
     * @param buf Buffer.
     * @param cnt Indirect count.
     */
    private void setIndirectCount(ByteBuffer buf, int cnt) {
        assert check(cnt): cnt;

        buf.put(INDIRECT_CNT_OFF, (byte)cnt);
    }

    /**
     * @param idx Index.
     * @return {@code true} If the index is valid.
     */
    public static boolean check(int idx) {
        return idx >= 0 && idx < 256;
    }

    /**
     * @param buf Buffer.
     * @return Indirect count.
     */
    private int getIndirectCount(ByteBuffer buf) {
        return buf.get(INDIRECT_CNT_OFF) & 0xFF;
    }

    /**
     * @param buf Buffer.
     * @param itemId Fixed item ID (the index used for referencing an entry from the outside).
     * @param directCnt Direct items count.
     * @param indirectCnt Indirect items count.
     * @return Found index of indirect item.
     */
    private static int findIndirectItemIndex(ByteBuffer buf, int itemId, int directCnt, int indirectCnt) {
        int low = directCnt;
        int high = directCnt + indirectCnt - 1;

        while (low <= high) {
            int mid = (low + high) >>> 1;

            int cmp = Integer.compare(itemId(getItem(buf, mid)), itemId);

            if (cmp < 0)
                low = mid + 1;
            else if (cmp > 0)
                high = mid - 1;
            else
                return mid; // found
        }

        throw new IllegalStateException("Item not found: " + itemId);
    }

    /**
     * @param buf Buffer.
     * @param itemId Fixed item ID (the index used for referencing an entry from the outside).
     * @return Data entry offset in bytes.
     */
    public int getDataOffset(ByteBuffer buf, int itemId) {
        assert check(itemId): itemId;

        int directCnt = getDirectCount(buf);

        assert directCnt > 0: directCnt;

        if (itemId >= directCnt) { // Need to do indirect lookup.
            int indirectCnt = getIndirectCount(buf);

            assert indirectCnt > 0: indirectCnt; // Must have indirect items here.

            int indirectItemIdx = findIndirectItemIndex(buf, itemId, directCnt, indirectCnt);

            assert indirectItemIdx >= directCnt && indirectItemIdx < directCnt + indirectCnt: indirectCnt;

            itemId = directItemIndex(getItem(buf, indirectItemIdx));

            assert itemId >= 0 && itemId < directCnt: itemId; // Direct item must be here.
        }

        return toOffset(getItem(buf, itemId));
    }

    /**
     * @param buf Buffer.
     * @param idx Item index.
     * @return Item.
     */
    private static short getItem(ByteBuffer buf, int idx) {
        return buf.getShort(itemOffset(idx));
    }

    /**
     * @param buf Buffer.
     * @param idx Item index.
     * @param item Item.
     */
    private static void setItem(ByteBuffer buf, int idx, short item) {
        buf.putShort(itemOffset(idx), item);
    }

    /**
     * @param idx Index of the item.
     * @return Offset in buffer.
     */
    private static int itemOffset(int idx) {
        assert check(idx): idx;

        return ITEMS_OFF + idx * ITEM_SIZE;
    }

    /**
     * @param directItem Direct item.
     * @return Offset of an entry payload inside of the page.
     */
    private static int toOffset(short directItem) {
        return directItem & 0xFFFF;
    }

    /**
     * @param dataOff Data offset.
     * @return Direct item.
     */
    private static short fromOffset(int dataOff) {
        assert dataOff > ITEMS_OFF + ITEM_SIZE && dataOff < 65536: dataOff;

        return (short)dataOff;
    }

    /**
     * @param indirectItem Indirect item.
     * @return Index of corresponding direct item.
     */
    private static int directItemIndex(short indirectItem) {
        return indirectItem & 0xFF;
    }

    /**
     * @param indirectItem Indirect item.
     * @return Fixed item ID (the index used for referencing an entry from the outside).
     */
    private static int itemId(short indirectItem) {
        return indirectItem >>> 8;
    }

    /**
     * @param itemId Fixed item ID (the index used for referencing an entry from the outside).
     * @param directItemIdx Index of corresponding direct item.
     * @return Indirect item.
     */
    private static short indirectItem(int itemId, int directItemIdx) {
        assert check(itemId): itemId;
        assert check(directItemIdx): directItemIdx;

        return (short)((itemId << 8) | directItemIdx);
    }

    /**
     * Move the last direct item to the free slot and reference it with indirect item on the same place.
     *
     * @param buf Buffer.
     * @param itemId Free slot.
     * @param directCnt Direct items count.
     */
    private static void moveLastItem(ByteBuffer buf, int itemId, int directCnt) {
        int lastItemId = directCnt - 1;

        setItem(buf, itemId, getItem(buf, lastItemId));
        setItem(buf, lastItemId, indirectItem(lastItemId, itemId));
    }

    /**
     * If we've moved the last item second time (it was already referenced by a indirect item),
     * we need to fix the existing indirect item and the last one must be overwritten.
     *
     * @param buf Buffer.
     * @param directCnt Direct items count.
     * @param indirectCnt Indirect items count.
     * @return {@code true} If it was indirect item and it was fixed.
     */
    private static boolean fixIndirectItem(ByteBuffer buf, int directCnt, int indirectCnt) {
        short item = getItem(buf, directCnt - 1); // Now it is a first indirect item after move.

        int itemId = itemId(item);

        if (itemId == directCnt - 1)
            return false; // It was a direct item initially, nothing to fix.

        // Find initial indirect item for moved last direct item.
        int indirectItemIdx = findIndirectItemIndex(buf, itemId, directCnt, indirectCnt);

        // Now it points to a new place.
        setItem(buf, indirectItemIdx, item);

        return true;
    }

    /**
     * @param buf Buffer.
     * @param itemId Fixed item ID (the index used for referencing an entry from the outside).
     */
    public void removeRow(ByteBuffer buf, int itemId) {
        assert check(itemId) : itemId;

        int directCnt = getDirectCount(buf);
        int indirectCnt = getIndirectCount(buf);

        assert directCnt > 0 : directCnt; // Direct count always represents overall number of live items.

        // Remove the last item on the page.
        if (directCnt == 1) {
            assert (indirectCnt == 0 && itemId == 0) ||
                (indirectCnt == 1 && itemId == itemId(getItem(buf, 1))) : itemId;

            setEmptyPage(buf);

            return; // TODO May be have a separate list of free pages?
        }

        // Get the entry size before the actual remove.
        int rmvEntrySize = getEntrySize(buf, getDataOffset(buf, itemId), false);

        if (itemId < directCnt)
            removeDirectItem(buf, itemId, directCnt, indirectCnt);
        else
            removeIndirectItem(buf, itemId, directCnt, indirectCnt);

        // Increase free space.
        setFreeSpace(buf, getFreeSpace(buf) + rmvEntrySize +
            ITEM_SIZE * (directCnt - getDirectCount(buf) + indirectCnt - getIndirectCount(buf)));
    }

    /**
     * @param buf Buffer.
     * @param itemId Item ID.
     * @param directCnt Direct items count.
     * @param indirectCnt Indirect items count.
     */
    private void removeDirectItem(ByteBuffer buf, int itemId, int directCnt, int indirectCnt) {
        if (itemId + 1 == directCnt) {
            // It is the last direct item.
            setDirectCount(buf, directCnt - 1);

            if (indirectCnt > 0)
                moveItems(buf, directCnt, indirectCnt, -1);
        }
        else {
            // Remove from the middle of direct items.
            moveLastItem(buf, itemId, directCnt);

            setDirectCount(buf, directCnt - 1);
            setIndirectCount(buf, indirectCnt + 1);
        }
    }

    /**
     * @param buf Buffer.
     * @param itemId Item ID.
     * @param directCnt Direct items count.
     * @param indirectCnt Indirect items count.
     */
    private void removeIndirectItem(ByteBuffer buf, int itemId, int directCnt, int indirectCnt) {
        // Need to remove indirect item.
        assert indirectCnt > 0: indirectCnt; // Must have indirect items here.

        // Need to found indirect and direct indexes for the given item ID.
        int indirectItemIdx = findIndirectItemIndex(buf, itemId, directCnt, indirectCnt);

        int allItemsCnt = directCnt + indirectCnt;

        assert indirectItemIdx >= directCnt && indirectItemIdx < allItemsCnt: indirectCnt;

        itemId = directItemIndex(getItem(buf, indirectItemIdx));

        assert itemId < directCnt: itemId; // Direct item index.

        boolean indirectFixed = true; // By default is true because for the last item it represents the same invariant.

        if (itemId + 1 != directCnt) { // Additional handling for a middle item.
            moveLastItem(buf, itemId, directCnt);

            indirectFixed = fixIndirectItem(buf, directCnt, indirectCnt);
        }

        // Move everything before indirect item 1 step back.
        // If the last item was a direct, then we have to keep it because it became indirect.
        if (indirectFixed)
            moveItems(buf, directCnt, indirectItemIdx - directCnt, -1);

        // Move everything after the found indirect index 2 step back (or 1 in case when the last was a direct item).
        moveItems(buf, indirectItemIdx + 1, allItemsCnt - indirectItemIdx - 1, indirectFixed ? -2 : -1);

        if (indirectFixed) // Otherwise we've added one and removed one indirect item.
            setIndirectCount(buf, indirectCnt - 1);

        // We always remove one direct item here.
        setDirectCount(buf, directCnt - 1);
    }

    /**
     * @param buf Buffer.
     * @param idx Index.
     * @param cnt Count.
     * @param step Step.
     */
    private static void moveItems(ByteBuffer buf, int idx, int cnt, int step) {
        assert cnt >= 0: cnt;

        if (cnt == 0)
            return;

        moveBytes(buf, itemOffset(idx), cnt * ITEM_SIZE, step * ITEM_SIZE);
    }

    /**
     * @param newEntrySizeWithItem New entry size as returned by {@link #getEntrySize(int, int)}.
     * @param firstEntryOff First entry data offset.
     * @param directCnt Direct items count.
     * @param indirectCnt Indirect items count.
     * @return {@code true} If there is enough space for the entry.
     */
    public static boolean isEnoughSpace(int newEntrySizeWithItem, int firstEntryOff, int directCnt, int indirectCnt) {
        return ITEMS_OFF + ITEM_SIZE * (directCnt + indirectCnt) <= firstEntryOff - newEntrySizeWithItem;
    }

    /**
     * @param buf Buffer.
     * @param newEntrySizeWithItem New entry size as returned by {@link #getEntrySize(int, int)}.
     * @return {@code true} If there is enough space for the entry.
     */
    public boolean isEnoughSpace(ByteBuffer buf, int newEntrySizeWithItem) {
        return isEnoughSpace(newEntrySizeWithItem,
            getFirstEntryOffset(buf), getDirectCount(buf), getIndirectCount(buf));
    }

    /**
     * @param coctx Cache object context.
     * @param buf Buffer.
     * @param key Key.
     * @param val Value.
     * @param ver Version.
     * @param entrySizeWithItem Entry size as returned by {@link #getEntrySize(int, int)}.
     * @return Item ID.
     * @throws IgniteCheckedException If failed.
     */
    public byte addRow(
        CacheObjectContext coctx,
        ByteBuffer buf,
        CacheObject key,
        CacheObject val,
        GridCacheVersion ver,
        int entrySizeWithItem
    ) throws IgniteCheckedException {
        if (entrySizeWithItem > buf.capacity() - ITEMS_OFF) // TODO span multiple data pages with a single large entry
            throw new IgniteException("Too big entry: " + key + " " + val);

        int directCnt = getDirectCount(buf);
        int indirectCnt = getIndirectCount(buf);
        int dataOff = getFirstEntryOffset(buf);

        // Compact if we do not have enough space for entry.
        if (!isEnoughSpace(entrySizeWithItem, dataOff, directCnt, indirectCnt)) {
            dataOff = compactDataEntries(buf, directCnt);

            assert isEnoughSpace(entrySizeWithItem, dataOff, directCnt, indirectCnt);
        }

        // Write data right before the first entry.
        dataOff -= entrySizeWithItem - ITEM_SIZE;

        writeRowData(coctx, buf, dataOff, entrySizeWithItem, key, val, ver);

        setFirstEntryOffset(buf, dataOff);

        int itemId = insertItem(buf, dataOff, directCnt, indirectCnt);

        assert check(itemId): itemId;

        // Update free space. If number of direct items did not change, then we were able to reuse item slot.
        setFreeSpace(buf, getFreeSpace(buf) - entrySizeWithItem + (getDirectCount(buf) == directCnt ? ITEM_SIZE : 0));

        assert getFreeSpace(buf) >= 0;

        return (byte)itemId;
    }

    /**
     * @param buf Buffer.
     * @param dataOff Data offset.
     * @param directCnt Direct items count.
     * @param indirectCnt Indirect items count.
     * @return Item ID (insertion index).
     */
    private int insertItem(ByteBuffer buf, int dataOff, int directCnt, int indirectCnt) {
        if (indirectCnt > 0) {
            // If the first indirect item is on correct place to become the last direct item, do the transition
            // and insert the new item into the free slot which was referenced by this first indirect item.
            short item = getItem(buf, directCnt);

            if (itemId(item) == directCnt) {
                int directItemIdx = directItemIndex(item);

                setItem(buf, directCnt, getItem(buf, directItemIdx));
                setItem(buf, directItemIdx, fromOffset(dataOff));

                setDirectCount(buf, directCnt + 1);
                setIndirectCount(buf, indirectCnt - 1);

                return directItemIdx;
            }
        }

        // Move all the indirect items forward to make a free slot and insert new item at the end of direct items.
        moveItems(buf, directCnt, indirectCnt, +1);

        setItem(buf, directCnt, fromOffset(dataOff));

        setDirectCount(buf, directCnt + 1);

        return directCnt;
    }

    /**
     * @param buf Buffer.
     * @param directCnt Direct items count.
     * @return New first entry offset.
     */
    private int compactDataEntries(ByteBuffer buf, int directCnt) {
        assert check(directCnt): directCnt;

        int[] offs = new int[directCnt];

        for (int i = 0; i < directCnt; i++) {
            int off = toOffset(getItem(buf, i));

            offs[i] = (off << 8) | i; // This way we'll be able to sort by offset using Arrays.sort(...).
        }

        Arrays.sort(offs);

        // Move right all of the entries if possible to make the page as compact as possible to its tail.
        int prevOff = buf.capacity();

        for (int i = directCnt - 1; i >= 0; i--) {
            int off = offs[i] >>> 8;

            assert off < prevOff: off;

            int entrySize = getEntrySize(buf, off, false);

            int delta = prevOff - (off + entrySize);

            if (delta != 0) { // Move right.
                assert delta > 0: delta;

                moveBytes(buf, off, entrySize, delta);

                int itemId = offs[i] & 0xFF;

                off += delta;

                setItem(buf, itemId, fromOffset(off));
            }

            prevOff = off;
        }

        return prevOff;
    }

    /**
     * @param buf Buffer.
     * @param off Offset.
     * @param cnt Count.
     * @param step Step.
     */
    private static void moveBytes(ByteBuffer buf, int off, int cnt, int step) {
        assert step != 0: step;
        assert off + step >= 0;
        assert off + step + cnt < buf.capacity();

        PageHandler.copyMemory(buf, buf, off, off + step, cnt);
    }

    /**
     * @param coctx Cache object context.
     * @param buf Buffer.
     * @param dataOff Data offset.
     * @param entrySize Entry size as returned by {@link #getEntrySize(int, int)}.
     * @param key Key.
     * @param val Value.
     * @param ver Version.
     */
    private void writeRowData(
        CacheObjectContext coctx,
        ByteBuffer buf,
        int dataOff,
        int entrySize,
        CacheObject key,
        CacheObject val,
        GridCacheVersion ver
    ) throws IgniteCheckedException {
        try {
            buf.position(dataOff);

            buf.putShort((short)entrySize);

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
}

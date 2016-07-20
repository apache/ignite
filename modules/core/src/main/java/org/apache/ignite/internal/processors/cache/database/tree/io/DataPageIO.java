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
import java.util.HashSet;
import java.util.Set;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.database.tree.util.PageHandler;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.internal.SB;

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
    private static final int KV_LEN_SIZE = 2;

    /** */
    public static final int VER_SIZE = 24;

    /** */
    private static final int LINK_SIZE = 8;

    /** */
    private static final int FRAGMENTED_FLAG = 0x8000;

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

        int entrySize = getEntrySize(keyLen, valLen);

        final int availablePageSpace = getAvailablePageSize(coctx);

        if (entrySize > availablePageSpace) {
            final int dataSize = keyLen + valLen + VER_SIZE;
            final int overheadSize = ITEM_SIZE + KV_LEN_SIZE + LINK_SIZE;

            assert overheadSize != availablePageSpace;

            final int chunks = (int) Math.ceil(Math.abs(dataSize /
                (double) (overheadSize - availablePageSpace)));

            entrySize = chunks * overheadSize + dataSize;
        }

        return entrySize;
    }

    /**
     * @param availablePageSpace Max available free space on data page.
     * @param entrySize Entry size in memory (with overhead).
     * @return Number of chunks (pages) on which entry will be split.
     */
    public static int getChunksNum(final int availablePageSpace, final int entrySize) {
        return (int) Math.ceil((double) entrySize / (double) availablePageSpace);
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
     * @param coctx Cache object context.
     * @return Available space for entry.
     */
    public static int getAvailablePageSize(final CacheObjectContext coctx) {
        final int pageSize = coctx.kernalContext().config().getDatabaseConfiguration().getPageSize();

        return pageSize - ITEMS_OFF - ITEM_SIZE;
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
     * @return Number of free entry slots.
     */
    public int getFreeItemSlots(ByteBuffer buf) {
        return 0xFF - getDirectCount(buf);
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
     * @return String representation.
     */
    public String printPageLayout(ByteBuffer buf) {
        int directCnt = getDirectCount(buf);
        int indirectCnt = getIndirectCount(buf);
        int free = getFreeSpace(buf);

        boolean valid = directCnt >= indirectCnt;

        SB b = new SB();

        b.appendHex(PageIO.getPageId(buf)).a(" [");

        int entriesSize = 0;

        for (int i = 0; i < directCnt; i++) {
            if (i != 0)
                b.a(", ");

            short item = getItem(buf, i);

            if (item < ITEMS_OFF || item >= buf.capacity())
                valid = false;

            entriesSize += getEntrySize(buf, item, false);

            b.a(item);
        }

        b.a("][");

        Set<Integer> set = new HashSet<>();

        for (int i = directCnt; i < directCnt + indirectCnt; i++) {
            if (i != directCnt)
                b.a(", ");

            short item = getItem(buf, i);

            int itemId = itemId(item);
            int directIdx = directItemIndex(item);

            if (!set.add(directIdx) || !set.add(itemId))
                valid = false;

            assert indirectItem(itemId, directIdx) == item;

            if (itemId < directCnt || directIdx < 0 || directIdx >= directCnt)
                valid = false;

            if (i > directCnt && itemId(getItem(buf, i - 1)) >= itemId)
                valid = false;


            b.a(itemId).a('^').a(directIdx);
        }

        b.a("][free=").a(free);

        int actualFree = buf.capacity() - ITEMS_OFF - (entriesSize + (directCnt + indirectCnt) * ITEM_SIZE);

        if (free != actualFree) {
            b.a(", actualFree=").a(actualFree);

            valid = false;
        }
        else
            b.a("]");

        assert valid : b.toString();

        return b.toString();
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

            assert indirectItemIdx >= directCnt : indirectItemIdx + " " + directCnt;
            assert indirectItemIdx < directCnt + indirectCnt: indirectItemIdx + " " + directCnt + " " + indirectCnt;

            itemId = directItemIndex(getItem(buf, indirectItemIdx));

            assert itemId >= 0 && itemId < directCnt: itemId + " " + directCnt + " " + indirectCnt; // Direct item.
        }

        return toOffset(getItem(buf, itemId));
    }

    /**
     * @param buf Byte buffer.
     * @param dataOff Points to the entry start.
     * @return Link to the next entry fragment or 0 if no fragments left or if entry is not fragmented.
     */
    public static long getNextFragmentLink(final ByteBuffer buf, final int dataOff) {
        final int pos = buf.position();

        buf.position(dataOff);

        try {
            final short size = buf.getShort();

            if ((size & FRAGMENTED_FLAG) == 0)
                return 0;

            return buf.getLong();
        }
        finally {
            buf.position(pos);
        }
    }

    /**
     * @param buf Byte buffer.
     * @param dataOff Points to the entry start.
     * @return Fragment size on current page (with removed fragmented flag).
     */
    public static int getFragmentSize(final ByteBuffer buf, final int dataOff) {
        final short size = buf.getShort(dataOff);

        return size & ~FRAGMENTED_FLAG & 0xFFFF;
    }

    /**
     * Sets position to start of actual fragment data and limit to it's end.
     *
     * @param buf Byte buffer.
     * @param dataOff Points to the entry start.
     */
    public static void setPositionAndLimitOnFragment(final ByteBuffer buf, final int dataOff) {
        final int size = getFragmentSize(buf, dataOff);

        buf.position(dataOff + KV_LEN_SIZE + LINK_SIZE);

        buf.limit(buf.position() + size);
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
     * @param freeDirectIdx Free slot.
     * @param directCnt Direct items count.
     * @param indirectCnt Indirect items count.
     * @return {@code true} If the last direct item already had corresponding indirect item.
     */
    private static boolean moveLastItem(ByteBuffer buf, int freeDirectIdx, int directCnt, int indirectCnt) {
        int lastIndirectId = findIndirectIndexForLastDirect(buf, directCnt, indirectCnt);

        int lastItemId = directCnt - 1;

        assert lastItemId != freeDirectIdx;

        short indirectItem = indirectItem(lastItemId, freeDirectIdx);

        assert itemId(indirectItem) == lastItemId && directItemIndex(indirectItem) == freeDirectIdx;

        setItem(buf, freeDirectIdx, getItem(buf, lastItemId));
        setItem(buf, lastItemId, indirectItem);

        assert getItem(buf, lastItemId) == indirectItem;

        if (lastIndirectId != -1) { // Fix pointer to direct item.
            setItem(buf, lastIndirectId, indirectItem(itemId(getItem(buf, lastIndirectId)), freeDirectIdx));

            return true;
        }

        return false;
    }

    /**
     * @param buf Buffer.
     * @param directCnt Direct items count.
     * @param indirectCnt Indirect items count.
     * @return Index of indirect item for the last direct item.
     */
    private static int findIndirectIndexForLastDirect(ByteBuffer buf, int directCnt, int indirectCnt) {
        int lastDirectId = directCnt - 1;

        for (int i = directCnt, end = directCnt + indirectCnt; i < end; i++) {
            short item = getItem(buf, i);

            if (directItemIndex(item) == lastDirectId)
                return i;
        }

        return -1;
    }

    /**
     * @param buf Buffer.
     * @param itemId Fixed item ID (the index used for referencing an entry from the outside).
     * @throws IgniteCheckedException If failed.
     */
    public void removeRow(ByteBuffer buf, int itemId) throws IgniteCheckedException {
        assert check(itemId) : itemId;

        // Record original counts to calculate delta in free space in the end of remove.
        final int directCnt = getDirectCount(buf);
        final int indirectCnt = getIndirectCount(buf);

        int curIndirectCnt = indirectCnt;

        assert directCnt > 0 : directCnt; // Direct count always represents overall number of live items.

        // Remove the last item on the page.
        if (directCnt == 1) {
            assert (indirectCnt == 0 && itemId == 0) ||
                (indirectCnt == 1 && itemId == itemId(getItem(buf, 1))) : itemId;

            setEmptyPage(buf);
        }
        else {
            // Get the entry size before the actual remove.
            int rmvEntrySize = getEntrySize(buf, getDataOffset(buf, itemId), false);

            // Entry size may have fragment flag set.
            if ((rmvEntrySize & FRAGMENTED_FLAG) > 0) {
                rmvEntrySize &= ~FRAGMENTED_FLAG;

                rmvEntrySize += LINK_SIZE;
            }

            int indirectId = 0;

            if (itemId >= directCnt) { // Need to remove indirect item.
                assert indirectCnt > 0;

                indirectId = findIndirectItemIndex(buf, itemId, directCnt, indirectCnt);

                assert indirectId >= directCnt;

                itemId = directItemIndex(getItem(buf, indirectId));

                assert itemId < directCnt;
            }

            boolean dropLast = true;

            if (itemId + 1 < directCnt) // It is not the last direct item.
                dropLast = moveLastItem(buf, itemId, directCnt, indirectCnt);

            if (indirectId == 0) {// For the last direct item with no indirect item.
                if (dropLast)
                    moveItems(buf, directCnt, indirectCnt, -1);
                else
                    curIndirectCnt++;
            }
            else {
                if (dropLast)
                    moveItems(buf, directCnt, indirectId - directCnt, -1);

                moveItems(buf, indirectId + 1, directCnt + indirectCnt - indirectId - 1, dropLast ? -2 : -1);

                if (dropLast)
                    curIndirectCnt--;
            }

            setIndirectCount(buf, curIndirectCnt);
            setDirectCount(buf, directCnt - 1);

            assert getIndirectCount(buf) <= getDirectCount(buf);

            // Increase free space.
            setFreeSpace(buf, getFreeSpace(buf) + rmvEntrySize +
                ITEM_SIZE * (directCnt - getDirectCount(buf) + indirectCnt - getIndirectCount(buf)));
        }
    }

    /**
     * @param buf Buffer.
     * @param idx Index.
     * @param cnt Count.
     * @param step Step.
     */
    private static void moveItems(ByteBuffer buf, int idx, int cnt, int step) {
        assert cnt >= 0: cnt;

        if (cnt != 0)
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
     * @param coctx Cache object context.
     * @param buf Buffer.
     * @param key Key.
     * @param val Value.
     * @param ver Version.
     * @param entrySizeWithItem Entry size as returned by {@link #getEntrySize(int, int)}.
     * @return Item ID.
     * @throws IgniteCheckedException If failed.
     */
    public int addRow(
        CacheObjectContext coctx,
        ByteBuffer buf,
        CacheObject key,
        CacheObject val,
        GridCacheVersion ver,
        int entrySizeWithItem
    ) throws IgniteCheckedException {
        if (entrySizeWithItem > buf.capacity() - ITEMS_OFF)
            throw new IgniteException("Too big entry [key=" + key + ", val=" + val +
                ", entrySizeWithItem=" + entrySizeWithItem + ", activeCap=" + (buf.capacity() - ITEMS_OFF) + ']');

        int directCnt = getDirectCount(buf);
        int indirectCnt = getIndirectCount(buf);
        int dataOff = getFirstEntryOffset(buf);

        // Compact if we do not have enough space for entry.
        dataOff = compactIfNeed(buf, entrySizeWithItem, directCnt, indirectCnt, dataOff);

        // Write data right before the first entry.
        dataOff -= entrySizeWithItem - ITEM_SIZE;

        writeRowData(coctx, buf, dataOff, entrySizeWithItem, key, val, ver);

        return writeItemId(buf, entrySizeWithItem, directCnt, indirectCnt, dataOff);
    }

    /**
     * @param buf Byte buffer.
     * @param entrySizeWithItem Full entry size.
     * @param directCnt Direct items count.
     * @param indirectCnt Indirect items count.
     * @param dataOff First entry offset.
     * @return First entry offset after compaction.
     */
    private int compactIfNeed(
        final ByteBuffer buf,
        final int entrySizeWithItem,
        final int directCnt,
        final int indirectCnt,
        int dataOff
    ) {
        if (!isEnoughSpace(entrySizeWithItem, dataOff, directCnt, indirectCnt)) {
            dataOff = compactDataEntries(buf, directCnt);

            assert isEnoughSpace(entrySizeWithItem, dataOff, directCnt, indirectCnt);
        }

        return dataOff;
    }

    /**
     * Put item reference on entry.
     *
     * @param buf Byte buffer.
     * @param entrySizeWithItem Full entry size.
     * @param directCnt Direct items count.
     * @param indirectCnt Indirect items count.
     * @param dataOff Data offset.
     * @return Item ID.
     */
    private int writeItemId(final ByteBuffer buf, final int entrySizeWithItem, final int directCnt,
        final int indirectCnt, final int dataOff) {
        setFirstEntryOffset(buf, dataOff);

        int itemId = insertItem(buf, dataOff, directCnt, indirectCnt);

        assert check(itemId): itemId;
        assert getIndirectCount(buf) <= getDirectCount(buf);

        // Update free space. If number of indirect items changed, then we were able to reuse an item slot.
        setFreeSpace(buf, getFreeSpace(buf) - entrySizeWithItem + (getIndirectCount(buf) != indirectCnt ? ITEM_SIZE : 0));

        assert getFreeSpace(buf) >= 0;
        assert (itemId & ~0xFF) == 0;

        return itemId;
    }

    /**
     * Writes next entry fragment.
     *
     * @param key Entry key.
     * @param val Entry value
     * @param ver Grid cache version.
     * @param buf Page buffer.
     * @param ctx Cache object context.
     * @param written Actual written entry bytes from the end.
     * @param totalEntrySize Total entry size with overhead.
     * @param chunkSize Fragment size.
     * @param chunks Number of fragments.
     * @param lastLink Last fragment link or 0 if it is the first one (from the entry end).
     * @return Updated written bytes value and fragment index in the page.
     * @throws IgniteCheckedException If failed.
     */
    public FragmentWritten writeRowFragment(
        final KeyCacheObject key,
        final CacheObject val,
        final GridCacheVersion ver,
        final ByteBuffer buf,
        final CacheObjectContext ctx,
        int written,
        final int totalEntrySize,
        final int chunkSize,
        final int chunks,
        final long lastLink
    ) throws IgniteCheckedException {
        final boolean lastChunk = written == 0;

        final int dataSize = key.valueBytesLength(ctx) + val.valueBytesLength(ctx) + VER_SIZE;

        final int toWrite = lastChunk ? totalEntrySize - chunkSize * (chunks - 1) : chunkSize;

        int directCnt = getDirectCount(buf);
        int indirectCnt = getIndirectCount(buf);
        int dataOff = getFirstEntryOffset(buf);

        // Compact if we do not have enough space for entry.
        dataOff = compactIfNeed(buf, toWrite, directCnt, indirectCnt, dataOff);

        dataOff -= toWrite - ITEM_SIZE;

        try {
            buf.position(dataOff);

            final int len = toWrite - ITEM_SIZE - KV_LEN_SIZE - LINK_SIZE;
            final int off = Math.abs(written - dataSize) - len;

            buf.putShort((short) (len | FRAGMENTED_FLAG));
            buf.putLong(lastLink);

            writeFragmentData(key, val, ver, buf, ctx, off, len);

            written += len;
        }
        finally {
            buf.position(0);
        }

        final int idx = writeItemId(buf, toWrite, directCnt, indirectCnt, dataOff);

        return new FragmentWritten(written, idx, dataOff + KV_LEN_SIZE + LINK_SIZE, lastChunk);
    }

    /**
     * Write prepared fragment data to page.
     *
     * @param dataBuf Actual fragment data.
     * @param buf Page buffer.
     * @param written Actual written entry bytes from the end.
     * @param lastLink Last fragment linc.
     * @return Updated written bytes value and fragment index in the page.
     */
    public FragmentWritten writeFragmentData(
        final ByteBuffer dataBuf,
        final ByteBuffer buf,
        int written,
        final long lastLink
    ) {
        final boolean lastChunk = written == 0;

        final int toWrite = dataBuf.remaining() + ITEM_SIZE + KV_LEN_SIZE + LINK_SIZE;

        int directCnt = getDirectCount(buf);
        int indirectCnt = getIndirectCount(buf);
        int dataOff = getFirstEntryOffset(buf);

        // Compact if we do not have enough space for entry.
        dataOff = compactIfNeed(buf, toWrite, directCnt, indirectCnt, dataOff);

        dataOff -= toWrite - ITEM_SIZE;

        try {
            buf.position(dataOff);

            final int len = dataBuf.remaining();

            buf.putShort((short) (len | FRAGMENTED_FLAG));
            buf.putLong(lastLink);

            buf.put(dataBuf);

            written += len;
        }
        finally {
            buf.position(0);
        }

        final int idx = writeItemId(buf, toWrite, directCnt, indirectCnt, dataOff);

        return new FragmentWritten(written, idx, dataOff, lastChunk);
    }

    /**
     * Write actual entry data according to state in {@code fctx}.
     *
     * @param totalOff Offset in actual entry data.
     * @param totalLen Data length that should be written in a fragment.
     * @throws IgniteCheckedException If fail.
     */
    private void writeFragmentData(
        final KeyCacheObject key,
        final CacheObject val,
        final GridCacheVersion ver,
        final ByteBuffer buf,
        final CacheObjectContext ctx,
        final int totalOff,
        final int totalLen
    ) throws IgniteCheckedException {
        int written = writeFragment(key, val, ver, buf, ctx, totalOff, totalLen, EntryPart.KEY);

        written += writeFragment(key, val, ver, buf, ctx, totalOff + written, totalLen - written, EntryPart.VAL);

        writeFragment(key, val, ver, buf, ctx, totalOff + written, totalLen - written, EntryPart.VER);
    }

    /**
     * Try to write fragment data.
     *
     * @param totalOff Offset in actual entry data.
     * @param totalLen Data length that should be written in a fragment.
     * @param type Type of the part of entry.
     * @return Actually written data.
     * @throws IgniteCheckedException If fail.
     */
    private int writeFragment(
        final KeyCacheObject key,
        final CacheObject val,
        final GridCacheVersion ver,
        final ByteBuffer buf,
        final CacheObjectContext ctx,
        final int totalOff,
        final int totalLen,
        final EntryPart type
    )
        throws IgniteCheckedException {
        final int prevLen;
        final int curLen;

        final int keySize = key.valueBytesLength(ctx);
        final int valSize = val.valueBytesLength(ctx);

        switch (type) {
            case KEY:
                prevLen = 0;
                curLen = keySize;

                break;

            case VAL:
                prevLen = keySize;
                curLen = keySize + valSize;

                break;

            case VER:
                prevLen = keySize + valSize;
                curLen = keySize + valSize + VER_SIZE;

                break;

            default:
                throw new IllegalArgumentException("Unknown entry part type: " + type);
        }

        if (curLen <= totalOff || totalLen == 0)
            return 0;

        final int len = Math.min(curLen - totalOff, totalLen);

        if (type == EntryPart.KEY || type == EntryPart.VAL) {
            final CacheObject co = type == EntryPart.KEY ? key : val;

            co.putValue(buf, totalOff - prevLen, len, ctx);
        }
        else {
            final ByteBuffer verBuf = ByteBuffer.allocate(VER_SIZE);

            verBuf.order(buf.order());

            writeGridCacheVersion(verBuf, ver);

            buf.put(verBuf.array(), totalOff - prevLen, len);
        }

        return len;
    }

    /**
     *
     */
    private enum EntryPart {
        /** */
        KEY,

        /** */
        VAL,

        /** */
        VER
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
     * Full-scan free space calculation procedure.
     *
     * @param buf Buffer to scan.
     * @return Actual free space in the buffer.
     */
    private int actualFreeSpace(ByteBuffer buf) {
        int directCnt = getDirectCount(buf);

        int entriesSize = 0;

        for (int i = 0; i < directCnt; i++) {
            int off = toOffset(getItem(buf, i));

            int entrySize = getEntrySize(buf, off, false);

            entriesSize += entrySize;
        }

        return buf.capacity() - ITEMS_OFF - entriesSize - (directCnt + getIndirectCount(buf)) * ITEM_SIZE;
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
        assert off + step + cnt <= buf.capacity() : "[off=" + off + ", step=" + step + ", cnt=" + cnt +
            ", cap=" + buf.capacity() + ']';

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

            writeGridCacheVersion(buf, ver);
        }
        finally {
            buf.position(0);
        }
    }

    /**
     * @param buf Byte buffer.
     * @param ver Version to write.
     */
    private static void writeGridCacheVersion(final ByteBuffer buf, final GridCacheVersion ver) {
        buf.putInt(ver.topologyVersion());
        buf.putInt(ver.nodeOrderAndDrIdRaw());
        buf.putLong(ver.globalTime());
        buf.putLong(ver.order());
    }

    /**
     *
     */
    public static class FragmentWritten {
        /** Total written entry bytes from the entry end. */
        private final int writtenBytes;

        /** Last fragment index. */
        private final int idx;

        /** Data offset that points to fragment actual data without overhead. */
        private final int dataOff;

        /** Last fragment flag. */
        private final boolean lastFragment;

        /**
         * @param writtenBytes Total written entry bytes from the entry end.
         * @param idx Last fragment index.
         * @param dataOff Data offset that points to fragment actual data without overhead.
         * @param lastFragment Last fragment flag.
         */
        public FragmentWritten(final int writtenBytes, final int idx, final int dataOff, final boolean lastFragment) {
            this.writtenBytes = writtenBytes;
            this.idx = idx;
            this.dataOff = dataOff;
            this.lastFragment = lastFragment;
        }

        /**
         * @return Total written entry bytes from the entry end.
         */
        public int writtenBytes() {
            return writtenBytes;
        }

        /**
         * @return Last fragment index.
         */
        public int writtenIndex() {
            return idx;
        }

        /**
         * @return Data offset that points to fragment actual data without overhead.
         */
        public int dataOffset() {
            return dataOff;
        }

        /**
         * @return {@code True} if it was a last fragment written.
         */
        public boolean lastFragment() {
            return lastFragment;
        }
    }
}

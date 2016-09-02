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
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.database.CacheDataRow;
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
    private static final int SHOW_ITEM = 0b0001;

    /** */
    private static final int SHOW_PAYLOAD_LEN = 0b0010;

    /** */
    private static final int SHOW_LINK = 0b0100;

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
    private static final int PAYLOAD_LEN_SIZE = 2;

    /** */
    private static final int LINK_SIZE = 8;

    /** */
    private static final int FRAGMENTED_FLAG = 0b10000000_00000000;

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
        setFirstEntryOffset(buf, buf.capacity());
        setRealFreeSpace(buf, buf.capacity() - ITEMS_OFF);
    }

    /**
     * @param buf Buffer.
     * @param dataOff Data offset.
     * @param show What elements of data page entry to show in the result.
     * @return Data page entry size.
     */
    private int getPageEntrySize(ByteBuffer buf, int dataOff, int show) {
        int payloadLen = buf.getShort(dataOff) & 0xFFFF;

        if ((payloadLen & FRAGMENTED_FLAG) != 0)
            payloadLen &= ~FRAGMENTED_FLAG; // We are fragmented and have a link.
        else
            show &= ~SHOW_LINK; // We are not fragmented, never have a link.

        return getPageEntrySize(payloadLen, show);
    }

    /**
     * @param payloadLen Length of the payload, may be a full data row or a row fragment length.
     * @param show What elements of data page entry to show in the result.
     * @return Data page entry size.
     */
    private int getPageEntrySize(int payloadLen, int show) {
        assert payloadLen > 0: payloadLen;

        int res = payloadLen;

        if ((show & SHOW_LINK) != 0)
            res += LINK_SIZE;

        if ((show & SHOW_ITEM) != 0)
            res += ITEM_SIZE;

        if ((show & SHOW_PAYLOAD_LEN) != 0)
            res += PAYLOAD_LEN_SIZE;

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
    private void setRealFreeSpace(ByteBuffer buf, int freeSpace) {
        assert freeSpace == actualFreeSpace(buf): freeSpace + " != " + actualFreeSpace(buf);

        buf.putShort(FREE_SPACE_OFF, (short)freeSpace);
    }

    /**
     * Free space refers to a "max row size (without any data page specific overhead) which is
     * guaranteed to fit into this data page".
     *
     * @param buf Buffer.
     * @return Free space.
     */
    public int getFreeSpace(ByteBuffer buf) {
        if (getFreeItemSlots(buf) == 0)
            return 0;

        int freeSpace = getRealFreeSpace(buf);

        // We reserve size here because of getFreeSpace() method semantics (see method javadoc).
        // It means that we must be able to accommodate a row of size which is equal to getFreeSpace(),
        // plus we will have data page overhead: header of the page as well as item, payload length and
        // possibly a link to the next row fragment.
        freeSpace -= ITEM_SIZE + PAYLOAD_LEN_SIZE + LINK_SIZE;

        return freeSpace < 0 ? 0 : freeSpace;
    }

    /**
     * Equivalent for {@link #actualFreeSpace(ByteBuffer)} but reads saved value.
     *
     * @param buf Buffer.
     * @return Free space.
     */
    private int getRealFreeSpace(ByteBuffer buf) {
        return buf.getShort(FREE_SPACE_OFF);
    }

    /**
     * @param buf Buffer.
     * @param cnt Direct count.
     */
    private void setDirectCount(ByteBuffer buf, int cnt) {
        assert checkCount(cnt): cnt;

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
        assert checkCount(cnt): cnt;

        buf.put(INDIRECT_CNT_OFF, (byte)cnt);
    }

    /**
     * @param idx Index.
     * @return {@code true} If the index is valid.
     */
    private boolean checkIndex(int idx) {
        return idx >= 0 && idx < 0xFF;
    }

    /**
     * @param cnt Counter value.
     * @return {@code true} If the counter fits 1 byte.
     */
    private boolean checkCount(int cnt) {
        return cnt >= 0 && cnt <= 0xFF;
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
    private int getFreeItemSlots(ByteBuffer buf) {
        return 0xFF - getDirectCount(buf);
    }

    /**
     * @param buf Buffer.
     * @param itemId Fixed item ID (the index used for referencing an entry from the outside).
     * @param directCnt Direct items count.
     * @param indirectCnt Indirect items count.
     * @return Found index of indirect item.
     */
    private int findIndirectItemIndex(ByteBuffer buf, int itemId, int directCnt, int indirectCnt) {
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
        int free = getRealFreeSpace(buf);

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

            entriesSize += getPageEntrySize(buf, item, SHOW_PAYLOAD_LEN | SHOW_LINK);

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
    private int getDataOffset(ByteBuffer buf, int itemId) {
        assert checkIndex(itemId): itemId;

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

        return directItemToOffset(getItem(buf, itemId));
    }

    /**
     * @param buf Byte buffer.
     * @param dataOff Points to the entry start.
     * @return Link to the next entry fragment or 0 if no fragments left or if entry is not fragmented.
     */
    private long getNextFragmentLink(ByteBuffer buf, int dataOff) {
        assert isFragmented(buf, dataOff);

        return buf.getLong(dataOff + PAYLOAD_LEN_SIZE);
    }

    /**
     * @param buf Buffer.
     * @param dataOff Data offset.
     * @return {@code true} If the data row is fragmented across multiple pages.
     */
    private boolean isFragmented(ByteBuffer buf, int dataOff) {
        return (buf.getShort(dataOff) & FRAGMENTED_FLAG) != 0;
    }

    /**
     * Sets position to start of actual fragment data and limit to it's end.
     *
     * @param buf Byte buffer.
     * @param itemId Item to position on.
     * @return Link to the next fragment or {@code 0} if it is the last fragment or the data row is not fragmented.
     */
    public long setPositionAndLimitOnPayload(final ByteBuffer buf, final int itemId) {
        int dataOff = getDataOffset(buf, itemId);

        boolean fragmented = isFragmented(buf, dataOff);
        long nextLink = fragmented ? getNextFragmentLink(buf, dataOff) : 0;
        int payloadSize = getPageEntrySize(buf, dataOff, 0);

        buf.position(dataOff + PAYLOAD_LEN_SIZE + (fragmented ? LINK_SIZE : 0));

        buf.limit(buf.position() + payloadSize);

        return nextLink;
    }

    /**
     * @param buf Buffer.
     * @param idx Item index.
     * @return Item.
     */
    private short getItem(ByteBuffer buf, int idx) {
        return buf.getShort(itemOffset(idx));
    }

    /**
     * @param buf Buffer.
     * @param idx Item index.
     * @param item Item.
     */
    private void setItem(ByteBuffer buf, int idx, short item) {
        buf.putShort(itemOffset(idx), item);
    }

    /**
     * @param idx Index of the item.
     * @return Offset in buffer.
     */
    private int itemOffset(int idx) {
        assert checkIndex(idx): idx;

        return ITEMS_OFF + idx * ITEM_SIZE;
    }

    /**
     * @param directItem Direct item.
     * @return Offset of an entry payload inside of the page.
     */
    private int directItemToOffset(short directItem) {
        return directItem & 0xFFFF;
    }

    /**
     * @param dataOff Data offset.
     * @return Direct item.
     */
    private short directItemFromOffset(int dataOff) {
        assert dataOff >= ITEMS_OFF + ITEM_SIZE && dataOff < Short.MAX_VALUE: dataOff;

        return (short)dataOff;
    }

    /**
     * @param indirectItem Indirect item.
     * @return Index of corresponding direct item.
     */
    private int directItemIndex(short indirectItem) {
        return indirectItem & 0xFF;
    }

    /**
     * @param indirectItem Indirect item.
     * @return Fixed item ID (the index used for referencing an entry from the outside).
     */
    private int itemId(short indirectItem) {
        return indirectItem >>> 8;
    }

    /**
     * @param itemId Fixed item ID (the index used for referencing an entry from the outside).
     * @param directItemIdx Index of corresponding direct item.
     * @return Indirect item.
     */
    private short indirectItem(int itemId, int directItemIdx) {
        assert checkIndex(itemId): itemId;
        assert checkIndex(directItemIdx): directItemIdx;

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
    private boolean moveLastItem(ByteBuffer buf, int freeDirectIdx, int directCnt, int indirectCnt) {
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
    private int findIndirectIndexForLastDirect(ByteBuffer buf, int directCnt, int indirectCnt) {
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
     * @return Next link for fragmented entries or {@code 0} if none.
     * @throws IgniteCheckedException If failed.
     */
    public long removeRow(ByteBuffer buf, int itemId) throws IgniteCheckedException {
        assert checkIndex(itemId) : itemId;

        final int dataOff = getDataOffset(buf, itemId);
        final long nextLink = isFragmented(buf, dataOff) ? getNextFragmentLink(buf, dataOff) : 0;

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
            int rmvEntrySize = getPageEntrySize(buf, dataOff, SHOW_PAYLOAD_LEN | SHOW_LINK);

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
            setRealFreeSpace(buf, getRealFreeSpace(buf) + rmvEntrySize +
                ITEM_SIZE * (directCnt - getDirectCount(buf) + indirectCnt - getIndirectCount(buf)));
        }

        return nextLink;
    }

    /**
     * @param buf Buffer.
     * @param idx Index.
     * @param cnt Count.
     * @param step Step.
     */
    private void moveItems(ByteBuffer buf, int idx, int cnt, int step) {
        assert cnt >= 0: cnt;

        if (cnt != 0)
            moveBytes(buf, itemOffset(idx), cnt * ITEM_SIZE, step * ITEM_SIZE);
    }

    /**
     * @param newEntryFullSize New entry full size (with item, length and link).
     * @param firstEntryOff First entry data offset.
     * @param directCnt Direct items count.
     * @param indirectCnt Indirect items count.
     * @return {@code true} If there is enough space for the entry.
     */
    private boolean isEnoughSpace(int newEntryFullSize, int firstEntryOff, int directCnt, int indirectCnt) {
        return ITEMS_OFF + ITEM_SIZE * (directCnt + indirectCnt) <= firstEntryOff - newEntryFullSize;
    }

    /**
     * Adds row to this data page and sets respective link to the given row object.
     *
     * @param coctx Cache object context.
     * @param buf Buffer.
     * @param row Cache data row.
     * @throws IgniteCheckedException If failed.
     */
    public void addRow(
        CacheObjectContext coctx,
        ByteBuffer buf,
        CacheDataRow row,
        int rowSize
    ) throws IgniteCheckedException {
        assert rowSize <= getFreeSpace(buf): "can't call addRow if not enough space for the whole row";

        int fullEntrySize = getPageEntrySize(rowSize, SHOW_PAYLOAD_LEN | SHOW_ITEM);

        int directCnt = getDirectCount(buf);
        int indirectCnt = getIndirectCount(buf);

        int dataOff = getDataOffsetForWrite(buf, fullEntrySize, directCnt, indirectCnt);

        writeRowData(coctx, buf, dataOff, rowSize, row);

        int itemId = addItem(buf, fullEntrySize, directCnt, indirectCnt, dataOff);

        setLink(row, buf, itemId);
    }

    /**
     * @param buf Byte buffer.
     * @param entryFullSize New entry full size (with item, length and link).
     * @param directCnt Direct items count.
     * @param indirectCnt Indirect items count.
     * @param dataOff First entry offset.
     * @return First entry offset after compaction.
     */
    private int compactIfNeed(
        final ByteBuffer buf,
        final int entryFullSize,
        final int directCnt,
        final int indirectCnt,
        int dataOff
    ) {
        if (!isEnoughSpace(entryFullSize, dataOff, directCnt, indirectCnt)) {
            dataOff = compactDataEntries(buf, directCnt);

            assert isEnoughSpace(entryFullSize, dataOff, directCnt, indirectCnt);
        }

        return dataOff;
    }

    /**
     * Put item reference on entry.
     *
     * @param buf Byte buffer.
     * @param fullEntrySize Full entry size (with link, payload size and item).
     * @param directCnt Direct items count.
     * @param indirectCnt Indirect items count.
     * @param dataOff Data offset.
     * @return Item ID.
     */
    private int addItem(final ByteBuffer buf, final int fullEntrySize, final int directCnt,
        final int indirectCnt, final int dataOff) {
        setFirstEntryOffset(buf, dataOff);

        int itemId = insertItem(buf, dataOff, directCnt, indirectCnt);

        assert checkIndex(itemId): itemId;
        assert getIndirectCount(buf) <= getDirectCount(buf);

        // Update free space. If number of indirect items changed, then we were able to reuse an item slot.
        setRealFreeSpace(buf, getRealFreeSpace(buf) - fullEntrySize +
            (getIndirectCount(buf) != indirectCnt ? ITEM_SIZE : 0));

        return itemId;
    }

    /**
     * @param buf Buffer.
     * @param fullEntrySize Full entry size.
     * @param directCnt Direct items count.
     * @param indirectCnt Indirect items count.
     * @return Offset in the buffer where the entry must be written.
     */
    private int getDataOffsetForWrite(ByteBuffer buf, int fullEntrySize, int directCnt, int indirectCnt) {
        int dataOff = getFirstEntryOffset(buf);

        // Compact if we do not have enough space for entry.
        dataOff = compactIfNeed(buf, fullEntrySize, directCnt, indirectCnt, dataOff);

        // We will write data right before the first entry.
        dataOff -= fullEntrySize - ITEM_SIZE;

        return dataOff;
    }

    /**
     * Adds maximum possible fragment of the given row to this data page and sets respective link to the row.
     *
     * @param coctx Cache object context.
     * @param buf Byte buffer.
     * @param row Cache data row.
     * @param written Number of bytes of row size that was already written.
     * @param rowSize Row size.
     * @return Written payload size.
     * @throws IgniteCheckedException If failed.
     */
    public int addRowFragment(
        CacheObjectContext coctx,
        ByteBuffer buf,
        CacheDataRow row,
        int written,
        int rowSize
    ) throws IgniteCheckedException {
        return addRowFragment(coctx, buf, written, rowSize, row.link(), row, null);
    }

    /**
     * Adds this payload as a fragment to this data page.
     *
     * @param coctx Cache object context.
     * @param buf Byte buffer.
     * @param payload Payload bytes.
     * @param lastLink Link to the previous written fragment (link to the tail).
     * @throws IgniteCheckedException If failed.
     */
    public void addRowFragment(
        CacheObjectContext coctx,
        ByteBuffer buf,
        byte[] payload,
        long lastLink
    ) throws IgniteCheckedException {
        addRowFragment(coctx, buf, 0, 0, lastLink, null, payload);
    }

    /**
     * Adds maximum possible fragment of the given row to this data page and sets respective link to the row.
     *
     * @param coctx Cache object context.
     * @param buf Byte buffer.
     * @param written Number of bytes of row size that was already written.
     * @param rowSize Row size.
     * @param lastLink Link to the previous written fragment (link to the tail).
     * @param row Row.
     * @param payload Payload bytes.
     * @return Written payload size.
     * @throws IgniteCheckedException If failed.
     */
    private int addRowFragment(
        CacheObjectContext coctx,
        ByteBuffer buf,
        int written,
        int rowSize,
        long lastLink,
        CacheDataRow row,
        byte[] payload
    ) throws IgniteCheckedException {
        assert payload == null ^ row == null;

        int directCnt = getDirectCount(buf);
        int indirectCnt = getIndirectCount(buf);

        int payloadSize = payload != null ? payload.length :
            Math.min(rowSize - written, getFreeSpace(buf));

        int fullEntrySize = getPageEntrySize(payloadSize, SHOW_PAYLOAD_LEN | SHOW_LINK | SHOW_ITEM);
        int dataOff = getDataOffsetForWrite(buf, fullEntrySize, directCnt, indirectCnt);

        try {
            buf.position(dataOff);

            buf.putShort((short) (payloadSize | FRAGMENTED_FLAG));
            buf.putLong(lastLink);

            if (payload == null) {
                int rowOff = rowSize - written - payloadSize;

                writeFragmentData(row, buf, coctx, rowOff, payloadSize);
            }
            else
                buf.put(payload);
        }
        finally {
            buf.position(0);
        }

        int itemId = addItem(buf, fullEntrySize, directCnt, indirectCnt, dataOff);

        if (row != null)
            setLink(row, buf, itemId);

        return payloadSize;
    }

    /**
     * @param row Row to set link to.
     * @param buf Page buffer.
     * @param itemId Item ID.
     */
    private void setLink(CacheDataRow row, ByteBuffer buf, int itemId) {
        row.link(PageIdUtils.link(getPageId(buf), itemId));
    }

    /**
     * Write row data fragment.
     *
     * @param row Row.
     * @param buf Byte buffer.
     * @param coctx Cache object context.
     * @param rowOff Offset in row data bytes.
     * @param payloadSize Data length that should be written in a fragment.
     * @throws IgniteCheckedException If failed.
     */
    private void writeFragmentData(
        final CacheDataRow row,
        final ByteBuffer buf,
        final CacheObjectContext coctx,
        final int rowOff,
        final int payloadSize
    ) throws IgniteCheckedException {
        final int keySize = row.key().valueBytesLength(coctx);
        final int valSize = row.value().valueBytesLength(coctx);

        int written = writeFragment(row, buf, coctx, rowOff, payloadSize, EntryPart.KEY, keySize, valSize);
        written += writeFragment(row, buf, coctx, rowOff + written, payloadSize - written, EntryPart.EXPIRE_TIME, keySize, valSize);
        written += writeFragment(row, buf, coctx, rowOff + written, payloadSize - written, EntryPart.VALUE, keySize, valSize);
        written += writeFragment(row, buf, coctx, rowOff + written, payloadSize - written, EntryPart.VERSION, keySize, valSize);

        assert written == payloadSize;
    }

    /**
     * Try to write fragment data.
     *
     * @param rowOff Offset in row data bytes.
     * @param payloadSize Data length that should be written in this fragment.
     * @param type Type of the part of entry.
     * @return Actually written data.
     * @throws IgniteCheckedException If fail.
     */
    private int writeFragment(
        final CacheDataRow row,
        final ByteBuffer buf,
        final CacheObjectContext coctx,
        final int rowOff,
        final int payloadSize,
        final EntryPart type,
        final int keySize,
        final int valSize
    ) throws IgniteCheckedException {
        if (payloadSize == 0)
            return 0;

        final int prevLen;
        final int curLen;

        switch (type) {
            case KEY:
                prevLen = 0;
                curLen = keySize;

                break;

            case EXPIRE_TIME:
                prevLen = keySize;
                curLen = keySize + 8;

                break;

            case VALUE:
                prevLen = keySize + 8;
                curLen = keySize + valSize + 8;

                break;

            case VERSION:
                prevLen = keySize + valSize + 8;
                curLen = keySize + valSize + CacheVersionIO.size(row.version(), false) + 8;

                break;

            default:
                throw new IllegalArgumentException("Unknown entry part type: " + type);
        }

        if (curLen <= rowOff)
            return 0;

        final int len = Math.min(curLen - rowOff, payloadSize);

        if (type == EntryPart.EXPIRE_TIME)
            writeExpireTimeFragment(buf, row.expireTime(), rowOff, len, prevLen);
        else if (type != EntryPart.VERSION) {
            // Write key or value.
            final CacheObject co = type == EntryPart.KEY ? row.key() : row.value();

            co.putValue(buf, rowOff - prevLen, len, coctx);
        }
        else
            writeVersionFragment(buf, row.version(), rowOff, len, prevLen);

        return len;
    }

    /**
     * @param buf Byte buffer.
     * @param ver Version.
     * @param rowOff Row offset.
     * @param len Length.
     * @param prevLen previous length.
     */
    private void writeVersionFragment(ByteBuffer buf, GridCacheVersion ver, int rowOff, int len, int prevLen) {
        int verSize = CacheVersionIO.size(ver, false);

        assert len <= verSize: len;

        if (verSize == len) { // Here we check for equality but not <= because version is the last.
            // Here we can write version directly.
            CacheVersionIO.write(buf, ver, false);
        }
        else {
            // We are in the middle of cache version.
            ByteBuffer verBuf = ByteBuffer.allocate(verSize);

            verBuf.order(buf.order());

            CacheVersionIO.write(verBuf, ver, false);

            buf.put(verBuf.array(), rowOff - prevLen, len);
        }
    }

    /**
     * @param buf Byte buffer.
     * @param expireTime Expire time.
     * @param rowOff Row offset.
     * @param len Length.
     * @param prevLen previous length.
     */
    private void writeExpireTimeFragment(ByteBuffer buf, long expireTime, int rowOff, int len, int prevLen) {
        int size = 8;

        if (size <= len)
            buf.putLong(expireTime);
        else {
            ByteBuffer timeBuf = ByteBuffer.allocate(size);

            timeBuf.order(buf.order());

            timeBuf.putLong(expireTime);

            buf.put(timeBuf.array(), rowOff - prevLen, len);
        }
    }

    /**
     *
     */
    private enum EntryPart {
        /** */
        KEY,

        /** */
        VALUE,

        /** */
        VERSION,

        /** */
        EXPIRE_TIME
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
                setItem(buf, directItemIdx, directItemFromOffset(dataOff));

                setDirectCount(buf, directCnt + 1);
                setIndirectCount(buf, indirectCnt - 1);

                return directItemIdx;
            }
        }

        // Move all the indirect items forward to make a free slot and insert new item at the end of direct items.
        moveItems(buf, directCnt, indirectCnt, +1);

        setItem(buf, directCnt, directItemFromOffset(dataOff));

        setDirectCount(buf, directCnt + 1);
        assert getDirectCount(buf) == directCnt + 1;

        return directCnt; // Previous directCnt will be our itemId.
    }

    /**
     * @param buf Buffer.
     * @param directCnt Direct items count.
     * @return New first entry offset.
     */
    private int compactDataEntries(ByteBuffer buf, int directCnt) {
        assert checkCount(directCnt): directCnt;

        int[] offs = new int[directCnt];

        for (int i = 0; i < directCnt; i++) {
            int off = directItemToOffset(getItem(buf, i));

            offs[i] = (off << 8) | i; // This way we'll be able to sort by offset using Arrays.sort(...).
        }

        Arrays.sort(offs);

        // Move right all of the entries if possible to make the page as compact as possible to its tail.
        int prevOff = buf.capacity();

        for (int i = directCnt - 1; i >= 0; i--) {
            int off = offs[i] >>> 8;

            assert off < prevOff: off;

            int entrySize = getPageEntrySize(buf, off, SHOW_PAYLOAD_LEN | SHOW_LINK);

            int delta = prevOff - (off + entrySize);

            if (delta != 0) { // Move right.
                assert delta > 0: delta;

                moveBytes(buf, off, entrySize, delta);

                int itemId = offs[i] & 0xFF;

                off += delta;

                setItem(buf, itemId, directItemFromOffset(off));
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
            int off = directItemToOffset(getItem(buf, i));

            int entrySize = getPageEntrySize(buf, off, SHOW_PAYLOAD_LEN | SHOW_LINK);

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
    private void moveBytes(ByteBuffer buf, int off, int cnt, int step) {
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
     * @param payloadSize Payload size.
     * @param row Data row.
     * @throws IgniteCheckedException If failed.
     */
    private void writeRowData(
        CacheObjectContext coctx,
        ByteBuffer buf,
        int dataOff,
        int payloadSize,
        CacheDataRow row
    ) throws IgniteCheckedException {
        try {
            buf.position(dataOff);

            buf.putShort((short)payloadSize);

            boolean ok = row.key().putValue(buf, coctx);

            assert ok;

            ok = row.value().putValue(buf, coctx);

            assert ok;

            CacheVersionIO.write(buf, row.version(), false);

            buf.putLong(row.expireTime());
        }
        finally {
            buf.position(0);
        }
    }
}

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

package org.apache.ignite.internal.processors.cache.persistence.tree.io;

import java.nio.ByteBuffer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.TrackingPageIsCorruptedException;
import org.apache.ignite.internal.processors.cache.persistence.tree.util.PageHandler;
import org.apache.ignite.internal.util.GridStringBuilder;
import org.apache.ignite.internal.util.GridUnsafe;
import org.jetbrains.annotations.Nullable;

/**
 * We use dedicated page for tracking pages updates.
 * Also we divide such 'tracking' pages on two half, first is used for check that page was changed or not
 * (during incremental snapshot), second - to accumulate changed for next snapshot.
 *
 * You cannot test change for not started snapshot! because it will cause of preparation for snapshot.
 *
 * Implementation. For each page there is own bit in both half. Tracking page is used for tracking N page after it.
 * N depends on page size (how many bytes we can use for tracking).
 *
 *
 *                       +-----------------------------------------+-----------------------------------------+
 *                       |                left half                |               right half                |
 * +---------+-----------+----+------------------------------------+----+------------------------------------+
 * |  HEADER | Last      |size|                                    |size|                                    |
 * |         |SnapshotTag|2b. |  tracking bits                     |2b. |  tracking bits                     |
 * +---------+--------- -+----+------------------------------------+----+------------------------------------+
 *
 */
public class TrackingPageIO extends PageIO {
    /** */
    public static final IOVersions<TrackingPageIO> VERSIONS = new IOVersions<>(
        new TrackingPageIO(1)
    );

    /** Corrupt flag mask. */
    public static final long CORRUPT_FLAG_MASK = 1L << 63;

    /** Corrupt flag mask. */
    public static final long CORRUPT_FLAG_FILTER_MASK = ~CORRUPT_FLAG_MASK;

    /** Last snapshot offset. */
    public static final int LAST_SNAPSHOT_TAG_OFFSET = COMMON_HEADER_END;

    /** Size field offset. */
    public static final int SIZE_FIELD_OFFSET = LAST_SNAPSHOT_TAG_OFFSET + 8;

    /** 'Size' field size. */
    public static final int SIZE_FIELD_SIZE = 2;

    /** Bitmap offset. */
    public static final int BITMAP_OFFSET = SIZE_FIELD_OFFSET + SIZE_FIELD_SIZE;

    /** Count of extra page. */
    public static final int COUNT_OF_EXTRA_PAGE = 1;

    /**
     * @param ver Page format version.
     */
    protected TrackingPageIO(int ver) {
        super(PageIO.T_PAGE_UPDATE_TRACKING, ver);
    }

    /**
     * Will mark pageId as changed for next (!) snapshotId
     *
     * @param buf Buffer.
     * @param pageId Page id.
     * @param nextSnapshotTag Tag of next snapshot.
     * @param pageSize Page size.
     *
     * @return <code>-1</code> if everything is ok, otherwise last saved tag.
     */
    public long markChanged(ByteBuffer buf, long pageId, long nextSnapshotTag, long lastSuccessfulSnapshotTag, int pageSize) {
        long tag = validateSnapshotTag(buf, nextSnapshotTag, lastSuccessfulSnapshotTag, pageSize);

        int cntOfPage = countOfPageToTrack(pageSize);

        int idxToUpdate = (PageIdUtils.pageIndex(pageId) - COUNT_OF_EXTRA_PAGE) % cntOfPage;

        int sizeOff = useLeftHalf(nextSnapshotTag) ? SIZE_FIELD_OFFSET : BITMAP_OFFSET + (cntOfPage >> 3);

        int idx = sizeOff + SIZE_FIELD_SIZE + (idxToUpdate >> 3);

        byte byteToUpdate = buf.get(idx);

        int updateTemplate = 1 << (idxToUpdate & 0b111);

        byte newVal = (byte)(byteToUpdate | updateTemplate);

        if (byteToUpdate == newVal)
            return tag;

        buf.put(idx, newVal);

        short newSize = (short)(buf.getShort(sizeOff) + 1);

        buf.putShort(sizeOff, newSize);

        assert newSize == countOfChangedPage(buf, nextSnapshotTag, pageSize);

        return tag;
    }

    /**
     * @param buf Buffer.
     * @param nextSnapshotTag Next snapshot tag.
     * @param lastSuccessfulSnapshotTag Last successful snapshot tag.
     * @param pageSize Page size.
     *
     * @return <code>-1</code> if everything is ok, otherwise last saved tag.
     */
    private long validateSnapshotTag(ByteBuffer buf, long nextSnapshotTag, long lastSuccessfulSnapshotTag, int pageSize) {
        assert nextSnapshotTag != lastSuccessfulSnapshotTag : "nextSnapshotTag = " + nextSnapshotTag +
            ", lastSuccessfulSnapshotId = " + lastSuccessfulSnapshotTag;

        long last = getLastSnapshotTag(buf);

        if (last > nextSnapshotTag) { //we have lost snapshot tag therefore should mark this tracking as corrupted
            PageHandler.zeroMemory(buf, LAST_SNAPSHOT_TAG_OFFSET, buf.capacity() - LAST_SNAPSHOT_TAG_OFFSET);

            setLastSnasphotTag(buf, nextSnapshotTag | CORRUPT_FLAG_MASK);

            return last;
        }

        if (nextSnapshotTag == last) //everything is ok
            return -1;

        int cntOfPage = countOfPageToTrack(pageSize);

        if (last <= lastSuccessfulSnapshotTag) { //we can drop our data
            setLastSnasphotTag(buf, nextSnapshotTag);

            PageHandler.zeroMemory(buf, SIZE_FIELD_OFFSET, buf.capacity() - SIZE_FIELD_OFFSET);
        } else { //we can't drop data, it is still necessary for incremental snapshots
            int len = cntOfPage >> 3;

            int sizeOff = useLeftHalf(nextSnapshotTag) ? SIZE_FIELD_OFFSET : BITMAP_OFFSET + len;
            int sizeOff2 = !useLeftHalf(nextSnapshotTag) ? SIZE_FIELD_OFFSET : BITMAP_OFFSET + len;

            if (last - lastSuccessfulSnapshotTag == 1) { //we should keep only data in last half
                //new data will be written in the same half, we should move old data to another half
                if ((nextSnapshotTag - last) % 2 == 0)
                    PageHandler.copyMemory(buf, sizeOff, buf, sizeOff2, len + SIZE_FIELD_SIZE);
            } else { //last - lastSuccessfulSnapshotId > 1, e.g. we should merge two half in one
                int newSize = 0;
                int i = 0;

                for (; i < len - 8; i += 8) {
                    long newVal = buf.getLong(sizeOff + SIZE_FIELD_SIZE + i) | buf.getLong(sizeOff2 + SIZE_FIELD_SIZE + i);

                    newSize += Long.bitCount(newVal);

                    buf.putLong(sizeOff2 + SIZE_FIELD_SIZE + i, newVal);
                }

                for (; i < len; i++) {
                    byte newVal = (byte) (buf.get(sizeOff + SIZE_FIELD_SIZE + i) | buf.get(sizeOff2 + SIZE_FIELD_SIZE + i));

                    newSize += Integer.bitCount(newVal & 0xFF);

                    buf.put(sizeOff2 + SIZE_FIELD_SIZE + i, newVal);
                }

                buf.putShort(sizeOff2, (short)newSize);
            }

            setLastSnasphotTag(buf, nextSnapshotTag);

            PageHandler.zeroMemory(buf, sizeOff, len + SIZE_FIELD_SIZE);
        }

        return -1;
    }

    /**
     * @param buf Buffer.
     * @param nextSnapshotTag Next snapshot tag.
     */
    private void setLastSnasphotTag(ByteBuffer buf, long nextSnapshotTag) {
        if (isCorrupted(buf))
            nextSnapshotTag = nextSnapshotTag | CORRUPT_FLAG_MASK;

        buf.putLong(LAST_SNAPSHOT_TAG_OFFSET, nextSnapshotTag);
    }

    /**
     * @param buf Buffer.
     * @param nextSnapshotTag Next snapshot tag.
     */
    private void setLastSnasphotTag0(ByteBuffer buf, long nextSnapshotTag) {
        buf.putLong(LAST_SNAPSHOT_TAG_OFFSET, nextSnapshotTag);
    }

    /**
     * @param addr address.
     * @param nextSnapshotTag Next snapshot tag.
     */
    private void setLastSnasphotTag0(long addr, long nextSnapshotTag) {
        GridUnsafe.putLong(addr + LAST_SNAPSHOT_TAG_OFFSET, nextSnapshotTag);
    }

    /**
     * @param buf Buffer.
     * @return Saved max seen snapshot tag.
     */
    private long getLastSnapshotTag(ByteBuffer buf) {
        return getLastSnapshotTag0(buf) & CORRUPT_FLAG_FILTER_MASK;
    }

    /**
     * @param buf Buffer.
     * @return Saved value in {@link TrackingPageIO#LAST_SNAPSHOT_TAG_OFFSET}.
     */
    private long getLastSnapshotTag0(ByteBuffer buf) {
        return buf.getLong(LAST_SNAPSHOT_TAG_OFFSET);
    }

    /**
     * @param addr Address of buffer start.
     * @return Saved max seen snapshot tag.
     */
    private long getLastSnapshotTag(long addr) {
        return getLastSnapshotTag0(addr) & CORRUPT_FLAG_FILTER_MASK;
    }

    /**
     * @param addr Address of buffer start.
     * @return Saved value in {@link TrackingPageIO#LAST_SNAPSHOT_TAG_OFFSET}.
     */
    private long getLastSnapshotTag0(long addr) {
        return GridUnsafe.getLong(addr + LAST_SNAPSHOT_TAG_OFFSET);
    }

    /**
     * @param buf Buffer.
     * @return Was tracking page marked as corrupted or not.
     */
    public boolean isCorrupted(ByteBuffer buf) {
        return getLastSnapshotTag0(buf) < 0; //if true it means that first bit set to 1
    }

    /**
     * Reset corrupted flag to false.
     *
     * @param buf Buffer.
     */
    public void resetCorruptFlag(ByteBuffer buf) {
        setLastSnasphotTag0(buf, getLastSnapshotTag(buf));
    }

    /**
     * Reset corrupted flag to false.
     *
     * @param addr Buffer.
     */
    public void resetCorruptFlag(long addr) {
        setLastSnasphotTag0(addr, getLastSnapshotTag(addr));
    }

    /**
     *
     * @param buf Buffer.
     * @param pageId Page id.
     * @param curSnapshotTag Snapshot tag.
     * @param lastSuccessfulSnapshotTag Last successful snapshot id.
     * @param pageSize Page size.
     *
     * @return Was that pageId marked as changed between previous snapshot finish and current snapshot start or not.
     * @throws TrackingPageIsCorruptedException if this tracking page was marked as corrupted.
     */
    public boolean wasChanged(ByteBuffer buf, long pageId, long curSnapshotTag, long lastSuccessfulSnapshotTag, int pageSize)
        throws TrackingPageIsCorruptedException {
        if (isCorrupted(buf))
            throw TrackingPageIsCorruptedException.INSTANCE;

        long lastTag = validateSnapshotTag(buf, curSnapshotTag + 1, lastSuccessfulSnapshotTag, pageSize);

        if (lastTag >= 0)
            throw new TrackingPageIsCorruptedException(lastTag, curSnapshotTag);

        if (countOfChangedPage(buf, curSnapshotTag, pageSize) < 1)
            return false;

        int cntOfPage = countOfPageToTrack(pageSize);

        int idxToTest = (PageIdUtils.pageIndex(pageId) - COUNT_OF_EXTRA_PAGE) % cntOfPage;

        byte byteToTest;

        if (useLeftHalf(curSnapshotTag))
            byteToTest = buf.get(BITMAP_OFFSET + (idxToTest >> 3));
        else
            byteToTest = buf.get(BITMAP_OFFSET + SIZE_FIELD_SIZE + ((idxToTest + cntOfPage) >> 3));

        int testTemplate = 1 << (idxToTest & 0b111);

        return ((byteToTest & testTemplate) ^ testTemplate) == 0;
    }

    /**
     * @param buf Buffer.
     * @param snapshotTag Snapshot tag.
     * @param pageSize Page size.
     *
     * @return Count of pages which were marked as change for given snapshotTag.
     */
    public short countOfChangedPage(ByteBuffer buf, long snapshotTag, int pageSize) {
        long dif = getLastSnapshotTag(buf) - snapshotTag;

        if (dif != 0 && dif != 1)
            return -1;

        if (useLeftHalf(snapshotTag))
            return buf.getShort(SIZE_FIELD_OFFSET);
        else
            return buf.getShort(BITMAP_OFFSET + (countOfPageToTrack(pageSize) >> 3));
    }

    /**
     * @param snapshotTag Snapshot id.
     *
     * @return true if snapshotTag is odd, otherwise - false.
     */
    private boolean useLeftHalf(long snapshotTag) {
        return (snapshotTag & 0b1) == 0;
    }

    /**
     * @param pageId Page id.
     * @param pageSize Page size.
     * @return Page id of tracking page which set pageId belongs to.
     */
    public long trackingPageFor(long pageId, int pageSize) {
        assert PageIdUtils.pageIndex(pageId) > 0;

        int pageIdx = ((PageIdUtils.pageIndex(pageId) - COUNT_OF_EXTRA_PAGE) /
            countOfPageToTrack(pageSize)) * countOfPageToTrack(pageSize) + COUNT_OF_EXTRA_PAGE;

        long trackingPageId = PageIdUtils.pageId(PageIdUtils.partId(pageId), PageIdUtils.flag(pageId), pageIdx);

        assert PageIdUtils.pageIndex(trackingPageId) <= PageIdUtils.pageIndex(pageId);

        return trackingPageId;
    }

    /**
     * @param pageSize Page size.
     *
     * @return How many page we can track with 1 page.
     */
    public int countOfPageToTrack(int pageSize) {
        return ((pageSize - SIZE_FIELD_OFFSET) / 2 - SIZE_FIELD_SIZE) << 3;
    }

    /**
     * @param buf Buffer.
     * @param start Start.
     * @param curSnapshotTag Snapshot id.
     * @param lastSuccessfulSnapshotTag  Last successful snapshot id.
     * @param pageSize Page size.
     * @return Passed pageId if it was changed or next closest one, if there is no changed page {@code null} will be returned.
     * @throws TrackingPageIsCorruptedException if this tracking page was marked as corrupted.
     */
    @Nullable public Long findNextChangedPage(ByteBuffer buf, long start, long curSnapshotTag,
        long lastSuccessfulSnapshotTag, int pageSize) throws TrackingPageIsCorruptedException {
        if (isCorrupted(buf))
            throw TrackingPageIsCorruptedException.INSTANCE;

        long lastTag = validateSnapshotTag(buf, curSnapshotTag + 1, lastSuccessfulSnapshotTag, pageSize);

        if (lastTag >= 0)
            throw new TrackingPageIsCorruptedException(lastTag, curSnapshotTag);

        int cntOfPage = countOfPageToTrack(pageSize);

        long trackingPage = trackingPageFor(start, pageSize);

        if (start == trackingPage)
            return trackingPage;

        if (countOfChangedPage(buf, curSnapshotTag, pageSize) <= 0)
            return null;

        int idxToStartTest = (PageIdUtils.pageIndex(start) - COUNT_OF_EXTRA_PAGE) % cntOfPage;

        int zeroIdx = useLeftHalf(curSnapshotTag) ? BITMAP_OFFSET : BITMAP_OFFSET + SIZE_FIELD_SIZE + (cntOfPage >> 3);

        int startIdx = zeroIdx + (idxToStartTest >> 3);

        int idx = startIdx;

        int stopIdx = zeroIdx + (cntOfPage >> 3);

        while (idx < stopIdx) {
            byte byteToTest = buf.get(idx);

            if (byteToTest != 0) {
                int foundSetBit;
                if ((foundSetBit = foundSetBit(byteToTest, idx == startIdx ? (idxToStartTest & 0b111) : 0)) != -1) {
                    long foundPageId = PageIdUtils.pageId(
                        PageIdUtils.partId(start),
                        PageIdUtils.flag(start),
                        PageIdUtils.pageIndex(trackingPage) + ((idx - zeroIdx) << 3) + foundSetBit);

                    assert wasChanged(buf, foundPageId, curSnapshotTag, lastSuccessfulSnapshotTag, pageSize);
                    assert trackingPageFor(foundPageId, pageSize) == trackingPage;

                    return foundPageId;
                }
            }

            idx++;
        }

        return null;
    }

    /**
     * @param byteToTest Byte to test.
     * @param firstBitToTest First bit to test.
     *
     * @return Index of bit which is set to 1, if there is no such index then <code>-1</code>.
     */
    private static int foundSetBit(byte byteToTest, int firstBitToTest) {
        assert firstBitToTest < 8;

        for (int i = firstBitToTest; i < 8; i++) {
            int testTemplate = 1 << i;

            if (((byteToTest & testTemplate) ^ testTemplate) == 0)
                return i;
        }

        return -1;
    }

    /** {@inheritDoc} */
    @Override protected void printPage(long addr, int pageSize, GridStringBuilder sb) throws IgniteCheckedException {
        sb.a("TrackingPage [\n\tlastSnapshotTag=").a(getLastSnapshotTag(addr))
            .a(",\n\tleftHalf={")
            .a("\n\t\tsize=").a(GridUnsafe.getShort(addr + SIZE_FIELD_OFFSET))
            .a("\n\t\tdata={");

        for (int i = 0; i < (countOfPageToTrack(pageSize) >> 3); i += 2)
            sb.appendHex(GridUnsafe.getShort(addr + BITMAP_OFFSET + i));

        sb.a("}\n\t},\n\trightHalf={")
            .a("\n\t\tsize=").a(GridUnsafe.getShort(addr + BITMAP_OFFSET + (countOfPageToTrack(pageSize) >> 3)))
            .a("\n\t\tdata={");

        for (int i = 0; i < (countOfPageToTrack(pageSize) >> 3); i += 2)
            sb.appendHex(GridUnsafe.getShort(addr + BITMAP_OFFSET + (countOfPageToTrack(pageSize) >> 3)
                 + SIZE_FIELD_SIZE + i));

        sb.a("}\n\t}\n]");
    }
}

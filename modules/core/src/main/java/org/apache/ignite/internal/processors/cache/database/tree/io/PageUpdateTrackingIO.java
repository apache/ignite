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
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.processors.cache.database.tree.util.PageHandler;

/**
 *
 */
public class PageUpdateTrackingIO extends PageIO {
    /** */
    public static final IOVersions<PageUpdateTrackingIO> VERSIONS = new IOVersions<>(
        new PageUpdateTrackingIO(1)
    );

    /** Last backup offset. */
    public static final int LAST_BACKUP_OFFSET = COMMON_HEADER_END;

    /** Size field offset. */
    public static final int SIZE_FIELD_OFFSET = LAST_BACKUP_OFFSET + 8;

    /** 'Size' field size. */
    public static final int SIZE_FIELD_SIZE = 2;

    /** Bitmap offset. */
    public static final int BITMAP_OFFSET = SIZE_FIELD_OFFSET + SIZE_FIELD_SIZE;
    public static final int COUNT_OF_EXTRA_PAGE = 1;

    /**
     * @param ver Page format version.
     */
    protected PageUpdateTrackingIO(int ver) {
        super(PageIO.T_PAGE_UPDATE_TRACKING, ver);
    }

    /**
     * Will mark pageId as changed for backupId.
     *
     * @param buf Buffer.
     * @param pageId Page id.
     * @param backupId Backup id.
     * @param pageSize Page size.
     */
    public boolean markChanged(ByteBuffer buf, long pageId, long backupId, int pageSize) {
        int cntOfPage = countOfPageToTrack(pageSize);

        int idxToUpdate = (PageIdUtils.pageIndex(pageId) - COUNT_OF_EXTRA_PAGE) % cntOfPage;

        long last = buf.getLong(LAST_BACKUP_OFFSET);

        int sizeOff = useLeftHalf(backupId) ? SIZE_FIELD_OFFSET : BITMAP_OFFSET + (cntOfPage >> 3);

        if (backupId == last) { //the same backup = keep going writing to the same part
            short newSize = (short)(countOfChangedPage(buf, backupId, pageSize) + 1);

            buf.putShort(sizeOff, newSize);

            assert newSize == countOfChangedPage(buf, backupId, pageSize);
        } else {
            buf.putLong(LAST_BACKUP_OFFSET, backupId);

            if (backupId - last > 1) //wipe all data in buffer
                PageHandler.zeroMemory(buf, SIZE_FIELD_OFFSET, buf.capacity() - SIZE_FIELD_OFFSET);
            else
                PageHandler.zeroMemory(buf, sizeOff + SIZE_FIELD_SIZE, (cntOfPage >> 3));

            buf.putShort(sizeOff, (short)1);
        }

        int idx = sizeOff + SIZE_FIELD_SIZE + (idxToUpdate >> 3);

        byte byteToUpdate = buf.get(idx);

        int updateTemplate = 1 << (idxToUpdate & 0b111);

        byte newVal =  (byte) (byteToUpdate | updateTemplate);

        buf.put(idx, newVal);

        return newVal != byteToUpdate;
    }

    /**
     * Will mark pageId as changed for backupId.
     *
     * @param buf Buffer.
     * @param pageId Page id.
     * @param curBackupId Backup id.
     * @param pageSize Page size.
     */
    public boolean markChanged(ByteBuffer buf, long pageId, long curBackupId, long lastSuccessfulBackupId, int pageSize) {
        validateBackupId(buf, curBackupId, lastSuccessfulBackupId, pageSize);

        int cntOfPage = countOfPageToTrack(pageSize);

        int idxToUpdate = (PageIdUtils.pageIndex(pageId) - COUNT_OF_EXTRA_PAGE) % cntOfPage;

        int sizeOff = useLeftHalf(curBackupId) ? SIZE_FIELD_OFFSET : BITMAP_OFFSET + (cntOfPage >> 3);

        short newSize = (short)(countOfChangedPage(buf, curBackupId, pageSize) + 1);

        buf.putShort(sizeOff, newSize);

        assert newSize == countOfChangedPage(buf, curBackupId, pageSize);

        int idx = sizeOff + SIZE_FIELD_SIZE + (idxToUpdate >> 3);

        byte byteToUpdate = buf.get(idx);

        int updateTemplate = 1 << (idxToUpdate & 0b111);

        byte newVal =  (byte) (byteToUpdate | updateTemplate);

        buf.put(idx, newVal);

        return newVal != byteToUpdate;
    }

    private void validateBackupId(ByteBuffer buf, long nextBackupId, long lastSuccessfulBackupId, int pageSize) {
        assert nextBackupId != lastSuccessfulBackupId;

        long last = buf.getLong(LAST_BACKUP_OFFSET);

        if (nextBackupId == last) //everything is ok
            return;

        int cntOfPage = countOfPageToTrack(pageSize);

        if (last <= lastSuccessfulBackupId) { //we can drop our data
            buf.putLong(LAST_BACKUP_OFFSET, nextBackupId);

            PageHandler.zeroMemory(buf, SIZE_FIELD_OFFSET, buf.capacity() - SIZE_FIELD_OFFSET);
        } else { //we can't drop data, it is still necessary for incremental backups
            int length = cntOfPage >> 3;

            int sizeOff = useLeftHalf(nextBackupId) ? SIZE_FIELD_OFFSET : BITMAP_OFFSET + length;
            int sizeOff2 = !useLeftHalf(nextBackupId) ? SIZE_FIELD_OFFSET : BITMAP_OFFSET + length;

            if (last - lastSuccessfulBackupId == 1) { //we should keep only data in last half
                //new data will be written in the same half, we should move old data to another half
                if ((nextBackupId - last) % 2 == 0)
                    PageHandler.copyMemory(buf, buf, sizeOff, sizeOff2, length + SIZE_FIELD_SIZE);
            } else { //last - lastSuccessfulBackupId > 1, e.g. we should merge two half in one
                int newSize = 0;
                for (int i = 0; i < length; i++) {
                    byte newVal = (byte) (buf.get(sizeOff + SIZE_FIELD_SIZE + i) | buf.get(sizeOff2 + SIZE_FIELD_SIZE + 1));

                    newSize += Integer.bitCount(newVal);

                    buf.put(sizeOff2 + SIZE_FIELD_SIZE + i, (byte)newVal);
                }

                buf.putShort(sizeOff2, (short)newSize);
            }

            buf.putLong(LAST_BACKUP_OFFSET, nextBackupId);

            PageHandler.zeroMemory(buf, sizeOff, length + SIZE_FIELD_SIZE);
        }
    }

    /**
     * Check that pageId was marked as changed for backup with set id.
     *
     * @param buf Buffer.
     * @param pageId Page id.
     * @param backupId Backup id.
     * @param pageSize Page size.
     */
    public boolean wasChanged(ByteBuffer buf, long pageId, long backupId, int pageSize) {
        if (countOfChangedPage(buf, backupId, pageSize) < 1)
            return false;

        int cntOfPage = countOfPageToTrack(pageSize);

        int idxToTest = (PageIdUtils.pageIndex(pageId) - COUNT_OF_EXTRA_PAGE) % cntOfPage;

        byte byteToTest;

        if (useLeftHalf(backupId))
            byteToTest = buf.get(BITMAP_OFFSET + (idxToTest >> 3));
        else
            byteToTest = buf.get(BITMAP_OFFSET + SIZE_FIELD_SIZE + ((idxToTest + cntOfPage) >> 3));

        int testTemplate = 1 << (idxToTest & 0b111);

        return ((byteToTest & testTemplate) ^ testTemplate) == 0;
    }

    /**
     * @param buf Buffer.
     * @param backupId Backup id.
     * @param pageSize Page size.
     *
     * @return count of pages which were marked as change for given backupId
     */
    public short countOfChangedPage(ByteBuffer buf, long backupId, int pageSize) {
        long dif = buf.getLong(LAST_BACKUP_OFFSET) - backupId;

        if (dif != 0 && dif != 1)
            return -1;

        if (useLeftHalf(backupId))
            return buf.getShort(SIZE_FIELD_OFFSET);
        else
            return buf.getShort(BITMAP_OFFSET + (countOfPageToTrack(pageSize) >> 3));
    }

    /**
     * @param backupId Backup id.
     *
     * @return true if backupId is odd, otherwise - false
     */
    boolean useLeftHalf(long backupId) {
        return (backupId & 0b1) == 0;
    }

    /**
     * @param pageId Page id.
     * @param pageSize Page size.
     * @return pageId of tracking page which set pageId belongs to
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
     * @return how many page we can track with 1 page
     */
    public int countOfPageToTrack(int pageSize) {
        return ((pageSize - SIZE_FIELD_OFFSET) / 2 - SIZE_FIELD_SIZE)  << 3;
    }

    /**
     * @param buf Buffer.
     * @param start Start.
     * @param backupId Backup id.
     * @param pageSize Page size.
     * @return set pageId if it was changed or next closest one, if there is no changed page null will be returned
     */
    public Long findNextChangedPage(ByteBuffer buf, long start, int backupId, int pageSize) {
        int cntOfPage = countOfPageToTrack(pageSize);

        long trackingPage = trackingPageFor(start, pageSize);

        if (start == trackingPage)
            return trackingPage;

        if (countOfChangedPage(buf, backupId, pageSize) <= 0)
            return null;

        int idxToStartTest = (PageIdUtils.pageIndex(start) - COUNT_OF_EXTRA_PAGE) % cntOfPage;

        int zeroIdx = useLeftHalf(backupId)? BITMAP_OFFSET : BITMAP_OFFSET + SIZE_FIELD_SIZE + (cntOfPage >> 3);

        int startIdx = zeroIdx + (idxToStartTest >> 3);

        int idx = startIdx;

        int stopIdx = zeroIdx + (cntOfPage >> 3);

        while (idx < stopIdx) {
            byte byteToTest = buf.get(idx);

            int foundSetBit;
            if ((foundSetBit = foundSetBit(byteToTest, idx == startIdx ? (idxToStartTest & 0b111) : 0)) != -1) {
                long foundPageId = PageIdUtils.pageId(
                    PageIdUtils.partId(start),
                    PageIdUtils.flag(start),
                    PageIdUtils.pageIndex(trackingPage) + ((idx - zeroIdx) << 3) + foundSetBit);

                assert wasChanged(buf, foundPageId, backupId, pageSize);
                assert trackingPageFor(foundPageId, pageSize) == trackingPage;

                return foundPageId;
            }

            idx++;
        }

        return null;
    }

    /**
     * @param byteToTest Byte to test.
     * @param firstBitToTest First bit to test.
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
}

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
    public static final int SIZE_FIELD_OFFSET = LAST_BACKUP_OFFSET + 1;

    /** 'Size' field size. */
    public static final int SIZE_FIELD_SIZE = 2;

    /** Bitmap offset. */
    public static final int BITMAP_OFFSET = SIZE_FIELD_OFFSET + SIZE_FIELD_SIZE;

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
    public static void markChanged(ByteBuffer buf, long pageId, long backupId, int pageSize) {
        int cntOfPage = countOfPageToTrack(pageSize);

        int idxToUpdate = PageIdUtils.pageIndex(pageId) % cntOfPage;

        byte last = buf.get(LAST_BACKUP_OFFSET);

        int sizeOff = useLeftHalf(backupId) ? SIZE_FIELD_OFFSET : BITMAP_OFFSET + (cntOfPage >> 3);

        if ((backupId & 0xF) == last) { //the same backup = keep going writing to the same part
            int newSize = countOfChangedPage(buf, backupId, pageSize) + 1;

            buf.putShort(sizeOff, (short) newSize);

            assert newSize == countOfChangedPage(buf, backupId, pageSize);
        } else {
            buf.put(LAST_BACKUP_OFFSET, (byte)(backupId & 0xF));

            buf.putShort(sizeOff, (short)1);

            PageHandler.zeroMemory(buf, sizeOff + SIZE_FIELD_SIZE, (cntOfPage >> 3));
        }

        int idx = sizeOff + SIZE_FIELD_SIZE + (idxToUpdate >> 3);

        byte byteToUpdate = buf.get(idx);

        int updateTemplate = 1 << (idxToUpdate & 0b111);

        byteToUpdate |= updateTemplate;

        buf.put(idx, byteToUpdate);
    }

    /**
     * Check that pageId was marked as changed for backup with set id.
     *
     * @param buf Buffer.
     * @param pageId Page id.
     * @param backupId Backup id.
     * @param pageSize Page size.
     */
    public static boolean wasChanged(ByteBuffer buf, long pageId, long backupId, int pageSize) {
        int cntOfPage = countOfPageToTrack(pageSize);

        int idxToTest = PageIdUtils.pageIndex(pageId) % cntOfPage;

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
    public static int countOfChangedPage(ByteBuffer buf, long backupId, int pageSize) {
        //check that last byte of backup id is the same as last backup id or less than 1
        assert ((buf.get(LAST_BACKUP_OFFSET) - (backupId & 0xF)) & 0b11111111111111111111111111111110) == 0;

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
    static boolean useLeftHalf(long backupId) {
        return (backupId & 0b1) == 0;
    }

    /**
     * @param pageSize Page size.
     *
     * @return how many page we can track with 1 page
     */
    public static int countOfPageToTrack(int pageSize) {
        return ((pageSize- SIZE_FIELD_OFFSET - 1) / 2 - SIZE_FIELD_SIZE)  << 3;
    }
}

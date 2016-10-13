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
import org.apache.ignite.internal.util.GridUnsafe;

public class PageUpdateTrackingIO extends PageIO {
    public static final int LAST_BACKUP_OFFSET = COMMON_HEADER_END;

    public static final int SIZE_FIELD_OFFSET = LAST_BACKUP_OFFSET + 1;

    public static final int SIZE_FIELD_SIZE = 2;

    public static final int BITMAP_OFFSET = SIZE_FIELD_OFFSET + SIZE_FIELD_SIZE;

    /** */
    public static final IOVersions<PageUpdateTrackingIO> VERSIONS = new IOVersions<>(
        new PageUpdateTrackingIO(1)
    );

    /**
     * @param ver Page format version.
     */
    protected PageUpdateTrackingIO(int ver) {
        super(PageIO.T_PAGE_UPDATE_TRACKING, ver);
    }

    public static void markChanged(ByteBuffer buf, long pageId, long backupId, int pageSize) {
        int countOfPage = countOfPageToTrack(pageSize);

        int idxToUpdate = PageIdUtils.pageIndex(pageId) % countOfPage;

        byte last = buf.get(LAST_BACKUP_OFFSET);

        int sizeOffset = useLeftHalf(backupId) ? SIZE_FIELD_OFFSET : BITMAP_OFFSET + (countOfPage >> 3);

        if ((backupId & 0xF) == last) { //the same backup = keep going writing to the same part
            int newSize = countOfChangedPage(buf, backupId, pageSize) + 1;

            buf.putShort(sizeOffset, (short) newSize);
        } else {
            buf.put((byte)(backupId & 0xF));

            buf.putShort(sizeOffset, (short)0);
            //TODO zero other part of buffer
        }

        int index = sizeOffset + SIZE_FIELD_SIZE + (idxToUpdate >> 3);

        byte byteToUpdate = buf.get(index);


        int updateTemplate = 1 << (idxToUpdate & 0b111);

        byteToUpdate |= updateTemplate;

        buf.put(index, byteToUpdate);
    }

    public static boolean wasChanged(ByteBuffer buf, long pageId, long backupId, int pageSize) {
        int countOfPage = countOfPageToTrack(pageSize);

        int idxToTest = PageIdUtils.pageIndex(pageId) % countOfPage;

        byte byteToTest;

        if (useLeftHalf(backupId))
            byteToTest = buf.get(BITMAP_OFFSET + (idxToTest >> 3));
        else
            byteToTest = buf.get(BITMAP_OFFSET + SIZE_FIELD_SIZE + ((idxToTest + countOfPage) >> 3));

        int testTemplate = 1 << (idxToTest & 0b111);

        return ((byteToTest & testTemplate) ^ testTemplate) == 0;
    }

    public static int countOfChangedPage(ByteBuffer buf, long backupId, int pageSize) {
        if (useLeftHalf(backupId))
            return buf.getShort(SIZE_FIELD_OFFSET);
        else
            return buf.getShort(BITMAP_OFFSET + countOfPageToTrack(pageSize));
    }

    static boolean useLeftHalf(long backupId) {
        return (backupId & 0b1) == 0;
    }

    public static int countOfPageToTrack(int pageSize) {
        return ((pageSize- SIZE_FIELD_OFFSET - 1) / 2 - SIZE_FIELD_SIZE)  << 3;
    }
}

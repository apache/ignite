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

package org.apache.ignite.internal.pagemem.impl;

import java.util.Random;

import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 *
 */
public class PageIdUtilsSelfTest extends GridCommonAbstractTest {
    /**
     * @throws Exception If failed.
     */
    public void testPageIdConstruction() throws Exception {
        assertEquals(0x00000001L, PageIdUtils.pageId(0, 1));
        assertEquals(0x1000000001L, PageIdUtils.pageId(1, 1));
        assertEquals(0x3FFFFFFFL, PageIdUtils.pageId(0, 0x3FFFFFFF));
        assertEquals(0x103FFFFFFFL, PageIdUtils.pageId(1, 0x3FFFFFFF));

        assertEquals(0xFFFF000000001L, PageIdUtils.pageId(0xFFFF, 1));
        assertEquals(0xFFFF03FFFFFFFL, PageIdUtils.pageId(0xFFFF, 0x3FFFFFFF));
    }

    /**
     * @throws Exception If failed.
     */
    public void testLinkConstruction() throws Exception {
        assertEquals(0x000FFFFFFFFFFFFFL, PageIdUtils.link(0xFFFFFFFFFFFFFL, 0));
        assertEquals(0x001FFFFFFFFFFFFFL, PageIdUtils.link(0xFFFFFFFFFFFFFL, 1));

        assertEquals(0x0000000000000000L, PageIdUtils.link(0, 0));
        assertEquals(0x0010000000000000L, PageIdUtils.link(0, 1));

        assertEquals(0x8000000000000000L, PageIdUtils.link(0, 2048));
        assertEquals(0x800FFFFFFFFFFFFFL, PageIdUtils.link(0xFFFFFFFFFFFFFL, 2048));

        assertEquals(0x8010000000000000L, PageIdUtils.link(0, 2049));
        assertEquals(0x801FFFFFFFFFFFFFL, PageIdUtils.link(0xFFFFFFFFFFFFFL, 2049));

        assertEquals(0xFFF0000000000000L, PageIdUtils.link(0, 4095));
        assertEquals(0xFFFFFFFFFFFFFFFFL, PageIdUtils.link(0xFFFFFFFFFFFFFL, 4095));
    }

    /**
     * @throws Exception If failed.
     */
    public void testFileIdExtraction() throws Exception {
        assertEquals(0, PageIdUtils.fileId(0x00000001L));
        assertEquals(1, PageIdUtils.fileId(0x1000000001L));
        assertEquals(0, PageIdUtils.fileId(0x3FFFFFFFL));
        assertEquals(1, PageIdUtils.fileId(0x10FFFFFFFFL));

        assertEquals(0xFFFF, PageIdUtils.fileId(0xFFFF000000001L));
        assertEquals(0xFFFF, PageIdUtils.fileId(0xFFFF0FFFFFFFFL));

        assertEquals(0, PageIdUtils.fileId(0xFFF00000_00000001L));
        assertEquals(1, PageIdUtils.fileId(0xFFF00010_00000001L));
        assertEquals(0, PageIdUtils.fileId(0xFFF00000_FFFFFFFFL));
        assertEquals(1, PageIdUtils.fileId(0xFFF00010_FFFFFFFFL));

        assertEquals(0xFFFF, PageIdUtils.fileId(0xFFFFFFF0_00000001L));
        assertEquals(0xFFFF, PageIdUtils.fileId(0xFFFFFFF0_FFFFFFFFL));
    }

    /**
     * @throws Exception If failed.
     */
    public void testOffsetExtraction() throws Exception {
        assertEquals(0, PageIdUtils.bytesOffset(0x000FFFFFFFFFFFFFL));
        assertEquals(8, PageIdUtils.bytesOffset(0x001FFFFFFFFFFFFFL));

        assertEquals(0, PageIdUtils.bytesOffset(0x0000000000000000L));
        assertEquals(8, PageIdUtils.bytesOffset(0x0010000000000000L));

        assertEquals(16384, PageIdUtils.bytesOffset(0x8000000000000000L));
        assertEquals(16384, PageIdUtils.bytesOffset(0x800FFFFFFFFFFFFFL));

        assertEquals(16392, PageIdUtils.bytesOffset(0x8010000000000000L));
        assertEquals(16392, PageIdUtils.bytesOffset(0x801FFFFFFFFFFFFFL));

        assertEquals(32760, PageIdUtils.bytesOffset(0xFFF0000000000000L));
        assertEquals(32760, PageIdUtils.bytesOffset(0xFFFFFFFFFFFFFFFFL));

        assertEquals(0, PageIdUtils.itemId(0x000FFFFFFFFFFFFFL));
        assertEquals(1, PageIdUtils.itemId(0x001FFFFFFFFFFFFFL));

        assertEquals(0, PageIdUtils.itemId(0x0000000000000000L));
        assertEquals(1, PageIdUtils.itemId(0x0010000000000000L));

        assertEquals(2048, PageIdUtils.itemId(0x8000000000000000L));
        assertEquals(2048, PageIdUtils.itemId(0x800FFFFFFFFFFFFFL));

        assertEquals(2049, PageIdUtils.itemId(0x8010000000000000L));
        assertEquals(2049, PageIdUtils.itemId(0x801FFFFFFFFFFFFFL));

        assertEquals(4095, PageIdUtils.itemId(0xFFF0000000000000L));
        assertEquals(4095, PageIdUtils.itemId(0xFFFFFFFFFFFFFFFFL));
    }

    /**
     * @throws Exception If failed.
     */
    public void testPageIdFromLink() throws Exception {
        assertEquals(0x000FFFFFFFFFFFFFL, PageIdUtils.pageId(0x000FFFFFFFFFFFFFL));
        assertEquals(0x000FFFFFFFFFFFFFL, PageIdUtils.pageId(0x100FFFFFFFFFFFFFL));
        assertEquals(0x000FFFFFFFFFFFFFL, PageIdUtils.pageId(0x010FFFFFFFFFFFFFL));
        assertEquals(0x000FFFFFFFFFFFFFL, PageIdUtils.pageId(0x001FFFFFFFFFFFFFL));
        assertEquals(0x000FFFFFFFFFFFFFL, PageIdUtils.pageId(0x800FFFFFFFFFFFFFL));
        assertEquals(0x000FFFFFFFFFFFFFL, PageIdUtils.pageId(0x080FFFFFFFFFFFFFL));
        assertEquals(0x000FFFFFFFFFFFFFL, PageIdUtils.pageId(0x008FFFFFFFFFFFFFL));
        assertEquals(0x000FFFFFFFFFFFFFL, PageIdUtils.pageId(0xFFFFFFFFFFFFFFFFL));

        assertEquals(0L, PageIdUtils.pageId(0x0000000000000000L));
        assertEquals(0L, PageIdUtils.pageId(0x1000000000000000L));
        assertEquals(0L, PageIdUtils.pageId(0x0100000000000000L));
        assertEquals(0L, PageIdUtils.pageId(0x0010000000000000L));
        assertEquals(0L, PageIdUtils.pageId(0x8000000000000000L));
        assertEquals(0L, PageIdUtils.pageId(0x0800000000000000L));
        assertEquals(0L, PageIdUtils.pageId(0x0080000000000000L));
        assertEquals(0L, PageIdUtils.pageId(0xFFF0000000000000L));
    }

    /**
     * @throws Exception if failed.
     */
    public void testFullPageId() throws Exception {
        Random rnd = new Random();

        // Check that link is not included in FullPageId for data pages.
        for (int i = 0; i < 50_000; i++) {
            int offset = rnd.nextInt(PageIdUtils.MAX_OFFSET_DWORDS + 1);
            int partId = rnd.nextInt(PageIdUtils.MAX_PART_ID + 1);
            int pageNum = rnd.nextInt();

            long pageId = PageIdUtils.pageId(partId, PageMemory.FLAG_DATA, pageNum);

            long link1 = PageIdUtils.link(pageId, offset);
            long link2 = PageIdUtils.link(pageId, 0);

            assertEquals(new FullPageId(link1, 1), new FullPageId(link2, 1));
        }

        // Check that partition ID is not included in FullPageId for data pages.
        for (int i = 0; i < 50_000; i++) {
            int offset = rnd.nextInt(PageIdUtils.MAX_OFFSET_DWORDS + 1);
            int partId = rnd.nextInt(PageIdUtils.MAX_PART_ID + 1);
            int pageNum = rnd.nextInt();

            long pageId1 = PageIdUtils.pageId(partId, PageMemory.FLAG_IDX, pageNum);
            long pageId2 = PageIdUtils.pageId(1, PageMemory.FLAG_IDX, pageNum);

            long link1 = PageIdUtils.link(pageId1, offset);
            long link2 = PageIdUtils.link(pageId2, 0);

            assertEquals(new FullPageId(link1, 1), new FullPageId(link2, 1));
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testRandomIds() throws Exception {
        Random rnd = new Random();

        for (int i = 0; i < 50_000; i++) {
            int offset = rnd.nextInt(PageIdUtils.MAX_OFFSET_DWORDS + 1);
            int fileId = rnd.nextInt(PageIdUtils.MAX_FILE_ID + 1);
            int pageNum = rnd.nextInt();

            long pageId = PageIdUtils.pageId(fileId, pageNum);

            String msg = "For values [offset=" + U.hexLong(offset) + ", fileId=" + U.hexLong(fileId) +
                ", pageNum=" + U.hexLong(pageNum) + ']';

            assertEquals(msg, pageId, PageIdUtils.pageId(pageId));
            assertEquals(msg, fileId, PageIdUtils.fileId(pageId));
            assertEquals(msg, 0, PageIdUtils.bytesOffset(pageId));
            assertEquals(msg, 0, PageIdUtils.itemId(pageId));

            long link = PageIdUtils.link(pageId, offset);

            assertEquals(msg, pageId, PageIdUtils.pageId(link));
            assertEquals(msg, offset, PageIdUtils.itemId(link));
            assertEquals(msg, offset * 8, PageIdUtils.bytesOffset(link));
            assertEquals(msg, pageId, PageIdUtils.pageId(link));
            assertEquals(msg, fileId, PageIdUtils.fileId(link));
        }
    }
}

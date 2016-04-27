/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
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
        assertEquals(0x40000001L, PageIdUtils.pageId(1, 1));
        assertEquals(0x3FFFFFFFL, PageIdUtils.pageId(0, 0x3FFFFFFF));
        assertEquals(0x7FFFFFFFL, PageIdUtils.pageId(1, 0x3FFFFFFF));

        assertEquals(0xFFFFFC0000001L, PageIdUtils.pageId(0x3FFFFF, 1));
        assertEquals(0xFFFFFFFFFFFFFL, PageIdUtils.pageId(0x3FFFFF, 0x3FFFFFFF));
    }

    /**
     * @throws Exception If failed.
     */
    public void testLinkConstruction() throws Exception {
        assertEquals(0x000FFFFFFFFFFFFFL, PageIdUtils.linkFromBytesOffset(0xFFFFFFFFFFFFFL, 0));
        assertEquals(0x001FFFFFFFFFFFFFL, PageIdUtils.linkFromBytesOffset(0xFFFFFFFFFFFFFL, 8));

        assertEquals(0x0000000000000000L, PageIdUtils.linkFromBytesOffset(0, 0));
        assertEquals(0x0010000000000000L, PageIdUtils.linkFromBytesOffset(0, 8));

        assertEquals(0x8000000000000000L, PageIdUtils.linkFromBytesOffset(0, 16384));
        assertEquals(0x800FFFFFFFFFFFFFL, PageIdUtils.linkFromBytesOffset(0xFFFFFFFFFFFFFL, 16384));

        assertEquals(0x8010000000000000L, PageIdUtils.linkFromBytesOffset(0, 16392));
        assertEquals(0x801FFFFFFFFFFFFFL, PageIdUtils.linkFromBytesOffset(0xFFFFFFFFFFFFFL, 16392));

        assertEquals(0xFFF0000000000000L, PageIdUtils.linkFromBytesOffset(0, 32760));
        assertEquals(0xFFFFFFFFFFFFFFFFL, PageIdUtils.linkFromBytesOffset(0xFFFFFFFFFFFFFL, 32760));

        assertEquals(0x000FFFFFFFFFFFFFL, PageIdUtils.linkFromDwordOffset(0xFFFFFFFFFFFFFL, 0));
        assertEquals(0x001FFFFFFFFFFFFFL, PageIdUtils.linkFromDwordOffset(0xFFFFFFFFFFFFFL, 1));

        assertEquals(0x0000000000000000L, PageIdUtils.linkFromDwordOffset(0, 0));
        assertEquals(0x0010000000000000L, PageIdUtils.linkFromDwordOffset(0, 1));

        assertEquals(0x8000000000000000L, PageIdUtils.linkFromDwordOffset(0, 2048));
        assertEquals(0x800FFFFFFFFFFFFFL, PageIdUtils.linkFromDwordOffset(0xFFFFFFFFFFFFFL, 2048));

        assertEquals(0x8010000000000000L, PageIdUtils.linkFromDwordOffset(0, 2049));
        assertEquals(0x801FFFFFFFFFFFFFL, PageIdUtils.linkFromDwordOffset(0xFFFFFFFFFFFFFL, 2049));

        assertEquals(0xFFF0000000000000L, PageIdUtils.linkFromDwordOffset(0, 4095));
        assertEquals(0xFFFFFFFFFFFFFFFFL, PageIdUtils.linkFromDwordOffset(0xFFFFFFFFFFFFFL, 4095));
    }

    /**
     * @throws Exception If failed.
     */
    public void testFileIdExtraction() throws Exception {
        assertEquals(0, PageIdUtils.fileId(0x00000001L));
        assertEquals(1, PageIdUtils.fileId(0x40000001L));
        assertEquals(0, PageIdUtils.fileId(0x3FFFFFFFL));
        assertEquals(1, PageIdUtils.fileId(0x7FFFFFFFL));

        assertEquals(0x3FFFFF, PageIdUtils.fileId(0xFFFFFC0000001L));
        assertEquals(0x3FFFFF, PageIdUtils.fileId(0xFFFFFFFFFFFFFL));

        assertEquals(0, PageIdUtils.fileId(0xFFF0000000000001L));
        assertEquals(1, PageIdUtils.fileId(0xFFF0000040000001L));
        assertEquals(0, PageIdUtils.fileId(0xFFF000003FFFFFFFL));
        assertEquals(1, PageIdUtils.fileId(0xFFF000007FFFFFFFL));

        assertEquals(0x3FFFFF, PageIdUtils.fileId(0xFFFFFFFFC0000001L));
        assertEquals(0x3FFFFF, PageIdUtils.fileId(0xFFFFFFFFFFFFFFFFL));
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

        assertEquals(0, PageIdUtils.dwordsOffset(0x000FFFFFFFFFFFFFL));
        assertEquals(1, PageIdUtils.dwordsOffset(0x001FFFFFFFFFFFFFL));

        assertEquals(0, PageIdUtils.dwordsOffset(0x0000000000000000L));
        assertEquals(1, PageIdUtils.dwordsOffset(0x0010000000000000L));

        assertEquals(2048, PageIdUtils.dwordsOffset(0x8000000000000000L));
        assertEquals(2048, PageIdUtils.dwordsOffset(0x800FFFFFFFFFFFFFL));

        assertEquals(2049, PageIdUtils.dwordsOffset(0x8010000000000000L));
        assertEquals(2049, PageIdUtils.dwordsOffset(0x801FFFFFFFFFFFFFL));

        assertEquals(4095, PageIdUtils.dwordsOffset(0xFFF0000000000000L));
        assertEquals(4095, PageIdUtils.dwordsOffset(0xFFFFFFFFFFFFFFFFL));
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
            int pageNum = rnd.nextInt(PageIdUtils.MAX_PAGE_NUM + 1);

            long pageId = PageIdUtils.pageId(partId, PageMemory.FLAG_DATA, pageNum);

            long link1 = PageIdUtils.linkFromDwordOffset(pageId, offset);
            long link2 = PageIdUtils.linkFromDwordOffset(pageId, 0);

            assertEquals(new FullPageId(link1, 1), new FullPageId(link2, 1));
        }

        // Check that partition ID is not included in FullPageId for data pages.
        for (int i = 0; i < 50_000; i++) {
            int offset = rnd.nextInt(PageIdUtils.MAX_OFFSET_DWORDS + 1);
            int partId = rnd.nextInt(PageIdUtils.MAX_PART_ID + 1);
            int pageNum = rnd.nextInt(PageIdUtils.MAX_PAGE_NUM + 1);

            long pageId1 = PageIdUtils.pageId(partId, PageMemory.FLAG_IDX, pageNum);
            long pageId2 = PageIdUtils.pageId(1, PageMemory.FLAG_IDX, pageNum);

            long link1 = PageIdUtils.linkFromDwordOffset(pageId1, offset);
            long link2 = PageIdUtils.linkFromDwordOffset(pageId2, 0);

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
            int pageNum = rnd.nextInt(PageIdUtils.MAX_PAGE_NUM + 1);

            long pageId = PageIdUtils.pageId(fileId, pageNum);

            String msg = "For vals [offset=" + U.hexLong(offset) + ", fileId=" + U.hexLong(fileId) +
                ", pageNum=" + U.hexLong(pageNum) + ']';

            assertEquals(msg, pageId, PageIdUtils.pageId(pageId));
            assertEquals(msg, fileId, PageIdUtils.fileId(pageId));
            assertEquals(msg, 0, PageIdUtils.bytesOffset(pageId));
            assertEquals(msg, 0, PageIdUtils.dwordsOffset(pageId));

            long link = PageIdUtils.linkFromDwordOffset(pageId, offset);

            assertEquals(msg, pageId, PageIdUtils.pageId(link));
            assertEquals(msg, offset, PageIdUtils.dwordsOffset(link));
            assertEquals(msg, offset * 8, PageIdUtils.bytesOffset(link));
            assertEquals(msg, pageId, PageIdUtils.pageId(link));
            assertEquals(msg, fileId, PageIdUtils.fileId(link));
        }
    }
}

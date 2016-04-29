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

package org.apache.ignite.internal.pagemem;

import static org.apache.ignite.internal.pagemem.PageIdAllocator.FLAG_DATA;

/**
 * Utility class for page ID parts manipulation.
 *
 * @see FullPageId
 */
public final class PageIdUtils {
    /** */
    private static final int PAGE_IDX_SIZE = 30;

    /** */
    private static final int FILE_ID_SIZE = 22;

    /** */
    private static final int PART_ID_SIZE = 14;

    /** */
    private static final int FLAG_SIZE = 3;

    /** */
    private static final int OFFSET_SIZE = 12;

    /** */
    private static final long PAGE_IDX_MASK = ~(-1 << PAGE_IDX_SIZE);

    /** */
    private static final long FILE_ID_MASK = ~(-1 << FILE_ID_SIZE);

    /** */
    private static final long OFFSET_MASK = ~(-1 << OFFSET_SIZE);

    /** */
    private static final long PART_ID_MASK = ~(-1 << PART_ID_SIZE);

    /** */
    private static final long FLAG_MASK = ~(-1 << FLAG_SIZE);

    /** */
    private static final long EFFECTIVE_NON_DATA_PAGE_ID_MASK =
        (FLAG_MASK << (PAGE_IDX_SIZE + PART_ID_SIZE)) | PAGE_IDX_MASK;

    /** Maximum page number. */
    public static final int MAX_PAGE_NUM = (1 << PAGE_IDX_SIZE) - 1;

    /** Maximum page number. */
    public static final int MAX_PART_ID = (1 << PART_ID_SIZE) - 1;

    /** Maximum file ID. */
    public static final int MAX_FILE_ID = (1 << FILE_ID_SIZE) - 1;

    /** Maximum offset in dwords. */
    public static final int MAX_OFFSET_DWORDS = (int)OFFSET_MASK;

    /**
     *
     */
    private PageIdUtils() {
        // No-op.
    }

    /**
     * Constructs a page link by the given page ID and bytes offset within the page. Bytes offset must be
     * aligned on a 8-byte border.
     *
     * @param pageId Page ID.
     * @param bytesOffset Bytes offset in the page aligned by a 8-byte boundary.
     * @return Page link.
     */
    public static long linkFromBytesOffset(long pageId, int bytesOffset) {
        assert (bytesOffset & 0x7) == 0;
        assert (pageId >> (PAGE_IDX_SIZE + FILE_ID_SIZE)) == 0;

        // (bytesOffset >> 3) << PAGE_IDX_SIZE
        return pageId | (((long)bytesOffset) << (FILE_ID_SIZE + PAGE_IDX_SIZE - 3));
    }

    /**
     * Constructs a page link by the given page ID and 8-byte words within the page.
     *
     * @param pageId Page ID.
     * @param dwordOffset Offset in 8-byte words.
     * @return Page link.
     */
    public static long linkFromDwordOffset(long pageId, int dwordOffset) {
        assert (pageId >> (PAGE_IDX_SIZE + FILE_ID_SIZE)) == 0;

        return pageId | (((long)dwordOffset) << (PAGE_IDX_SIZE + FILE_ID_SIZE));
    }

    /**
     * Constructs a page ID by the given file ID and page index.
     *
     * @param fileId File ID.
     * @param pageIdx Page index.
     * @return Page ID.
     */
    public static long pageId(int fileId, long pageIdx) {
        assert (pageIdx & ~PAGE_IDX_MASK) == 0;

        long pageId = 0;

        pageId = (pageId << FILE_ID_SIZE) | (fileId & FILE_ID_MASK);
        pageId = (pageId << PAGE_IDX_SIZE) | (pageIdx & PAGE_IDX_MASK);

        return pageId;
    }

    /**
     * Extracts a page index from the given pageId.
     *
     * @param pageId Page id.
     * @return Page ID.
     */
    public static int pageIndex(long pageId) {
        return (int)(pageId & PAGE_IDX_MASK); // 30 bytes
    }

    /**
     * Extracts a page ID from the given page link.
     *
     * @param link Page link.
     * @return Page ID.
     */
    public static long pageId(long link) {
        return link & ~(OFFSET_MASK << (FILE_ID_SIZE + PAGE_IDX_SIZE));
    }

    /**
     * @param link Page link.
     * @return Effective page id.
     */
    public static long effectivePageId(long link) {
        return flag(link) == FLAG_DATA ? pageId(link) : link & EFFECTIVE_NON_DATA_PAGE_ID_MASK;
    }

    /**
     * Extracts a file ID from the given page ID or page link.
     *
     * @param linkOrPageId Page link or page ID.
     * @return File ID.
     */
    public static int fileId(long linkOrPageId) {
        return (int)((linkOrPageId >> PAGE_IDX_SIZE) & FILE_ID_MASK);
    }

    /**
     * Extracts offset in bytes from the given page link.
     *
     * @param link Page link.
     * @return Offset within the page in bytes.
     */
    public static int bytesOffset(long link) {
        return (int)((link >> (PAGE_IDX_SIZE + FILE_ID_SIZE)) & OFFSET_MASK) << 3;
    }

    /**
     * Extracts offset in 8-byte words from the given page link.
     *
     * @param link Page link.
     * @return Offset in 8-byte words.
     */
    public static int dwordsOffset(long link) {
        return (int)((link >> (PAGE_IDX_SIZE + FILE_ID_SIZE)) & OFFSET_MASK);
    }

    /**
     * @param partId Partition ID.
     * @return Part ID constructed from the given cache ID and partition ID.
     */
    public static long pageId(int partId, byte flag, long pageIdx) {
        long fileId = 0;

        fileId = (fileId << FLAG_SIZE) | (flag & FLAG_MASK);
        fileId = (fileId << PART_ID_SIZE) | (partId & PART_ID_MASK);

        return pageId((int)fileId, pageIdx);
    }

    /**
     * @param pageId Page ID.
     * @return Flag.
     */
    public static byte flag(long pageId) {
        return (byte) (( pageId >>> (PART_ID_SIZE + PAGE_IDX_SIZE) ) & FLAG_MASK);
    }

    /**
     * @param pageId Page ID.
     * @return Partition.
     */
    public static int partId(long pageId) {
        return (int) ((pageId >>> PAGE_IDX_SIZE) & PART_ID_MASK);
    }

    /**
     * @param pageId Page ID.
     * @return New page ID.
     */
    public static long rotatePageId(long pageId) {
        assert flag(pageId) == PageIdAllocator.FLAG_IDX; // Possible only for index pages.

        int partId = partId(pageId);
        long pageIdx = pageIndex(pageId);

        return pageId(partId + 1, PageIdAllocator.FLAG_IDX, pageIdx);
    }

    /**
     * @param pageId Page ID.
     * @return Page ID with masked partition ID.
     */
    public static long maskPartId(long pageId) {
        assert flag(pageId) == PageIdAllocator.FLAG_IDX; // Possible only for index pages.

        return pageId & ~(PART_ID_MASK << PAGE_IDX_SIZE);
    }
}

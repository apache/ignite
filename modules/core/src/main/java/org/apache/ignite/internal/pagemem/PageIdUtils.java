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

/**
 * TODO describe the bits structure.
 */
public final class PageIdUtils {
    /** */
    private static final long FILE_ID_MASK  = 0x000FFFFFC0000000L;

    /** */
    private static final long PAGE_NUM_MASK = 0x000000003FFFFFFFL;

    /** */
    private static final int OFFSET_SHIFTED_MASK = 0xFFF;

    /** */
    private static final int FILE_ID_SIZE = Long.bitCount(FILE_ID_MASK);

    /** */
    private static final int PART_ID_SIZE = 14;

    /** */
    private static final int PART_ID_MASK = ~(-1 << PART_ID_SIZE);

    /** */
    private static final int FLAG_SIZE = 3;

    /** */
    private static final int FLAG_MASK = ~(-1 << FLAG_SIZE);

    /** */
    private static final int PAGE_NUM_SIZE = Long.bitCount(PAGE_NUM_MASK);

    /** Maximum page number. */
    public static final int MAX_PAGE_NUM = (1 << PAGE_NUM_SIZE) - 1;

    /** Maximum file ID. */
    public static final int MAX_FILE_ID = (1 << FILE_ID_SIZE) - 1;

    /** Maximum offset in dwords. */
    public static final int MAX_OFFSET_DWORDS = OFFSET_SHIFTED_MASK;

    /** Maximum part number. */
    public static final int MAX_PART_ID = (1 << PART_ID_SIZE) - 1;

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
        assert (pageId >> (PAGE_NUM_SIZE + FILE_ID_SIZE)) == 0;

        // (bytesOffset >> 3) << PAGE_NUM_SIZE
        return pageId | (((long)bytesOffset) << (FILE_ID_SIZE + PAGE_NUM_SIZE - 3));
    }

    /**
     * Constructs a page link by the given page ID and 8-byte words within the page.
     *
     * @param pageId Page ID.
     * @param dwordOffset Offset in 8-byte words.
     * @return Page link.
     */
    public static long linkFromDwordOffset(long pageId, int dwordOffset) {
        assert (pageId >> (PAGE_NUM_SIZE + FILE_ID_SIZE)) == 0;

        return pageId | (((long)dwordOffset) << (PAGE_NUM_SIZE + FILE_ID_SIZE));
    }

    /**
     * Constructs a page ID by the given file ID and page index.
     *
     * @param fileId File ID.
     * @param pageIdx Page index.
     * @return Page ID.
     */
    public static long pageId(int fileId, long pageIdx) {
        assert (pageIdx & ~PAGE_NUM_MASK) == 0;

        return (( ((long)fileId) << PAGE_NUM_SIZE) & FILE_ID_MASK ) | pageIdx;
    }

    /**
     * Extracts a page index from the given pageId.
     *
     * @param pageId Page id.
     * @return Page ID.
     */
    public static long pageIdx(long pageId) {
        return pageId & PAGE_NUM_MASK;
    }

    /**
     * Extracts a page ID from the given page link.
     *
     * @param link Page link.
     * @return Page ID.
     */
    public static long pageId(long link) {
        return link & (FILE_ID_MASK | PAGE_NUM_MASK);
    }

    /**
     * Extracts a file ID from the given page ID or page link.
     *
     * @param linkOrPageId Page link or page ID.
     * @return File ID.
     */
    public static int fileId(long linkOrPageId) {
        return (int)((linkOrPageId & FILE_ID_MASK) >> PAGE_NUM_SIZE);
    }

    /**
     * Extracts offset in bytes from the given page link.
     *
     * @param link Page link.
     * @return Offset within the page in bytes.
     */
    public static int bytesOffset(long link) {
        return (int)((link >> (PAGE_NUM_SIZE + FILE_ID_SIZE)) & OFFSET_SHIFTED_MASK) << 3;
    }

    /**
     * Extracts offset in 8-byte words from the given page link.
     *
     * @param link Page link.
     * @return Offset in 8-byte words.
     */
    public static int dwordsOffset(long link) {
        return (int)(link >> (PAGE_NUM_SIZE + FILE_ID_SIZE)) & OFFSET_SHIFTED_MASK;
    }

    /**
     * @param cacheId Cache ID.
     * @param partId Partition ID.
     * @return File ID constructed from the given cache ID and partition ID.
     */
    public static int fileId(int cacheId, int partId) {
        return (cacheId & 0xFF << 14) | partId;
    }

    /**
     * @param cacheId Cache ID.
     * @param partId Partition ID.
     * @return Part ID constructed from the given cache ID and partition ID.
     */
    public static long pageId(int cacheId, int partId, byte flag, long pageIdx) {
        int fileId = cacheId;

        fileId = (fileId << FLAG_SIZE) | (flag & FLAG_MASK);

        fileId = (fileId << PART_ID_SIZE) | (partId & PART_ID_MASK);

        return pageId(fileId, pageIdx);
    }

    public static byte flag(long pageId) {
        return (byte) (( pageId >>> (PART_ID_SIZE + PAGE_NUM_SIZE) ) & FLAG_MASK);
    }

    public static int partId(long pageId) {
        return (int) ((pageId >>> PAGE_NUM_SIZE) & PART_ID_MASK);
    }
}

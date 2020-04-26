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

import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Utility class for page ID parts manipulation.
 *
 * @see FullPageId
 */
public final class PageIdUtils {
    /** */
    public static final int PAGE_IDX_SIZE = 32;

    /** */
    public static final int PART_ID_SIZE = 16;

    /** */
    public static final int FLAG_SIZE = 8;

    /** */
    public static final int OFFSET_SIZE = 8;

    /** */
    public static final int TAG_SIZE = 16;

    /** */
    public static final long PAGE_IDX_MASK = ~(-1L << PAGE_IDX_SIZE);

    /** */
    public static final long OFFSET_MASK = ~(-1L << OFFSET_SIZE);

    /** */
    public static final long TAG_MASK = ~(-1L << TAG_SIZE);

    /** Page Index is a monotonically growing number within each partition */
    public static final long PART_ID_MASK = ~(-1L << PART_ID_SIZE);

    /** Flags mask. Flags consists from a number of reserved bits, and page type (data/index page) */
    public static final long FLAG_MASK = ~(-1L << FLAG_SIZE);

    /** */
    private static final long EFFECTIVE_PAGE_ID_MASK = ~(-1L << (PAGE_IDX_SIZE + PART_ID_SIZE));

    /** */
    private static final long PAGE_ID_MASK = ~(-1L << (PAGE_IDX_SIZE + PART_ID_SIZE + FLAG_SIZE));

    /** Max itemid number. */
    public static final int MAX_ITEMID_NUM = 0xFE;

    /** Maximum page number. */
    public static final long MAX_PAGE_NUM = (1L << PAGE_IDX_SIZE) - 1;

    /** Maximum page number. */
    public static final int MAX_PART_ID = (1 << PART_ID_SIZE) - 1;

    /**
     *
     */
    private PageIdUtils() {
        // No-op.
    }

    /**
     * Constructs a page link by the given page ID and 8-byte words within the page.
     *
     * @param pageId Page ID.
     * @param itemId Item ID.
     * @return Page link.
     */
    public static long link(long pageId, int itemId) {
        assert itemId >= 0 && itemId <= MAX_ITEMID_NUM : itemId;
        assert (pageId >> (FLAG_SIZE + PART_ID_SIZE + PAGE_IDX_SIZE)) == 0 : U.hexLong(pageId);

        return pageId | (((long)itemId) << (FLAG_SIZE + PART_ID_SIZE + PAGE_IDX_SIZE));
    }

    /**
     * Extracts a page index from the given page ID.
     *
     * @param pageId Page ID.
     * @return Page index.
     */
    public static int pageIndex(long pageId) {
        return (int)(pageId & PAGE_IDX_MASK); // 4 bytes
    }

    /**
     * Extracts a page ID from the given page link.
     *
     * @param link Page link.
     * @return Page ID.
     */
    public static long pageId(long link) {
        return flag(link) == PageIdAllocator.FLAG_IDX ? link : link & PAGE_ID_MASK;
    }

    /**
     * @param link Page link.
     * @return Effective page id.
     */
    public static long effectivePageId(long link) {
        return link & EFFECTIVE_PAGE_ID_MASK;
    }

    /**
     * @param pageId Page id.
     * @return {@code True} if page id is equal to effective page id.
     */
    public static boolean isEffectivePageId(long pageId) {
        return (pageId & ~EFFECTIVE_PAGE_ID_MASK) == 0;
    }

    /**
     * Index of the item inside of data page.
     *
     * @param link Page link.
     * @return Offset in 8-byte words.
     */
    public static int itemId(long link) {
        return (int)((link >> (PAGE_IDX_SIZE + PART_ID_SIZE + FLAG_SIZE)) & OFFSET_MASK);
    }

    /**
     * Tag of pageId
     *
     * @param link Page link.
     * @return tag - item id + flags
     */
    public static int tag(long link) {
        return (int)((link >> (PAGE_IDX_SIZE + PART_ID_SIZE)) & TAG_MASK);
    }

    /**
     * @param partId Partition ID.
     * @param flag Flags (a number of reserved bits, and page type (data/index page))
     * @param pageIdx Page index, monotonically growing number within each partition
     * @return Page ID constructed from the given pageIdx and partition ID, see {@link FullPageId}
     */
    public static long pageId(int partId, byte flag, int pageIdx) {
        long pageId = flag & FLAG_MASK;

        pageId = (pageId << PART_ID_SIZE) | (partId & PART_ID_MASK);
        pageId = (pageId << (PAGE_IDX_SIZE)) | (pageIdx & PAGE_IDX_MASK);

        return pageId;
    }

    /**
     * @param pageId Page ID.
     * @return Flag.
     */
    public static byte flag(long pageId) {
        return (byte)((pageId >>> (PART_ID_SIZE + PAGE_IDX_SIZE)) & FLAG_MASK);
    }

    /**
     * @param pageId Page ID.
     * @return Partition.
     */
    public static int partId(long pageId) {
        return (int)((pageId >>> PAGE_IDX_SIZE ) & PART_ID_MASK);
    }

    /**
     * @param pageId Page ID.
     * @return New page ID.
     */
    public static long rotatePageId(long pageId) {
        long updatedRotationId = (pageId >>> PAGE_IDX_SIZE + PART_ID_SIZE + FLAG_SIZE) + 1;

        if (updatedRotationId > MAX_ITEMID_NUM)
            updatedRotationId = 1; // We always want non-zero updatedRotationId

        return (pageId & PAGE_ID_MASK) |
            (updatedRotationId << (PAGE_IDX_SIZE + PART_ID_SIZE + FLAG_SIZE));
    }

    /**
     * Masks partition ID from full page ID.
     * @param pageId Page ID to mask partition ID from.
     */
    public static long maskPartitionId(long pageId) {
        return pageId & ~((-1L << PAGE_IDX_SIZE) & (~(-1L << PAGE_IDX_SIZE + PART_ID_SIZE)));
    }

    /**
     * Change page type.
     *
     * @param pageId Old page ID.
     * @param type New page type.
     * @return Changed page ID.
     */
    public static long changeType(long pageId, byte type) {
        return pageId(partId(pageId), type, pageIndex(pageId));
    }

    /**
     * @param pageId Page id.
     */
    public static String toDetailString(long pageId) {
        return "pageId=" + pageId +
            "(offset=" + itemId(pageId) +
            ", flags=" + Integer.toBinaryString(flag(pageId)) +
            ", partId=" + partId(pageId) +
            ", index=" + pageIndex(pageId) +
            ")";
    }

    /**
     * @param pageId Page ID.
     * @param partId Partition ID.
     */
    public static long changePartitionId(long pageId, int partId) {
        byte flag = flag(pageId);
        int pageIdx = pageIndex(pageId);

        return pageId(partId, flag, pageIdx);
    }
}

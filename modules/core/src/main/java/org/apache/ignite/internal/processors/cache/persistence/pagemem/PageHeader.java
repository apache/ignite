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

package org.apache.ignite.internal.processors.cache.persistence.pagemem;

import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.util.GridUnsafe;

/**
 *
 */
class PageHeader {
    /** */
    public static final long PAGE_MARKER = 0x0000000000000001L;

    /** Dirty flag. */
    private static final long DIRTY_FLAG = 0x0100000000000000L;

    /** Page relative pointer. Does not change once a page is allocated. */
    private static final int RELATIVE_PTR_OFFSET = 8;

    /** Page ID offset */
    private static final int PAGE_ID_OFFSET = 16;

    /** Page cache group ID offset. */
    private static final int PAGE_CACHE_ID_OFFSET = 24;

    /** Page pin counter offset. */
    private static final int PAGE_PIN_CNT_OFFSET = 28;

    /** Page temp copy buffer relative pointer offset. */
    private static final int PAGE_TMP_BUF_OFFSET = 40;

    /**
     * @param absPtr Absolute pointer to initialize.
     * @param relative Relative pointer to write.
     */
    public static void initNew(long absPtr, long relative) {
        relative(absPtr, relative);

        tempBufferPointer(absPtr, PageMemoryImpl.INVALID_REL_PTR);

        GridUnsafe.putLong(absPtr, PAGE_MARKER);
        GridUnsafe.putInt(absPtr + PAGE_PIN_CNT_OFFSET, 0);
    }

    /**
     * @param absPtr Absolute pointer.
     * @return Dirty flag.
     */
    public static boolean dirty(long absPtr) {
        return flag(absPtr, DIRTY_FLAG);
    }

    /**
     * @param absPtr Page absolute pointer.
     * @param dirty Dirty flag.
     * @return Previous value of dirty flag.
     */
    public static boolean dirty(long absPtr, boolean dirty) {
        return flag(absPtr, DIRTY_FLAG, dirty);
    }

    /**
     * @param absPtr Absolute pointer.
     * @param flag Flag mask.
     * @return Flag value.
     */
    private static boolean flag(long absPtr, long flag) {
        assert (flag & 0xFFFFFFFFFFFFFFL) == 0;
        assert Long.bitCount(flag) == 1;

        long relPtrWithFlags = GridUnsafe.getLong(absPtr + RELATIVE_PTR_OFFSET);

        return (relPtrWithFlags & flag) != 0;
    }

    /**
     * Sets flag.
     *
     * @param absPtr Absolute pointer.
     * @param flag Flag mask.
     * @param set New flag value.
     * @return Previous flag value.
     */
    private static boolean flag(long absPtr, long flag, boolean set) {
        assert (flag & 0xFFFFFFFFFFFFFFL) == 0;
        assert Long.bitCount(flag) == 1;

        long relPtrWithFlags = GridUnsafe.getLong(absPtr + RELATIVE_PTR_OFFSET);

        boolean was = (relPtrWithFlags & flag) != 0;

        if (set)
            relPtrWithFlags |= flag;
        else
            relPtrWithFlags &= ~flag;

        GridUnsafe.putLong(absPtr + RELATIVE_PTR_OFFSET, relPtrWithFlags);

        return was;
    }

    /**
     * @param absPtr Page pointer.
     * @return If page is pinned.
     */
    public static boolean isAcquired(long absPtr) {
        return GridUnsafe.getInt(absPtr + PAGE_PIN_CNT_OFFSET) > 0;
    }

    /**
     * @param absPtr Absolute pointer.
     */
    public static void acquirePage(long absPtr) {
        GridUnsafe.incrementAndGetInt(absPtr + PAGE_PIN_CNT_OFFSET);
    }

    /**
     * @param absPtr Absolute pointer.
     */
    public static int releasePage(long absPtr) {
        return GridUnsafe.decrementAndGetInt(absPtr + PAGE_PIN_CNT_OFFSET);
    }

    /**
     * @param absPtr Absolute pointer.
     * @return Number of acquires for the page.
     */
    public static int pinCount(long absPtr) {
        return GridUnsafe.getIntVolatile(null, absPtr);
    }

    /**
     * Reads relative pointer from the page at the given absolute position.
     *
     * @param absPtr Absolute memory pointer to the page header.
     * @return Relative pointer written to the page.
     */
    public static long readRelative(long absPtr) {
        return GridUnsafe.getLong(absPtr + RELATIVE_PTR_OFFSET) & PageMemoryImpl.RELATIVE_PTR_MASK;
    }

    /**
     * Writes relative pointer to the page at the given absolute position.
     *
     * @param absPtr Absolute memory pointer to the page header.
     * @param relPtr Relative pointer to write.
     */
    public static void relative(long absPtr, long relPtr) {
        GridUnsafe.putLong(absPtr + RELATIVE_PTR_OFFSET, relPtr & PageMemoryImpl.RELATIVE_PTR_MASK);
    }

    /**
     * Volatile write for current timestamp to page in {@code absAddr} address.
     *
     * @param absPtr Absolute page address.
     */
    public static void writeTimestamp(final long absPtr, long tstamp) {
        tstamp >>= 8;

        GridUnsafe.putLongVolatile(null, absPtr, (tstamp << 8) | 0x01);
    }

    /**
     * Read for timestamp from page in {@code absAddr} address.
     *
     * @param absPtr Absolute page address.
     * @return Timestamp.
     */
    public static long readTimestamp(final long absPtr) {
        long markerAndTs = GridUnsafe.getLong(absPtr);

        // Clear last byte as it is occupied by page marker.
        return markerAndTs & ~0xFF;
    }

    /**
     * Sets pointer to checkpoint buffer.
     *
     * @param absPtr Page absolute pointer.
     * @param tmpRelPtr Temp buffer relative pointer or {@link PageMemoryImpl#INVALID_REL_PTR} if page is not copied to checkpoint
     * buffer.
     */
    public static void tempBufferPointer(long absPtr, long tmpRelPtr) {
        GridUnsafe.putLong(absPtr + PAGE_TMP_BUF_OFFSET, tmpRelPtr);
    }

    /**
     * Gets pointer to checkpoint buffer or {@link PageMemoryImpl#INVALID_REL_PTR} if page is not copied to checkpoint buffer.
     *
     * @param absPtr Page absolute pointer.
     * @return Temp buffer relative pointer.
     */
    public static long tempBufferPointer(long absPtr) {
        return GridUnsafe.getLong(absPtr + PAGE_TMP_BUF_OFFSET);
    }

    /**
     * Reads page ID from the page at the given absolute position.
     *
     * @param absPtr Absolute memory pointer to the page header.
     * @return Page ID written to the page.
     */
    public static long readPageId(long absPtr) {
        return GridUnsafe.getLong(absPtr + PAGE_ID_OFFSET);
    }

    /**
     * Writes page ID to the page at the given absolute position.
     *
     * @param absPtr Absolute memory pointer to the page header.
     * @param pageId Page ID to write.
     */
    private static void pageId(long absPtr, long pageId) {
        GridUnsafe.putLong(absPtr + PAGE_ID_OFFSET, pageId);
    }

    /**
     * Reads cache group ID from the page at the given absolute pointer.
     *
     * @param absPtr Absolute memory pointer to the page header.
     * @return Cache group ID written to the page.
     */
    private static int readPageGroupId(final long absPtr) {
        return GridUnsafe.getInt(absPtr + PAGE_CACHE_ID_OFFSET);
    }

    /**
     * Writes cache group ID from the page at the given absolute pointer.
     *
     * @param absPtr Absolute memory pointer to the page header.
     * @param grpId Cache group ID to write.
     */
    private static void pageGroupId(final long absPtr, final int grpId) {
        GridUnsafe.putInt(absPtr + PAGE_CACHE_ID_OFFSET, grpId);
    }

    /**
     * Reads page ID and cache group ID from the page at the given absolute pointer.
     *
     * @param absPtr Absolute memory pointer to the page header.
     * @return Full page ID written to the page.
     */
    public static FullPageId fullPageId(final long absPtr) {
        return new FullPageId(readPageId(absPtr), readPageGroupId(absPtr));
    }

    /**
     * Writes page ID and cache group ID from the page at the given absolute pointer.
     *
     * @param absPtr Absolute memory pointer to the page header.
     * @param fullPageId Full page ID to write.
     */
    public static void fullPageId(final long absPtr, final FullPageId fullPageId) {
        pageId(absPtr, fullPageId.pageId());

        pageGroupId(absPtr, fullPageId.groupId());
    }
}

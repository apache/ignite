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

import org.apache.ignite.configuration.PageReplacementMode;
import org.apache.ignite.internal.util.GridUnsafe;

/**
 * Pages Segmented-LRU (SLRU) list implementation.
 *
 * @see PageReplacementMode#SEGMENTED_LRU
 */
public class SegmentedLruPageList {
    /** Ratio to limit count of protected pages. */
    private static final double PROTECTED_TO_TOTAL_PAGES_RATIO = 0.5;

    /** Null page index. */
    static final int NULL_IDX = -1;

    /** Index of the head page of LRU list. */
    private int headIdx = NULL_IDX;

    /** Index of the tail page of LRU list. */
    private int tailIdx = NULL_IDX;

    /** Index of the tail page of probationary segment. */
    private int probTailIdx = NULL_IDX;

    /** Count of protected pages in the list. */
    private int protectedPagesCnt;

    /** Protected pages segment limit. */
    private final int protectedPagesLimit;

    /** Pointer to memory region to store links. */
    private final long linksPtr;

    /** Pointer to memory region to store protected flags. */
    private final long flagsPtr;

    /**
     * @param totalPagesCnt Total pages count.
     * @param memPtr Pointer to memory region.
     */
    public SegmentedLruPageList(int totalPagesCnt, long memPtr) {
        linksPtr = memPtr;
        flagsPtr = memPtr + (((long)totalPagesCnt) << 3);

        GridUnsafe.setMemory(linksPtr, ((long)totalPagesCnt) << 3, (byte)0xFF);
        GridUnsafe.zeroMemory(flagsPtr, (totalPagesCnt + 7) >> 3);

        protectedPagesLimit = (int)(totalPagesCnt * PROTECTED_TO_TOTAL_PAGES_RATIO);
    }

    /**
     * Remove page from the head of LRU list.
     *
     * @return Page index or {@code -1} if list is empty.
     */
    public synchronized int poll() {
        int idx = headIdx;

        if (idx != NULL_IDX)
            remove(idx);

        return idx;
    }

    /**
     * Remove page from LRU list by page index.
     *
     * @param pageIdx Page index.
     */
    public synchronized void remove(int pageIdx) {
        remove0(pageIdx, protectedPage(pageIdx));
    }

    /**
     * @param pageIdx Page index.
     * @param clearProtectedFlag Clear protected page flag.
     */
    private void remove0(int pageIdx, boolean clearProtectedFlag) {
        assert pageIdx != NULL_IDX;

        int prevIdx = prev(pageIdx);
        int nextIdx = next(pageIdx);

        if (pageIdx == probTailIdx)
            probTailIdx = prevIdx;

        if (prevIdx == NULL_IDX) {
            assert headIdx == pageIdx : "Unexpected LRU page index [headIdx=" + headIdx + ", pageIdx=" + pageIdx + ']';

            headIdx = nextIdx;
        }
        else
            next(prevIdx, nextIdx);

        if (nextIdx == NULL_IDX) {
            assert tailIdx == pageIdx : "Unexpected LRU page index [tailIdx=" + tailIdx + ", pageIdx=" + pageIdx + ']';

            tailIdx = prevIdx;
        }
        else
            prev(nextIdx, prevIdx);

        clearLinks(pageIdx);

        if (clearProtectedFlag) {
            protectedPagesCnt--;

            protectedPage(pageIdx, false);
        }
    }

    /**
     * Add page to the tail of protected or probationary LRU list.
     *
     * @param pageIdx Page index.
     * @param protectedPage Protected page flag.
     */
    public synchronized void addToTail(int pageIdx, boolean protectedPage) {
        assert prev(pageIdx) == NULL_IDX : prev(pageIdx);
        assert next(pageIdx) == NULL_IDX : next(pageIdx);

        if (headIdx == NULL_IDX || tailIdx == NULL_IDX) {
            // In case of empty list.
            assert headIdx == NULL_IDX : headIdx;
            assert tailIdx == NULL_IDX : tailIdx;
            assert probTailIdx == NULL_IDX : probTailIdx;
            assert protectedPagesCnt == 0 : protectedPagesCnt;

            headIdx = pageIdx;
            tailIdx = pageIdx;

            if (protectedPage) {
                protectedPagesCnt = 1;

                protectedPage(pageIdx, true);
            }
            else
                probTailIdx = pageIdx;

            return;
        }

        if (protectedPage) {
            // Protected page - insert to the list tail.
            assert next(tailIdx) == NULL_IDX : "Unexpected LRU page index [pageIdx=" + pageIdx +
                ", tailIdx=" + tailIdx + ", nextLruIdx=" + next(tailIdx) + ']';

            link(tailIdx, pageIdx);

            tailIdx = pageIdx;

            protectedPage(pageIdx, true);

            // Move one page from protected segment to probationary segment if there are too many protected pages.
            if (protectedPagesCnt >= protectedPagesLimit) {
                probTailIdx = probTailIdx != NULL_IDX ? next(probTailIdx) : headIdx;

                assert probTailIdx != NULL_IDX;

                protectedPage(probTailIdx, false);
            }
            else
                protectedPagesCnt++;
        }
        else {
            if (probTailIdx == NULL_IDX) {
                // First page in the probationary list - insert to the head.
                assert prev(headIdx) == NULL_IDX : "Unexpected LRU page index [pageIdx=" + pageIdx +
                    ", headIdx=" + headIdx + ", prevLruIdx=" + prev(headIdx) + ']';

                link(pageIdx, headIdx);

                headIdx = pageIdx;
            }
            else {
                int protectedIdx = next(probTailIdx);

                link(probTailIdx, pageIdx);

                if (protectedIdx == NULL_IDX) {
                    // There are no protected pages in the list.
                    assert probTailIdx == tailIdx :
                        "Unexpected LRU page index [probTailIdx=" + probTailIdx + ", tailIdx=" + tailIdx + ']';

                    tailIdx = pageIdx;
                }
                else {
                    // Link with last protected page.
                    link(pageIdx, protectedIdx);
                }
            }

            probTailIdx = pageIdx;
        }
    }

    /**
     * Move page to the tail of protected LRU list.
     *
     * @param pageIdx Page index.
     */
    public synchronized void moveToTail(int pageIdx) {
        if (tailIdx == pageIdx)
            return;

        remove0(pageIdx, false);

        if (protectedPage(pageIdx)) {
            link(tailIdx, pageIdx);

            tailIdx = pageIdx;
        }
        else
            addToTail(pageIdx, true);
    }

    /**
     * Link two pages.
     *
     * @param prevIdx Previous page index.
     * @param nextIdx Next page index.
     */
    private void link(int prevIdx, int nextIdx) {
        prev(nextIdx, prevIdx);
        next(prevIdx, nextIdx);
    }

    /**
     * Clear page links.
     *
     * @param pageIdx Page index.
     */
    private void clearLinks(int pageIdx) {
        GridUnsafe.putLong(linksPtr + (((long)pageIdx) << 3), -1L);
    }

    /**
     * Gets link to the previous page in the list.
     *
     * @param pageIdx Page index.
     */
    int prev(int pageIdx) {
        return GridUnsafe.getInt(linksPtr + (((long)pageIdx) << 3));
    }

    /**
     * Gets link to the next page in the list.
     *
     * @param pageIdx Page index.
     */
    int next(int pageIdx) {
        return GridUnsafe.getInt(linksPtr + (((long)pageIdx) << 3) + 4);
    }

    /**
     * Gets protected page flag.
     *
     * @param pageIdx Page index.
     */
    boolean protectedPage(int pageIdx) {
        long flags = GridUnsafe.getLong(flagsPtr + ((pageIdx >> 3) & (~7)));

        return (flags & (1L << pageIdx)) != 0L;
    }

    /**
     * Sets link to the previous page in the list.
     *
     * @param pageIdx Page index.
     * @param prevIdx Previous page index.
     */
    private void prev(int pageIdx, int prevIdx) {
        GridUnsafe.putInt(linksPtr + (((long)pageIdx) << 3), prevIdx);
    }

    /**
     * Sets link to the next page in the list.
     *
     * @param pageIdx Page index.
     * @param nextIdx Next page index.
     */
    private void next(int pageIdx, int nextIdx) {
        GridUnsafe.putInt(linksPtr + (((long)pageIdx) << 3) + 4, nextIdx);
    }

    /**
     * Sets protected page flag.
     *
     * @param pageIdx Page index.
     * @param protectedPage Protected page flag.
     */
    private void protectedPage(int pageIdx, boolean protectedPage) {
        long ptr = flagsPtr + ((pageIdx >> 3) & (~7));

        if (protectedPage)
            GridUnsafe.putLong(ptr, GridUnsafe.getLong(ptr) | (1L << pageIdx));
        else
            GridUnsafe.putLong(ptr, GridUnsafe.getLong(ptr) & ~(1L << pageIdx));
    }

    /**
     * Gets the index of the head page of LRU list.
     */
    synchronized int headIdx() {
        return headIdx;
    }

    /**
     * Gets the indexof the tail page of probationary segment.
     */
    synchronized int probTailIdx() {
        return probTailIdx;
    }

    /**
     * Gets the index of the tail page of LRU list.
     */
    synchronized int tailIdx() {
        return tailIdx;
    }

    /**
     * Gets protected pages count.
     */
    synchronized int protectedPagesCount() {
        return protectedPagesCnt;
    }

    /**
     * Gets protected pages limit.
     */
    int protectedPagesLimit() {
        return protectedPagesLimit;
    }

    /**
     * Memory required to service {@code pagesCnt} pages.
     *
     * @param pagesCnt Pages count.
     */
    public static long requiredMemory(int pagesCnt) {
        return pagesCnt * 8 /* links = 2 ints per page */ +
            ((pagesCnt + 63) / 8) & (~7L) /* protected flags = 1 bit per page + 8 byte align */;
    }
}

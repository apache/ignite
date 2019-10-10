/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.store;

import java.util.ArrayList;
import java.util.List;

import org.h2.mvstore.DataUtils;
import org.h2.util.MathUtils;

/**
 * A list that maintains ranges of free space (in blocks).
 */
public class FreeSpaceList {

    /**
     * The first usable block.
     */
    private final int firstFreeBlock;

    /**
     * The block size in bytes.
     */
    private final int blockSize;

    private List<BlockRange> freeSpaceList = new ArrayList<>();

    public FreeSpaceList(int firstFreeBlock, int blockSize) {
        this.firstFreeBlock = firstFreeBlock;
        if (Integer.bitCount(blockSize) != 1) {
            throw DataUtils.newIllegalArgumentException("Block size is not a power of 2");
        }
        this.blockSize = blockSize;
        clear();
    }

    /**
     * Reset the list.
     */
    public synchronized void clear() {
        freeSpaceList.clear();
        freeSpaceList.add(new BlockRange(firstFreeBlock,
                Integer.MAX_VALUE - firstFreeBlock));
    }

    /**
     * Allocate a number of blocks and mark them as used.
     *
     * @param length the number of bytes to allocate
     * @return the start position in bytes
     */
    public synchronized long allocate(int length) {
        int required = getBlockCount(length);
        for (BlockRange pr : freeSpaceList) {
            if (pr.length >= required) {
                int result = pr.start;
                this.markUsed(pr.start * blockSize, length);
                return result * blockSize;
            }
        }
        throw DataUtils.newIllegalStateException(
                DataUtils.ERROR_INTERNAL,
                "Could not find a free page to allocate");
    }

    /**
     * Mark the space as in use.
     *
     * @param pos the position in bytes
     * @param length the number of bytes
     */
    public synchronized void markUsed(long pos, int length) {
        int start = (int) (pos / blockSize);
        int required = getBlockCount(length);
        BlockRange found = null;
        int i = 0;
        for (BlockRange pr : freeSpaceList) {
            if (start >= pr.start && start < (pr.start + pr.length)) {
                found = pr;
                break;
            }
            i++;
        }
        if (found == null) {
            throw DataUtils.newIllegalStateException(
                    DataUtils.ERROR_INTERNAL,
                    "Cannot find spot to mark as used in free list");
        }
        if (start + required > found.start + found.length) {
            throw DataUtils.newIllegalStateException(
                    DataUtils.ERROR_INTERNAL,
                    "Runs over edge of free space");
        }
        if (found.start == start) {
            // if the used space is at the beginning of a free-space-range
            found.start += required;
            found.length -= required;
            if (found.length == 0) {
                // if the free-space-range is now empty, remove it
                freeSpaceList.remove(i);
            }
        } else if (found.start + found.length == start + required) {
            // if the used space is at the end of a free-space-range
            found.length -= required;
        } else {
            // it's in the middle, so split the existing entry
            int length1 = start - found.start;
            int start2 = start + required;
            int length2 = found.start + found.length - start - required;

            found.length = length1;
            BlockRange newRange = new BlockRange(start2, length2);
            freeSpaceList.add(i + 1, newRange);
        }
    }

    /**
     * Mark the space as free.
     *
     * @param pos the position in bytes
     * @param length the number of bytes
     */
    public synchronized void free(long pos, int length) {
        int start = (int) (pos / blockSize);
        int required = getBlockCount(length);
        BlockRange found = null;
        int i = 0;
        for (BlockRange pr : freeSpaceList) {
            if (pr.start > start) {
                found = pr;
                break;
            }
            i++;
        }
        if (found == null) {
            throw DataUtils.newIllegalStateException(
                    DataUtils.ERROR_INTERNAL,
                    "Cannot find spot to mark as unused in free list");
        }
        if (start + required == found.start) {
            // if the used space is adjacent to the beginning of a
            // free-space-range
            found.start = start;
            found.length += required;
            // compact: merge the previous entry into this one if
            // they are now adjacent
            if (i > 0) {
                BlockRange previous = freeSpaceList.get(i - 1);
                if (previous.start + previous.length == found.start) {
                    previous.length += found.length;
                    freeSpaceList.remove(i);
                }
            }
            return;
        }
        if (i > 0) {
            // if the used space is adjacent to the end of a free-space-range
            BlockRange previous = freeSpaceList.get(i - 1);
            if (previous.start + previous.length == start) {
                previous.length += required;
                return;
            }
        }

        // it is between 2 entries, so add a new one
        BlockRange newRange = new BlockRange(start, required);
        freeSpaceList.add(i, newRange);
    }

    private int getBlockCount(int length) {
        if (length <= 0) {
            throw DataUtils.newIllegalStateException(
                    DataUtils.ERROR_INTERNAL, "Free space invalid length");
        }
        return MathUtils.roundUpInt(length, blockSize) / blockSize;
    }

    @Override
    public String toString() {
        return freeSpaceList.toString();
    }

    /**
     * A range of free blocks.
     */
    private static final class BlockRange {

        /**
         * The starting point, in blocks.
         */
        int start;

        /**
         * The length, in blocks.
         */
        int length;

        public BlockRange(int start, int length) {
            this.start = start;
            this.length = length;
        }

        @Override
        public String toString() {
            if (start + length == Integer.MAX_VALUE) {
                return Integer.toHexString(start) + "-";
            }
            return Integer.toHexString(start) + "-" +
                Integer.toHexString(start + length - 1);
        }

    }

}

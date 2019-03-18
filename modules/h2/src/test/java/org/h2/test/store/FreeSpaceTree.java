/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.store;

import java.util.TreeSet;

import org.h2.mvstore.DataUtils;
import org.h2.util.MathUtils;

/**
 * A list that maintains ranges of free space (in blocks) in a file.
 */
public class FreeSpaceTree {

    /**
     * The first usable block.
     */
    private final int firstFreeBlock;

    /**
     * The block size in bytes.
     */
    private final int blockSize;

    /**
     * The list of free space.
     */
    private TreeSet<BlockRange> freeSpace = new TreeSet<>();

    public FreeSpaceTree(int firstFreeBlock, int blockSize) {
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
        freeSpace.clear();
        freeSpace.add(new BlockRange(firstFreeBlock,
                Integer.MAX_VALUE - firstFreeBlock));
    }

    /**
     * Allocate a number of blocks and mark them as used.
     *
     * @param length the number of bytes to allocate
     * @return the start position in bytes
     */
    public synchronized long allocate(int length) {
        int blocks = getBlockCount(length);
        BlockRange x = null;
        for (BlockRange b : freeSpace) {
            if (b.blocks >= blocks) {
                x = b;
                break;
            }
        }
        long pos = getPos(x.start);
        if (x.blocks == blocks) {
            freeSpace.remove(x);
        } else {
            x.start += blocks;
            x.blocks -= blocks;
        }
        return pos;
    }

    /**
     * Mark the space as in use.
     *
     * @param pos the position in bytes
     * @param length the number of bytes
     */
    public synchronized void markUsed(long pos, int length) {
        int start = getBlock(pos);
        int blocks = getBlockCount(length);
        BlockRange x = new BlockRange(start, blocks);
        BlockRange prev = freeSpace.floor(x);
        if (prev == null) {
            throw DataUtils.newIllegalStateException(
                    DataUtils.ERROR_INTERNAL, "Free space already marked");
        }
        if (prev.start == start) {
            if (prev.blocks == blocks) {
                // match
                freeSpace.remove(prev);
            } else {
                // cut the front
                prev.start += blocks;
                prev.blocks -= blocks;
            }
        } else if (prev.start + prev.blocks == start + blocks) {
            // cut the end
            prev.blocks -= blocks;
        } else {
            // insert an entry
            x.start = start + blocks;
            x.blocks = prev.start + prev.blocks - x.start;
            freeSpace.add(x);
            prev.blocks = start - prev.start;
        }
    }

    /**
     * Mark the space as free.
     *
     * @param pos the position in bytes
     * @param length the number of bytes
     */
    public synchronized void free(long pos, int length) {
        int start = getBlock(pos);
        int blocks = getBlockCount(length);
        BlockRange x = new BlockRange(start, blocks);
        BlockRange next = freeSpace.ceiling(x);
        if (next == null) {
            throw DataUtils.newIllegalStateException(
                    DataUtils.ERROR_INTERNAL, "Free space sentinel is missing");
        }
        BlockRange prev = freeSpace.lower(x);
        if (prev != null) {
            if (prev.start + prev.blocks == start) {
                // extend the previous entry
                prev.blocks += blocks;
                if (prev.start + prev.blocks == next.start) {
                    // merge with the next entry
                    prev.blocks += next.blocks;
                    freeSpace.remove(next);
                }
                return;
            }
        }
        if (start + blocks == next.start) {
            // extend the next entry
            next.start -= blocks;
            next.blocks += blocks;
            return;
        }
        freeSpace.add(x);
    }

    private long getPos(int block) {
        return (long) block * (long) blockSize;
    }

    private int getBlock(long pos) {
        return (int) (pos / blockSize);
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
        return freeSpace.toString();
    }

    /**
     * A range of free blocks.
     */
    private static final class BlockRange implements Comparable<BlockRange> {

        /**
         * The starting point (the block number).
         */
        public int start;

        /**
         * The length, in blocks.
         */
        public int blocks;

        public BlockRange(int start, int blocks) {
            this.start = start;
            this.blocks = blocks;
        }

        @Override
        public int compareTo(BlockRange o) {
            return Integer.compare(start, o.start);
        }

        @Override
        public String toString() {
            if (blocks + start == Integer.MAX_VALUE) {
                return Integer.toHexString(start) + "-";
            }
            return Integer.toHexString(start) + "-" +
                Integer.toHexString(start + blocks - 1);
        }

    }

}

/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore;

import java.util.BitSet;

import org.h2.util.MathUtils;

/**
 * A free space bit set.
 */
public class FreeSpaceBitSet {

    private static final boolean DETAILED_INFO = false;

    /**
     * The first usable block.
     */
    private final int firstFreeBlock;

    /**
     * The block size in bytes.
     */
    private final int blockSize;

    /**
     * The bit set.
     */
    private final BitSet set = new BitSet();

    /**
     * Create a new free space map.
     *
     * @param firstFreeBlock the first free block
     * @param blockSize the block size
     */
    public FreeSpaceBitSet(int firstFreeBlock, int blockSize) {
        this.firstFreeBlock = firstFreeBlock;
        this.blockSize = blockSize;
        clear();
    }

    /**
     * Reset the list.
     */
    public void clear() {
        set.clear();
        set.set(0, firstFreeBlock);
    }

    /**
     * Check whether one of the blocks is in use.
     *
     * @param pos the position in bytes
     * @param length the number of bytes
     * @return true if a block is in use
     */
    public boolean isUsed(long pos, int length) {
        int start = getBlock(pos);
        int blocks = getBlockCount(length);
        for (int i = start; i < start + blocks; i++) {
            if (!set.get(i)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Check whether one of the blocks is free.
     *
     * @param pos the position in bytes
     * @param length the number of bytes
     * @return true if a block is free
     */
    public boolean isFree(long pos, int length) {
        int start = getBlock(pos);
        int blocks = getBlockCount(length);
        for (int i = start; i < start + blocks; i++) {
            if (set.get(i)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Allocate a number of blocks and mark them as used.
     *
     * @param length the number of bytes to allocate
     * @return the start position in bytes
     */
    public long allocate(int length) {
        int blocks = getBlockCount(length);
        for (int i = 0;;) {
            int start = set.nextClearBit(i);
            int end = set.nextSetBit(start + 1);
            if (end < 0 || end - start >= blocks) {
                set.set(start, start + blocks);
                return getPos(start);
            }
            i = end;
        }
    }

    /**
     * Mark the space as in use.
     *
     * @param pos the position in bytes
     * @param length the number of bytes
     */
    public void markUsed(long pos, int length) {
        int start = getBlock(pos);
        int blocks = getBlockCount(length);
        set.set(start, start + blocks);
    }

    /**
     * Mark the space as free.
     *
     * @param pos the position in bytes
     * @param length the number of bytes
     */
    public void free(long pos, int length) {
        int start = getBlock(pos);
        int blocks = getBlockCount(length);
        set.clear(start, start + blocks);
    }

    private long getPos(int block) {
        return (long) block * (long) blockSize;
    }

    private int getBlock(long pos) {
        return (int) (pos / blockSize);
    }

    private int getBlockCount(int length) {
        return MathUtils.roundUpInt(length, blockSize) / blockSize;
    }

    /**
     * Get the fill rate of the space in percent. The value 0 means the space is
     * completely free, and 100 means it is completely full.
     *
     * @return the fill rate (0 - 100)
     */
    public int getFillRate() {
        int total = set.length(), count = 0;
        for (int i = 0; i < total; i++) {
            if (set.get(i)) {
                count++;
            }
        }
        if (count == 0) {
            return 0;
        }
        return Math.max(1, (int) (100L * count / total));
    }

    /**
     * Get the position of the first free space.
     *
     * @return the position.
     */
    public long getFirstFree() {
        return getPos(set.nextClearBit(0));
    }

    /**
     * Get the position of the last (infinite) free space.
     *
     * @return the position.
     */
    public long getLastFree() {
        return getPos(set.previousSetBit(set.size()-1) + 1);
    }

    @Override
    public String toString() {
        StringBuilder buff = new StringBuilder();
        if (DETAILED_INFO) {
            int onCount = 0, offCount = 0;
            int on = 0;
            for (int i = 0; i < set.length(); i++) {
                if (set.get(i)) {
                    onCount++;
                    on++;
                } else {
                    offCount++;
                }
                if ((i & 1023) == 1023) {
                    buff.append(String.format("%3x", on)).append(' ');
                    on = 0;
                }
            }
            buff.append('\n')
                    .append(" on ").append(onCount).append(" off ").append(offCount)
                    .append(' ').append(100 * onCount / (onCount+offCount)).append("% used ");
        }
        buff.append('[');
        for (int i = 0;;) {
            if (i > 0) {
                buff.append(", ");
            }
            int start = set.nextClearBit(i);
            buff.append(Integer.toHexString(start)).append('-');
            int end = set.nextSetBit(start + 1);
            if (end < 0) {
                break;
            }
            buff.append(Integer.toHexString(end - 1));
            i = end + 1;
        }
        buff.append(']');
        return buff.toString();
    }

}
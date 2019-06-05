/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

import java.util.Arrays;

/**
 * A list of bits.
 */
public final class BitField {

    private static final int ADDRESS_BITS = 6;
    private static final int BITS = 64;
    private static final int ADDRESS_MASK = BITS - 1;
    private long[] data;
    private int maxLength;

    public BitField() {
        this(64);
    }

    public BitField(int capacity) {
        data = new long[capacity >>> 3];
    }

    /**
     * Get the index of the next bit that is not set.
     *
     * @param fromIndex where to start searching
     * @return the index of the next disabled bit
     */
    public int nextClearBit(int fromIndex) {
        int i = fromIndex >> ADDRESS_BITS;
        int max = data.length;
        for (; i < max; i++) {
            if (data[i] == -1) {
                continue;
            }
            int j = Math.max(fromIndex, i << ADDRESS_BITS);
            for (int end = j + 64; j < end; j++) {
                if (!get(j)) {
                    return j;
                }
            }
        }
        return max << ADDRESS_BITS;
    }

    /**
     * Get the bit at the given index.
     *
     * @param i the index
     * @return true if the bit is enabled
     */
    public boolean get(int i) {
        int addr = i >> ADDRESS_BITS;
        if (addr >= data.length) {
            return false;
        }
        return (data[addr] & getBitMask(i)) != 0;
    }

    /**
     * Get the next 8 bits at the given index.
     * The index must be a multiple of 8.
     *
     * @param i the index
     * @return the next 8 bits
     */
    public int getByte(int i) {
        int addr = i >> ADDRESS_BITS;
        if (addr >= data.length) {
            return 0;
        }
        return (int) (data[addr] >>> (i & (7 << 3)) & 255);
    }

    /**
     * Combine the next 8 bits at the given index with OR.
     * The index must be a multiple of 8.
     *
     * @param i the index
     * @param x the next 8 bits (0 - 255)
     */
    public void setByte(int i, int x) {
        int addr = i >> ADDRESS_BITS;
        checkCapacity(addr);
        data[addr] |= ((long) x) << (i & (7 << 3));
        if (maxLength < i && x != 0) {
            maxLength = i + 7;
        }
    }

    /**
     * Set bit at the given index to 'true'.
     *
     * @param i the index
     */
    public void set(int i) {
        int addr = i >> ADDRESS_BITS;
        checkCapacity(addr);
        data[addr] |= getBitMask(i);
        if (maxLength < i) {
            maxLength = i;
        }
    }

    /**
     * Set bit at the given index to 'false'.
     *
     * @param i the index
     */
    public void clear(int i) {
        int addr = i >> ADDRESS_BITS;
        if (addr >= data.length) {
            return;
        }
        data[addr] &= ~getBitMask(i);
    }

    private static long getBitMask(int i) {
        return 1L << (i & ADDRESS_MASK);
    }

    private void checkCapacity(int size) {
        if (size >= data.length) {
            expandCapacity(size);
        }
    }

    private void expandCapacity(int size) {
        while (size >= data.length) {
            int newSize = data.length == 0 ? 1 : data.length * 2;
            data = Arrays.copyOf(data, newSize);
        }
    }

    /**
     * Enable or disable a number of bits.
     *
     * @param fromIndex the index of the first bit to enable or disable
     * @param toIndex one plus the index of the last bit to enable or disable
     * @param value the new value
     */
    public void set(int fromIndex, int toIndex, boolean value) {
        // go backwards so that OutOfMemory happens
        // before some bytes are modified
        for (int i = toIndex - 1; i >= fromIndex; i--) {
            set(i, value);
        }
        if (value) {
            if (toIndex > maxLength) {
                maxLength = toIndex;
            }
        } else {
            if (toIndex >= maxLength) {
                maxLength = fromIndex;
            }
        }
    }

    private void set(int i, boolean value) {
        if (value) {
            set(i);
        } else {
            clear(i);
        }
    }

    /**
     * Get the index of the highest set bit plus one, or 0 if no bits are set.
     *
     * @return the length of the bit field
     */
    public int length() {
        int m = maxLength >> ADDRESS_BITS;
        while (m > 0 && data[m] == 0) {
            m--;
        }
        maxLength = (m << ADDRESS_BITS) +
            (64 - Long.numberOfLeadingZeros(data[m]));
        return maxLength;
    }

}

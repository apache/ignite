/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.dev.hash;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * A minimum perfect hash function tool. It needs about 2.2 bits per key.
 */
public class IntPerfectHash {

    /**
     * Large buckets are typically divided into buckets of this size.
     */
    private static final int DIVIDE = 6;

    /**
     * The maximum size of a small bucket (one that is not further split if
     * possible).
     */
    private static final int MAX_SIZE = 12;

    /**
     * The maximum offset for hash functions of small buckets. At most that many
     * hash functions are tried for the given size.
     */
    private static final int[] MAX_OFFSETS = { 0, 0, 8, 18, 47, 123, 319, 831, 2162,
            5622, 14617, 38006, 98815 };

    /**
     * The output value to split the bucket into many (more than 2) smaller
     * buckets.
     */
    private static final int SPLIT_MANY = 3;

    /**
     * The minimum output value for a small bucket of a given size.
     */
    private static final int[] SIZE_OFFSETS = new int[MAX_OFFSETS.length + 1];

    static {
        int last = SPLIT_MANY + 1;
        for (int i = 0; i < MAX_OFFSETS.length; i++) {
            SIZE_OFFSETS[i] = last;
            last += MAX_OFFSETS[i];
        }
        SIZE_OFFSETS[SIZE_OFFSETS.length - 1] = last;
    }

    /**
     * The description of the hash function. Used for calculating the hash of a
     * key.
     */
    private final byte[] data;

    /**
     * Create a hash object to convert keys to hashes.
     *
     * @param data the data returned by the generate method
     */
    public IntPerfectHash(byte[] data) {
        this.data = data;
    }

    /**
     * Get the hash function description.
     *
     * @return the data
     */
    public byte[] getData() {
        return data;
    }

    /**
     * Calculate the hash value for the given key.
     *
     * @param x the key
     * @return the hash value
     */
    public int get(int x) {
        return get(0, x, 0);
    }

    /**
     * Get the hash value for the given key, starting at a certain position and
     * level.
     *
     * @param pos the start position
     * @param x the key
     * @param level the level
     * @return the hash value
     */
    private int get(int pos, int x, int level) {
        int n = readVarInt(data, pos);
        if (n < 2) {
            return 0;
        } else if (n > SPLIT_MANY) {
            int size = getSize(n);
            int offset = getOffset(n, size);
            return hash(x, level, offset, size);
        }
        pos++;
        int split;
        if (n == SPLIT_MANY) {
            split = readVarInt(data, pos);
            pos += getVarIntLength(data, pos);
        } else {
            split = n;
        }
        int h = hash(x, level, 0, split);
        int s;
        int start = pos;
        for (int i = 0; i < h; i++) {
            pos = getNextPos(pos);
        }
        s = getSizeSum(start, pos);
        return s + get(pos, x, level + 1);
    }

    /**
     * Get the position of the next sibling.
     *
     * @param pos the position of this branch
     * @return the position of the next sibling
     */
    private int getNextPos(int pos) {
        int n = readVarInt(data, pos);
        pos += getVarIntLength(data, pos);
        if (n < 2 || n > SPLIT_MANY) {
            return pos;
        }
        int split;
        if (n == SPLIT_MANY) {
            split = readVarInt(data, pos);
            pos += getVarIntLength(data, pos);
        } else {
            split = n;
        }
        for (int i = 0; i < split; i++) {
            pos = getNextPos(pos);
        }
        return pos;
    }

    /**
     * The sum of the sizes between the start and end position.
     *
     * @param start the start position
     * @param end the end position (excluding)
     * @return the sizes
     */
    private int getSizeSum(int start, int end) {
        int s = 0;
        for (int pos = start; pos < end;) {
            int n = readVarInt(data, pos);
            pos += getVarIntLength(data, pos);
            if (n < 2) {
                s += n;
            } else if (n > SPLIT_MANY) {
                s += getSize(n);
            } else if (n == SPLIT_MANY) {
                pos += getVarIntLength(data, pos);
            }
        }
        return s;
    }

    private static void writeSizeOffset(ByteStream out, int size,
            int offset) {
        writeVarInt(out, SIZE_OFFSETS[size] + offset);
    }

    private static int getOffset(int n, int size) {
        return n - SIZE_OFFSETS[size];
    }

    private static int getSize(int n) {
        for (int i = 0; i < SIZE_OFFSETS.length; i++) {
            if (n < SIZE_OFFSETS[i]) {
                return i - 1;
            }
        }
        return 0;
    }

    /**
     * Generate the minimal perfect hash function data from the given list.
     *
     * @param list the data
     * @return the hash function description
     */
    public static byte[] generate(ArrayList<Integer> list) {
        ByteStream out = new ByteStream();
        generate(list, 0, out);
        return out.toByteArray();
    }

    private static void generate(ArrayList<Integer> list, int level, ByteStream out) {
        int size = list.size();
        if (size <= 1) {
            out.write((byte) size);
            return;
        }
        if (level > 32) {
            throw new IllegalStateException("Too many recursions; " +
                    " incorrect universal hash function?");
        }
        if (size <= MAX_SIZE) {
            int maxOffset = MAX_OFFSETS[size];
            int testSize = size;
            nextOffset:
            for (int offset = 0; offset < maxOffset; offset++) {
                int bits = 0;
                for (int i = 0; i < size; i++) {
                    int x = list.get(i);
                    int h = hash(x, level, offset, testSize);
                    if ((bits & (1 << h)) != 0) {
                        continue nextOffset;
                    }
                    bits |= 1 << h;
                }
                writeSizeOffset(out, size, offset);
                return;
            }
        }
        int split;
        if (size > 57 * DIVIDE) {
            split = size / (36 * DIVIDE);
        } else {
            split = (size - 47) / DIVIDE;
        }
        split = Math.max(2, split);
        ArrayList<ArrayList<Integer>> lists = new ArrayList<>(split);
        for (int i = 0; i < split; i++) {
            lists.add(new ArrayList<Integer>(size / split));
        }
        for (int x : list) {
            ArrayList<Integer> l = lists.get(hash(x, level, 0, split));
            l.add(x);
        }
        if (split >= SPLIT_MANY) {
            out.write((byte) SPLIT_MANY);
        }
        writeVarInt(out, split);
        list.clear();
        list.trimToSize();
        for (ArrayList<Integer> s2 : lists) {
            generate(s2, level + 1, out);
        }
    }

    private static int hash(int x, int level, int offset, int size) {
        x += level + offset * 32;
        x = ((x >>> 16) ^ x) * 0x45d9f3b;
        x = ((x >>> 16) ^ x) * 0x45d9f3b;
        x = (x >>> 16) ^ x;
        return Math.abs(x % size);
    }

    private static int writeVarInt(ByteStream out, int x) {
        int len = 0;
        while ((x & ~0x7f) != 0) {
            out.write((byte) (0x80 | (x & 0x7f)));
            x >>>= 7;
            len++;
        }
        out.write((byte) x);
        return ++len;
    }

    private static int readVarInt(byte[] d, int pos) {
        int x = d[pos++];
        if (x >= 0) {
            return x;
        }
        x &= 0x7f;
        for (int s = 7; s < 64; s += 7) {
            int b = d[pos++];
            x |= (b & 0x7f) << s;
            if (b >= 0) {
                break;
            }
        }
        return x;
    }

    private static int getVarIntLength(byte[] d, int pos) {
        int x = d[pos++];
        if (x >= 0) {
            return 1;
        }
        int len = 2;
        for (int s = 7; s < 64; s += 7) {
            int b = d[pos++];
            if (b >= 0) {
                break;
            }
            len++;
        }
        return len;
    }

    /**
     * A stream of bytes.
     */
    static class ByteStream {

        private byte[] data;
        private int pos;

        ByteStream() {
            this.data = new byte[16];
        }

        ByteStream(byte[] data) {
            this.data = data;
        }

        /**
         * Read a byte.
         *
         * @return the byte, or -1.
         */
        int read() {
            return pos < data.length ? (data[pos++] & 255) : -1;
        }

        /**
         * Write a byte.
         *
         * @param value the byte
         */
        void write(byte value) {
            if (pos >= data.length) {
                data = Arrays.copyOf(data, data.length * 2);
            }
            data[pos++] = value;
        }

        /**
         * Get the byte array.
         *
         * @return the byte array
         */
        byte[] toByteArray() {
            return Arrays.copyOf(data, pos);
        }

    }

    /**
     * A helper class for bit arrays.
     */
    public static class BitArray {

        /**
         * Set a bit in the array.
         *
         * @param data the array
         * @param x the bit index
         * @param value the new value
         * @return the bit array (if the passed one was too small)
         */
        public static byte[] setBit(byte[] data, int x, boolean value) {
            int pos = x / 8;
            if (pos >= data.length) {
                data = Arrays.copyOf(data, pos + 1);
            }
            if (value) {
                data[pos] |= 1 << (x & 7);
            } else {
                data[pos] &= 255 - (1 << (x & 7));
            }
            return data;
        }

        /**
         * Get a bit in a bit array.
         *
         * @param data the array
         * @param x the bit index
         * @return the value
         */
        public static boolean getBit(byte[] data, int x) {
            return (data[x / 8] & (1 << (x & 7))) != 0;
        }

        /**
         * Count the number of set bits.
         *
         * @param data the array
         * @return the number of set bits
         */
        public static int countBits(byte[] data) {
            int count = 0;
            for (byte x : data) {
                count += Integer.bitCount(x & 255);
            }
            return count;
        }


    }

}

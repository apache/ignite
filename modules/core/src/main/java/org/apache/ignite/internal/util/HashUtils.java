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

package org.apache.ignite.internal.util;

/**
 * Implement hash function for byte arrays.
 *
 * <p>Based on https://commons.apache.org/proper/commons-codec/jacoco/org.apache.commons.codec.digest/MurmurHash3.java.html.
 */
public class HashUtils {
    private static final long C1 = 0x87c37b91114253d5L;
    private static final long C2 = 0x4cf5ad432745937fL;
    private static final int R1 = 31;
    private static final int R2 = 27;
    private static final int R3 = 33;
    private static final int M = 5;
    private static final int N1 = 0x52dce729;
    private static final int N2 = 0x38495ab5;

    /** No instance methods. */
    private HashUtils() {
    }

    /**
     * Generates 32-bit hash from the byte array with a seed of zero.
     *
     * @param data The input byte array.
     * @return The 32-bit hash.
     */
    public static int hash32(final byte[] data) {
        long hash = hash64(data);

        return (int) (hash ^ (hash >>> 32));
    }

    /**
     * Generates 32-bit hash from the byte array with the given offset, length and seed.
     *
     * @param data   The input byte array.
     * @param offset The first element of array.
     * @param length The length of array.
     * @param seed   The initial seed value.
     * @return The 32-bit hash.
     */
    public static int hash32(final byte[] data, final int offset, final int length, final int seed) {
        long hash = hash64(data, offset, length, seed);

        return (int) (hash ^ (hash >>> 32));
    }

    /**
     * Generates 64-bit hash from the byte array with a seed of zero.
     *
     * @param data The input byte array.
     * @return The 64-bit hash.
     */
    public static long hash64(final byte[] data) {
        return hash64(data, 0, data.length, 0);
    }

    /**
     * Generates 64-bit hash from the byte array with the given offset, length and seed.
     *
     * @param data   The input byte array.
     * @param offset The first element of array.
     * @param length The length of array.
     * @param seed   The initial seed value.
     * @return The 64-bit hash.
     */
    public static long hash64(final byte[] data, final int offset, final int length, final int seed) {
        // Use an unsigned 32-bit integer as the seed
        return hashInternal(data, offset, length, seed & 0xffffffffL);
    }

    /**
     * Generates 64-bit hash from the byte array with the given offset, length and seed.
     *
     * @param data   The input byte array.
     * @param offset The first element of array.
     * @param length The length of array.
     * @param seed   The initial seed value.
     * @return The 64-bit hash.
     */
    private static long hashInternal(final byte[] data, final int offset, final int length, final long seed) {
        long h1 = seed;
        long h2 = seed;
        final int nblocks = length >> 4;

        // body
        for (int i = 0; i < nblocks; i++) {
            final int index = offset + (i << 4);
            long k1 = getLittleEndianLong(data, index);
            long k2 = getLittleEndianLong(data, index + 8);

            // mix functions for k1
            k1 *= C1;
            k1 = Long.rotateLeft(k1, R1);
            k1 *= C2;
            h1 ^= k1;
            h1 = Long.rotateLeft(h1, R2);
            h1 += h2;
            h1 = h1 * M + N1;

            // mix functions for k2
            k2 *= C2;
            k2 = Long.rotateLeft(k2, R3);
            k2 *= C1;
            h2 ^= k2;
            h2 = Long.rotateLeft(h2, R1);
            h2 += h1;
            h2 = h2 * M + N2;
        }

        // tail
        long k1 = 0;
        long k2 = 0;
        final int index = offset + (nblocks << 4);
        switch (offset + length - index) {
            case 15:
                k2 ^= ((long) data[index + 14] & 0xff) << 48;
                // fallthrough
            case 14:
                k2 ^= ((long) data[index + 13] & 0xff) << 40;
                // fallthrough
            case 13:
                k2 ^= ((long) data[index + 12] & 0xff) << 32;
                // fallthrough
            case 12:
                k2 ^= ((long) data[index + 11] & 0xff) << 24;
                // fallthrough
            case 11:
                k2 ^= ((long) data[index + 10] & 0xff) << 16;
                // fallthrough
            case 10:
                k2 ^= ((long) data[index + 9] & 0xff) << 8;
                // fallthrough
            case 9:
                k2 ^= data[index + 8] & 0xff;
                k2 *= C2;
                k2 = Long.rotateLeft(k2, R3);
                k2 *= C1;
                h2 ^= k2;

                // fallthrough
            case 8:
                k1 ^= ((long) data[index + 7] & 0xff) << 56;
                // fallthrough
            case 7:
                k1 ^= ((long) data[index + 6] & 0xff) << 48;
                // fallthrough
            case 6:
                k1 ^= ((long) data[index + 5] & 0xff) << 40;
                // fallthrough
            case 5:
                k1 ^= ((long) data[index + 4] & 0xff) << 32;
                // fallthrough
            case 4:
                k1 ^= ((long) data[index + 3] & 0xff) << 24;
                // fallthrough
            case 3:
                k1 ^= ((long) data[index + 2] & 0xff) << 16;
                // fallthrough
            case 2:
                k1 ^= ((long) data[index + 1] & 0xff) << 8;
                // fallthrough
            case 1:
                k1 ^= data[index] & 0xff;
                k1 *= C1;
                k1 = Long.rotateLeft(k1, R1);
                k1 *= C2;
                h1 ^= k1;
                // fallthrough
            default:
                break;
        }

        // finalization
        h1 ^= length;
        h2 ^= length;

        h1 += h2;
        h2 += h1;

        h1 = fmix64(h1);
        h2 = fmix64(h2);

        return h1 + h2;
    }

    /**
     * Gets the little-endian long from 8 bytes starting at the specified index.
     *
     * @param data  The data.
     * @param index The index.
     * @return The little-endian long.
     */
    private static long getLittleEndianLong(final byte[] data, final int index) {
        return (((long) data[index] & 0xff))
                | (((long) data[index + 1] & 0xff) << 8)
                | (((long) data[index + 2] & 0xff) << 16)
                | (((long) data[index + 3] & 0xff) << 24)
                | (((long) data[index + 4] & 0xff) << 32)
                | (((long) data[index + 5] & 0xff) << 40)
                | (((long) data[index + 6] & 0xff) << 48)
                | (((long) data[index + 7] & 0xff) << 56);
    }

    /**
     * Performs the final avalanche mix step of the 64-bit hash function.
     *
     * @param hash The current hash.
     * @return The final hash.
     */
    private static long fmix64(long hash) {
        hash ^= (hash >>> 33);
        hash *= 0xff51afd7ed558ccdL;
        hash ^= (hash >>> 33);
        hash *= 0xc4ceb9fe1a85ec53L;
        hash ^= (hash >>> 33);
        return hash;
    }
}

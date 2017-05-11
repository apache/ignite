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

package org.apache.ignite.ml.math;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * This is a very fast, non-cryptographic hash suitable for general hash-based lookup.
 *
 * See http://murmurhash.googlepages.com/ for mre details.
 */
public class MurmurHash {
    /** Hide it. */
    private MurmurHash() {
    }

    /**
     * This produces exactly the same hash values as the final C+ version of MurmurHash3 and is
     * thus suitable for producing the same hash values across platforms.
     *
     * The 32 bit x86 version of this hash should be the fastest variant for relatively short keys like IDs.
     *
     * Note - The x86 and x64 versions do _not_ produce the same results, as the algorithms are
     * optimized for their respective platforms.
     *
     * See also http://github.com/yonik/java_util for future updates to this method.
     *
     * @param data
     * @param off
     * @param len
     * @param seed
     * @return 32 bit hash platform compatible with C++ MurmurHash3 implementation on x86.
     */
    public static int hash3X86(byte[] data, int off, int len, int seed) {
        int c1 = 0xcc9e2d51;
        int c2 = 0x1b873593;

        int h1 = seed;
        int roundedEnd = off + (len & 0xfffffffc);  // Round down to 4 byte block.

        for (int i = off; i < roundedEnd; i += 4) {
            int k1 = (data[i] & 0xff) | ((data[i + 1] & 0xff) << 8) | ((data[i + 2] & 0xff) << 16) | (data[i + 3] << 24);

            k1 *= c1;
            k1 = (k1 << 15) | (k1 >>> 17);
            k1 *= c2;

            h1 ^= k1;
            h1 = (h1 << 13) | (h1 >>> 19);
            h1 = h1 * 5 + 0xe6546b64;
        }

        // Tail.
        int k1 = 0;

        switch (len & 0x03) {
            case 3:
                k1 = (data[roundedEnd + 2] & 0xff) << 16;
                // Fallthrough - WTF?
            case 2:
                k1 |= (data[roundedEnd + 1] & 0xff) << 8;
                // Fallthrough - WTF?
            case 1:
                k1 |= data[roundedEnd] & 0xff;
                k1 *= c1;
                k1 = (k1 << 15) | (k1 >>> 17);
                k1 *= c2;
                h1 ^= k1;
            default:
        }

        // Finalization.
        h1 ^= len;

        h1 ^= h1 >>> 16;
        h1 *= 0x85ebca6b;
        h1 ^= h1 >>> 13;
        h1 *= 0xc2b2ae35;
        h1 ^= h1 >>> 16;

        return h1;
    }

    /**
     * Hashes an int.
     *
     * @param data The int to hash.
     * @param seed The seed for the hash.
     * @return The 32 bit hash of the bytes in question.
     */
    public static int hash(int data, int seed) {
        byte[] arr = new byte[] {
            (byte)(data >>> 24),
            (byte)(data >>> 16),
            (byte)(data >>> 8),
            (byte)data
        };

        return hash(ByteBuffer.wrap(arr), seed);
    }

    /**
     * Hashes bytes in an array.
     *
     * @param data The bytes to hash.
     * @param seed The seed for the hash.
     * @return The 32 bit hash of the bytes in question.
     */
    public static int hash(byte[] data, int seed) {
        return hash(ByteBuffer.wrap(data), seed);
    }

    /**
     * Hashes bytes in part of an array.
     *
     * @param data The data to hash.
     * @param off Where to start munging.
     * @param len How many bytes to process.
     * @param seed The seed to start with.
     * @return The 32-bit hash of the data in question.
     */
    public static int hash(byte[] data, int off, int len, int seed) {
        return hash(ByteBuffer.wrap(data, off, len), seed);
    }

    /**
     * Hashes the bytes in a buffer from the current position to the limit.
     *
     * @param buf The bytes to hash.
     * @param seed The seed for the hash.
     * @return The 32 bit murmur hash of the bytes in the buffer.
     */
    public static int hash(ByteBuffer buf, int seed) {
        ByteOrder byteOrder = buf.order();
        buf.order(ByteOrder.LITTLE_ENDIAN);

        int m = 0x5bd1e995;
        int r = 24;

        int h = seed ^ buf.remaining();

        while (buf.remaining() >= 4) {
            int k = buf.getInt();

            k *= m;
            k ^= k >>> r;
            k *= m;

            h *= m;
            h ^= k;
        }

        if (buf.remaining() > 0) {
            ByteBuffer finish = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);

            finish.put(buf).rewind();

            h ^= finish.getInt();
            h *= m;
        }

        h ^= h >>> 13;
        h *= m;
        h ^= h >>> 15;

        buf.order(byteOrder);

        return h;
    }

    /**
     * @param data
     * @param seed
     */
    public static long hash64A(byte[] data, int seed) {
        return hash64A(ByteBuffer.wrap(data), seed);
    }

    /**
     * @param data
     * @param off
     * @param len
     * @param seed
     */
    public static long hash64A(byte[] data, int off, int len, int seed) {
        return hash64A(ByteBuffer.wrap(data, off, len), seed);
    }

    /**
     * @param buf
     * @param seed
     */
    public static long hash64A(ByteBuffer buf, int seed) {
        ByteOrder byteOrder = buf.order();
        buf.order(ByteOrder.LITTLE_ENDIAN);

        long m = 0xc6a4a7935bd1e995L;
        int r = 47;

        long h = seed ^ (buf.remaining() * m);

        while (buf.remaining() >= 8) {
            long k = buf.getLong();

            k *= m;
            k ^= k >>> r;
            k *= m;

            h ^= k;
            h *= m;
        }

        if (buf.remaining() > 0) {
            ByteBuffer finish = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN);

            finish.put(buf).rewind();

            h ^= finish.getLong();
            h *= m;
        }

        h ^= h >>> r;
        h *= m;
        h ^= h >>> r;

        buf.order(byteOrder);

        return h;
    }
}

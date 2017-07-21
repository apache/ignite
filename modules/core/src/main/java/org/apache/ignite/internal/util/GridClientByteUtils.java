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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.UUID;

/**
 * Primitive to byte array and backward conversions.
 */
public abstract class GridClientByteUtils {
    /**
     * Converts primitive {@code short} type to byte array.
     *
     * @param s Short value.
     * @return Array of bytes.
     */
    public static byte[] shortToBytes(short s) {
        return toBytes(s, new byte[2], 0, 2);
    }

    /**
     * Converts primitive {@code int} type to byte array.
     *
     * @param i Integer value.
     * @return Array of bytes.
     */
    public static byte[] intToBytes(int i) {
        return toBytes(i, new byte[4], 0, 4);
    }

    /**
     * Converts primitive {@code long} type to byte array.
     *
     * @param l Long value.
     * @return Array of bytes.
     */
    public static byte[] longToBytes(long l) {
        return toBytes(l, new byte[8], 0, 8);
    }

    /**
     * Converts primitive {@code short} type to byte array and stores it in specified
     * byte array.
     *
     * @param s Short value.
     * @param bytes Array of bytes.
     * @param off Offset in {@code bytes} array.
     * @return Number of bytes overwritten in {@code bytes} array.
     */
    public static int shortToBytes(short s, byte[] bytes, int off) {
        toBytes(s, bytes, off, 2);

        return 2;
    }

    /**
     * Converts primitive {@code int} type to byte array and stores it in specified
     * byte array.
     *
     * @param i Integer value.
     * @param bytes Array of bytes.
     * @param off Offset in {@code bytes} array.
     * @return Number of bytes overwritten in {@code bytes} array.
     */
    public static int intToBytes(int i, byte[] bytes, int off) {
        toBytes(i, bytes, off, 4);

        return 4;
    }

    /**
     * Converts primitive {@code long} type to byte array and stores it in specified
     * byte array.
     *
     * @param l Long value.
     * @param bytes Array of bytes.
     * @param off Offset in {@code bytes} array.
     * @return Number of bytes overwritten in {@code bytes} array.
     */
    public static int longToBytes(long l, byte[] bytes, int off) {
        toBytes(l, bytes, off, 8);

        return 8;
    }

    /**
     * Converts an UUID to byte array.
     *
     * @param uuid UUID value.
     * @return Encoded into byte array {@link UUID}.
     */
    public static byte[] uuidToBytes(UUID uuid) {
        byte[] bytes = new byte[(Long.SIZE >> 3) * 2];

        uuidToBytes(uuid, bytes, 0);

        return bytes;
    }

    /**
     * Converts {@code UUID} type to byte array and stores it in specified byte array.
     *
     * @param uuid UUID to convert.
     * @param bytes Array of bytes.
     * @param off Offset in {@code bytes} array.
     * @return Number of bytes overwritten in {@code bytes} array.
     */
    public static int uuidToBytes(UUID uuid, byte[] bytes, int off) {
        ByteBuffer buf = ByteBuffer.wrap(bytes, off, 16);

        buf.order(ByteOrder.BIG_ENDIAN);

        if (uuid != null) {
            buf.putLong(uuid.getMostSignificantBits());
            buf.putLong(uuid.getLeastSignificantBits());
        }
        else {
            buf.putLong(0);
            buf.putLong(0);
        }

        return 16;
    }

    /**
     * Constructs {@code short} from byte array.
     *
     * @param bytes Array of bytes.
     * @param off Offset in {@code bytes} array.
     * @return Short value.
     */
    public static short bytesToShort(byte[] bytes, int off) {
        return (short)fromBytes(bytes, off, 2);
    }

    /**
     * Constructs {@code int} from byte array.
     *
     * @param bytes Array of bytes.
     * @param off Offset in {@code bytes} array.
     * @return Integer value.
     */
    public static int bytesToInt(byte[] bytes, int off) {
        return (int)fromBytes(bytes, off, 4);
    }

    /**
     * Constructs {@code long} from byte array.
     *
     * @param bytes Array of bytes.
     * @param off Offset in {@code bytes} array.
     * @return Long value.
     */
    public static long bytesToLong(byte[] bytes, int off) {
        return fromBytes(bytes, off, 8);
    }

    /**
     * Constructs {@link UUID} from byte array.
     *
     * @param bytes Array of bytes.
     * @param off Offset in {@code bytes} array.
     * @return UUID value.
     */
    public static UUID bytesToUuid(byte[] bytes, int off) {
        ByteBuffer buf = ByteBuffer.wrap(bytes, off, 16);

        buf.order(ByteOrder.BIG_ENDIAN);

        long most = buf.getLong();
        long least = buf.getLong();

        return most != 0 && least != 0 ? new UUID(most, least) : null;
    }

    /**
     * Converts primitive {@code long} type to byte array and stores it in specified
     * byte array. The highest byte in the value is the first byte in result array.
     *
     * @param l Unsigned long value.
     * @param bytes Bytes array to write result to.
     * @param off Offset in the target array to write result to.
     * @param limit Limit of bytes to write into output.
     * @return Number of bytes overwritten in {@code bytes} array.
     */
    private static byte[] toBytes(long l, byte[] bytes, int off, int limit) {
        assert bytes != null;
        assert limit <= 8;
        assert bytes.length >= off + limit;

        for (int i = limit - 1; i >= 0; i--) {
            bytes[off + i] = (byte)(l & 0xFF);
            l >>>= 8;
        }

        return bytes;
    }

    /**
     * Constructs {@code long} from byte array. The first byte in array is the highest byte in result.
     *
     * @param bytes Array of bytes.
     * @param off Offset in {@code bytes} array.
     * @param limit Amount of bytes to use in the source array.
     * @return Unsigned long value.
     */
    private static long fromBytes(byte[] bytes, int off, int limit) {
        assert bytes != null;
        assert limit <= 8;
        assert bytes.length >= off + limit;

        long res = 0;

        for (int i = 0; i < limit; i++) {
            res <<= 8;
            res |= bytes[off + i] & 0xFF;
        }

        return res;
    }
}
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

package org.apache.ignite.internal.processors.query.h2.database.util;

import java.math.BigDecimal;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.util.GridUnsafe;
import org.h2.util.MathUtils;
import org.h2.value.Value;

/**
 *
 */
public class CompareUtils {
    /** */
    private static final int UTF_8_MIN_2_BYTES = 0x80;

    /** */
    private static final int UTF_8_MIN_3_BYTES = 0x800;

    /** */
    private static final int UTF_8_MIN_4_BYTES = 0x10000;

    /** */
    private static final int UTF_8_MAX_CODE_POINT = 0x10ffff;

    /** */
    private static final BigDecimal MAX_LONG_DECIMAL = BigDecimal.valueOf(Long.MAX_VALUE);

    /** */
    private static final BigDecimal MIN_LONG_DECIMAL = BigDecimal.valueOf(Long.MIN_VALUE);

    /**
     * @param x Value.
     * @return Byte value.
     */
    public static byte convertToByte(long x) {
        if (x > Byte.MAX_VALUE || x < Byte.MIN_VALUE)
            throw new IgniteException("Numeric value out of range: " + x);

        return (byte) x;
    }

    /**
     * @param x Value.
     * @return Short value.
     */
    public static short convertToShort(long x) {
        if (x > Short.MAX_VALUE || x < Short.MIN_VALUE)
            throw new IgniteException("Numeric value out of range: " + x);

        return (short) x;
    }

    /**
     * @param x Value.
     * @return Int value.
     */
    public static int convertToInt(long x) {
        if (x > Integer.MAX_VALUE || x < Integer.MIN_VALUE)
            throw new IgniteException("Numeric value out of range: " + x);

        return (int) x;
    }

    /**
     * @param x Value.
     * @return Long value.
     */
    public static long convertToLong(double x) {
        if (x > Long.MAX_VALUE || x < Long.MIN_VALUE)
            throw new IgniteException("Numeric value out of range: " + x);

        return Math.round(x);
    }

    /**
     * @param x Value.
     * @return Long value.
     */
    public static long convertToLong(BigDecimal x) {
        if (x.compareTo(MAX_LONG_DECIMAL) > 0 || x.compareTo(MIN_LONG_DECIMAL) < 0)
            throw new IgniteException("Numeric value out of range: " + x);

        return x.setScale(0, BigDecimal.ROUND_HALF_UP).longValue();
    }

    /**
     * @param v Value1.
     * @param val Value2.
     * @return Compare result.
     */
    public static int compareBoolean(boolean v, Value val) {
        boolean v2 = val.getBoolean();

        return (v == v2) ? 0 : (v ? 1 : -1);
    }

    /**
     * @param v Value1.
     * @param val Value2.
     * @return Compare result.
     */
    public static int compareByte(byte v, Value val) {
        byte v2 = val.getByte();

        return MathUtils.compareInt(v, v2);
    }

    /**
     * @param val1Addr First string UTF-8 bytes address.
     * @param val1Len Number of bytes in first string.
     * @param val2Bytes Second string bytes.
     * @param val2Off Second string offset.
     * @param val2Len Number of bytes in second string.
     * @return Compare result.
     * @throws IgniteCheckedException In case of error.
     */
    public static int compareUtf8(long val1Addr,
                                  int val1Len,
                                  byte[] val2Bytes,
                                  int val2Off,
                                  int val2Len) throws IgniteCheckedException {
        int len = Math.min(val1Len, val2Len);

        for (int i = 0; i < len; i++) {
            int b1 = GridUnsafe.getByte(val1Addr + i) & 0xFF;
            int b2 = val2Bytes[val2Off + i] & 0xFF;

            if (b1 != b2)
                return b1 > b2 ? 1 : -1;
        }

        return Integer.compare(val1Len, val2Len);
    }

    /**
     * @param addr UTF-8 bytes address.
     * @param len Number of bytes to decode.
     * @param str String to compare with.
     * @return Compare result.
     * @throws IgniteCheckedException In case of error.
     */
    public static int compareUtf8(long addr, int len, String str) throws IgniteCheckedException {
        int pos = 0;

        int cntr = 0;

        int strLen = str.length();

        // ASCII only optimized loop.
        while (pos < len) {
            byte ch = GridUnsafe.getByte(addr + pos);

            if (ch >= 0) {
                char c0 = (char)ch;

                if (cntr < strLen) {
                    char c1 = str.charAt(cntr);

                    if (c0 != c1)
                        return c0 - c1;
                }
                else
                    return 1;

                cntr++;

                pos++;
            }
            else
                break;
        }

        // TODO: check index bounds.

        while (pos < len) {
            int ch = GridUnsafe.getByte(addr + pos++) & 0xff;

            // Convert UTF-8 to 21-bit codepoint.
            if (ch < 0x80) {
                // 0xxxxxxx -- length 1.
            }
            else if (ch < 0xc0) {
                // 10xxxxxx -- illegal!
                throw new IgniteException("Illegal UTF-8 sequence.");
            }
            else if (ch < 0xe0) {
                // 110xxxxx 10xxxxxx
                ch = ((ch & 0x1f) << 6);

                checkByte(GridUnsafe.getByte(addr + pos));

                ch |= (GridUnsafe.getByte(addr + pos++) & 0x3f);

                checkMinimal(ch, UTF_8_MIN_2_BYTES);
            }
            else if (ch < 0xf0) {
                // 1110xxxx 10xxxxxx 10xxxxxx
                ch = ((ch & 0x0f) << 12);

                checkByte(GridUnsafe.getByte(addr + pos));

                ch |= ((GridUnsafe.getByte(addr + pos++) & 0x3f) << 6);

                checkByte(GridUnsafe.getByte(addr + pos));

                ch |= (GridUnsafe.getByte(addr + pos++) & 0x3f);

                checkMinimal(ch, UTF_8_MIN_3_BYTES);
            }
            else if (ch < 0xf8) {
                // 11110xxx 10xxxxxx 10xxxxxx 10xxxxxx
                ch = ((ch & 0x07) << 18);

                checkByte(GridUnsafe.getByte(addr + pos));

                ch |= ((GridUnsafe.getByte(addr + pos++) & 0x3f) << 12);

                checkByte(GridUnsafe.getByte(addr + pos));

                ch |= ((GridUnsafe.getByte(addr + pos++) & 0x3f) << 6);

                checkByte(GridUnsafe.getByte(addr + pos));

                ch |= (GridUnsafe.getByte(addr + pos++) & 0x3f);

                checkMinimal(ch, UTF_8_MIN_4_BYTES);
            }
            else
                throw new IgniteException("Illegal UTF-8 sequence.");

            if (ch > UTF_8_MAX_CODE_POINT)
                throw new IgniteException("Illegal UTF-8 sequence.");

            // Convert 21-bit codepoint to Java chars:
            //   0..ffff are represented directly as a single char
            //   10000..10ffff are represented as a "surrogate pair" of two chars
            if (ch > 0xffff) {
                // Use a surrogate pair to represent it.
                ch -= 0x10000;  // ch is now 0..fffff (20 bits)

                char c0 = (char)(0xd800 + (ch >> 10));   // Top 10 bits.

                if (cntr < strLen) {
                    char c1 = str.charAt(cntr);

                    if (c0 != c1)
                        return c0 - c1;
                }
                else
                    return 1;

                cntr++;

                c0 = (char)(0xdc00 + (ch & 0x3ff)); // Bottom 10 bits.

                if (cntr < strLen) {
                    char c1 = str.charAt(cntr);

                    if (c0 != c1)
                        return c0 - c1;
                }
                else
                    return 1;

                cntr++;
            }
            else if (ch >= 0xd800 && ch < 0xe000)
                // Not allowed to encode the surrogate range directly.
                throw new IgniteException("Illegal UTF-8 sequence.");
            else {
                // Normal case.
                char c0 = (char)ch;

                if (cntr < strLen) {
                    char c1 = str.charAt(cntr);

                    if (c0 != c1)
                        return c0 - c1;
                }
                else
                    return 1;

                cntr++;
            }
        }

        // Check if we ran past the end without seeing an exception.
        if (pos > len)
            throw new IgniteException("Illegal UTF-8 sequence.");

        return cntr - strLen;
    }

    /**
     * @param ch UTF-8 byte.
     * @throws IgniteException In case of error.
     */
    private static void checkByte(int ch) {
        if ((ch & 0xc0) != 0x80)
            throw new IgniteException("Illegal UTF-8 sequence.");
    }

    /**
     * @param ch UTF-8 byte.
     * @param minVal Minimum value.
     * @throws IgniteException In case of error.
     */
    private static void checkMinimal(int ch, int minVal) {
        if (ch >= minVal)
            return;

        throw new IgniteException("Illegal UTF-8 sequence.");
    }
}

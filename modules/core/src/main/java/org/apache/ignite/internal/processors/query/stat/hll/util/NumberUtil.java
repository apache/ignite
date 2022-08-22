/*
 * Copyright 2013 Aggregate Knowledge, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.internal.processors.query.stat.hll.util;

/**
 * A collection of utilities to work with numbers.
 *
 * @author rgrzywinski
 */
public class NumberUtil {
    // loge(2) (log-base e of 2)
    public static final double LOGE_2 = 0.6931471805599453;

    // ************************************************************************
    /**
     * Computes the <code>log2</code> (log-base-two) of the specified value.
     *
     * @param  value the <code>double</code> for which the <code>log2</code> is
     *         desired.
     * @return the <code>log2</code> of the specified value
     */
    public static double log2(final double value) {
        // REF:  http://en.wikipedia.org/wiki/Logarithmic_scale (conversion of bases)
        return Math.log(value) / LOGE_2;
    }

    // ========================================================================
    // the hex characters
    private static final char[] HEX = { '0', '1', '2', '3', '4', '5', '6', '7',
        '8', '9', 'A', 'B', 'C', 'D', 'E', 'F' };

    // ------------------------------------------------------------------------
    /**
     * Converts the specified array of <code>byte</code>s into a string of
     * hex characters (low <code>byte</code> first).
     *
     * @param  bytes the array of <code>byte</code>s that are to be converted.
     *         This cannot be <code>null</code> though it may be empty.
     * @param  offset the offset in <code>bytes</code> at which the bytes will
     *         be taken.  This cannot be negative and must be less than
     *         <code>bytes.length - 1</code>.
     * @param  cnt the number of bytes to be retrieved from the specified array.
     *         This cannot be negative.  If greater than <code>bytes.length - offset</code>
     *         then that value is used.
     * @return a string of at most <code>count</code> characters that represents
     *         the specified byte array in hex.  This will never be <code>null</code>
     *         though it may be empty if <code>bytes</code> is empty or <code>count</code>
     *         is zero.
     * @throws IllegalArgumentException if <code>offset</code> is greater than
     *         or equal to <code>bytes.length</code>.
     * @see #fromHex(String, int, int)
     */
    public static String toHex(final byte[] bytes, final int offset, final int cnt) {
        if (offset >= bytes.length) throw new IllegalArgumentException("Offset is greater than the length (" + offset +
            " >= " + bytes.length + ").")/*by contract*/;
        final int byteCnt = Math.min( (bytes.length - offset), cnt);
        final int upperBound = byteCnt + offset;

        final char[] chars = new char[byteCnt * 2/*two chars per byte*/];
        int charIdx = 0;

        for (int i = offset; i < upperBound; i++) {
            final byte val = bytes[i];
            chars[charIdx++] = HEX[(val >>> 4) & 0x0F];
            chars[charIdx++] = HEX[val & 0x0F];
        }

        return new String(chars);
    }

    /**
     * Converts the specified array of hex characters into an array of <code>byte</code>s
     * (low <code>byte</code> first).
     *
     * @param  string the string of hex characters to be converted into <code>byte</code>s.
     *         This cannot be <code>null</code> though it may be blank.
     * @param  offset the offset in the string at which the characters will be
     *         taken.  This cannot be negative and must be less than <code>string.length() - 1</code>.
     * @param  count the number of characters to be retrieved from the specified
     *         string.  This cannot be negative and must be divisible by two
     *         (since there are two characters per <code>byte</code>).
     * @return the array of <code>byte</code>s that were converted from the
     *         specified string (in the specified range).  This will never be
     *         <code>null</code> though it may be empty if <code>string</code>
     *         is empty or <code>count</code> is zero.
     * @throws IllegalArgumentException if <code>offset</code> is greater than
     *         or equal to <code>string.length()</code> or if <code>count</code>
     *         is not divisible by two.
     * @see #toHex(byte[], int, int)
     */
    public static byte[] fromHex(final String string, final int offset, final int count) {
        if (offset >= string.length())
            throw new IllegalArgumentException("Offset is greater than the length (" + offset + " >= " +
                string.length() + ").")/*by contract*/;

        if ( (count & 0x01) != 0)
            throw new IllegalArgumentException("Count is not divisible by two (" + count + ").")/*by contract*/;

        final int charCount = Math.min((string.length() - offset), count);
        final int upperBound = offset + charCount;

        final byte[] bytes = new byte[charCount >>> 1/*aka /2*/];
        int byteIndex = 0/*beginning*/;

        for (int i = offset; i < upperBound; i += 2)
            bytes[byteIndex++] = (byte)(( (digit(string.charAt(i)) << 4) | digit(string.charAt(i + 1))) & 0xFF);

        return bytes;
    }

    // ------------------------------------------------------------------------
    /**
     * @param  character a hex character to be converted to a <code>byte</code>.
     *         This cannot be a character other than [a-fA-F0-9].
     * @return the value of the specified character.  This will be a value <code>0</code>
     *         through <code>15</code>.
     * @throws IllegalArgumentException if the specified character is not in
     *         [a-fA-F0-9]
     */
    private static final int digit(final char character) {
        switch (character) {
            case '0':
                return 0;
            case '1':
                return 1;
            case '2':
                return 2;
            case '3':
                return 3;
            case '4':
                return 4;
            case '5':
                return 5;
            case '6':
                return 6;
            case '7':
                return 7;
            case '8':
                return 8;
            case '9':
                return 9;
            case 'a':
            case 'A':
                return 10;
            case 'b':
            case 'B':
                return 11;
            case 'c':
            case 'C':
                return 12;
            case 'd':
            case 'D':
                return 13;
            case 'e':
            case 'E':
                return 14;
            case 'f':
            case 'F':
                return 15;

            default:
                throw new IllegalArgumentException("Character is not in [a-fA-F0-9] ('" + character + "').");
        }
    }
}

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
 * A collection of bit utilities.
 *
 * @author rgrzywinski
 */
public class BitUtil {
    /**
     * The set of least-significant bits for a given <code>byte</code>.  <code>-1</code>
     * is used if no bits are set (so as to not be confused with "index of zero"
     * meaning that the least significant bit is the 0th (1st) bit).
     *
     * @see #leastSignificantBit(long)
     */
    private static final int[] LEAST_SIGNIFICANT_BIT = {
        -1, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
        4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
        5, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
        4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
        6, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
        4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
        5, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
        4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
        7, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
        4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
        5, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
        4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
        6, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
        4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
        5, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
        4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0
    };

    /**
     * Computes the least-significant bit of the specified <code>long</code>
     * that is set to <code>1</code>. Zero-indexed.
     *
     * @param  value the <code>long</code> whose least-significant bit is desired.
     * @return the least-significant bit of the specified <code>long</code>.
     *         <code>-1</code> is returned if there are no bits set.
     */
    // REF:  http://stackoverflow.com/questions/757059/position-of-least-significant-bit-that-is-set
    // REF:  http://www-graphics.stanford.edu/~seander/bithacks.html
    public static int leastSignificantBit(final long value) {
        if (value == 0L) return -1/*by contract*/;
        if ((value & 0xFFL) != 0) return LEAST_SIGNIFICANT_BIT[(int)( (value >>> 0) & 0xFF)] + 0;
        if ((value & 0xFFFFL) != 0) return LEAST_SIGNIFICANT_BIT[(int)( (value >>> 8) & 0xFF)] + 8;
        if ((value & 0xFFFFFFL) != 0) return LEAST_SIGNIFICANT_BIT[(int)( (value >>> 16) & 0xFF)] + 16;
        if ((value & 0xFFFFFFFFL) != 0) return LEAST_SIGNIFICANT_BIT[(int)( (value >>> 24) & 0xFF)] + 24;
        if ((value & 0xFFFFFFFFFFL) != 0) return LEAST_SIGNIFICANT_BIT[(int)( (value >>> 32) & 0xFF)] + 32;
        if ((value & 0xFFFFFFFFFFFFL) != 0) return LEAST_SIGNIFICANT_BIT[(int)( (value >>> 40) & 0xFF)] + 40;
        if ((value & 0xFFFFFFFFFFFFFFL) != 0) return LEAST_SIGNIFICANT_BIT[(int)( (value >>> 48) & 0xFF)] + 48;
        return LEAST_SIGNIFICANT_BIT[(int)( (value >>> 56) & 0xFFL)] + 56;
    }
}

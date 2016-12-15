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

package org.apache.ignite.hadoop.io;

import com.google.common.primitives.Longs;
import com.google.common.primitives.UnsignedBytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.ignite.internal.processors.hadoop.io.OffheapRawMemory;
import org.apache.ignite.internal.processors.hadoop.io.PartiallyOffheapRawComparatorEx;
import org.apache.ignite.internal.util.GridUnsafe;

/**
 * Partial raw comparator for {@link Text} data type.
 * <p>
 * Implementation is borrowed from {@code org.apache.hadoop.io.FastByteComparisons} and adopted to Ignite
 * infrastructure.
 */
public class TextPartiallyRawComparator implements PartiallyRawComparator<Text>, PartiallyOffheapRawComparatorEx<Text> {
    /** {@inheritDoc} */
    @Override public int compare(Text val1, RawMemory val2Buf) {
        if (val2Buf instanceof OffheapRawMemory) {
            OffheapRawMemory val2Buf0 = (OffheapRawMemory)val2Buf;

            return compare(val1, val2Buf0.pointer(), val2Buf0.length());
        }
        else
            throw new UnsupportedOperationException("Text can be compared only with offheap memory.");
    }

    /** {@inheritDoc} */
    @Override public int compare(Text val1, long val2Ptr, int val2Len) {
        int len2 = WritableUtils.decodeVIntSize(GridUnsafe.getByte(val2Ptr));

        return compareBytes(val1.getBytes(), val1.getLength(), val2Ptr + len2, val2Len - len2);
    }

    /**
     * Internal comparison routine.
     *
     * @param buf1 Bytes 1.
     * @param len1 Length 1.
     * @param ptr2 Pointer 2.
     * @param len2 Length 2.
     * @return Result.
     */
    @SuppressWarnings("SuspiciousNameCombination")
    private static int compareBytes(byte[] buf1, int len1, long ptr2, int len2) {
        int minLength = Math.min(len1, len2);

        int minWords = minLength / Longs.BYTES;

        for (int i = 0; i < minWords * Longs.BYTES; i += Longs.BYTES) {
            long lw = GridUnsafe.getLong(buf1, GridUnsafe.BYTE_ARR_OFF + i);
            long rw = GridUnsafe.getLong(ptr2 + i);

            long diff = lw ^ rw;

            if (diff != 0) {
                if (GridUnsafe.BIG_ENDIAN)
                    return (lw + Long.MIN_VALUE) < (rw + Long.MIN_VALUE) ? -1 : 1;

                // Use binary search
                int n = 0;
                int y;
                int x = (int) diff;

                if (x == 0) {
                    x = (int) (diff >>> 32);

                    n = 32;
                }

                y = x << 16;

                if (y == 0)
                    n += 16;
                else
                    x = y;

                y = x << 8;

                if (y == 0)
                    n += 8;

                return (int) (((lw >>> n) & 0xFFL) - ((rw >>> n) & 0xFFL));
            }
        }

        // The epilogue to cover the last (minLength % 8) elements.
        for (int i = minWords * Longs.BYTES; i < minLength; i++) {
            int res = UnsignedBytes.compare(buf1[i], GridUnsafe.getByte(ptr2 + i));

            if (res != 0)
                return res;
        }

        return len1 - len2;
    }
}

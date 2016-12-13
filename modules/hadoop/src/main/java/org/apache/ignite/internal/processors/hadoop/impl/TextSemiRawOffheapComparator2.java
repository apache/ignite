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

package org.apache.ignite.internal.processors.hadoop.impl;

import com.google.common.primitives.Longs;
import com.google.common.primitives.UnsignedBytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.ignite.internal.processors.hadoop.shuffle.SemiRawOffheapComparator;
import org.apache.ignite.internal.util.GridUnsafe;

/**
 * Semi-raw offheap comparator for Text.
 */
public class TextSemiRawOffheapComparator2 implements SemiRawOffheapComparator<Text> {
    /** {@inheritDoc} */
    @Override public int compare(Text key1, long ptr2, int len2) {
        byte[] bytes1 = key1.getBytes();
        int len1 = key1.getLength();

        int len22 = WritableUtils.decodeVIntSize(GridUnsafe.getByte(ptr2));

        return compareBytes(bytes1, 0, len1, ptr2 + len22, len2 - len22);
    }

    /**
     * Internal comparison routine.
     *
     * @param buf1 Bytes 1.
     * @param off1 Offset 1.
     * @param len1 Length 1.
     * @param ptr2 Pointer 2.
     * @param len2 Length 2.
     * @return Result.
     */
    static int compareBytes(byte[] buf1, int off1, int len1, long ptr2, int len2) {
        int minLength = Math.min(len1, len2);
        int minWords = minLength / Longs.BYTES;

        /*
         * Compare 8 bytes at a time. Benchmarking shows comparing 8 bytes at a
         * time is no slower than comparing 4 bytes at a time even on 32-bit.
         * On the other hand, it is substantially faster on 64-bit.
         */
        for (int i = 0; i < minWords * Longs.BYTES; i += Longs.BYTES) {
            long lw = GridUnsafe.getLong(buf1, GridUnsafe.BYTE_ARR_OFF + off1 + i);
            long rw = GridUnsafe.getLong(ptr2 + i);

            long diff = lw ^ rw;

            if (diff != 0) {
                if (GridUnsafe.BIG_ENDIAN)
                    throw new UnsupportedOperationException("BIG ENDIAN is not supported yet, sorry dude!");

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
            int res = UnsignedBytes.compare(buf1[off1 + i], GridUnsafe.getByte(ptr2 + i));

            if (res != 0)
                return res;
        }

        return len1 - len2;
    }
}

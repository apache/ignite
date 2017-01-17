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

import org.apache.ignite.internal.util.typedef.internal.U;

import java.io.Serializable;
import java.util.Arrays;

/**
 * Holds set of integers.
 * <p/>
 * Note: implementation is not thread safe.
 */
public class GridIntSet implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Segment index. Sorted in ascending order by segment id. */
    private short[] segIds = new short[0]; // TODO FIXME allow preallocate.

    private short segmentsUsed = 0;

    private short[][] segments = new short[0][0];

    public static final short SEGMENT_SIZE = 1024;

    public static final int SHORT_BITS = Short.SIZE;

    public static final int MAX_WORDS = SEGMENT_SIZE / SHORT_BITS;

    public static final int WORD_SHIFT_BITS = 4;

    public static final int MAX_SEGMENTS = Short.MAX_VALUE;

    public GridIntSet() {
    }

    public GridIntSet(int first, int cnt) {
    }

    public boolean add(int v) {
        U.debug("Add " + v);

        short segId = (short) (v / SEGMENT_SIZE);

        short inc = (short) (v - segId * SEGMENT_SIZE); // TODO use modulo bit hack.

        return segmentAdd(segmentIndex(segId), inc);
    }

    /** Add value to index using insertion sort */
    private int indexAdd(short base) {
        if (segIds.length == 0) {
            segIds = new short[1];

            segIds[0] = base;

            segmentsUsed++;

            return 0;
        }

        int idx = Arrays.binarySearch(segIds, 0, segmentsUsed, base);

        int pos = -idx;

//        if (idx == segmentsUsed) {
//            // append.
//
//            // need resize
//            if (idx >= segIds.length) {
//                int newSize = Math.min(segIds.length * 2, MAX_SEGMENTS);
//
//                segIds = Arrays.copyOf(segIds, newSize);
//            }
//        }

        // Insert a segment.
        if (pos >= 0) {
            int newSize = segIds.length * 2;

            short[] tmp = new short[newSize];

            System.arraycopy(segIds, 0, tmp, 0, pos-1);

            tmp[pos] = base;

            int len = segIds.length - pos;

            if (len > 0)
                System.arraycopy(segIds, pos, tmp, pos+1, len);

            segIds = tmp;
        }
        else
            return idx;

        return pos;
    }

    private void checkInvariants() {

    }

    private boolean segmentAdd(int segId, short bit) {
        short[] segment = segments[segId];

        int wordIdx = bit >>> WORD_SHIFT_BITS;

        // Resize if needed.
        if (wordIdx >= segment.length) {
            int newSize = Math.min(MAX_WORDS, Math.max(segment.length * 2, wordIdx + 1));

            segment = segments[segId] = Arrays.copyOf(segment, newSize);
        }

        short wordBit = (short) (bit - wordIdx * SHORT_BITS);

        assert 0 <= wordBit && wordBit < SHORT_BITS : "Word bit is within range";

        segment[(wordIdx)] |= (1 << wordBit);

        return false; // TODO
    }

    public boolean contains(int v) {
        return false;
    }

    private int segmentIndex(int v) {
        assert segIds.length > 0 && segments.length > 0 : "At least one segment";

        return 0;
    }

//    private int insertSegment(int idx, short[] seg) {
//
//    }

    public void dump() {
        for (short[] segment : segments) {
            for (short word : segment)
                U.debug(Integer.toBinaryString(word & 0xFFFF));
        }
    }

    public Iterator iterator() {
        return null;
    }

    public int size() {
        return 0;
    }

    public static class Iterator {

        public boolean hasNext() {
            return false;
        }

        public int next() {
            return 0;
        }
    }
}

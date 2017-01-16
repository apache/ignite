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

    private short[] segIds = new short[1];

    private short[][] segments = new short[1][1];

    public static final short SEGMENT_SIZE = 1024;

    public static final int SHORT_BITS = Short.SIZE;

    public static final int MAX_WORDS = SEGMENT_SIZE / SHORT_BITS;

    public static final int SHIFT_BITS = 4;

    public boolean add(int v) {
        U.debug("Add " + v);

        short base = (short) (v / SEGMENT_SIZE);

        short inc = (short) (v - base * SEGMENT_SIZE);

        return segmentAdd(segmentIndex(base), inc);
    }

    private boolean segmentAdd(int segId, short bit) {
        short[] segment = segments[segId];

        int wordIdx = bit >>> SHIFT_BITS;

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
}

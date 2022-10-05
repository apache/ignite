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

package org.apache.ignite.internal.processors.cache.persistence.pagemem;

import java.util.function.LongUnaryOperator;
import org.apache.ignite.configuration.PageReplacementMode;
import org.apache.ignite.internal.util.GridUnsafe;

/**
 * Clock page replacement algorithm implementation.
 *
 * @see PageReplacementMode#CLOCK
 */
public class ClockPageReplacementFlags {
    /** Total pages count. */
    private final int pagesCnt;

    /** Index of the next candidate ("hand"). */
    private int curIdx;

    /** Pointer to memory region to store page hit flags. */
    private final long flagsPtr;

    /**
     * @param totalPagesCnt Total pages count.
     * @param memPtr Pointer to memory region.
     */
    ClockPageReplacementFlags(int totalPagesCnt, long memPtr) {
        pagesCnt = totalPagesCnt;
        flagsPtr = memPtr;

        GridUnsafe.zeroMemory(flagsPtr, (totalPagesCnt + 7) >> 3);
    }

    /**
     * Find page to replace.
     *
     * @return Page index to replace.
     */
    public int poll() {
        // This method is always executed under exclusive lock, no other synchronization or CAS required.
        while (true) {
            if (curIdx >= pagesCnt)
                curIdx = 0;

            long ptr = flagsPtr + ((curIdx >> 3) & (~7L));

            long flags = GridUnsafe.getLong(ptr);

            if (((curIdx & 63) == 0) && (flags == ~0L)) {
                GridUnsafe.putLong(ptr, 0L);

                curIdx += 64;

                continue;
            }

            long mask = ~0L << curIdx;

            int bitIdx = Long.numberOfTrailingZeros(~flags & mask);

            if (bitIdx == 64) {
                GridUnsafe.putLong(ptr, flags & ~mask);

                curIdx = (curIdx & ~63) + 64;
            }
            else {
                mask &= ~(~0L << bitIdx);

                GridUnsafe.putLong(ptr, flags & ~mask);

                curIdx = (curIdx & ~63) + bitIdx + 1;

                if (curIdx <= pagesCnt)
                    return curIdx - 1;
            }
        }
    }

    /**
     * Get page hit flag.
     *
     * @param pageIdx Page index.
     */
    boolean getFlag(int pageIdx) {
        long flags = GridUnsafe.getLong(flagsPtr + ((pageIdx >> 3) & (~7L)));

        return (flags & (1L << pageIdx)) != 0L;
    }

    /**
     * Clear page hit flag.
     *
     * @param pageIdx Page index.
     */
    public void clearFlag(int pageIdx) {
        compareAndSwapFlag(pageIdx, flags -> flags & ~(1L << pageIdx));
    }

    /**
     * Set page hit flag.
     *
     * @param pageIdx Page index.
     */
    public void setFlag(int pageIdx) {
        compareAndSwapFlag(pageIdx, flags -> flags | (1L << pageIdx));
    }

    /**
     * CAS page hit flag value.
     *
     * @param pageIdx Page index.
     * @param func Function to apply to flags.
     */
    private void compareAndSwapFlag(int pageIdx, LongUnaryOperator func) {
        long ptr = flagsPtr + ((pageIdx >> 3) & (~7L));

        long oldFlags;
        long newFlags;

        do {
            oldFlags = GridUnsafe.getLong(ptr);
            newFlags = func.applyAsLong(oldFlags);

            if (oldFlags == newFlags)
                return;
        }
        while (!GridUnsafe.compareAndSwapLong(null, ptr, oldFlags, newFlags));
    }

    /**
     * Memory required to service {@code pagesCnt} pages.
     *
     * @param pagesCnt Pages count.
     */
    public static long requiredMemory(int pagesCnt) {
        return ((pagesCnt + 63) / 8) & (~7L) /* 1 bit per page + 8 byte align */;
    }
}

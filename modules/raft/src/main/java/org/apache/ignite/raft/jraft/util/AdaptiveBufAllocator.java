/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.raft.jraft.util;

import java.util.ArrayList;
import java.util.List;

/**
 * The code comes from https://github.com/netty/netty/blob/4.1/transport/src/main/java/io/netty/channel/AdaptiveRecvByteBufAllocator.java
 */
public class AdaptiveBufAllocator {

    private static final int DEFAULT_MINIMUM = 64;
    private static final int DEFAULT_INITIAL = 512;
    private static final int DEFAULT_MAXIMUM = 524288;

    private static final int INDEX_INCREMENT = 4;
    private static final int INDEX_DECREMENT = 1;

    private static final int[] SIZE_TABLE;

    static {
        final List<Integer> sizeTable = new ArrayList<>();
        for (int i = 16; i < 512; i += 16) {
            sizeTable.add(i);
        }

        for (int i = 512; i > 0; i <<= 1) {
            sizeTable.add(i);
        }

        SIZE_TABLE = new int[sizeTable.size()];
        for (int i = 0; i < SIZE_TABLE.length; i++) {
            SIZE_TABLE[i] = sizeTable.get(i);
        }
    }

    public static final AdaptiveBufAllocator DEFAULT = new AdaptiveBufAllocator();

    private static int getSizeTableIndex(final int size) {
        for (int low = 0, high = SIZE_TABLE.length - 1; ; ) {
            if (high < low) {
                return low;
            }
            if (high == low) {
                return high;
            }

            final int mid = low + high >>> 1;
            final int a = SIZE_TABLE[mid];
            final int b = SIZE_TABLE[mid + 1];
            if (size > b) {
                low = mid + 1;
            }
            else if (size < a) {
                high = mid - 1;
            }
            else if (size == a) {
                return mid;
            }
            else {
                return mid + 1;
            }
        }
    }

    public interface Handle {

        /**
         * Creates a new buffer whose capacity is probably large enough to write all outbound data and small enough not
         * to waste its space.
         */
        ByteBufferCollector allocate();

        /**
         * Gets a buffer from recyclers whose capacity is probably large enough to write all outbound data and small
         * enough not to waste its space, recycling is needed.
         */
        ByteBufferCollector allocateByRecyclers();

        /**
         * Similar to {@link #allocate()} except that it does not allocate anything but just tells the capacity.
         */
        int guess();

        /**
         * Records the the actual number of wrote bytes in the previous write operation so that the allocator allocates
         * the buffer with potentially more correct capacity.
         *
         * @param actualWroteBytes the actual number of wrote bytes in the previous allocate operation
         */
        void record(final int actualWroteBytes);
    }

    private static final class HandleImpl implements Handle {

        private final int minIndex;
        private final int maxIndex;
        private int index;
        private int nextAllocateBufSize;
        private boolean decreaseNow;

        HandleImpl(int minIndex, int maxIndex, int initial) {
            this.minIndex = minIndex;
            this.maxIndex = maxIndex;

            this.index = getSizeTableIndex(initial);
            this.nextAllocateBufSize = SIZE_TABLE[this.index];
        }

        @Override
        public ByteBufferCollector allocate() {
            return ByteBufferCollector.allocate(guess());
        }

        @Override
        public ByteBufferCollector allocateByRecyclers() {
            return ByteBufferCollector.allocateByRecyclers(guess());
        }

        @Override
        public int guess() {
            return this.nextAllocateBufSize;
        }

        @Override
        public void record(final int actualWroteBytes) {
            if (actualWroteBytes <= SIZE_TABLE[Math.max(0, this.index - INDEX_DECREMENT - 1)]) {
                if (this.decreaseNow) {
                    this.index = Math.max(this.index - INDEX_DECREMENT, this.minIndex);
                    this.nextAllocateBufSize = SIZE_TABLE[this.index];
                    this.decreaseNow = false;
                }
                else {
                    this.decreaseNow = true;
                }
            }
            else if (actualWroteBytes >= this.nextAllocateBufSize) {
                this.index = Math.min(this.index + INDEX_INCREMENT, this.maxIndex);
                this.nextAllocateBufSize = SIZE_TABLE[this.index];
                this.decreaseNow = false;
            }
        }
    }

    private final int minIndex;
    private final int maxIndex;
    private final int initial;

    /**
     * Creates a new predictor with the default parameters.  With the default parameters, the expected buffer size
     * starts from {@code 512}, does not go down below {@code 64}, and does not go up above {@code 524288}.
     */
    private AdaptiveBufAllocator() {
        this(DEFAULT_MINIMUM, DEFAULT_INITIAL, DEFAULT_MAXIMUM);
    }

    /**
     * Creates a new predictor with the specified parameters.
     *
     * @param minimum the inclusive lower bound of the expected buffer size
     * @param initial the initial buffer size when no feed back was received
     * @param maximum the inclusive upper bound of the expected buffer size
     */
    public AdaptiveBufAllocator(int minimum, int initial, int maximum) {
        Requires.requireTrue(minimum > 0, "minimum: " + minimum);
        Requires.requireTrue(initial >= minimum, "initial: " + initial);
        Requires.requireTrue(initial <= maximum, "maximum: " + maximum);

        final int minIndex = getSizeTableIndex(minimum);
        if (SIZE_TABLE[minIndex] < minimum) {
            this.minIndex = minIndex + 1;
        }
        else {
            this.minIndex = minIndex;
        }

        final int maxIndex = getSizeTableIndex(maximum);
        if (SIZE_TABLE[maxIndex] > maximum) {
            this.maxIndex = maxIndex - 1;
        }
        else {
            this.maxIndex = maxIndex;
        }

        this.initial = initial;
    }

    public Handle newHandle() {
        return new AdaptiveBufAllocator.HandleImpl(this.minIndex, this.maxIndex, this.initial);
    }
}

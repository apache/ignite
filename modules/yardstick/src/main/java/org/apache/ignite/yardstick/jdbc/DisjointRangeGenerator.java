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

package org.apache.ignite.yardstick.jdbc;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Generates random fixed width ranges from [1..tableSize] interval.
 * It is guaranteed that different threads get disjoint ranges.
 */
public class DisjointRangeGenerator {
    /** Max number of threads that can use this generator. */
    private final int maxThreadsCnt;

    /** Width of generated range. */
    private final long rangeWidth;

    /** Size of test data table. */
    private final long tableSize;

    /** Counter of threads, that are using this idGen */
    private final AtomicInteger registeredThreads = new AtomicInteger(0);

    /**
     * Number within [0..threadsCount).
     * Thread order is used to provide each thread with exclusive set of id ranges
     * to avoid concurrent updates same rows in test table.
     */
    private final ThreadLocal<Integer> threadOrder = new ThreadLocal<Integer>() {
        @Override protected Integer initialValue() {
            if (maxThreadsCnt == registeredThreads.get())
                throw new IllegalStateException("Limit of threads using this generator exceeded. Limit is " + maxThreadsCnt);

            return registeredThreads.getAndIncrement();
        }
    };

    /**
     * @param maxThreadsCnt Max threads count.
     * @param tableSize Table size.
     * @param rangeWidth Range width.
     */
    public DisjointRangeGenerator(int maxThreadsCnt, long tableSize, long rangeWidth) {
        if (tableSize < maxThreadsCnt * rangeWidth)
            throw new IllegalArgumentException("Table size is too small to generate ranges");

        this.maxThreadsCnt = maxThreadsCnt;
        this.rangeWidth = rangeWidth;
        this.tableSize = tableSize;
    }

    /** Width of generated range. */
    public long rangeWidth() {
        return rangeWidth;
    }

    /**
     * Get new random range from [1..tableSize] interval.
     *
     * @return start id of range.
     */
    public long nextRangeStartId() {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        long blockWidth = rangeWidth * maxThreadsCnt;
        long blocksCnt = tableSize / blockWidth;

        long firstBlockId = rnd.nextLong(blocksCnt) * blockWidth + 1;

        long off = threadOrder.get() * rangeWidth;

        return firstBlockId + off;

    }

    /**
     * Get the end of the range that starts with given id.
     *
     * @param startId start id of the range.
     * @return id of the range (inclusive).
     */
    public long endRangeId(long startId) {
        return startId + rangeWidth - 1;
    }
}

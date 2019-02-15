/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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
        if (tableSize < maxThreadsCnt *rangeWidth)
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

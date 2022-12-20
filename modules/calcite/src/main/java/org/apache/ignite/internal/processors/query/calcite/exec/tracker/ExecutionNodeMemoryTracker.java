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

package org.apache.ignite.internal.processors.query.calcite.exec.tracker;

/**
 * Memory tracker for execution nodes.
 */
public class ExecutionNodeMemoryTracker<Row> implements RowTracker<Row> {
    /** Size of batch to report to query memory tracker (to reduce contention). Should be power of 2. */
    static final long BATCH_SIZE = 0x10000L;

    /** */
    private final MemoryTracker qryMemoryTracker;

    /** */
    private final ObjectSizeCalculator<Row> sizeCalculator = new ObjectSizeCalculator<Row>();

    /** */
    private long allocated;

    /** Size, reported to query memory tracker. */
    private long prevReported;

    /** */
    private final long rowOverhead;

    /** */
    public ExecutionNodeMemoryTracker(MemoryTracker qryMemoryTracker, long rowOverhead) {
        this.qryMemoryTracker = qryMemoryTracker;
        this.rowOverhead = rowOverhead;
    }

    /** {@inheritDoc} */
    @Override public void onRowAdded(Row obj) {
        long size = sizeCalculator.sizeOf(obj);

        size += rowOverhead;
        long newAllocated = allocated + size;

        if (newAllocated > prevReported) {
            long newReported = (newAllocated + (BATCH_SIZE - 1)) & -BATCH_SIZE; // Align to batch size.
            qryMemoryTracker.onMemoryAllocated(newReported - prevReported);
            prevReported = newReported;
        }

        allocated = newAllocated;
    }

    /** {@inheritDoc} */
    @Override public void onRowRemoved(Row obj) {
        long size = sizeCalculator.sizeOf(obj);

        size += rowOverhead;
        size = Math.min(size, allocated);

        if (size > 0) {
            long newAllocated = allocated - size;

            if (newAllocated <= prevReported - BATCH_SIZE) {
                long newReported = (newAllocated + (BATCH_SIZE - 1)) & -BATCH_SIZE; // Align to batch size.
                qryMemoryTracker.onMemoryReleased(prevReported - newReported);
                prevReported = newReported;
            }

            allocated = newAllocated;
        }
    }

    /** {@inheritDoc} */
    @Override public void reset() {
        if (prevReported > 0)
            qryMemoryTracker.onMemoryReleased(prevReported);

        allocated = 0;
    }
}

/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query.h2;

import org.h2.engine.Constants;
import org.h2.engine.Session;
import org.h2.expression.Expression;
import org.h2.result.H2BaseLocalResult;
import org.h2.value.Value;
import org.h2.value.ValueRow;

/**
 * H2 local result with memory tracker.
 */
public class H2ManagedLocalResult extends H2BaseLocalResult {
    /** Query memory tracker. */
    private QueryMemoryTracker mem;

    /** Allocated memory. */
    private long allocMem;

    /**
     * Constructor.
     *
     * @param ses the session
     * @param memTracker Query memory tracker.
     * @param expressions the expression array
     * @param visibleColCnt the number of visible columns
     */
    public H2ManagedLocalResult(Session ses, QueryMemoryTracker memTracker, Expression[] expressions,
        int visibleColCnt) {
        super(ses, expressions, visibleColCnt);

        this.mem = memTracker;
    }

    /** {@inheritDoc} */
    @Override protected void onUpdate(ValueRow distinctRowKey, Value[] oldRow, Value[] row) {
        assert !isClosed();

        if (oldRow != null) {
            long rowSize = Constants.MEMORY_ARRAY + oldRow.length * Constants.MEMORY_POINTER;

            for (int i = 0; i < oldRow.length; i++)
                rowSize += oldRow[i].getMemory();

            allocMem -= rowSize;

            mem.free(rowSize);
        }

        long rowSize = Constants.MEMORY_ARRAY + row.length * Constants.MEMORY_POINTER;

        if (distinctRowKey != null)
            rowSize += distinctRowKey.getMemory();

        for (int i = 0; i < row.length; i++)
            rowSize += row[i].getMemory();

        allocMem += rowSize;

        mem.allocate(rowSize);
    }

    /** {@inheritDoc} */
    public long memoryAllocated() {
        return allocMem;
    }

    /** {@inheritDoc} */
    @Override public void close() {
        boolean closed = isClosed();

        super.close();

        if (!closed)
            mem.free(allocMem);
    }
}

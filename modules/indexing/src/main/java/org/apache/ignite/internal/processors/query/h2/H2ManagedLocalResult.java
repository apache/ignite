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
    private H2MemoryTracker mem;

    /** Reserved memory. */
    private long memReserved;

    /**
     * Constructor.
     *
     * @param ses the session
     * @param memTracker Query memory tracker.
     * @param expressions the expression array
     * @param visibleColCnt the number of visible columns
     */
    public H2ManagedLocalResult(Session ses, H2MemoryTracker memTracker, Expression[] expressions,
        int visibleColCnt) {
        super(ses, expressions, visibleColCnt);

        this.mem = memTracker;
    }

    /** {@inheritDoc} */
    @Override protected void onUpdate(ValueRow distinctRowKey, Value[] oldRow, Value[] row) {
        assert !isClosed();
        assert row != null;

        long memory;

        // Replace old row.
        if (oldRow != null) {
            memory = (row.length - oldRow.length) * Constants.MEMORY_POINTER;

            for (int i = 0; i < oldRow.length; i++)
                memory -= oldRow[i].getMemory();
        }
        // Add new row.
        else {
            memory = Constants.MEMORY_ARRAY + row.length * Constants.MEMORY_POINTER;

            if (distinctRowKey != null)
                memory += distinctRowKey.getMemory();
        }

        for (int i = 0; i < row.length; i++)
            memory += row[i].getMemory();

        if (memory < 0)
            mem.release(-memory);
        else
            mem.reserve(memory);

        memReserved += memory;
    }

    /** */
    public long memoryReserved() {
        return memReserved;
    }

    /** {@inheritDoc} */
    @Override public void close() {
        if (!isClosed()) {
            super.close();

            onClose();
        }
    }

    /**
     * @return Query memory tracker.
     */
    public H2MemoryTracker getMemoryTracker() {
        return mem;
    }

    /** Close event handler. */
    protected void onClose() {
        // Allow results to be collected by GC before mark memory released.
        distinctRows = null;
        rows = null;

        mem.release(memReserved);
    }
}

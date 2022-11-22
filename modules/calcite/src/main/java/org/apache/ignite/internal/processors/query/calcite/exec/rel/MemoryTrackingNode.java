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

package org.apache.ignite.internal.processors.query.calcite.exec.rel;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.tracker.ObjectSizeCalculator;
import org.apache.ignite.internal.processors.query.calcite.exec.tracker.RowTracker;

/**
 * Abstract memory tracking node.
 */
public abstract class MemoryTrackingNode<Row> extends AbstractNode<Row> {
    /** Estimated overhead of row in hash map (assuming key and value is an objects composed from the original row fields). */
    protected static final long HASH_MAP_ROW_OVERHEAD = ObjectSizeCalculator.OBJ_REF_SIZE * 4 / 3 /* load factor */ +
        ObjectSizeCalculator.HASH_MAP_ENTRY_SIZE;

    /** Estimated overhead of row in array (assuming overhead is only size of extra reference to the row). */
    protected static final long ARRAY_ROW_OVERHEAD = ObjectSizeCalculator.OBJ_REF_SIZE;

    /** Default row overhead (assuming rows are stored in arrays by default). */
    protected static final long DFLT_ROW_OVERHEAD = ARRAY_ROW_OVERHEAD;

    /** Memory tracker for the current execution node. */
    protected final RowTracker<Row> nodeMemoryTracker;

    /**
     * @param ctx Execution context.
     * @param rowType Row type.
     * @param rowOverhead Row overhead in bytes for each row.
     */
    protected MemoryTrackingNode(ExecutionContext<Row> ctx, RelDataType rowType, long rowOverhead) {
        super(ctx, rowType);

        nodeMemoryTracker = ctx.createNodeMemoryTracker(rowOverhead);
    }

    /** */
    protected MemoryTrackingNode(ExecutionContext<Row> ctx, RelDataType rowType) {
        this(ctx, rowType, DFLT_ROW_OVERHEAD);
    }

    /** {@inheritDoc} */
    @Override protected void closeInternal() {
        nodeMemoryTracker.reset();

        super.closeInternal();
    }
}

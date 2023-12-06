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

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Predicate;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.jetbrains.annotations.Nullable;

/**
 * Scan storage node.
 */
public class ScanStorageNode<Row> extends ScanNode<Row> {
    /** */
    @Nullable private final AtomicLong processedRowsCntr;

    /**
     * @param storageName Storage (index or table) name.
     * @param ctx Execution context.
     * @param rowType Row type.
     * @param src Source.
     * @param filter Row filter.
     * @param rowTransformer Row transformer (projection).
     */
    public ScanStorageNode(
        String storageName,
        ExecutionContext<Row> ctx,
        RelDataType rowType,
        Iterable<Row> src,
        @Nullable Predicate<Row> filter,
        @Nullable Function<Row, Row> rowTransformer
    ) {
        super(ctx, rowType, src, filter, rowTransformer);

        processedRowsCntr = context().ioTracker().processedRowsCounter("Scanned " + storageName);
    }

    /**
     * @param storageName Storage (index or table) name.
     * @param ctx Execution context.
     * @param rowType Row type.
     * @param src Source.
     */
    public ScanStorageNode(String storageName, ExecutionContext<Row> ctx, RelDataType rowType, Iterable<Row> src) {
        this(storageName, ctx, rowType, src, null, null);
    }

    /** {@inheritDoc} */
    @Override protected int processNextBatch() throws Exception {
        try {
            context().ioTracker().startTracking();

            int processed = super.processNextBatch();

            if (processedRowsCntr != null)
                processedRowsCntr.addAndGet(processed);

            return processed;
        }
        finally {
            context().ioTracker().stopTracking();
        }
    }

    /** */
    @Override public void closeInternal() {
        super.closeInternal();

        context().ioTracker().flush();
    }
}

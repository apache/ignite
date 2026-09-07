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

import java.util.Iterator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;

import static java.util.Objects.requireNonNull;

/** Scan of the current delta owned by the enclosing recursive union. */
public class RecursiveTableScanNode<Row> extends ScanNode<Row> {
    /** Late-bound current delta source. */
    private final RecursiveRows<Row> rows;

    /** */
    public RecursiveTableScanNode(ExecutionContext<Row> ctx, RelDataType rowType) {
        this(ctx, rowType, new RecursiveRows<>());
    }

    /** */
    private RecursiveTableScanNode(ExecutionContext<Row> ctx, RelDataType rowType, RecursiveRows<Row> rows) {
        super(ctx, rowType, rows);

        this.rows = rows;
    }

    /** Binds this scan to the recursive union that owns its current delta. */
    void bind(RepeatUnionNode<Row> repeatUnion) {
        assert rows.repeatUnion == null;

        rows.repeatUnion = repeatUnion;
    }

    /** Late-bound current delta source. */
    private static class RecursiveRows<Row> implements Iterable<Row> {
        /** Owning recursive union. */
        private RepeatUnionNode<Row> repeatUnion;

        /** {@inheritDoc} */
        @Override public Iterator<Row> iterator() {
            return requireNonNull(repeatUnion, "Recursive table scan is not bound to a repeat union")
                .current().iterator();
        }
    }
}

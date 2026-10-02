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

import java.util.Collections;
import java.util.Iterator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;

import static java.util.Objects.requireNonNull;

/** Scan of the current delta owned by the enclosing recursive union. */
public class RecursiveTableScanNode<Row> extends ScanNode<Row> {
    /** Owning recursive union, bound when its sources are registered. */
    private RepeatUnionNode<Row> repeatUnion;

    /** */
    public RecursiveTableScanNode(ExecutionContext<Row> ctx, RelDataType rowType) {
        super(ctx, rowType, Collections.emptyList());
    }

    /** Binds this scan to the recursive union that owns its current delta. */
    void bind(RepeatUnionNode<Row> repeatUnion) {
        assert this.repeatUnion == null;

        this.repeatUnion = requireNonNull(repeatUnion);
    }

    /** {@inheritDoc} */
    @Override protected Iterator<Row> sourceIterator() {
        return requireNonNull(repeatUnion, "Recursive table scan is not bound to a repeat union")
            .current().iterator();
    }
}

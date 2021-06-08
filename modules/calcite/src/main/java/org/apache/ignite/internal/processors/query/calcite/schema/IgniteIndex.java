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
package org.apache.ignite.internal.processors.query.calcite.schema;

import java.util.Collections;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.internal.cache.query.index.Index;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndex;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.IndexScan;
import org.apache.ignite.internal.processors.query.calcite.metadata.ColocationGroup;
import org.jetbrains.annotations.Nullable;

/**
 * Ignite scannable index.
 */
public class IgniteIndex {
    /** */
    private final RelCollation collation;

    /** */
    private final String idxName;

    /** */
    private final Index idx;

    /** */
    private final IgniteTable tbl;

    /** */
    public IgniteIndex(RelCollation collation, String name, Index idx, IgniteTable tbl) {
        this.collation = collation;
        idxName = name;
        this.idx = idx;
        this.tbl = tbl;
    }

    /** */
    public RelCollation collation() {
        return collation;
    }

    /** */
    public String name() {
        return idxName;
    }

    /** */
    public IgniteTable table() {
        return tbl;
    }

    /** */
    public <Row> Iterable<Row> scan(
        ExecutionContext<Row> execCtx,
        ColocationGroup group,
        Predicate<Row> filters,
        Supplier<Row> lowerIdxConditions,
        Supplier<Row> upperIdxConditions,
        Function<Row, Row> rowTransformer,
        @Nullable ImmutableBitSet requiredColumns) {
        UUID localNodeId = execCtx.planningContext().localNodeId();
        if (group.nodeIds().contains(localNodeId) && idx != null) {
            return new IndexScan<>(execCtx, table().descriptor(), idx.unwrap(InlineIndex.class), collation.getKeys(),
                group.partitions(localNodeId), filters, lowerIdxConditions, upperIdxConditions, rowTransformer,
                requiredColumns);
        }

        return Collections.emptyList();
    }
}

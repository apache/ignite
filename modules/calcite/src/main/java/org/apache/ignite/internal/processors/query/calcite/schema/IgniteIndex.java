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

import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.RangeIterable;
import org.apache.ignite.internal.processors.query.calcite.metadata.ColocationGroup;
import org.apache.ignite.internal.processors.query.calcite.prepare.bounds.SearchBounds;
import org.apache.ignite.internal.processors.query.calcite.rel.logical.IgniteLogicalIndexScan;
import org.jetbrains.annotations.Nullable;

/**
 * Ignite scannable index.
 */
public interface IgniteIndex {
    /** */
    public RelCollation collation();

    /** */
    public String name();

    /** */
    public IgniteTable table();

    /**
     * Converts index into relational expression.
     *
     * @param cluster         Custer.
     * @param relOptTbl       Table.
     * @param proj            List of required projections.
     * @param cond            Conditions to filter rows.
     * @param requiredColumns Set of columns to extract from original row.
     * @return Table relational expression.
     */
    public IgniteLogicalIndexScan toRel(
        RelOptCluster cluster,
        RelOptTable relOptTbl,
        @Nullable List<RexNode> proj,
        @Nullable RexNode cond,
        @Nullable ImmutableBitSet requiredColumns
    );

    /**
     * Converts condition to index find predicate.
     *
     * @param cluster         Custer.
     * @param cond            Conditions to filter rows.
     * @param requiredColumns Set of columns to extract from original row.
     * @return Index condition.
     */
    public List<SearchBounds> toSearchBounds(
        RelOptCluster cluster,
        @Nullable RexNode cond,
        @Nullable ImmutableBitSet requiredColumns
    );

    /** */
    public <Row> Iterable<Row> scan(
        ExecutionContext<Row> execCtx,
        ColocationGroup grp,
        Predicate<Row> filters,
        RangeIterable<Row> ranges,
        Function<Row, Row> rowTransformer,
        @Nullable ImmutableBitSet requiredColumns
    );

    /**
     * Calculates index records number.
     *
     * @param ectx Execution context.
     * @param grp  Colocation group.
     * @return Index records number for {@code group}.
     */
    public long count(ExecutionContext<?> ectx, ColocationGroup grp);

    /**
     * Gets first or last index records from all index segments. Doesn't contain nulls. Values number might not
     * match segments number.
     *
     * @return List of first or last values from all index segments.
     */
    public <Row> List<Row> findFirstOrLast(
        boolean first,
        ExecutionContext<Row> ectx,
        ColocationGroup grp,
        @Nullable ImmutableBitSet requiredColumns
    );
}

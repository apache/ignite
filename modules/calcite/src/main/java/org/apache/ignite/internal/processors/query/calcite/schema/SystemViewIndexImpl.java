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
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.SystemViewScan;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.RangeIterable;
import org.apache.ignite.internal.processors.query.calcite.metadata.ColocationGroup;
import org.apache.ignite.internal.processors.query.calcite.prepare.bounds.SearchBounds;
import org.apache.ignite.internal.processors.query.calcite.rel.logical.IgniteLogicalIndexScan;
import org.apache.ignite.internal.processors.query.calcite.util.RexUtils;
import org.apache.ignite.internal.util.typedef.F;
import org.jetbrains.annotations.Nullable;

/**
 * Ignite scannable system view index.
 */
public class SystemViewIndexImpl implements IgniteIndex {
    /** */
    private final String idxName;

    /** */
    private final SystemViewTableImpl tbl;

    /** */
    public SystemViewIndexImpl(SystemViewTableImpl tbl) {
        this.tbl = tbl;
        idxName = tbl.descriptor().name() + "_IDX";
    }

    /** */
    @Override public RelCollation collation() {
        return RelCollations.EMPTY;
    }

    /** */
    @Override public String name() {
        return idxName;
    }

    /** */
    @Override public IgniteTable table() {
        return tbl;
    }

    /** {@inheritDoc} */
    @Override public IgniteLogicalIndexScan toRel(
        RelOptCluster cluster,
        RelOptTable relOptTbl,
        @Nullable List<RexNode> proj,
        @Nullable RexNode cond,
        @Nullable ImmutableBitSet requiredColumns
    ) {
        return IgniteLogicalIndexScan.create(cluster, cluster.traitSet(), relOptTbl, idxName, proj, cond, requiredColumns);
    }

    /** */
    @Override public <Row> Iterable<Row> scan(
        ExecutionContext<Row> execCtx,
        ColocationGroup grp,
        Predicate<Row> filters,
        RangeIterable<Row> ranges,
        Function<Row, Row> rowTransformer,
        @Nullable ImmutableBitSet requiredColumns
    ) {
        return new SystemViewScan<>(
            execCtx,
            tbl.descriptor(),
            ranges,
            filters,
            rowTransformer,
            requiredColumns
        );
    }

    /** {@inheritDoc} */
    @Override public long count(ExecutionContext<?> ectx, ColocationGroup grp) {
        return tbl.descriptor().systemView().size();
    }

    /** {@inheritDoc} */
    @Override public <Row> List<Row> findFirstOrLast(boolean first, ExecutionContext<Row> ectx,
        ColocationGroup grp, @Nullable ImmutableBitSet requiredColumns) {
        Iterator<Row> it = scan(ectx, grp, null, null, null, requiredColumns).iterator();

        Row curRow = null;

        while (F.isEmptyOrNulls(curRow) && it.hasNext())
            curRow = it.next();

        // No take-last underlying implementation. A minor ticket might be brought for the system views.
        Row res = F.isEmptyOrNulls(curRow) ? null : curRow;

        while (!first && it.hasNext())
            res = F.isEmptyOrNulls(curRow = it.next()) ? res : curRow;

        return res == null ? F.asList(res) : Collections.emptyList();
    }

    /** {@inheritDoc} */
    @Override public List<SearchBounds> toSearchBounds(
        RelOptCluster cluster,
        @Nullable RexNode cond,
        @Nullable ImmutableBitSet requiredColumns
    ) {
        if (cond == null)
            return null;

        RelDataType rowType = tbl.getRowType(cluster.getTypeFactory());

        return RexUtils.buildHashSearchBounds(cluster, cond, rowType, requiredColumns, true);
    }
}

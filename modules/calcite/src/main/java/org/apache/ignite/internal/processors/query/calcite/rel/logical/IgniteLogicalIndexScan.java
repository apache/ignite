/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query.calcite.rel.logical;

import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.ignite.internal.processors.query.calcite.rel.AbstractIndexScan;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteTable;
import org.apache.ignite.internal.processors.query.calcite.trait.TraitUtils;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.processors.query.calcite.util.IndexConditions;
import org.apache.ignite.internal.processors.query.calcite.util.RexUtils;
import org.jetbrains.annotations.Nullable;

/** */
public class IgniteLogicalIndexScan extends AbstractIndexScan {
    /** Creates a IgniteLogicalIndexScan. */
    public static IgniteLogicalIndexScan create(
        RelOptCluster cluster,
        RelTraitSet traits,
        RelOptTable table,
        String idxName,
        @Nullable List<RexNode> proj,
        @Nullable RexNode cond,
        @Nullable ImmutableBitSet requiredColumns
    ) {
        IgniteTable tbl = table.unwrap(IgniteTable.class);
        IgniteTypeFactory typeFactory = Commons.typeFactory(cluster);
        RelDataType rowType = tbl.getRowType(typeFactory, requiredColumns);
        RelCollation collation = tbl.getIndex(idxName).collation();

        if (requiredColumns != null) {
            Mappings.TargetMapping targetMapping = Commons.mapping(requiredColumns,
                tbl.getRowType(typeFactory).getFieldCount());
            collation = collation.apply(targetMapping);
            if (proj != null)
                collation = TraitUtils.projectCollation(collation, proj, rowType);
        }

        IndexConditions idxCond = new IndexConditions();

        if (collation != null && !collation.getFieldCollations().isEmpty()) {
            idxCond = RexUtils.buildSortedIndexConditions(
                cluster,
                collation,
                cond,
                tbl.getRowType(typeFactory),
                requiredColumns);
        }

        return new IgniteLogicalIndexScan(
            cluster,
            traits,
            table,
            idxName,
            proj,
            cond,
            idxCond,
            requiredColumns);
    }

    /**
     * Creates a TableScan.
     * @param cluster Cluster that this relational expression belongs to
     * @param traits Traits of this relational expression
     * @param tbl Table definition.
     * @param idxName Index name.
     * @param proj Projects.
     * @param cond Filters.
     * @param idxCond Index conditions.
     * @param requiredCols Participating columns.
     */
    private IgniteLogicalIndexScan(
        RelOptCluster cluster,
        RelTraitSet traits,
        RelOptTable tbl,
        String idxName,
        @Nullable List<RexNode> proj,
        @Nullable RexNode cond,
        @Nullable IndexConditions idxCond,
        @Nullable ImmutableBitSet requiredCols
    ) {
        super(cluster, traits, List.of(), tbl, idxName, proj, cond, idxCond, requiredCols);
    }
}

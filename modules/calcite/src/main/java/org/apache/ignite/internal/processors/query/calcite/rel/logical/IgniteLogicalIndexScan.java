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
import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.ignite.internal.processors.query.calcite.rel.AbstractIndexScan;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteTable;
import org.apache.ignite.internal.processors.query.calcite.trait.TraitUtils;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.processors.query.calcite.util.RexUtils;
import org.apache.ignite.internal.util.typedef.F;
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
        @Nullable ImmutableBitSet requiredColunms
    ) {
        IgniteTable tbl = table.unwrap(IgniteTable.class);
        IgniteTypeFactory typeFactory = Commons.typeFactory(cluster);
        RelDataType rowType = tbl.getRowType(typeFactory, requiredColunms);
        RelCollation collation = tbl.getIndex(idxName).collation();

        if (requiredColunms != null) {
            Mappings.TargetMapping targetMapping = Commons.mapping(requiredColunms,
                tbl.getRowType(typeFactory).getFieldCount());
            if (collation == null)
                System.out.println();
            collation = collation.apply(targetMapping);
            if (proj != null)
                collation = TraitUtils.projectCollation(collation, proj, rowType);
        }

        Pair<List<RexNode>, List<RexNode>> pair = RexUtils.buildIndexConditions(cluster, collation, cond, rowType);

        List<RexNode> lowerIdxCond = F.isEmpty(pair.left) ? null : pair.left;
        List<RexNode> upperIdxCond = F.isEmpty(pair.right) ? null : pair.right;

        return new IgniteLogicalIndexScan(cluster, traits, table, idxName, proj, cond, lowerIdxCond, upperIdxCond, requiredColunms);
    }

    /**
     * Creates a TableScan.
     * @param cluster Cluster that this relational expression belongs to
     * @param traits Traits of this relational expression
     * @param tbl Table definition.
     * @param idxName Index name.
     * @param proj Projects.
     * @param cond Filters.
     * @param lowerCond Lower condition.
     * @param upperCond Upper condition.
     * @param requiredColunms Participating colunms.
     */
    private IgniteLogicalIndexScan(
        RelOptCluster cluster,
        RelTraitSet traits,
        RelOptTable tbl,
        String idxName,
        @Nullable List<RexNode> proj,
        @Nullable RexNode cond,
        @Nullable List<RexNode> lowerCond,
        @Nullable List<RexNode> upperCond,
        @Nullable ImmutableBitSet requiredColunms
    ) {
        super(cluster, traits, ImmutableList.of(), tbl, idxName, proj, cond, lowerCond, upperCond, requiredColunms);
    }
}

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

package org.apache.ignite.internal.processors.query.calcite.rel;

import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.internal.processors.query.calcite.metadata.cost.IgniteCost;
import org.apache.ignite.internal.processors.query.calcite.util.IndexConditions;
import org.jetbrains.annotations.Nullable;

/**
 * Class with index conditions info.
 */
public abstract class AbstractIndexScan extends ProjectableFilterableTableScan {
    /**
     *
     */
    protected final String idxName;

    /**
     *
     */
    protected final IndexConditions idxCond;

    /**
     * Constructor used for deserialization.
     *
     * @param input Serialized representation.
     */
    protected AbstractIndexScan(RelInput input) {
        super(input);
        idxName = input.getString("index");
        idxCond = new IndexConditions(input);
    }

    /**
     *
     */
    protected AbstractIndexScan(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            List<RelHint> hints,
            RelOptTable table,
            String idxName,
            @Nullable List<RexNode> proj,
            @Nullable RexNode cond,
            @Nullable IndexConditions idxCond,
            @Nullable ImmutableBitSet reqColumns
    ) {
        super(cluster, traitSet, hints, table, proj, cond, reqColumns);

        this.idxName = idxName;
        this.idxCond = idxCond;
    }

    /** {@inheritDoc} */
    @Override
    protected RelWriter explainTerms0(RelWriter pw) {
        pw = pw.item("index", idxName);
        pw = super.explainTerms0(pw);

        return idxCond.explainTerms(pw);
    }

    /**
     *
     */
    public String indexName() {
        return idxName;
    }

    /**
     * @return Lower index condition.
     */
    public List<RexNode> lowerCondition() {
        return idxCond == null ? null : idxCond.lowerCondition();
    }

    /**
     * @return Lower index condition.
     */
    public List<RexNode> lowerBound() {
        return idxCond == null ? null : idxCond.lowerBound();
    }

    /**
     * @return Upper index condition.
     */
    public List<RexNode> upperCondition() {
        return idxCond == null ? null : idxCond.upperCondition();
    }

    /**
     * @return Upper index condition.
     */
    public List<RexNode> upperBound() {
        return idxCond == null ? null : idxCond.upperBound();
    }

    /** {@inheritDoc} */
    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        double rows = table.getRowCount();

        double cost = rows * IgniteCost.ROW_PASS_THROUGH_COST;

        if (condition != null) {
            RexBuilder builder = getCluster().getRexBuilder();

            double selectivity = 1;

            cost = 0;

            if (lowerCondition() != null) {
                double selectivity0 = mq.getSelectivity(this, RexUtil.composeConjunction(builder, lowerCondition()));

                selectivity -= 1 - selectivity0;

                cost += Math.log(rows);
            }

            if (upperCondition() != null && lowerCondition() != null && !lowerCondition().equals(upperCondition())) {
                double selectivity0 = mq.getSelectivity(this, RexUtil.composeConjunction(builder, upperCondition()));

                selectivity -= 1 - selectivity0;
            }

            rows *= selectivity;

            if (rows <= 0) {
                rows = 1;
            }

            cost += rows * (IgniteCost.ROW_COMPARISON_COST + IgniteCost.ROW_PASS_THROUGH_COST);
        }

        return planner.getCostFactory().makeCost(rows, cost, 0);
    }

    /**
     *
     */
    public IndexConditions indexConditions() {
        return idxCond;
    }
}

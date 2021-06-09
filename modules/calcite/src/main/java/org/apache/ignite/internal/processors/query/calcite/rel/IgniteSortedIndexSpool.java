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
import java.util.Objects;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Spool;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.ignite.internal.processors.query.calcite.metadata.cost.IgniteCost;
import org.apache.ignite.internal.processors.query.calcite.metadata.cost.IgniteCostFactory;
import org.apache.ignite.internal.processors.query.calcite.util.IndexConditions;

/**
 * Relational operator that returns the sorted contents of a table
 * and allow to lookup rows by specified bounds.
 */
public class IgniteSortedIndexSpool extends AbstractIgniteSpool implements IgniteRel {
    /** */
    private final RelCollation collation;

    /** Index condition. */
    private final IndexConditions idxCond;

    /** Filters. */
    protected final RexNode condition;

    /** */
    public IgniteSortedIndexSpool(
        RelOptCluster cluster,
        RelTraitSet traits,
        RelNode input,
        RelCollation collation,
        RexNode condition,
        IndexConditions idxCond
    ) {
        super(cluster, traits, Type.LAZY, input);

        assert Objects.nonNull(idxCond);
        assert Objects.nonNull(condition);

        this.idxCond = idxCond;
        this.condition = condition;
        this.collation = collation;
    }

    /**
     * Constructor used for deserialization.
     *
     * @param input Serialized representation.
     */
    public IgniteSortedIndexSpool(RelInput input) {
        this(input.getCluster(),
            input.getTraitSet().replace(IgniteConvention.INSTANCE),
            input.getInputs().get(0),
            input.getCollation(),
            input.getExpression("condition"),
            new IndexConditions(input)
        );
    }

    /** {@inheritDoc} */
    @Override public <T> T accept(IgniteRelVisitor<T> visitor) {
        return visitor.visit(this);
    }

    /** */
    @Override public IgniteRel clone(RelOptCluster cluster, List<IgniteRel> inputs) {
        return new IgniteSortedIndexSpool(cluster, getTraitSet(), inputs.get(0), collation, condition, idxCond);
    }

    /** {@inheritDoc} */
    @Override protected Spool copy(RelTraitSet traitSet, RelNode input, Type readType, Type writeType) {
        return new IgniteSortedIndexSpool(getCluster(), traitSet, input, collation, condition, idxCond);
    }

    /** {@inheritDoc} */
    @Override public boolean isEnforcer() {
        return true;
    }

    /** */
    @Override public RelWriter explainTerms(RelWriter pw) {
        RelWriter writer = super.explainTerms(pw);

        writer.item("condition", condition);
        writer.item("collation", collation);

        return idxCond.explainTerms(writer);
    }

    /** {@inheritDoc} */
    @Override public double estimateRowCount(RelMetadataQuery mq) {
        return mq.getRowCount(getInput()) * mq.getSelectivity(this, null);
    }

    /** */
    public IndexConditions indexCondition() {
        return idxCond;
    }

    /** */
    @Override public RelCollation collation() {
        return collation;
    }

    /** */
    public RexNode condition() {
        return condition;
    }

    /** {@inheritDoc} */
    @Override public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        double rowCnt = mq.getRowCount(getInput());
        double bytesPerRow = getRowType().getFieldCount() * IgniteCost.AVERAGE_FIELD_SIZE;
        double totalBytes = rowCnt * bytesPerRow;
        double cpuCost;

        if (idxCond.lowerCondition() != null)
            cpuCost = Math.log(rowCnt) * IgniteCost.ROW_COMPARISON_COST;
        else
            cpuCost = rowCnt * IgniteCost.ROW_PASS_THROUGH_COST;

        IgniteCostFactory costFactory = (IgniteCostFactory)planner.getCostFactory();

        return costFactory.makeCost(rowCnt, cpuCost, 0, totalBytes, 0);
    }
}

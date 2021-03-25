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
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Spool;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.internal.processors.query.calcite.metadata.cost.IgniteCost;
import org.apache.ignite.internal.processors.query.calcite.metadata.cost.IgniteCostFactory;
import org.apache.ignite.internal.processors.query.calcite.util.IndexConditions;

/**
 * Relational operator that returns the sorted contents of a table
 * and allow to lookup rows by specified bounds.
 */
public class IgniteHashIndexSpool extends Spool implements IgniteRel {
    /** Index condition. */
    private final IndexConditions idxCond;

    /** Filters. */
    protected final RexNode condition;

    /** */
    public IgniteHashIndexSpool(
        RelOptCluster cluster,
        RelTraitSet traits,
        RelNode input,
        RexNode condition,
        IndexConditions idxCond
    ) {
        super(cluster, traits, input, Type.LAZY, Type.EAGER);

        assert Objects.nonNull(idxCond);
        assert Objects.nonNull(condition);

        this.idxCond = idxCond;
        this.condition = condition;
    }

    /**
     * Constructor used for deserialization.
     *
     * @param input Serialized representation.
     */
    public IgniteHashIndexSpool(RelInput input) {
        this(input.getCluster(),
            input.getTraitSet().replace(IgniteConvention.INSTANCE),
            input.getInputs().get(0),
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
        return new IgniteHashIndexSpool(cluster, getTraitSet(), inputs.get(0), condition, idxCond);
    }

    /** {@inheritDoc} */
    @Override protected Spool copy(RelTraitSet traitSet, RelNode input, Type readType, Type writeType) {
        return new IgniteHashIndexSpool(getCluster(), traitSet, input, condition, idxCond);
    }

    /** {@inheritDoc} */
    @Override public boolean isEnforcer() {
        return true;
    }

    /** */
    @Override public RelWriter explainTerms(RelWriter pw) {
        RelWriter writer = super.explainTerms(pw);

        writer.item("condition", condition);

        return idxCond.explainTerms(writer);
    }

    /** {@inheritDoc} */
    @Override public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        double rowCnt = mq.getRowCount(getInput());
        double bytesPerRow = (getRowType().getFieldCount() - indexCondition().keys().size()) * IgniteCost.AVERAGE_FIELD_SIZE;
        double totalBytes = rowCnt * bytesPerRow;
        double cpuCost = IgniteCost.HASH_LOOKUP_COST;

        IgniteCostFactory costFactory = (IgniteCostFactory)planner.getCostFactory();

        return costFactory.makeCost(rowCnt, cpuCost, 0, totalBytes, 0);
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
    public RexNode condition() {
        return condition;
    }
}

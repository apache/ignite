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
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Spool;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.ignite.internal.processors.query.calcite.util.IndexConditions;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.query.calcite.trait.TraitUtils.changeTraits;

/**
 * Relational operator that returns the sorted contents of a table
 * and allow to lookup rows by specified keys.
 */
public class IgniteIndexSpool extends Spool implements IgniteRel {
    /** Index condition. */
    private IndexConditions idxCond;

    /** */
    public IgniteIndexSpool(
        RelOptCluster cluster,
        RelTraitSet traits,
        RelNode input
    ) {
        this(cluster, traits, input, null);
    }

    /** */
    public IgniteIndexSpool(
        RelOptCluster cluster,
        RelTraitSet traits,
        RelNode input,
        @Nullable IndexConditions idxCond
    ) {
        super(cluster, traits, input, Type.LAZY, Type.EAGER);
        this.idxCond = idxCond;
    }

    /**
     * Constructor used for deserialization.
     *
     * @param input Serialized representation.
     */
    public IgniteIndexSpool(RelInput input) {
        super(
            changeTraits(input, IgniteConvention.INSTANCE).getCluster(),
            changeTraits(input, IgniteConvention.INSTANCE).getTraitSet(),
            changeTraits(input, IgniteConvention.INSTANCE).getInput(),
            changeTraits(input, IgniteConvention.INSTANCE).getEnum("readType", Type.class),
            changeTraits(input, IgniteConvention.INSTANCE).getEnum("writeType", Type.class)
        );
    }

    /** {@inheritDoc} */
    @Override public <T> T accept(IgniteRelVisitor<T> visitor) {
        return visitor.visit(this);
    }

    /** */
    @Override public IgniteRel clone(RelOptCluster cluster, List<IgniteRel> inputs) {
        return new IgniteIndexSpool(cluster, getTraitSet(), inputs.get(0), idxCond);
    }

    /** {@inheritDoc} */
    @Override protected Spool copy(RelTraitSet traitSet, RelNode input, Type readType, Type writeType) {
        return new IgniteIndexSpool(getCluster(), traitSet, input, idxCond);
    }

    /** {@inheritDoc} */
    @Override public boolean isEnforcer() {
        return true;
    }

    /** */
    @Override public RelWriter explainTerms(RelWriter pw) {
        RelWriter writer = super.explainTerms(pw);

        if (idxCond != null) {
            writer
                .item("lower", idxCond.lowerCondition())
                .item("upper", idxCond.upperCondition())
                .item("lowerBound", idxCond.lowerBound())
                .item("upperBound", idxCond.upperBound());
        }

        return writer;
    }

    /** {@inheritDoc} */
    @Override public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        // TODO: add memory usage to cost
        double rowCount = mq.getRowCount(this);
        rowCount = RelMdUtil.addEpsilon(rowCount);

        return planner.getCostFactory().makeCost(rowCount, 0, 0).multiplyBy(2);
    }

    /** {@inheritDoc} */
    @Override public double estimateRowCount(RelMetadataQuery mq) {
        return mq.getRowCount(getInput()) * mq.getSelectivity(this, null);
    }

    /** */
    public IndexConditions indexCondition() {
        return idxCond;
    }
}

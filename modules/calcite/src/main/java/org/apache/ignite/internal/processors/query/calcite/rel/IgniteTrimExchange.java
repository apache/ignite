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
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Exchange;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.ignite.internal.processors.query.calcite.metadata.cost.IgniteCost;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;

import static org.apache.calcite.rel.RelDistribution.Type.HASH_DISTRIBUTED;
import static org.apache.ignite.internal.processors.query.calcite.trait.TraitUtils.changeTraits;

/**
 *
 */
public class IgniteTrimExchange extends Exchange implements SourceAwareIgniteRel {
    /** */
    private final long sourceId;

    /** */
    public IgniteTrimExchange(RelOptCluster cluster, RelTraitSet traits, RelNode input,
        RelDistribution distribution) {
        this(-1, cluster, traits, input, distribution);
    }

    /** */
    public IgniteTrimExchange(long sourceId, RelOptCluster cluster, RelTraitSet traits, RelNode input, RelDistribution distribution) {
        super(cluster, traits, input, distribution);

        assert distribution.getType() == HASH_DISTRIBUTED;

        this.sourceId = sourceId;
    }

    /** */
    public IgniteTrimExchange(RelInput input) {
        super(changeTraits(input, IgniteConvention.INSTANCE, input.getDistribution()));

        Object srcIdObj = input.get("sourceId");
        if (srcIdObj != null)
            sourceId = ((Number)srcIdObj).longValue();
        else
            sourceId = -1;
    }

    /** {@inheritDoc} */
    @Override public long sourceId() {
        return sourceId;
    }

    /** {@inheritDoc} */
    @Override public IgniteDistribution distribution() {
        return (IgniteDistribution)distribution;
    }

    /** {@inheritDoc} */
    @Override public boolean isEnforcer() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw)
            .itemIf("sourceId", sourceId, sourceId != -1);
    }

    /** {@inheritDoc} */
    @Override public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        double rowCount = mq.getRowCount(getInput());

        return planner.getCostFactory().makeCost(rowCount,
            rowCount * (IgniteCost.ROW_COMPARISON_COST + IgniteCost.ROW_PASS_THROUGH_COST), 0);
    }

    /** {@inheritDoc} */
    @Override public Exchange copy(RelTraitSet traits, RelNode input, RelDistribution distribution) {
        return new IgniteTrimExchange(getCluster(), traits, input, distribution);
    }

    /** {@inheritDoc} */
    @Override public <T> T accept(IgniteRelVisitor<T> visitor) {
        return visitor.visit(this);
    }

    /** {@inheritDoc} */
    @Override public IgniteRel clone(long sourceId) {
        return new IgniteTrimExchange(sourceId, getCluster(), getTraitSet(), getInput(), getDistribution());
    }

    /** {@inheritDoc} */
    @Override public IgniteRel clone(RelOptCluster cluster, List<IgniteRel> inputs) {
        return new IgniteTrimExchange(sourceId, cluster, getTraitSet(), sole(inputs), getDistribution());
    }
}

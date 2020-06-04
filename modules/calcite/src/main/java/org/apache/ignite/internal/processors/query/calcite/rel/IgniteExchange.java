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

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Exchange;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.Pair;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;

import static org.apache.ignite.internal.processors.query.calcite.trait.TraitUtils.fixTraits;

/**
 * Relational expression that imposes a particular distribution on its input
 * without otherwise changing its content.
 */
public class IgniteExchange extends Exchange implements IgniteRel {
    /**
     * Creates an Exchange.
     *
     * @param cluster   Cluster this relational expression belongs to
     * @param traitSet  Trait set
     * @param input     Input relational expression
     * @param distribution Distribution specification
     */
    public IgniteExchange(RelOptCluster cluster, RelTraitSet traitSet, RelNode input, RelDistribution distribution) {
        super(cluster, traitSet, input, distribution);
    }

    public IgniteExchange(RelInput input) {
        super(Commons.changeTraits(input, IgniteConvention.INSTANCE));
    }

    /** {@inheritDoc} */
    @Override public IgniteDistribution distribution() {
        return (IgniteDistribution)distribution;
    }

    /** {@inheritDoc} */
    @Override public Exchange copy(RelTraitSet traitSet, RelNode newInput, RelDistribution newDistribution) {
        return new IgniteExchange(getCluster(), traitSet, newInput, newDistribution);
    }

    /** {@inheritDoc} */
    @Override public <T> T accept(IgniteRelVisitor<T> visitor) {
        return visitor.visit(this);
    }

    /** {@inheritDoc} */
    @Override public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        double rowCount = mq.getRowCount(this);
        double bytesPerRow = getRowType().getFieldCount() * 4;
        return planner.getCostFactory().makeCost(rowCount * bytesPerRow, rowCount, 0);
    }

    /** {@inheritDoc} */
    @Override public Pair<RelTraitSet, List<RelTraitSet>> passThroughTraits(RelTraitSet required) {
        required = fixTraits(required);

        IgniteDistribution distribution = Commons.distribution(required);

        if (!distribution().satisfies(distribution))
            return null;

        return Pair.of(required.replace(distribution()), ImmutableList.of(required.replace(IgniteDistributions.any())));
    }

    /** {@inheritDoc} */
    @Override public Pair<RelTraitSet, List<RelTraitSet>> deriveTraits(RelTraitSet childTraits, int childId) {
        assert childId == 0;

        childTraits = fixTraits(childTraits);

        return Pair.of(childTraits.replace(distribution()), ImmutableList.of(childTraits.replace(IgniteDistributions.any())));
    }

    /** {@inheritDoc} */
    @Override public boolean isEnforcer() {
        return true;
    }
}

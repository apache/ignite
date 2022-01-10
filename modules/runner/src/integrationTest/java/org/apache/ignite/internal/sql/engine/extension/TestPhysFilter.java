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

package org.apache.ignite.internal.sql.engine.extension;

import static org.apache.ignite.internal.sql.engine.trait.TraitUtils.changeTraits;

import java.util.List;
import java.util.Set;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Pair;
import org.apache.ignite.internal.sql.engine.rel.IgniteRel;
import org.apache.ignite.internal.sql.engine.rel.IgniteRelVisitor;
import org.apache.ignite.internal.sql.engine.trait.CorrelationTrait;
import org.apache.ignite.internal.sql.engine.trait.TraitUtils;
import org.apache.ignite.internal.sql.engine.trait.TraitsAwareIgniteRel;
import org.apache.ignite.internal.sql.engine.util.RexUtils;

/**
 * A test filter node.
 */
public class TestPhysFilter extends Filter implements TraitsAwareIgniteRel {
    /**
     * Constructor.
     *
     * @param cluster   Cluster this node belongs to.
     * @param traits    A set of the traits this node satisfy.
     * @param input     An input relation.
     * @param condition The predicate to filter put input rows.
     */
    public TestPhysFilter(RelOptCluster cluster, RelTraitSet traits, RelNode input, RexNode condition) {
        super(cluster, traits, input, condition);
    }

    /**
     * Constructor.
     *
     * @param input Context to recover this relation from.
     */
    public TestPhysFilter(RelInput input) {
        super(changeTraits(input, TestExtension.CONVENTION));
    }

    /** {@inheritDoc} */
    @Override
    public Filter copy(RelTraitSet traitSet, RelNode input, RexNode condition) {
        return new TestPhysFilter(getCluster(), traitSet, input, condition);
    }

    /** {@inheritDoc} */
    @Override
    public <T> T accept(IgniteRelVisitor<T> visitor) {
        return visitor.visit(this);
    }

    /** {@inheritDoc} */
    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        return planner.getCostFactory().makeZeroCost();
    }

    /** {@inheritDoc} */
    @Override
    public IgniteRel clone(RelOptCluster cluster, List<IgniteRel> inputs) {
        return new TestPhysFilter(cluster, getTraitSet(), sole(inputs), getCondition());
    }

    /** {@inheritDoc} */
    @Override
    public List<Pair<RelTraitSet, List<RelTraitSet>>> deriveRewindability(RelTraitSet nodeTraits,
            List<RelTraitSet> inTraits) {
        if (!TraitUtils.rewindability(inTraits.get(0)).rewindable() && RexUtils.hasCorrelation(getCondition())) {
            return List.of();
        }

        return List.of(Pair.of(nodeTraits.replace(TraitUtils.rewindability(inTraits.get(0))),
                inTraits));
    }

    /** {@inheritDoc} */
    @Override
    public List<Pair<RelTraitSet, List<RelTraitSet>>> deriveDistribution(RelTraitSet nodeTraits,
            List<RelTraitSet> inTraits) {
        return List.of(Pair.of(nodeTraits.replace(TraitUtils.distribution(inTraits.get(0))),
                inTraits));
    }

    /** {@inheritDoc} */
    @Override
    public List<Pair<RelTraitSet, List<RelTraitSet>>> deriveCollation(RelTraitSet nodeTraits,
            List<RelTraitSet> inTraits) {
        return List.of(Pair.of(nodeTraits.replace(TraitUtils.collation(inTraits.get(0))),
                inTraits));
    }

    /** {@inheritDoc} */
    @Override
    public List<Pair<RelTraitSet, List<RelTraitSet>>> deriveCorrelation(RelTraitSet nodeTraits,
            List<RelTraitSet> inTraits) {
        Set<CorrelationId> corrIds = RexUtils.extractCorrelationIds(getCondition());

        corrIds.addAll(TraitUtils.correlation(inTraits.get(0)).correlationIds());

        return List.of(Pair.of(nodeTraits.replace(CorrelationTrait.correlations(corrIds)), inTraits));
    }

    /** {@inheritDoc} */
    @Override
    public List<RelNode> derive(List<List<RelTraitSet>> inputTraits) {
        return TraitUtils.derive(TestExtension.CONVENTION, this, inputTraits);
    }

    /** {@inheritDoc} */
    @Override
    public Pair<RelTraitSet, List<RelTraitSet>> passThroughTraits(RelTraitSet required) {
        return TraitUtils.passThrough(TestExtension.CONVENTION, this, required);
    }


}

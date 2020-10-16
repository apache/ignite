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

package org.apache.ignite.internal.processors.query.calcite.trait;

import java.util.List;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.DeriveMode;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.util.Pair;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;

import static org.apache.ignite.internal.processors.query.calcite.trait.TraitUtils.fixTraits;

/** */
public interface TraitsAwareIgniteRel extends IgniteRel {
    /** {@inheritDoc} */
    @Override public default RelNode passThrough(RelTraitSet required) {
        required = fixTraits(required);

        List<RelNode> nodes = TraitsPropagationContext.forPassingThrough(this, required)
            .propagate(this::passThroughCollation)
            .propagate(this::passThroughDistribution)
            .propagate(this::passThroughRewindability)
            .nodes(this::createNode);

        RelOptPlanner planner = getCluster().getPlanner();

        assert planner instanceof VolcanoPlanner;

        for (RelNode node : nodes) {
            RelTraitSet traits = node.getTraitSet();

            // try to fix traits somehow.
            if (!traits.satisfies(required))
                node = TraitUtils.enforce(node, required);

            if (node != null) {
                boolean satisfies = node.getTraitSet().satisfies(required);

                assert satisfies : "current rel=" + getRelTypeName() + ", traits=" + traits + ", required=" + required;

                planner.register(node, this);
            }
        }

        return RelOptRule.convert(this, required);
    }

    /** {@inheritDoc} */
    @Override public default List<RelNode> derive(List<List<RelTraitSet>> inTraits) {
        return TraitsPropagationContext.forDerivation(this, inTraits)
            .propagate(this::deriveCollation)
            .propagate(this::deriveDistribution)
            .propagate(this::deriveRewindability)
            .nodes(this::createNode);
    }

    /** {@inheritDoc} */
    @Override default Pair<RelTraitSet, List<RelTraitSet>> passThroughTraits(RelTraitSet required) {
        throw new RuntimeException(getClass().getName() + "#passThroughTraits() is not implemented.");
    }

    /** {@inheritDoc} */
    @Override default Pair<RelTraitSet, List<RelTraitSet>> deriveTraits(RelTraitSet childTraits, int childId) {
        throw new RuntimeException(getClass().getName() + "#deriveTraits() is not implemented.");
    }

    /** {@inheritDoc} */
    @Override public default DeriveMode getDeriveMode() {
        return DeriveMode.OMAKASE;
    }

    /**
     * Creates a node for given traits combination.
     *
     * @param outTraits Relational node output traits.
     * @param inTraits Relational node input traits.
     * @return Relational node for given traits combination.
     */
    default RelNode createNode(RelTraitSet outTraits, List<RelTraitSet> inTraits) {
        return copy(outTraits, Commons.transform(Ord.zip(inTraits),
            o -> RelOptRule.convert(getInput(o.i), o.e)));
    }

    /**
     * Propagates rewindability trait in up-to-bottom manner.
     *
     * @param outTraits Relational node output traits.
     * @param inTraits Relational node input traits.
     * @return List of possible input-output traits combinations.
     */
    List<Pair<RelTraitSet, List<RelTraitSet>>> passThroughRewindability(RelTraitSet outTraits, List<RelTraitSet> inTraits);

    /**
     * Propagates distribution trait in up-to-bottom manner.
     *
     * @param outTraits Relational node output traits.
     * @param inTraits Relational node input traits.
     * @return List of possible input-output traits combinations.
     */
    List<Pair<RelTraitSet, List<RelTraitSet>>> passThroughDistribution(RelTraitSet outTraits, List<RelTraitSet> inTraits);

    /**
     * Propagates collation trait in up-to-bottom manner.
     *
     * @param outTraits Relational node output traits.
     * @param inTraits Relational node input traits.
     * @return List of possible input-output traits combinations.
     */
    List<Pair<RelTraitSet, List<RelTraitSet>>> passThroughCollation(RelTraitSet outTraits, List<RelTraitSet> inTraits);

    /**
     * Propagates rewindability trait in bottom-up manner.
     *
     * @param outTraits Relational node output traits.
     * @param inTraits Relational node input traits.
     * @return List of possible input-output traits combinations.
     */
    List<Pair<RelTraitSet, List<RelTraitSet>>> deriveRewindability(RelTraitSet outTraits, List<RelTraitSet> inTraits);

    /**
     * Propagates distribution trait in bottom-up manner.
     *
     * @param outTraits Relational node output traits.
     * @param inTraits Relational node input traits.
     * @return List of possible input-output traits combinations.
     */
    List<Pair<RelTraitSet, List<RelTraitSet>>> deriveDistribution(RelTraitSet outTraits, List<RelTraitSet> inTraits);

    /**
     * Propagates collation trait in bottom-up manner.
     *
     * @param outTraits Relational node output traits.
     * @param inTraits Relational node input traits.
     * @return List of possible input-output traits combinations.
     */
    List<Pair<RelTraitSet, List<RelTraitSet>>> deriveCollation(RelTraitSet outTraits, List<RelTraitSet> inTraits);
}

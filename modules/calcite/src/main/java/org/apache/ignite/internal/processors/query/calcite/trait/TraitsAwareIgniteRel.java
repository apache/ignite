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
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.util.Pair;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;

/** */
public interface TraitsAwareIgniteRel extends IgniteRel {
    /** {@inheritDoc} */
    @Override public default List<RelNode> derive(List<List<RelTraitSet>> inTraits) {
        return TraitUtils.derive(this, inTraits);
    }

    /** {@inheritDoc} */
    @Override default Pair<RelTraitSet, List<RelTraitSet>> passThroughTraits(RelTraitSet required) {
        return TraitUtils.passThrough(this, required);
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
     * @param nodeTraits Relational node output traits.
     * @param inTraits Relational node input traits.
     * @return Relational node for given traits combination.
     */
    default RelNode createNode(RelTraitSet nodeTraits, List<RelTraitSet> inTraits) {
        return copy(nodeTraits, Commons.transform(Ord.zip(inTraits),
            o -> RelOptRule.convert(getInput(o.i), o.e)));
    }

    /**
     * Propagates rewindability trait in up-to-bottom manner.
     *
     * @param nodeTraits Relational node output traits.
     * @param inTraits Relational node input traits.
     * @return List of possible input-output traits combinations.
     */
    default Pair<RelTraitSet, List<RelTraitSet>> passThroughRewindability(RelTraitSet nodeTraits, List<RelTraitSet> inTraits) {
        RewindabilityTrait rewindability = TraitUtils.rewindability(nodeTraits);

        return Pair.of(nodeTraits, Commons.transform(inTraits, t -> t.replace(rewindability)));
    }

    /**
     * Propagates distribution trait in up-to-bottom manner.
     *
     * @param nodeTraits Relational node output traits.
     * @param inTraits Relational node input traits.
     * @return List of possible input-output traits combinations.
     */
    default Pair<RelTraitSet, List<RelTraitSet>> passThroughDistribution(RelTraitSet nodeTraits, List<RelTraitSet> inTraits) {
        IgniteDistribution distribution = TraitUtils.distribution(nodeTraits);

        return Pair.of(nodeTraits, Commons.transform(inTraits, t -> t.replace(distribution)));
    }

    /**
     * Propagates collation trait in up-to-bottom manner.
     *
     * @param nodeTraits Relational node output traits.
     * @param inTraits Relational node input traits.
     * @return List of possible input-output traits combinations.
     */
    default Pair<RelTraitSet, List<RelTraitSet>> passThroughCollation(RelTraitSet nodeTraits, List<RelTraitSet> inTraits) {
        if (inTraits.size() > 1)
            throw new RuntimeException(getClass().getName() + "#passThroughCollation() is not implemented.");

        RelCollation collation = TraitUtils.collation(nodeTraits);

        return Pair.of(nodeTraits, Commons.transform(inTraits, t -> t.replace(collation)));
    }

    /**
     * Propagates correlation trait in up-to-bottom manner.
     *
     * @param nodeTraits Relational node output traits.
     * @param inTraits Relational node input traits.
     * @return List of possible input-output traits combinations.
     */
    default Pair<RelTraitSet, List<RelTraitSet>> passThroughCorrelation(RelTraitSet nodeTraits, List<RelTraitSet> inTraits) {
        CorrelationTrait correlation = TraitUtils.correlation(nodeTraits);

        return Pair.of(nodeTraits, Commons.transform(inTraits, t -> t.replace(correlation)));
    }

    /**
     * Propagates rewindability trait in bottom-up manner.
     *
     * @param nodeTraits Relational node output traits.
     * @param inTraits Relational node input traits.
     * @return List of possible input-output traits combinations.
     */
    List<Pair<RelTraitSet, List<RelTraitSet>>> deriveRewindability(RelTraitSet nodeTraits, List<RelTraitSet> inTraits);

    /**
     * Propagates distribution trait in bottom-up manner.
     *
     * @param nodeTraits Relational node output traits.
     * @param inTraits Relational node input traits.
     * @return List of possible input-output traits combinations.
     */
    List<Pair<RelTraitSet, List<RelTraitSet>>> deriveDistribution(RelTraitSet nodeTraits, List<RelTraitSet> inTraits);

    /**
     * Propagates collation trait in bottom-up manner.
     *
     * @param nodeTraits Relational node output traits.
     * @param inTraits Relational node input traits.
     * @return List of possible input-output traits combinations.
     */
    List<Pair<RelTraitSet, List<RelTraitSet>>> deriveCollation(RelTraitSet nodeTraits, List<RelTraitSet> inTraits);

    /**
     * Propagates correlation trait in bottom-up manner.
     *
     * @param nodeTraits Relational node output traits.
     * @param inTraits Relational node input traits.
     * @return List of possible input-output traits combinations.
     */
    List<Pair<RelTraitSet, List<RelTraitSet>>> deriveCorrelation(RelTraitSet nodeTraits, List<RelTraitSet> inTraits);
}

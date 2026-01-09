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

package org.apache.ignite.internal.processors.query.calcite.rel.agg;

import java.util.List;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteAggregate;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteConvention;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.processors.query.calcite.trait.TraitUtils;
import org.apache.ignite.internal.processors.query.calcite.trait.TraitsAwareIgniteRel;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;

/**
 *
 */
public abstract class IgniteColocatedAggregateBase extends IgniteAggregate implements TraitsAwareIgniteRel {
    /** */
    protected IgniteColocatedAggregateBase(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelNode input,
        ImmutableBitSet groupSet,
        List<ImmutableBitSet> groupSets,
        List<AggregateCall> aggCalls
    ) {
        super(cluster, traitSet, input, groupSet, groupSets, aggCalls);
    }

    /** */
    protected IgniteColocatedAggregateBase(RelInput input) {
        super(TraitUtils.changeTraits(input, IgniteConvention.INSTANCE));
    }

    /** {@inheritDoc} */
    @Override public Pair<RelTraitSet, List<RelTraitSet>> passThroughDistribution(
        RelTraitSet nodeTraits,
        List<RelTraitSet> inTraits
    ) {
        IgniteDistribution distr = TraitUtils.distribution(nodeTraits);

        if (distr == IgniteDistributions.single() || distr.function().correlated())
            return Pair.of(nodeTraits, Commons.transform(inTraits, t -> t.replace(distr)));

        return null;
    }

    /** {@inheritDoc} */
    @Override public List<Pair<RelTraitSet, List<RelTraitSet>>> deriveRewindability(
        RelTraitSet nodeTraits,
        List<RelTraitSet> inputTraits
    ) {
        return ImmutableList.of(Pair.of(nodeTraits, ImmutableList.of(inputTraits.get(0))));
    }

    /** {@inheritDoc} */
    @Override public List<Pair<RelTraitSet, List<RelTraitSet>>> deriveDistribution(
        RelTraitSet nodeTraits,
        List<RelTraitSet> inputTraits
    ) {
        IgniteDistribution inDistribution = TraitUtils.distribution(inputTraits.get(0));

        if (inDistribution.satisfies(IgniteDistributions.single()))
            return ImmutableList.of(Pair.of(nodeTraits.replace(inDistribution), inputTraits));

        if (inDistribution.getType() == RelDistribution.Type.HASH_DISTRIBUTED) {
            for (Integer key : inDistribution.getKeys()) {
                if (!groupSet.get(key))
                    return ImmutableList.of();
            }

            // Group set contains all distribution keys, shift distribution keys according to used columns.
            IgniteDistribution outDistribution = inDistribution.apply(Commons.mapping(groupSet, rowType.getFieldCount()));

            return ImmutableList.of(Pair.of(nodeTraits.replace(outDistribution), inputTraits));
        }

        return ImmutableList.of();
    }

    /** {@inheritDoc} */
    @Override public List<Pair<RelTraitSet, List<RelTraitSet>>> deriveCorrelation(
        RelTraitSet nodeTraits,
        List<RelTraitSet> inTraits
    ) {
        return ImmutableList.of(Pair.of(nodeTraits.replace(TraitUtils.correlation(inTraits.get(0))),
            inTraits));
    }
}

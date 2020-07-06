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
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.mapping.MappingType;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.ignite.internal.processors.query.calcite.trait.DistributionFunction;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;
import org.apache.ignite.internal.processors.query.calcite.trait.RewindabilityTrait;
import org.apache.ignite.internal.processors.query.calcite.trait.TraitUtils;
import org.apache.ignite.internal.processors.query.calcite.trait.TraitsAwareIgniteRel;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.NotNull;

import static org.apache.calcite.plan.RelOptRule.convert;
import static org.apache.calcite.rel.RelDistribution.Type.HASH_DISTRIBUTED;
import static org.apache.calcite.util.ImmutableIntList.range;
import static org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions.broadcast;
import static org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions.hash;
import static org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions.random;
import static org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions.single;
import static org.apache.ignite.internal.processors.query.calcite.trait.TraitUtils.changeTraits;

/**
 *
 */
public class IgniteAggregate extends Aggregate implements TraitsAwareIgniteRel {
    /** {@inheritDoc} */
    public IgniteAggregate(RelOptCluster cluster, RelTraitSet traitSet, RelNode input, ImmutableBitSet groupSet, List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls) {
        super(cluster, traitSet, input, groupSet, groupSets, aggCalls);
    }

    /** {@inheritDoc} */
    public IgniteAggregate(RelInput input) {
        super(changeTraits(input, IgniteConvention.INSTANCE));
    }

    /** {@inheritDoc} */
    @Override public Aggregate copy(RelTraitSet traitSet, RelNode input, ImmutableBitSet groupSet, List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls) {
        return new IgniteAggregate(getCluster(), traitSet, input, groupSet, groupSets, aggCalls);
    }

    /** {@inheritDoc} */
    @Override public <T> T accept(IgniteRelVisitor<T> visitor) {
        return visitor.visit(this);
    }

    /** {@inheritDoc} */
    @Override public List<Pair<RelTraitSet, List<RelTraitSet>>> passThroughRewindability(RelTraitSet nodeTraits, List<RelTraitSet> inputTraits) {
        RewindabilityTrait rewindability = TraitUtils.rewindability(nodeTraits);

        return ImmutableList.of(Pair.of(nodeTraits, ImmutableList.of(inputTraits.get(0).replace(rewindability))));
    }

    /** {@inheritDoc} */
    @Override public List<Pair<RelTraitSet, List<RelTraitSet>>> passThroughDistribution(RelTraitSet nodeTraits, List<RelTraitSet> inputTraits) {
        RelTraitSet in = inputTraits.get(0);

        ImmutableList.Builder<Pair<RelTraitSet, List<RelTraitSet>>> b = ImmutableList.builder();

        IgniteDistribution distribution = TraitUtils.distribution(nodeTraits);

        RelDistribution.Type distrType = distribution.getType();

        switch (distrType) {
            case SINGLETON:
            case BROADCAST_DISTRIBUTED:
                b.add(Pair.of(nodeTraits, ImmutableList.of(in.replace(random())))); // Map-reduce aggregate
                b.add(Pair.of(nodeTraits, ImmutableList.of(in.replace(distribution))));

                break;
            case HASH_DISTRIBUTED:
            case RANDOM_DISTRIBUTED:
                if (!groupSet.isEmpty() && isSimple(this)) {
                    DistributionFunction function = distrType == HASH_DISTRIBUTED
                        ? distribution.function()
                        : DistributionFunction.HashDistribution.INSTANCE;

                    IgniteDistribution outDistr = hash(range(0, groupSet.cardinality()), function);

                    if (outDistr.satisfies(distribution)) {
                        IgniteDistribution inDistr = hash(groupSet.asList(), function);

                        b.add(Pair.of(nodeTraits.replace(outDistr), ImmutableList.of(in.replace(inDistr))));

                        break;
                    }
                }

                b.add(Pair.of(nodeTraits.replace(single()), ImmutableList.of(in.replace(single()))));
            default:
                break;
        }

        return b.build();
    }

    /** {@inheritDoc} */
    @Override public List<Pair<RelTraitSet, List<RelTraitSet>>> passThroughCollation(RelTraitSet nodeTraits, List<RelTraitSet> inputTraits) {
        return ImmutableList.of(Pair.of(nodeTraits.replace(RelCollations.EMPTY),
            ImmutableList.of(inputTraits.get(0).replace(RelCollations.EMPTY))));
    }

    /** {@inheritDoc} */
    @Override public List<Pair<RelTraitSet, List<RelTraitSet>>> deriveRewindability(RelTraitSet nodeTraits, List<RelTraitSet> inputTraits) {
        RelTraitSet in = inputTraits.get(0);

        RewindabilityTrait rewindability = isMapReduce(nodeTraits, in)
            ? RewindabilityTrait.ONE_WAY
            : TraitUtils.rewindability(in);

        return ImmutableList.of(Pair.of(nodeTraits.replace(rewindability), ImmutableList.of(in.replace(rewindability))));
    }

    /** {@inheritDoc} */
    @Override public List<Pair<RelTraitSet, List<RelTraitSet>>> deriveDistribution(RelTraitSet nodeTraits, List<RelTraitSet> inputTraits) {
        RelTraitSet in = inputTraits.get(0);

        ImmutableList.Builder<Pair<RelTraitSet, List<RelTraitSet>>> b = ImmutableList.builder();

        IgniteDistribution distribution = TraitUtils.distribution(in);

        RelDistribution.Type distrType = distribution.getType();

        switch (distrType) {
            case SINGLETON:
            case BROADCAST_DISTRIBUTED:
                b.add(Pair.of(nodeTraits.replace(distribution), ImmutableList.of(in)));

                break;

            case HASH_DISTRIBUTED:
                if (!groupSet.isEmpty() && isSimple(this)) {

                    Mappings.TargetMapping mapping = partialMapping(
                        getInput().getRowType().getFieldCount(), groupSet);

                    IgniteDistribution outDistr = distribution.apply(mapping);

                    if (outDistr.getType() == HASH_DISTRIBUTED)
                        b.add(Pair.of(nodeTraits.replace(outDistr), ImmutableList.of(in)));
                }

            case RANDOM_DISTRIBUTED:
                // Map-reduce aggregates
                b.add(Pair.of(nodeTraits.replace(single()), ImmutableList.of(in.replace(random()))));
                b.add(Pair.of(nodeTraits.replace(broadcast()), ImmutableList.of(in.replace(random()))));

                break;
            default:
                break;
        }

        return b.build();
    }

    /** {@inheritDoc} */
    @Override public List<Pair<RelTraitSet, List<RelTraitSet>>> deriveCollation(RelTraitSet nodeTraits, List<RelTraitSet> inputTraits) {
        return ImmutableList.of(Pair.of(nodeTraits.replace(RelCollations.EMPTY),
            ImmutableList.of(inputTraits.get(0).replace(RelCollations.EMPTY))));
    }

    /** {@inheritDoc} */
    @Override public @NotNull RelNode createNode(RelTraitSet outTraits, List<RelTraitSet> inTraits) {
        RelTraitSet in = inTraits.get(0);

        if (!isMapReduce(outTraits, in))
            return copy(outTraits, ImmutableList.of(convert(getInput(), in)));

        if (U.assertionsEnabled()) {
            ImmutableList<RelTrait> diff = in.difference(outTraits);

            assert diff.size() == 1 && F.first(diff) == TraitUtils.distribution(outTraits);
        }

        RelNode map = new IgniteMapAggregate(getCluster(), in, convert(getInput(), in), groupSet, groupSets, aggCalls);
        return new IgniteReduceAggregate(getCluster(), outTraits, convert(map, outTraits), groupSet, groupSets, aggCalls, getRowType());
    }

    /** */
    private boolean isMapReduce(RelTraitSet out, RelTraitSet in) {
        return TraitUtils.distribution(out).satisfies(single())
            && TraitUtils.distribution(in).satisfies(random());
    }

    /** */
    @NotNull public static Mappings.TargetMapping partialMapping(int inputFieldCount, ImmutableBitSet groupSet) {
        Mappings.TargetMapping mapping =
            Mappings.create(MappingType.INVERSE_FUNCTION,
                inputFieldCount, groupSet.cardinality());

        for (Ord<Integer> group : Ord.zip(groupSet))
            mapping.set(group.e, group.i);
        return mapping;
    }
}

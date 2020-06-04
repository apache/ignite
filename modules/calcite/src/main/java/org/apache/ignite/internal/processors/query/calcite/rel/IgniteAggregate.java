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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.DeriveMode;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.mapping.MappingType;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.ignite.internal.processors.query.calcite.trait.DistributionFunction;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.util.typedef.F;

import static org.apache.calcite.plan.RelOptRule.convert;
import static org.apache.calcite.rel.RelDistribution.Type.HASH_DISTRIBUTED;
import static org.apache.calcite.util.ImmutableIntList.range;
import static org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions.any;
import static org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions.broadcast;
import static org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions.hash;
import static org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions.random;
import static org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions.single;

/**
 *
 */
public class IgniteAggregate extends Aggregate implements IgniteRel {
    /** {@inheritDoc} */
    public IgniteAggregate(RelOptCluster cluster, RelTraitSet traitSet, RelNode input, ImmutableBitSet groupSet, List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls) {
        super(cluster, traitSet, input, groupSet, groupSets, aggCalls);
    }

    /** {@inheritDoc} */
    public IgniteAggregate(RelInput input) {
        super(Commons.changeTraits(input, IgniteConvention.INSTANCE));
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
    @Override public RelNode passThrough(RelTraitSet required) {
        IgniteDistribution toDistr = Commons.distribution(required);

        // Hash aggregate erases collation and only distribution trait can be passed through.
        // So that, it's no use to pass ANY distribution.
        if (toDistr == any())
            return null;

        List<Pair<IgniteDistribution, IgniteDistribution>> distributions = new ArrayList<>();
        RelDistribution.Type distrType = toDistr.getType();

        switch (distrType) {
            case SINGLETON:
            case BROADCAST_DISTRIBUTED:
                if (!groupSet.isEmpty() && Group.induce(groupSet, groupSets) == Group.SIMPLE) {
                    distributions.add(Pair.of(toDistr, random()));
                    distributions.add(Pair.of(toDistr, hash(groupSet.asList())));
                }

                distributions.add(Pair.of(toDistr, toDistr));

                break;
            case RANDOM_DISTRIBUTED:
            case HASH_DISTRIBUTED:
                if (!groupSet.isEmpty() && Group.induce(groupSet, groupSets) == Group.SIMPLE)
                    break;

                DistributionFunction function = distrType == HASH_DISTRIBUTED
                    ? toDistr.function()
                    : DistributionFunction.HashDistribution.INSTANCE;

                IgniteDistribution outDistr = hash(range(0, groupSet.cardinality()), function);

                if (distrType == HASH_DISTRIBUTED && !outDistr.satisfies(toDistr))
                    break;

                IgniteDistribution inDistr = hash(groupSet.asList(), function);

                distributions.add(Pair.of(outDistr, inDistr));

            default:
                break;
        }

        List<RelNode> nodes = createNodes(distributions);

        RelOptPlanner planner = getCluster().getPlanner();
        for (int i = 1; i < nodes.size(); i++)
            planner.register(nodes.get(i), this);

        return F.first(nodes);
    }

    /** {@inheritDoc} */
    @Override public List<RelNode> derive(List<List<RelTraitSet>> inputTraits) {
        assert inputTraits.size() == 1;

        Set<IgniteDistribution> inDistrs = inputTraits.get(0).stream()
            .map(Commons::distribution)
            // Hash aggregate erases collation and only distribution trait can be passed.
            // So that, it's no use to pass ANY distribution.
            .filter(d -> d != any())
            .collect(Collectors.toSet());

        Set<Pair<IgniteDistribution, IgniteDistribution>> pairs = new HashSet<>();

        if (inDistrs.contains(single()))
            pairs.add(Pair.of(single(), single()));

        if (inDistrs.contains(broadcast()))
            pairs.add(Pair.of(broadcast(), broadcast()));

        if (inDistrs.contains(random())) {
            // Map-reduce cases
            pairs.add(Pair.of(single(), random()));
            pairs.add(Pair.of(broadcast(), random()));
        }

        if (!groupSet.isEmpty() && isSimple(this)) {
            int cardinality = groupSet.cardinality();

            // Here we check whether all distribution keys contain in group set
            for (IgniteDistribution in : inDistrs) {
                if (in.getType() != HASH_DISTRIBUTED)
                    continue;

                ImmutableIntList keys = in.getKeys();

                if (keys.size() != cardinality)
                    continue;

                Mappings.TargetMapping mapping = partialMapping(Math.max(Commons.max(keys), cardinality), cardinality);

                IgniteDistribution out = in.apply(mapping);

                if (out.getType() != RelDistribution.Type.RANDOM_DISTRIBUTED)
                    pairs.add(Pair.of(out, in));
            }

            IgniteDistribution in = hash(groupSet.asList());
            IgniteDistribution out = hash(range(0, cardinality));
            pairs.add(Pair.of(out, in));
        }

        return createNodes(pairs);
    }

    /** {@inheritDoc} */
    @Override public DeriveMode getDeriveMode() {
        return DeriveMode.OMAKASE;
    }

    /** */
    private Mappings.TargetMapping partialMapping(int inputFieldCount, int outputFieldCount) {
        Mappings.TargetMapping mapping =
            Mappings.create(MappingType.INVERSE_FUNCTION,
                inputFieldCount, outputFieldCount);

        for (Ord<Integer> group : Ord.zip(groupSet))
            mapping.set(group.e, group.i);

        return mapping;
    }

    /** */
    private List<RelNode> createNodes(Collection<Pair<IgniteDistribution, IgniteDistribution>> distrs) {
        RelOptCluster cluster = getCluster();
        List<RelNode> newRels = new ArrayList<>(distrs.size());

        for (Pair<IgniteDistribution, IgniteDistribution> pair : distrs) {
            RelTraitSet outTraits = cluster.traitSetOf(IgniteConvention.INSTANCE)
                .replace(pair.left);
            RelTraitSet inTraits = cluster.traitSetOf(IgniteConvention.INSTANCE)
                .replace(pair.right);
            RelNode input0 = convert(input, inTraits);

            if (pair.left.satisfies(single()) && pair.right.satisfies(random())) {
                RelTraitSet mapTraits = input0.getTraitSet()
                    .replace(IgniteMapAggregate.distribution(pair.right, groupSet, groupSets, aggCalls));

                input0 = new IgniteMapAggregate(cluster, mapTraits, input0, groupSet, groupSets, aggCalls);
                input0 = convert(input0, pair.left);

                newRels.add(new IgniteReduceAggregate(cluster, outTraits, input0, groupSet, groupSets, aggCalls, getRowType()));
            }
            else
                newRels.add(new IgniteAggregate(cluster, outTraits, input0, groupSet, groupSets, aggCalls));
        }

        return newRels;
    }
}

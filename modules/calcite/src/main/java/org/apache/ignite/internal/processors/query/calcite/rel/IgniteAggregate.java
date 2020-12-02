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
import java.util.List;
import com.google.common.collect.ImmutableList;
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
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;
import org.apache.ignite.internal.processors.query.calcite.trait.RewindabilityTrait;
import org.apache.ignite.internal.processors.query.calcite.trait.TraitUtils;
import org.apache.ignite.internal.processors.query.calcite.trait.TraitsAwareIgniteRel;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
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
        super(cluster, traitSet, ImmutableList.of(), input, groupSet, groupSets, aggCalls);
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
        // Aggregate is rewindable if its input is rewindable.

        RewindabilityTrait rewindability = TraitUtils.rewindability(nodeTraits);

        return ImmutableList.of(Pair.of(nodeTraits, ImmutableList.of(inputTraits.get(0).replace(rewindability))));
    }

    /** {@inheritDoc} */
    @Override public List<Pair<RelTraitSet, List<RelTraitSet>>> passThroughDistribution(RelTraitSet nodeTraits, List<RelTraitSet> inputTraits) {
        // Distribution propagation is based on next rules:
        // 1) Any aggregation is possible on single or broadcast distribution.
        // 2) hash-distributed aggregation is possible in case it's a simple aggregate having hash distributed input
        //    and all of input distribution keys are parts of aggregation group and vice versa.
        // 3) Map-reduce aggregation is possible in case it's a simple aggregate and its input has random distribution.

        RelTraitSet in = inputTraits.get(0);

        List<Pair<RelTraitSet, List<RelTraitSet>>> res = new ArrayList<>();

        IgniteDistribution distribution = TraitUtils.distribution(nodeTraits);

        RelDistribution.Type distrType = distribution.getType();

        switch (distrType) {
            case SINGLETON:
            case BROADCAST_DISTRIBUTED:
                res.add(Pair.of(nodeTraits, ImmutableList.of(in.replace(distribution))));

                if (isSimple(this))
                    res.add(Pair.of(nodeTraits, ImmutableList.of(in.replace(random())))); // Map-reduce aggregate

                break;

            case RANDOM_DISTRIBUTED:
                if (!groupSet.isEmpty() && isSimple(this)) {
                    IgniteDistribution outDistr = hash(range(0, groupSet.cardinality()));
                    IgniteDistribution inDistr = hash(groupSet.asList());

                    res.add(Pair.of(nodeTraits.replace(outDistr), ImmutableList.of(in.replace(inDistr))));
                }

                break;

            case HASH_DISTRIBUTED:
                ImmutableIntList keys = distribution.getKeys();

                if (isSimple(this) && groupSet.cardinality() == keys.size()) {
                    Mappings.TargetMapping mapping = Commons.inverseMapping(
                        groupSet, getInput().getRowType().getFieldCount());

                    List<Integer> srcKeys = new ArrayList<>(keys.size());

                    for (int key : keys) {
                        int src = mapping.getSourceOpt(key);

                        if (src == -1)
                            break;

                        srcKeys.add(src);
                    }

                    if (srcKeys.size() == keys.size()) {
                        res.add(Pair.of(nodeTraits, ImmutableList.of(in.replace(hash(srcKeys, distribution.function())))));

                        break;
                    }
                }

                break;

            default:
                break;
        }

        if (!res.isEmpty())
            return res;

        return ImmutableList.of(Pair.of(nodeTraits.replace(single()), ImmutableList.of(in.replace(single()))));
    }

    /** {@inheritDoc} */
    @Override public List<Pair<RelTraitSet, List<RelTraitSet>>> passThroughCollation(RelTraitSet nodeTraits, List<RelTraitSet> inputTraits) {
        // Since it's a hash aggregate it erases collation.

        return ImmutableList.of(Pair.of(nodeTraits.replace(RelCollations.EMPTY),
            ImmutableList.of(inputTraits.get(0).replace(RelCollations.EMPTY))));
    }

    /** {@inheritDoc} */
    @Override public List<Pair<RelTraitSet, List<RelTraitSet>>> deriveRewindability(RelTraitSet nodeTraits, List<RelTraitSet> inputTraits) {
        // Aggregate is rewindable if its input is rewindable.

        RelTraitSet in = inputTraits.get(0);

        RewindabilityTrait rewindability = isMapReduce(nodeTraits, in)
            ? RewindabilityTrait.ONE_WAY
            : TraitUtils.rewindability(in);

        return ImmutableList.of(Pair.of(nodeTraits.replace(rewindability), ImmutableList.of(in.replace(rewindability))));
    }

    /** {@inheritDoc} */
    @Override public List<Pair<RelTraitSet, List<RelTraitSet>>> deriveDistribution(RelTraitSet nodeTraits, List<RelTraitSet> inputTraits) {
        // Distribution propagation is based on next rules:
        // 1) Any aggregation is possible on single or broadcast distribution.
        // 2) hash-distributed aggregation is possible in case it's a simple aggregate having hash distributed input
        //    and all of input distribution keys are parts aggregation group.
        // 3) Map-reduce aggregation is possible in case it's a simple aggregate and its input has random distribution.

        RelTraitSet in = inputTraits.get(0);

        List<Pair<RelTraitSet, List<RelTraitSet>>> res = new ArrayList<>();

        IgniteDistribution distribution = TraitUtils.distribution(in);

        RelDistribution.Type distrType = distribution.getType();

        switch (distrType) {
            case SINGLETON:
            case BROADCAST_DISTRIBUTED:
                res.add(Pair.of(nodeTraits.replace(distribution), ImmutableList.of(in)));

                break;

            case HASH_DISTRIBUTED:
                if (isSimple(this)) {
                    ImmutableIntList keys = distribution.getKeys();

                    if (groupSet.cardinality() == keys.size()) {
                        Mappings.TargetMapping mapping = Commons.inverseMapping(
                            groupSet, getInput().getRowType().getFieldCount());

                        IgniteDistribution outDistr = distribution.apply(mapping);

                        if (outDistr.getType() == HASH_DISTRIBUTED)
                            res.add(Pair.of(nodeTraits.replace(outDistr), ImmutableList.of(in)));
                    }
                }

                break;

            case RANDOM_DISTRIBUTED:
                // Map-reduce aggregates
                if (isSimple(this)) {
                    res.add(Pair.of(nodeTraits.replace(single()), ImmutableList.of(in.replace(random()))));
                    res.add(Pair.of(nodeTraits.replace(broadcast()), ImmutableList.of(in.replace(random()))));
                }

                break;

            default:
                break;
        }

        if (!res.isEmpty())
            return res;

        return ImmutableList.of(Pair.of(nodeTraits.replace(single()), ImmutableList.of(in.replace(single()))));
    }

    /** {@inheritDoc} */
    @Override public List<Pair<RelTraitSet, List<RelTraitSet>>> deriveCollation(RelTraitSet nodeTraits, List<RelTraitSet> inputTraits) {
        // Since it's a hash aggregate it erases collation.

        return ImmutableList.of(Pair.of(nodeTraits.replace(RelCollations.EMPTY),
            ImmutableList.of(inputTraits.get(0).replace(RelCollations.EMPTY))));
    }

    /** {@inheritDoc} */
    @Override public List<Pair<RelTraitSet, List<RelTraitSet>>> passThroughCorrelation(RelTraitSet nodeTraits,
        List<RelTraitSet> inTraits) {
        return ImmutableList.of(Pair.of(nodeTraits,
            ImmutableList.of(inTraits.get(0).replace(TraitUtils.correlation(nodeTraits)))));
    }

    /** {@inheritDoc} */
    @Override public List<Pair<RelTraitSet, List<RelTraitSet>>> deriveCorrelation(RelTraitSet nodeTraits,
        List<RelTraitSet> inTraits) {
        return ImmutableList.of(Pair.of(nodeTraits.replace(TraitUtils.correlation(inTraits.get(0))),
            inTraits));
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

    /** {@inheritDoc} */
    @Override public IgniteRel clone(RelOptCluster cluster, List<IgniteRel> inputs) {
        return new IgniteAggregate(cluster, getTraitSet(), sole(inputs),
            getGroupSet(), getGroupSets(), getAggCallList());
    }
}

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
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.util.Pair;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;
import org.apache.ignite.internal.processors.query.calcite.trait.RewindabilityTrait;
import org.apache.ignite.internal.processors.query.calcite.trait.TraitUtils;
import org.apache.ignite.internal.processors.query.calcite.trait.TraitsAwareIgniteRel;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;

/**
 *
 */
public class IgniteUnionAll extends Union implements TraitsAwareIgniteRel {
    /** */
    public IgniteUnionAll(RelOptCluster cluster, RelTraitSet traits, List<RelNode> inputs) {
        super(cluster, traits, inputs, true);
    }

    /** */
    public IgniteUnionAll(RelInput input) {
        this(
            input.getCluster(),
            input.getTraitSet().replace(IgniteConvention.INSTANCE),
            input.getInputs());
    }

    /** {@inheritDoc} */
    @Override public SetOp copy(RelTraitSet traitSet, List<RelNode> inputs, boolean all) {
        assert all;

        return new IgniteUnionAll(getCluster(), traitSet, inputs);
    }

    /** {@inheritDoc} */
    @Override public <T> T accept(IgniteRelVisitor<T> visitor) {
        return visitor.visit(this);
    }

    /** {@inheritDoc} */
    @Override public List<Pair<RelTraitSet, List<RelTraitSet>>> passThroughRewindability(RelTraitSet nodeTraits, List<RelTraitSet> inputTraits) {
        // Union node requires the same traits from all its inputs.

        RewindabilityTrait rewindability = TraitUtils.rewindability(nodeTraits);

        return ImmutableList.of(Pair.of(nodeTraits,
            Commons.transform(inputTraits, t -> t.replace(rewindability))));
    }

    /** {@inheritDoc} */
    @Override public List<Pair<RelTraitSet, List<RelTraitSet>>> passThroughDistribution(RelTraitSet nodeTraits, List<RelTraitSet> inputTraits) {
        // Union node requires the same traits from all its inputs.

        IgniteDistribution distribution = TraitUtils.distribution(nodeTraits);

        return ImmutableList.of(Pair.of(nodeTraits,
            Commons.transform(inputTraits, t -> t.replace(distribution))));
    }

    /** {@inheritDoc} */
    @Override public List<Pair<RelTraitSet, List<RelTraitSet>>> passThroughCollation(RelTraitSet nodeTraits, List<RelTraitSet> inputTraits) {
        // Union node erases collation. TODO union all using merge sort algorythm

        return ImmutableList.of(Pair.of(nodeTraits.replace(RelCollations.EMPTY),
            Commons.transform(inputTraits, t -> t.replace(RelCollations.EMPTY))));
    }

    /** {@inheritDoc} */
    @Override public List<Pair<RelTraitSet, List<RelTraitSet>>> deriveRewindability(RelTraitSet nodeTraits, List<RelTraitSet> inputTraits) {
        // Union node requires the same traits from all its inputs.

        boolean rewindable = inputTraits.stream()
            .map(TraitUtils::rewindability)
            .allMatch(RewindabilityTrait::rewindable);

        if (rewindable)
            return ImmutableList.of(Pair.of(nodeTraits.replace(RewindabilityTrait.REWINDABLE), inputTraits));

        return ImmutableList.of(Pair.of(nodeTraits.replace(RewindabilityTrait.ONE_WAY),
            Commons.transform(inputTraits, t -> t.replace(RewindabilityTrait.ONE_WAY))));
    }

    /** {@inheritDoc} */
    @Override public List<Pair<RelTraitSet, List<RelTraitSet>>> deriveDistribution(RelTraitSet nodeTraits, List<RelTraitSet> inputTraits) {
        // Union node requires the same traits from all its inputs.

        Set<IgniteDistribution> distributions = inputTraits.stream()
            .map(TraitUtils::distribution)
            .collect(Collectors.toSet());

        ImmutableList.Builder<Pair<RelTraitSet, List<RelTraitSet>>> b = ImmutableList.builder();

        for (IgniteDistribution distribution : distributions)
            b.add(Pair.of(nodeTraits.replace(distribution),
                Commons.transform(inputTraits, t -> t.replace(distribution))));

        return b.build();
    }

    /** {@inheritDoc} */
    @Override public List<Pair<RelTraitSet, List<RelTraitSet>>> deriveCollation(RelTraitSet nodeTraits, List<RelTraitSet> inputTraits) {
        // Union node erases collation. TODO union all using merge sort algorythm

        return ImmutableList.of(Pair.of(nodeTraits.replace(RelCollations.EMPTY),
            Commons.transform(inputTraits, t -> t.replace(RelCollations.EMPTY))));
    }
}

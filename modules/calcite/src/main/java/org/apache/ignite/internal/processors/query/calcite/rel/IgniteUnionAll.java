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

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
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
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.util.typedef.internal.U;

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
    @Override public Collection<Pair<RelTraitSet, List<RelTraitSet>>> passThroughRewindability(
        Collection<Pair<RelTraitSet, List<RelTraitSet>>> traits) {
        HashSet<Pair<RelTraitSet, List<RelTraitSet>>> traits0 = U.newHashSet(traits.size());

        for (Pair<RelTraitSet, List<RelTraitSet>> pair : traits) {
            RelTraitSet out = pair.left;
            RewindabilityTrait rewindability = TraitUtils.rewindability(out);

            traits0.add(Pair.of(out, Commons.transform(pair.right, t -> t.replace(rewindability))));
        }

        return traits0;
    }

    /** {@inheritDoc} */
    @Override public Collection<Pair<RelTraitSet, List<RelTraitSet>>> passThroughDistribution(
        Collection<Pair<RelTraitSet, List<RelTraitSet>>> traits) {
        HashSet<Pair<RelTraitSet, List<RelTraitSet>>> traits0 = U.newHashSet(traits.size());

        for (Pair<RelTraitSet, List<RelTraitSet>> pair : traits) {
            RelTraitSet out = pair.left;
            IgniteDistribution distribution = TraitUtils.distribution(out);

            traits0.add(Pair.of(out, Commons.transform(pair.right, t -> t.replace(distribution))));
        }

        return traits0;
    }

    /** {@inheritDoc} */
    @Override public Collection<Pair<RelTraitSet, List<RelTraitSet>>> passThroughCollation(
        Collection<Pair<RelTraitSet, List<RelTraitSet>>> traits) {
        HashSet<Pair<RelTraitSet, List<RelTraitSet>>> traits0 = U.newHashSet(traits.size());

        for (Pair<RelTraitSet, List<RelTraitSet>> pair : traits) {
            RelTraitSet out = pair.left;

            traits0.add(Pair.of(out.replace(RelCollations.EMPTY),
                Commons.transform(pair.right, t -> t.replace(RelCollations.EMPTY))));
        }

        return traits0;
    }

    /** {@inheritDoc} */
    @Override public Collection<Pair<RelTraitSet, List<RelTraitSet>>> deriveRewindability(
        Collection<Pair<RelTraitSet, List<RelTraitSet>>> traits) {
        HashSet<Pair<RelTraitSet, List<RelTraitSet>>> traits0 = U.newHashSet(traits.size());

        for (Pair<RelTraitSet, List<RelTraitSet>> pair : traits) {
            RelTraitSet out = pair.left;

            boolean rewindable = pair.right.stream()
                .map(TraitUtils::rewindability)
                .allMatch(RewindabilityTrait::rewindable);

            if (rewindable)
                traits0.add(Pair.of(out.replace(RewindabilityTrait.REWINDABLE), pair.right));
            else
                traits0.add(Pair.of(out.replace(RewindabilityTrait.ONE_WAY),
                    Commons.transform(pair.right, t -> t.replace(RewindabilityTrait.ONE_WAY))));
        }

        return traits0;
    }

    /** {@inheritDoc} */
    @Override public Collection<Pair<RelTraitSet, List<RelTraitSet>>> deriveDistribution(
        Collection<Pair<RelTraitSet, List<RelTraitSet>>> traits) {
        HashSet<Pair<RelTraitSet, List<RelTraitSet>>> traits0 = U.newHashSet(traits.size());

        for (Pair<RelTraitSet, List<RelTraitSet>> pair : traits) {
            RelTraitSet out = pair.left;

            Set<IgniteDistribution> distributions = pair.right.stream()
                .map(TraitUtils::distribution)
                .collect(Collectors.toSet());

            for (IgniteDistribution distribution : distributions)
                traits0.add(Pair.of(out.replace(distribution),
                    Commons.transform(pair.right, t -> t.replace(distribution))));
        }

        return traits0;
    }

    /** {@inheritDoc} */
    @Override public Collection<Pair<RelTraitSet, List<RelTraitSet>>> deriveCollation(
        Collection<Pair<RelTraitSet, List<RelTraitSet>>> traits) {
        HashSet<Pair<RelTraitSet, List<RelTraitSet>>> traits0 = U.newHashSet(traits.size());

        for (Pair<RelTraitSet, List<RelTraitSet>> pair : traits) {
            RelTraitSet out = pair.left;

            traits0.add(Pair.of(out.replace(RelCollations.EMPTY),
                Commons.transform(pair.right, t -> t.replace(RelCollations.EMPTY))));
        }

        return traits0;
    }
}

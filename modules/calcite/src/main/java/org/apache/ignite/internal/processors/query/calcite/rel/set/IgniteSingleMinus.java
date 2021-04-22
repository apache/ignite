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

package org.apache.ignite.internal.processors.query.calcite.rel.set;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.util.Pair;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRelVisitor;
import org.apache.ignite.internal.processors.query.calcite.trait.CorrelationTrait;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.processors.query.calcite.trait.RewindabilityTrait;
import org.apache.ignite.internal.processors.query.calcite.trait.TraitUtils;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;

/**
 * Physical node for MINUS (EXCEPT) operator which inputs satisfy SINGLE distribution.
 */
public class IgniteSingleMinus extends IgniteMinusBase {
    /** {@inheritDoc} */
    public IgniteSingleMinus(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        List<RelNode> inputs,
        boolean all
    ) {
        super(cluster, traitSet, inputs, all);
    }

    /** */
    public IgniteSingleMinus(RelInput input) {
        super(input);
    }

    /** {@inheritDoc} */
    @Override public List<Pair<RelTraitSet, List<RelTraitSet>>> deriveRewindability(
        RelTraitSet nodeTraits,
        List<RelTraitSet> inputTraits
    ) {
        boolean rewindable = inputTraits.stream()
            .map(TraitUtils::rewindability)
            .allMatch(RewindabilityTrait::rewindable);

        if (rewindable)
            return ImmutableList.of(Pair.of(nodeTraits.replace(RewindabilityTrait.REWINDABLE), inputTraits));

        return ImmutableList.of(Pair.of(nodeTraits.replace(RewindabilityTrait.ONE_WAY),
            Commons.transform(inputTraits, t -> t.replace(RewindabilityTrait.ONE_WAY))));
    }

    /** {@inheritDoc} */
    @Override public Pair<RelTraitSet, List<RelTraitSet>> passThroughDistribution(RelTraitSet nodeTraits, List<RelTraitSet> inTraits) {
        IgniteDistribution distr = TraitUtils.distribution(nodeTraits);

        if (IgniteDistributions.single().satisfies(distr)) {
            return Pair.of(nodeTraits.replace(IgniteDistributions.single()),
                Commons.transform(inTraits, t -> t.replace(IgniteDistributions.single())));
        }

        return null;
    }

    /** {@inheritDoc} */
    @Override public List<Pair<RelTraitSet, List<RelTraitSet>>> deriveDistribution(
        RelTraitSet nodeTraits,
        List<RelTraitSet> inputTraits
    ) {
        boolean single = inputTraits.stream()
            .map(TraitUtils::distribution)
            .allMatch(d -> d.satisfies(IgniteDistributions.single()));

        if (!single)
            return ImmutableList.of();

        return ImmutableList.of(Pair.of(nodeTraits.replace(IgniteDistributions.single()), inputTraits));
    }

    /** {@inheritDoc} */
    @Override public List<Pair<RelTraitSet, List<RelTraitSet>>> deriveCorrelation(
        RelTraitSet nodeTraits,
        List<RelTraitSet> inTraits
    ) {
        Set<CorrelationId> correlationIds = inTraits.stream()
            .map(TraitUtils::correlation)
            .flatMap(corrTr -> corrTr.correlationIds().stream())
            .collect(Collectors.toSet());

        return ImmutableList.of(Pair.of(nodeTraits.replace(CorrelationTrait.correlations(correlationIds)),
            inTraits));
    }

    /** {@inheritDoc} */
    @Override public IgniteSingleMinus copy(RelTraitSet traitSet, List<RelNode> inputs, boolean all) {
        return new IgniteSingleMinus(getCluster(), traitSet, inputs, all);
    }

    /** {@inheritDoc} */
    @Override public IgniteRel clone(RelOptCluster cluster, List<IgniteRel> inputs) {
        return new IgniteSingleMinus(cluster, getTraitSet(), Commons.cast(inputs), all);
    }

    /** {@inheritDoc} */
    @Override public <T> T accept(IgniteRelVisitor<T> visitor) {
        return visitor.visit(this);
    }

    /** {@inheritDoc} */
    @Override protected int aggregateFieldsCount() {
        return getInput(0).getRowType().getFieldCount() + COUNTER_FIELDS_CNT;
    }
}

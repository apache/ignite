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
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.util.Pair;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRelVisitor;
import org.apache.ignite.internal.processors.query.calcite.trait.CorrelationTrait;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.processors.query.calcite.trait.RewindabilityTrait;
import org.apache.ignite.internal.processors.query.calcite.trait.TraitUtils;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;

/**
 * Physical node for MAP phase of MINUS (EXCEPT) operator.
 */
public class IgniteMapMinus extends IgniteMinusBase {
    /** */
    public IgniteMapMinus(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        List<RelNode> inputs,
        boolean all
    ) {
        super(cluster, traitSet, inputs, all);
    }

    /** */
    public IgniteMapMinus(RelInput input) {
        super(input);
    }

    /** {@inheritDoc} */
    @Override public IgniteMapMinus copy(RelTraitSet traitSet, List<RelNode> inputs, boolean all) {
        return new IgniteMapMinus(getCluster(), traitSet, inputs, all);
    }

    /** {@inheritDoc} */
    @Override public IgniteRel clone(RelOptCluster cluster, List<IgniteRel> inputs) {
        return new IgniteMapMinus(cluster, getTraitSet(), Commons.cast(inputs), all);
    }

    /** {@inheritDoc} */
    @Override public <T> T accept(IgniteRelVisitor<T> visitor) {
        return visitor.visit(this);
    }

    /** {@inheritDoc} */
    @Override protected RelDataType deriveRowType() {
        RelDataTypeFactory typeFactory = Commons.typeFactory(getCluster());

        assert typeFactory instanceof IgniteTypeFactory;

        RelDataTypeFactory.Builder builder = new RelDataTypeFactory.Builder(typeFactory);

        builder.add("GROUP_KEY", typeFactory.createJavaType(Object.class/*GroupKey.class*/));
        builder.add("COUNTERS", typeFactory.createJavaType(int[].class));

        return builder.build();
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
    @Override public List<Pair<RelTraitSet, List<RelTraitSet>>> deriveDistribution(
        RelTraitSet nodeTraits,
        List<RelTraitSet> inputTraits
    ) {
        if (inputTraits.stream().allMatch(t -> TraitUtils.distribution(t).satisfies(IgniteDistributions.single())))
            return ImmutableList.of(); // If all distributions are single or broadcast IgniteSingleMinus should be used.

        if (inputTraits.stream().anyMatch(t -> TraitUtils.distribution(t) == IgniteDistributions.single()))
            return ImmutableList.of(); // Mixing of single and random is prohibited.

        return ImmutableList.of(
            Pair.of(nodeTraits.replace(IgniteDistributions.random()), Commons.transform(inputTraits,
                t -> t.replace(IgniteDistributions.random())))
        );
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
    @Override protected int aggregateFieldsCount() {
        return getInput(0).getRowType().getFieldCount() + COUNTER_FIELDS_CNT;
    }
}

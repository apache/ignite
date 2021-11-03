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
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.util.Pair;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.agg.AggregateType;
import org.apache.ignite.internal.processors.query.calcite.trait.CorrelationTrait;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.processors.query.calcite.trait.RewindabilityTrait;
import org.apache.ignite.internal.processors.query.calcite.trait.TraitUtils;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;

/**
 * Physical node for set op (MINUS, INTERSECT) operator which inputs satisfy SINGLE distribution.
 */
public interface IgniteSingleSetOp extends IgniteSetOp {
    /** {@inheritDoc} */
    @Override
    public default List<Pair<RelTraitSet, List<RelTraitSet>>> deriveRewindability(
            RelTraitSet nodeTraits,
            List<RelTraitSet> inputTraits
    ) {
        boolean rewindable = inputTraits.stream()
                .map(TraitUtils::rewindability)
                .allMatch(RewindabilityTrait::rewindable);
    
        if (rewindable) {
            return List.of(Pair.of(nodeTraits.replace(RewindabilityTrait.REWINDABLE), inputTraits));
        }
        
        return List.of(Pair.of(nodeTraits.replace(RewindabilityTrait.ONE_WAY),
                Commons.transform(inputTraits, t -> t.replace(RewindabilityTrait.ONE_WAY))));
    }
    
    /** {@inheritDoc} */
    @Override
    public default Pair<RelTraitSet, List<RelTraitSet>> passThroughDistribution(RelTraitSet nodeTraits,
            List<RelTraitSet> inTraits) {
        if (TraitUtils.distribution(nodeTraits) == IgniteDistributions.single()) {
            return Pair.of(nodeTraits, Commons.transform(inTraits, t -> t.replace(IgniteDistributions.single())));
        }
        
        return null;
    }
    
    /** {@inheritDoc} */
    @Override
    public default List<Pair<RelTraitSet, List<RelTraitSet>>> deriveDistribution(
            RelTraitSet nodeTraits,
            List<RelTraitSet> inputTraits
    ) {
        boolean single = inputTraits.stream()
                .map(TraitUtils::distribution)
                .allMatch(d -> d.satisfies(IgniteDistributions.single()));
    
        if (!single) {
            return List.of();
        }
        
        return List.of(Pair.of(nodeTraits.replace(IgniteDistributions.single()), inputTraits));
    }
    
    /** {@inheritDoc} */
    @Override
    public default List<Pair<RelTraitSet, List<RelTraitSet>>> deriveCorrelation(
            RelTraitSet nodeTraits,
            List<RelTraitSet> inTraits
    ) {
        Set<CorrelationId> correlationIds = inTraits.stream()
                .map(TraitUtils::correlation)
                .flatMap(corrTr -> corrTr.correlationIds().stream())
                .collect(Collectors.toSet());
        
        return List.of(Pair.of(nodeTraits.replace(CorrelationTrait.correlations(correlationIds)),
                inTraits));
    }
    
    /** {@inheritDoc} */
    @Override
    public default AggregateType aggregateType() {
        return AggregateType.SINGLE;
    }
}

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
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.util.Pair;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.agg.AggregateType;
import org.apache.ignite.internal.processors.query.calcite.trait.CorrelationTrait;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.processors.query.calcite.trait.TraitUtils;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;

/**
 * Physical node for set op (MINUS, INTERSECT) operator which inputs are colocated.
 */
public interface IgniteColocatedSetOp extends IgniteSetOp {
    /** {@inheritDoc} */
    @Override public default Pair<RelTraitSet, List<RelTraitSet>> passThroughDistribution(RelTraitSet nodeTraits,
        List<RelTraitSet> inTraits) {
        if (TraitUtils.distribution(nodeTraits) == IgniteDistributions.single())
            return Pair.of(nodeTraits, Commons.transform(inTraits, t -> t.replace(IgniteDistributions.single())));

        return null;
    }

    /** {@inheritDoc} */
    @Override public default List<Pair<RelTraitSet, List<RelTraitSet>>> deriveDistribution(
        RelTraitSet nodeTraits,
        List<RelTraitSet> inputTraits
    ) {
        boolean haveSingle = false;
        IgniteDistribution hashDistribution = null;

        for (RelTraitSet traits : inputTraits) {
            IgniteDistribution distribution = TraitUtils.distribution(traits);

            if (distribution == IgniteDistributions.single()) {
                if (hashDistribution != null) // Single incompatible with hash.
                    return ImmutableList.of();

                haveSingle = true;
            }
            else if (distribution.getType() == RelDistribution.Type.HASH_DISTRIBUTED) {
                if (haveSingle) // Hash incompatible with single.
                    return ImmutableList.of();

                if (hashDistribution == null)
                    hashDistribution = distribution;
                else if (!hashDistribution.satisfies(distribution))
                    return ImmutableList.of();

            }
            else if (distribution != IgniteDistributions.broadcast())
                return ImmutableList.of();
        }

        assert hashDistribution == null || !haveSingle;

        if (haveSingle)
            return ImmutableList.of(Pair.of(nodeTraits.replace(IgniteDistributions.single()), inputTraits));
        else if (hashDistribution != null) {
            IgniteDistribution distribution = hashDistribution;

            return ImmutableList.of(Pair.of(nodeTraits.replace(distribution),
                Commons.transform(inputTraits, t -> t.replace(distribution))));
        }
        else
            return ImmutableList.of(Pair.of(nodeTraits.replace(IgniteDistributions.broadcast()), inputTraits));
    }

    /** {@inheritDoc} */
    @Override public default List<Pair<RelTraitSet, List<RelTraitSet>>> deriveCorrelation(
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
    @Override public default AggregateType aggregateType() {
        return AggregateType.SINGLE;
    }
}

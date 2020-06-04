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
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Exchange;
import org.apache.calcite.util.Pair;
import org.apache.ignite.internal.processors.query.calcite.trait.DistributionTraitDef;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.processors.query.calcite.trait.TraitUtils;

import static org.apache.calcite.rel.RelDistribution.Type.BROADCAST_DISTRIBUTED;
import static org.apache.calcite.rel.RelDistribution.Type.HASH_DISTRIBUTED;
import static org.apache.ignite.internal.processors.query.calcite.trait.TraitUtils.changeTraits;
import static org.apache.ignite.internal.processors.query.calcite.trait.TraitUtils.fixTraits;

/**
 *
 */
public class IgniteTrimExchange extends Exchange implements IgniteRel {
    /** */
    public IgniteTrimExchange(RelOptCluster cluster, RelTraitSet traits, RelNode input, RelDistribution distribution) {
        super(cluster, traits, input, distribution);

        assert distribution.getType() == HASH_DISTRIBUTED;
        assert input.getTraitSet().getTrait(DistributionTraitDef.INSTANCE).getType() == BROADCAST_DISTRIBUTED;
    }

    public IgniteTrimExchange(RelInput input) {
        super(changeTraits(input, IgniteConvention.INSTANCE));
    }

    /** {@inheritDoc} */
    @Override public IgniteDistribution distribution() {
        return (IgniteDistribution)distribution;
    }

    /** {@inheritDoc} */
    @Override public Exchange copy(RelTraitSet traits, RelNode input, RelDistribution distribution) {
        return new IgniteTrimExchange(getCluster(), traits, input, distribution);
    }

    /** {@inheritDoc} */
    @Override public <T> T accept(IgniteRelVisitor<T> visitor) {
        return visitor.visit(this);
    }

    /** {@inheritDoc} */
    @Override public boolean isEnforcer() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public Pair<RelTraitSet, List<RelTraitSet>> passThroughTraits(RelTraitSet required) {
        required = fixTraits(required);

        IgniteDistribution distribution = TraitUtils.distribution(required);

        if (!distribution().satisfies(distribution))
            return null;

        return Pair.of(required.replace(distribution()), ImmutableList.of(required.replace(IgniteDistributions.broadcast())));
    }

    /** {@inheritDoc} */
    @Override public Pair<RelTraitSet, List<RelTraitSet>> deriveTraits(RelTraitSet childTraits, int childId) {
        assert childId == 0;

        childTraits = fixTraits(childTraits);

        return Pair.of(childTraits.replace(distribution()), ImmutableList.of(childTraits.replace(IgniteDistributions.broadcast())));
    }
}

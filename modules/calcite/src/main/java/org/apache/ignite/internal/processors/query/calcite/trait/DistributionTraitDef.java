/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query.calcite.trait;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Exchange;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteExchange;

/**
 *
 */
public class DistributionTraitDef extends RelTraitDef<IgniteDistribution> {
    /** */
    public static final DistributionTraitDef INSTANCE = new DistributionTraitDef();

    @Override public Class<IgniteDistribution> getTraitClass() {
        return IgniteDistribution.class;
    }

    @Override public String getSimpleName() {
        return "distr";
    }

    @Override public RelNode convert(RelOptPlanner planner, RelNode rel, IgniteDistribution targetDist, boolean allowInfiniteCostConverters) {
        if (rel.getConvention() == Convention.NONE)
            return null;

        RelDistribution srcDist = rel.getTraitSet().getTrait(INSTANCE);

        if (srcDist == targetDist) // has to be interned
            return rel;

        switch(targetDist.getType()){
            case HASH_DISTRIBUTED:
            case BROADCAST_DISTRIBUTED:
            case SINGLETON:
                Exchange exchange = new IgniteExchange(rel.getCluster(), rel.getTraitSet().replace(targetDist), rel, targetDist);
                RelNode newRel = planner.register(exchange, rel);
                RelTraitSet newTraits = rel.getTraitSet().replace(targetDist);

                if (!newRel.getTraitSet().equals(newTraits))
                    newRel = planner.changeTraits(newRel, newTraits);

                return newRel;
            case ANY:
                return rel;
            default:
                return null;
        }
    }

    @Override public boolean canConvert(RelOptPlanner planner, IgniteDistribution fromTrait, IgniteDistribution toTrait) {
        return true;
    }

    @Override public IgniteDistribution getDefault() {
        return IgniteDistributions.any();
    }
}

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

import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.rel.logical.IgniteLogicalExchange;

/**
 *
 */
public class IgniteDistributionTraitDef extends RelTraitDef<IgniteDistribution> {
    /** */
    public static final IgniteDistributionTraitDef INSTANCE = new IgniteDistributionTraitDef();

    @Override public Class<IgniteDistribution> getTraitClass() {
        return IgniteDistribution.class;
    }

    @Override public String getSimpleName() {
        return "distr";
    }

    @Override public RelNode convert(RelOptPlanner planner, RelNode rel, IgniteDistribution targetDist, boolean allowInfiniteCostConverters) {
        IgniteDistribution srcDist = rel.getTraitSet().getTrait(INSTANCE);

        // Source and Target have the same trait.
        if (srcDist.equals(targetDist))
            return rel;

        if (rel.getConvention() != IgniteRel.LOGICAL_CONVENTION)
            return null;

        switch(targetDist.type()){
            case HASH:
            case BROADCAST:
            case SINGLE:
                return new IgniteLogicalExchange(rel.getCluster(), rel.getTraitSet().replace(targetDist), rel);
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
        return IgniteDistribution.ANY;
    }
}

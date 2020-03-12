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

package org.apache.ignite.internal.processors.query.calcite.trait;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteExchange;

/**
 *
 */
public class DistributionTraitDef extends RelTraitDef<IgniteDistribution> {
    /** */
    public static final DistributionTraitDef INSTANCE = new DistributionTraitDef();

    /** {@inheritDoc} */
    @Override public Class<IgniteDistribution> getTraitClass() {
        return IgniteDistribution.class;
    }

    /** {@inheritDoc} */
    @Override public String getSimpleName() {
        return "distr";
    }

    /** {@inheritDoc} */
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
                return register(planner, rel,
                    new IgniteExchange(rel.getCluster(), rel.getTraitSet().replace(targetDist), rel, targetDist));
            case ANY:
                return rel;
            default:
                return null;
        }
    }

    /** {@inheritDoc} */
    @Override public boolean canConvert(RelOptPlanner planner, IgniteDistribution fromTrait, IgniteDistribution toTrait) {
        return true;
    }

    /** {@inheritDoc} */
    @Override public IgniteDistribution getDefault() {
        return IgniteDistributions.any();
    }

    /** */
    private RelNode register(RelOptPlanner planner, RelNode rel, RelNode replace) {
        RelNode registered = planner.register(replace, rel);

        if (!registered.getTraitSet().equals(replace.getTraitSet()))
            registered = planner.changeTraits(registered, replace.getTraitSet());

        return registered;
    }
}

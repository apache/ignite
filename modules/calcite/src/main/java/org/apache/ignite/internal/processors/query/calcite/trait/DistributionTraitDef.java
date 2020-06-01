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

import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalExchange;

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
    @Override public RelNode convert(RelOptPlanner planner, RelNode rel, IgniteDistribution toDist, boolean allowInfiniteCostConverters) {
        if (toDist.getType() == RelDistribution.Type.ANY)
            return null;

        LogicalExchange exchange = LogicalExchange.create(rel, toDist);
        RelNode newRel = planner.register(exchange, rel);
        RelTraitSet newTraits = rel.getTraitSet().replace(toDist);
        if (!newRel.getTraitSet().equals(newTraits))
            newRel = planner.changeTraits(newRel, newTraits);

        return newRel;
    }

    /** {@inheritDoc} */
    @Override public boolean canConvert(RelOptPlanner planner, IgniteDistribution fromTrait, IgniteDistribution toTrait) {
        return true;
    }

    /** {@inheritDoc} */
    @Override public IgniteDistribution getDefault() {
        return IgniteDistributions.any();
    }
}

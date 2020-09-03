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
import org.apache.calcite.rel.RelNode;

public class RewindabilityTraitDef extends RelTraitDef<RewindabilityTrait> {
    /** */
    public static final RewindabilityTraitDef INSTANCE = new RewindabilityTraitDef();

    /** {@inheritDoc} */
    @Override public Class<RewindabilityTrait> getTraitClass() {
        return RewindabilityTrait.class;
    }

    /** {@inheritDoc} */
    @Override public String getSimpleName() {
        return "rewindability";
    }

    /** {@inheritDoc} */
    @Override public RelNode convert(RelOptPlanner planner, RelNode rel, RewindabilityTrait toTrait, boolean allowInfiniteCostConverters) {
        return TraitUtils.convertRewindability(planner, toTrait, rel);
    }

    /** {@inheritDoc} */
    @Override public boolean canConvert(RelOptPlanner planner, RewindabilityTrait fromTrait, RewindabilityTrait toTrait) {
        return true;
    }

    /** {@inheritDoc} */
    @Override public RewindabilityTrait getDefault() {
        return RewindabilityTrait.ONE_WAY;
    }
}

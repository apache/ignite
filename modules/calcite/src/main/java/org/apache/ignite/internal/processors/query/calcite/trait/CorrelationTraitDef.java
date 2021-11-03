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

/**
 *
 */
public class CorrelationTraitDef extends RelTraitDef<CorrelationTrait> {
    /**
     *
     */
    public static final CorrelationTraitDef INSTANCE = new CorrelationTraitDef();

    /** {@inheritDoc} */
    @Override
    public Class<CorrelationTrait> getTraitClass() {
        return CorrelationTrait.class;
    }

    /** {@inheritDoc} */
    @Override
    public String getSimpleName() {
        return "correlation";
    }

    /** {@inheritDoc} */
    @Override
    public RelNode convert(RelOptPlanner planner, RelNode rel, CorrelationTrait toTrait, boolean allowInfiniteCostConverters) {
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public boolean canConvert(RelOptPlanner planner, CorrelationTrait fromTrait, CorrelationTrait toTrait) {
        return fromTrait.satisfies(toTrait);
    }

    /** {@inheritDoc} */
    @Override
    public CorrelationTrait getDefault() {
        return CorrelationTrait.UNCORRELATED;
    }
}

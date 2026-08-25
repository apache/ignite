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

package org.apache.ignite.internal.processors.query.calcite.rule;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteConvention;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;
import org.apache.ignite.internal.processors.query.calcite.trait.RewindabilityTrait;
import org.apache.ignite.internal.processors.query.calcite.trait.TraitUtils;

import static org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions.single;

/** Applies recursive iteration traits to an input that does not depend on the current delta. */
public class RecursiveStaticInputConverterRule extends ConverterRule {
    /** Instance. */
    public static final RelOptRule INSTANCE = new RecursiveStaticInputConverterRule();

    /** */
    private RecursiveStaticInputConverterRule() {
        super(Config.INSTANCE.withConversion(
            RecursiveCteUtils.StaticInput.class,
            Convention.NONE,
            IgniteConvention.INSTANCE,
            "RecursiveStaticInputConverterRule"
        ));
    }

    /** {@inheritDoc} */
    @Override public RelNode convert(RelNode rel) {
        RecursiveCteUtils.StaticInput input = (RecursiveCteUtils.StaticInput)rel;
        RelTraitSet traits = rel.getCluster().traitSetOf(IgniteConvention.INSTANCE).replace(single());

        IgniteDistribution inputDistribution =
            (IgniteDistribution)rel.getCluster().getMetadataQuery()
                .distribution(RecursiveCteUtils.original(input.getInput()));

        if (inputDistribution.satisfies(single()))
            return convert(input.getInput(), traits.replace(RewindabilityTrait.REWINDABLE));

        RelNode convertedInput = convert(input.getInput(), traits);

        return TraitUtils.convertRewindability(
            rel.getCluster().getPlanner(),
            RewindabilityTrait.REWINDABLE,
            convertedInput
        );
    }
}

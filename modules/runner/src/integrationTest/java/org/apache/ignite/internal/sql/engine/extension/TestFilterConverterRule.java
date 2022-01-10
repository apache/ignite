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

package org.apache.ignite.internal.sql.engine.extension;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistributions;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A converter from logical filter to the test physical filter relation.
 */
public class TestFilterConverterRule extends ConverterRule {
    /** An instance of this rule. */
    public static final RelOptRule INSTANCE = new TestFilterConverterRule();

    private TestFilterConverterRule() {
        super(Config.INSTANCE
                .withConversion(LogicalFilter.class, Convention.NONE, TestExtension.CONVENTION, "TestFilterConverter"));
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable RelNode convert(RelNode rel) {
        RelOptCluster cluster = rel.getCluster();
        RelTraitSet traits = rel.getTraitSet()
                .replace(TestExtension.CONVENTION)
                .replace(IgniteDistributions.single());

        LogicalFilter filter = (LogicalFilter) rel;

        RelTraitSet inputTraits = filter.getInput().getTraitSet()
                .replace(TestExtension.CONVENTION)
                .replace(IgniteDistributions.single());

        return new TestPhysFilter(cluster, traits, convert(filter.getInput(), inputTraits), filter.getCondition());
    }

}

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

import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteConvention;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteUnionAll;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.immutables.value.Value;

/**
 *
 */
@Value.Enclosing
public class UnionConverterRule extends RelRule<UnionConverterRule.Config> {
    /** Instance. */
    public static final RelOptRule INSTANCE = Config.DEFAULT.toRule();

    /** */
    public UnionConverterRule(Config cfg) {
        super(cfg);
    }

    /** {@inheritDoc} */
    @Override public void onMatch(RelOptRuleCall call) {
        final LogicalUnion union = call.rel(0);

        RelOptCluster cluster = union.getCluster();
        RelTraitSet traits = cluster.traitSetOf(IgniteConvention.INSTANCE);
        List<RelNode> inputs = Commons.transform(union.getInputs(), input -> convert(input, traits));

        RelNode res = new IgniteUnionAll(cluster, traits, inputs);

        if (!union.all) {
            final RelBuilder relBuilder = relBuilderFactory.create(union.getCluster(), null);

            relBuilder
                .push(res)
                .aggregate(relBuilder.groupKey(ImmutableBitSet.range(union.getRowType().getFieldCount())));

            res = convert(relBuilder.build(), union.getTraitSet());
        }

        call.transformTo(res);
    }

    /**
     *
     */
    @Value.Immutable
    public interface Config extends RelRule.Config {
        /** */
        UnionConverterRule.Config DEFAULT = ImmutableUnionConverterRule.Config.of()
            .withDescription("UnionConverterRule")
            .withOperandFor(LogicalUnion.class);

        /** Defines an operand tree for the given classes. */
        default UnionConverterRule.Config withOperandFor(Class<? extends LogicalUnion> union) {
            return withOperandSupplier(
                o0 -> o0.operand(union).anyInputs()
            )
                .as(UnionConverterRule.Config.class);
        }

        /** {@inheritDoc} */
        @Override default UnionConverterRule toRule() {
            return new UnionConverterRule(this);
        }
    }
}

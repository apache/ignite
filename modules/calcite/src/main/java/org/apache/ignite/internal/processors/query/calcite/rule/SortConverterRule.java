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

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteConvention;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteLimit;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteSort;

/**
 * Converter rule for sort operator.
 */
public class SortConverterRule extends RelRule<SortConverterRule.Config> {
    /**
     *
     */
    public static final RelOptRule INSTANCE =
            SortConverterRule.Config.DEFAULT
                    .as(SortConverterRule.Config.class).toRule();

    /** Creates a LimitConverterRule. */
    protected SortConverterRule(SortConverterRule.Config config) {
        super(config);
    }

    /** Rule configuration. */
    public interface Config extends RelRule.Config {
        SortConverterRule.Config DEFAULT = EMPTY
                .withOperandSupplier(b ->
                        b.operand(LogicalSort.class).anyInputs())
                .as(SortConverterRule.Config.class);

        /** {@inheritDoc} */
        @Override
        default SortConverterRule toRule() {
            return new SortConverterRule(this);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void onMatch(RelOptRuleCall call) {
        final Sort sort = call.rel(0);
        RelOptCluster cluster = sort.getCluster();
        RelTraitSet outTraits = cluster.traitSetOf(IgniteConvention.INSTANCE).replace(sort.getCollation());
        RelTraitSet inTraits = cluster.traitSetOf(IgniteConvention.INSTANCE);
        RelNode input = convert(sort.getInput(), inTraits);

        if (sort.fetch != null || sort.offset != null) {
            RelTraitSet traits = cluster.traitSetOf(IgniteConvention.INSTANCE).replace(sort.getCollation());

            call.transformTo(new IgniteLimit(cluster, traits, convert(sort.getInput(), traits), sort.offset,
                    sort.fetch));

            return;
        }

        call.transformTo(new IgniteSort(cluster, outTraits, input, sort.getCollation()));
    }
}

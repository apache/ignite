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

import com.google.common.collect.ImmutableMap;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteConvention;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteLimit;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteSort;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.immutables.value.Value;

/**
 * Converter rule for sort operator.
 */
@Value.Enclosing
public class SortConverterRule extends RelRule<SortConverterRule.Config> {
    /** */
    public static final RelOptRule INSTANCE = SortConverterRule.Config.DEFAULT.toRule();

    /** Creates a LimitConverterRule. */
    protected SortConverterRule(SortConverterRule.Config config) {
        super(config);
    }

    /** Rule configuration. */
    @Value.Immutable
    public interface Config extends RelRule.Config {
        /** Default config. */
        SortConverterRule.Config DEFAULT = ImmutableSortConverterRule.Config.of()
            .withOperandSupplier(b ->
                b.operand(LogicalSort.class).anyInputs());

        /** {@inheritDoc} */
        @Override default SortConverterRule toRule() {
            return new SortConverterRule(this);
        }
    }

    /** {@inheritDoc} */
    @Override public void onMatch(RelOptRuleCall call) {
        final Sort sort = call.rel(0);
        RelOptCluster cluster = sort.getCluster();

        if (sort.fetch != null || sort.offset != null) {
            RelTraitSet traits = cluster.traitSetOf(IgniteConvention.INSTANCE)
                .replace(sort.getCollation())
                .replace(IgniteDistributions.single());

            if (sort.collation == RelCollations.EMPTY || sort.fetch == null) {
                call.transformTo(new IgniteLimit(cluster, traits, convert(sort.getInput(), traits), sort.offset,
                    sort.fetch));
            }
            else {
                RelNode igniteSort = new IgniteSort(
                    cluster,
                    cluster.traitSetOf(IgniteConvention.INSTANCE).replace(sort.getCollation()),
                    convert(sort.getInput(), cluster.traitSetOf(IgniteConvention.INSTANCE)),
                    sort.getCollation(),
                    sort.offset,
                    sort.fetch,
                    false
                );

                call.transformTo(
                    new IgniteLimit(cluster, traits, convert(igniteSort, traits), sort.offset, sort.fetch),
                    ImmutableMap.of(
                        new IgniteLimit(cluster, traits, convert(sort.getInput(), traits), sort.offset, sort.fetch),
                        sort
                    )
                );
            }
        }
        else {
            RelTraitSet outTraits = cluster.traitSetOf(IgniteConvention.INSTANCE).replace(sort.getCollation());
            RelTraitSet inTraits = cluster.traitSetOf(IgniteConvention.INSTANCE);
            RelNode input = convert(sort.getInput(), inTraits);

            call.transformTo(new IgniteSort(cluster, outTraits, input, sort.getCollation(), false));
        }
    }
}

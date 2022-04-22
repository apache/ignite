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
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalCalc;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.logical.LogicalExchange;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalIntersect;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalMatch;
import org.apache.calcite.rel.logical.LogicalMinus;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteConvention;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteLimit;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteSort;
import org.apache.ignite.internal.processors.query.calcite.rel.logical.IgniteLogicalTableScan;
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

            ((RelSubset)sort.getInput()).getOriginal().accept(new RelShuttleImpl() {
                @Override public RelNode visit(TableScan scan) {
                    IgniteLogicalTableScan tscan = (IgniteLogicalTableScan)scan;

                    tscan.

                    return null;
                }
            });


            call.transformTo(new IgniteLimit(cluster, traits, convert(sort.getInput(), traits), sort.offset,
                sort.fetch));

//            RelTraitSet limitTraits = cluster.traitSetOf(IgniteConvention.INSTANCE)
//                .replace(sort.getCollation())
//                .replace(IgniteDistributions.single());
//
//            RelNode limitInput;
//
//            if (sort.collation == RelCollations.EMPTY)
//                limitInput = sort.getInput();
//            else {
//                RelTraitSet traits = cluster.traitSetOf(IgniteConvention.INSTANCE)
//                    .replace(sort.getCollation())
//                    .replace(IgniteDistributions.single());
//
//                call.transformTo(new IgniteLimit(cluster, traits, convert(sort.getInput(), traits), sort.offset,
//                    sort.fetch));
//
//                RelTraitSet sortTraits = cluster.traitSetOf(IgniteConvention.INSTANCE)
//                    .replace(sort.getCollation());
//
//                limitInput = new IgniteSort(
//                    cluster,
//                    sortTraits,
//                    convert(sort.getInput(), cluster.traitSetOf(IgniteConvention.INSTANCE)),
//                    sort.getCollation(),
//                    false,
//                    sort.offset,
//                    sort.fetch
//                );
//            }
//
//            call.transformTo(new IgniteLimit(cluster, limitTraits, convert(limitInput, limitTraits), sort.offset,
//                sort.fetch));
        }
    }
}

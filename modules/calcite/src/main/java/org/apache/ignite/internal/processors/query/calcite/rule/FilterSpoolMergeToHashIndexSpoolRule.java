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

import static org.apache.ignite.internal.processors.query.calcite.util.RexUtils.isBinaryComparison;
import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;

import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.Spool;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteFilter;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteHashIndexSpool;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableSpool;
import org.apache.ignite.internal.processors.query.calcite.trait.CorrelationTrait;
import org.apache.ignite.internal.processors.query.calcite.trait.TraitUtils;
import org.apache.ignite.internal.processors.query.calcite.util.RexUtils;

/**
 * Rule that pushes filter into the spool.
 */
public class FilterSpoolMergeToHashIndexSpoolRule extends RelRule<FilterSpoolMergeToHashIndexSpoolRule.Config> {
    /** Instance. */
    public static final RelOptRule INSTANCE = Config.DEFAULT.toRule();

    /**
     *
     */
    private FilterSpoolMergeToHashIndexSpoolRule(Config cfg) {
        super(cfg);
    }

    /** {@inheritDoc} */
    @Override
    public void onMatch(RelOptRuleCall call) {
        final IgniteFilter filter = call.rel(0);
        final IgniteTableSpool spool = call.rel(1);

        RelOptCluster cluster = spool.getCluster();
        RexBuilder builder = RexUtils.builder(cluster);

        RelTraitSet trait = spool.getTraitSet();
        CorrelationTrait filterCorr = TraitUtils.correlation(filter);

        if (filterCorr.correlated()) {
            trait = trait.replace(filterCorr);
        }

        final RelNode input = spool.getInput();

        RexNode condition0 = RexUtil.expandSearch(builder, null, filter.getCondition());

        condition0 = RexUtil.toCnf(builder, condition0);

        List<RexNode> conjunctions = RelOptUtil.conjunctions(condition0);

        //TODO: https://issues.apache.org/jira/browse/IGNITE-14916
        for (RexNode rexNode : conjunctions) {
            if (!isBinaryComparison(rexNode)) {
                return;
            }
        }

        List<RexNode> searchRow = RexUtils.buildHashSearchRow(
                cluster,
                condition0,
                spool.getRowType()
        );

        if (nullOrEmpty(searchRow)) {
            return;
        }

        RelNode res = new IgniteHashIndexSpool(
                cluster,
                trait.replace(RelCollations.EMPTY),
                input,
                searchRow,
                filter.getCondition()
        );

        call.transformTo(res);
    }

    /**
     *
     */
    @SuppressWarnings("ClassNameSameAsAncestorName")
    public interface Config extends RelRule.Config {
        /**
         *
         */
        Config DEFAULT = RelRule.Config.EMPTY
                .withRelBuilderFactory(RelFactories.LOGICAL_BUILDER)
                .withDescription("FilterSpoolMergeToHashIndexSpoolRule")
                .as(FilterSpoolMergeToHashIndexSpoolRule.Config.class)
                .withOperandFor(IgniteFilter.class, IgniteTableSpool.class);

        /** Defines an operand tree for the given classes. */
        default Config withOperandFor(Class<? extends Filter> filterClass, Class<? extends Spool> spoolClass) {
            return withOperandSupplier(
                    o0 -> o0.operand(filterClass)
                            .oneInput(o1 -> o1.operand(spoolClass)
                                    .anyInputs()
                            )
            )
                    .as(Config.class);
        }

        /** {@inheritDoc} */
        @Override
        default FilterSpoolMergeToHashIndexSpoolRule toRule() {
            return new FilterSpoolMergeToHashIndexSpoolRule(this);
        }
    }
}

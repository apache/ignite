/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.internal.processors.query.calcite.rule;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteSort;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;

/**
 * TODO: Add class description.
 */
public class IgniteSortRule  extends RelOptRule {

    public static final RelOptRule INSTANCE = new IgniteSortRule();

    private IgniteSortRule() {
        super(Commons.any(LogicalSort.class, RelNode.class), "IgniteSortRule");
    }

    @Override public void onMatch(RelOptRuleCall call) {
        final Sort sort = call.rel(0);
        final RelNode input = sort.getInput();
        final RelTraitSet traits = sort.getTraitSet().plus(IgniteRel.IGNITE_CONVENTION);

        final RelNode convertedInput = convert(input, input.getTraitSet().plus(IgniteRel.IGNITE_CONVENTION).simplify());
        call.transformTo(new IgniteSort(sort.getCluster(), traits, convertedInput, sort.getCollation()));
    }
}

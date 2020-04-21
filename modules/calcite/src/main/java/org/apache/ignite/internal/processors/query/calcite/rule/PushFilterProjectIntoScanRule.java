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

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rex.RexNode;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableScan;

/**
 * TODO: Add class description.
 */
public class PushFilterProjectIntoScanRule  extends RelOptRule {

    public static final PushFilterProjectIntoScanRule FILTER_INTO_SCAN =
        new PushFilterProjectIntoScanRule(Filter.class, "IgniteFilterIntoScanRule");

    private PushFilterProjectIntoScanRule(Class<? extends RelNode> clazz, String desc) {
        super(operand(clazz,
            operand(IgniteTableScan.class, none())),
            RelFactories.LOGICAL_BUILDER,
            desc);
    }

    @Override public boolean matches(RelOptRuleCall call) {
        return super.matches(call); // TODO: CODE: implement.
    }

    @Override public void onMatch(RelOptRuleCall call) {
        IgniteTableScan scan = call.rel(1);
        Filter rel = call.rel(0);

        List<RexNode> filters = currentFilters(scan);

        if (filters == null)
            filters = new ArrayList<>(1);

        filters.add(rel.getCondition());


        call.transformTo(
            new IgniteTableScan(scan.getCluster(), scan.getTraitSet(), scan.getTable(), scan.indexName(), filters));
    }

    private static List<RexNode> currentFilters(TableScan scan) {
        return scan instanceof IgniteTableScan ? ((IgniteTableScan)scan).filters() : null;
    }
}

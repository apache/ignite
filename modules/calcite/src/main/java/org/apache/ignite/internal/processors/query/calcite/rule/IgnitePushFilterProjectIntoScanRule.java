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
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableScan;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteTable;
import org.apache.ignite.internal.util.typedef.F;

/**
 * TODO: Add class description.
 */
public class IgnitePushFilterProjectIntoScanRule extends RelOptRule {

    public static final IgnitePushFilterProjectIntoScanRule FILTER_INTO_SCAN =
        new IgnitePushFilterProjectIntoScanRule(Filter.class, "IgniteFilterIntoScanRule");

    public static final IgnitePushFilterProjectIntoScanRule PROJECT_INTO_SCAN =
        new IgnitePushFilterProjectIntoScanRule(Project.class, "IgniteProjectIntoScanRule");

    private IgnitePushFilterProjectIntoScanRule(Class<? extends RelNode> clazz, String desc) {
        super(operand(clazz,
            operandJ(TableScan.class, null, IgnitePushFilterProjectIntoScanRule::isIgniteTable,
                none())),
            RelFactories.LOGICAL_BUILDER,
            desc);
    }

    private static boolean isIgniteTable(TableScan scan) {
        final RelOptTable tbl = scan.getTable();
        return tbl.unwrap(IgniteTable.class) != null;
    }

    @Override public void onMatch(RelOptRuleCall call) {
        TableScan scan = call.rel(1);
        RelNode rel = call.rel(0);

        assert rel instanceof Filter || rel instanceof Project : "Wrong rel class: " + rel;

        if (rel instanceof Project && !((Project)rel).isMapping())
            return; // We can push thr mapping only.

        ImmutableIntList projects;
        List<RexNode> filters;

        if (scan instanceof IgniteTableScan) {
            IgniteTableScan igniteScan = (IgniteTableScan)scan;

            List<RexNode> oldFilters = igniteScan.filters();

            filters = new ArrayList<>(oldFilters != null ? oldFilters.size() + 1 : 1);

            if (!F.isEmpty(oldFilters))
                filters.addAll(oldFilters);

            projects = igniteScan.projects();

            if (projects == null)
                projects = scan.identity();
        }
        else {
            filters = new ArrayList<>(1);
            projects = scan.identity();
        }

        if (rel instanceof Filter) {
            Filter filter = (Filter)rel;

            Mapping mapping = Mappings.target(projects, scan.getTable().getRowType().getFieldCount());

            RexNode newFilter = RexUtil.apply(mapping, filter.getCondition());

            filters.add(newFilter);
        }
        else {
            Project proj = (Project)rel;

            Mapping mapping = (Mapping)Project.getPartialMapping(
                proj.getInput().getRowType().getFieldCount(),
                proj.getProjects());

            projects = ImmutableIntList.copyOf(Mappings.apply(mapping, projects));
        }

        call.transformTo(new IgniteTableScan(scan.getCluster(), scan.getTraitSet(), scan.getTable(), filters, projects));
    }
}

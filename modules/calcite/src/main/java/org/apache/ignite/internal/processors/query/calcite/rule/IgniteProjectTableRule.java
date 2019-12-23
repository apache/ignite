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

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.calcite.interpreter.Bindables;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.ProjectableFilterableTable;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.Mappings;

/**
 * TODO: Add class description.
 */
public class IgniteProjectTableRule  extends RelOptRule {

    public IgniteProjectTableRule() {
        super(operand(Project.class,
            operandJ(TableScan.class, null, IgniteProjectTableRule::isIgniteTable, none())),
            RelFactories.LOGICAL_BUILDER,
            "IgniteProjectTableRule");
    }

    private static boolean isIgniteTable(TableScan scan) {
        // We can only push projects into a ProjectableFilterableTable.
        final RelOptTable table = scan.getTable();
        return table.unwrap(ProjectableFilterableTable.class) != null;
    }

    @Override public void onMatch(RelOptRuleCall call) {
        final Project project = call.rel(0);
        final TableScan scan = call.rel(2);
        final RelOptTable table = scan.getTable();
        assert table.unwrap(ProjectableFilterableTable.class) != null;
        if (!project.isMapping()) {
            return;
        }
        final Mappings.TargetMapping mapping = Project.getPartialMapping(
            project.getInput().getRowType().getFieldCount(),
            project.getProjects());

        final ImmutableIntList projects;
        final ImmutableList<RexNode> filters;
        if (scan instanceof Bindables.BindableTableScan) {
            final Bindables.BindableTableScan bindableScan =
                (Bindables.BindableTableScan) scan;
            filters = bindableScan.filters;
            projects = bindableScan.projects;
        } else {
            filters = ImmutableList.of();
            projects = scan.identity();
        }

        final List<Integer> projects2 =
            Mappings.apply((Mapping) mapping, projects);
        call.transformTo(
            Bindables.BindableTableScan.create(scan.getCluster(), scan.getTable(),
                filters, projects2));
    }
}

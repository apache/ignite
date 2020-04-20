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
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableScan;

/**
 * TODO: Add class description.
 */
public class PushFilterProjectIntoScanRule  extends RelOptRule {

    public static final PushFilterProjectIntoScanRule FILTER_INTO_SCAN =
        new PushFilterProjectIntoScanRule(Filter.class, "IgniteFilterIntoScanRule");

    public static final PushFilterProjectIntoScanRule PROJECT_INTO_SCAN =
        new PushFilterProjectIntoScanRule(Project.class, "IgniteProjectIntoScanRule");

    private PushFilterProjectIntoScanRule(Class<? extends RelNode> clazz, String desc) {
        super(operand(clazz,
            operand(TableScan.class, none())),
            RelFactories.LOGICAL_BUILDER,
            desc);
    }

    @Override public boolean matches(RelOptRuleCall call) {
        return super.matches(call); // TODO: CODE: implement.
    }

    @Override public void onMatch(RelOptRuleCall call) {
        IgniteTableScan scan = call.rel(1);
        RelNode rel = call.rel(0);

        assert rel instanceof Filter || rel instanceof Project : "Wrong rel class: " + rel;

        if (rel instanceof Project && !((Project)rel).isMapping())
            return; // We can push the mapping only.

        int[] projects = currentProjects(scan);
        List<RexNode> filters = currentFilters(scan);

        if (rel instanceof Filter) {
           // if (fiObjects.equals(filters, ((Filter)rel).getCondition()))

            System.out.println("Old filters=" + filters + ", new filters=" + ((Filter)rel).getCondition());
            filters = pushFilter(scan, (Filter)rel, projects, filters);
        }
        else
            projects = pushProject(scan, (Project)rel, projects);

        call.transformTo(
            new IgniteTableScan(scan.getCluster(), scan.getTraitSet(), scan.getTable(), scan.indexName(), filters, projects));
    }

    public int[] pushProject(TableScan scan, Project proj, int[] projects) {
        if (projects == null)
            projects = scan.identity().toIntArray();

        Mapping mapping = (Mapping)Project.getPartialMapping(
            proj.getInput().getRowType().getFieldCount(),
            proj.getProjects());

        List<Integer> projects0 = Mappings.apply(mapping, ImmutableIntList.of(projects));

        projects = ImmutableIntList.copyOf(projects0).toIntArray();

        return projects;
    }

    public List<RexNode> pushFilter(TableScan scan, Filter filter, int[] projects, List<RexNode> filters) {
        RexNode newFilter;

//        if (projects != null) { TODO projects and filters mapping to each other
//            Mapping mapping = Mappings.target(ImmutableIntList.of(projects), scan.getTable().getRowType().getFieldCount());
//
//            newFilter = RexUtil.apply(mapping, filter.getCondition());
//        }
//        else
            newFilter = filter.getCondition();


        filters = filters == null ? new ArrayList<>(1) : new ArrayList<>(filters);

        filters.add(newFilter);

        return filters;
    }

    private static int[] currentProjects(TableScan scan) {
        return scan instanceof IgniteTableScan ? ((IgniteTableScan)scan).projects() : null;
    }

    private static List<RexNode> currentFilters(TableScan scan) {
        return scan instanceof IgniteTableScan ? ((IgniteTableScan)scan).filters() : null;
    }
}

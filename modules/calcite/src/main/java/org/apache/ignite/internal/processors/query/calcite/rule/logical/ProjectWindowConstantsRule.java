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

package org.apache.ignite.internal.processors.query.calcite.rule.logical;

import java.util.ArrayList;
import java.util.List;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.logical.LogicalWindow;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexWindowBound;
import org.apache.calcite.rex.RexWindowBounds;
import org.apache.calcite.tools.RelBuilder;
import org.immutables.value.Value;

/**
 * A rule to split window rel with constants to:
 * - project with constants
 * - window without constants
 * - project removing constants.
 */
@Value.Enclosing
public class ProjectWindowConstantsRule extends RelRule<ProjectWindowConstantsRule.Config> implements TransformationRule {
    /** */
    public static final ProjectWindowConstantsRule INSTANCE = new ProjectWindowConstantsRule(ProjectWindowConstantsRule.Config.DEFAULT);

    /** */
    private ProjectWindowConstantsRule(Config cfg) {
        super(cfg);
    }

    /** */
    @Override public void onMatch(RelOptRuleCall call) {
        LogicalWindow window = call.rel(0);
        assert !window.constants.isEmpty();

        RelNode input = window.getInput();

        RelNode projectWithConstants = buildProjectWithConstants(input, window.constants);
        RelNode newWindow = buildWindowWithoutConstants(window, input, projectWithConstants);
        RelNode projectWithoutConstants = buildProjectExcludeConstants(window, input, newWindow);

        assert window.getRowType().equalsSansFieldNames(projectWithoutConstants.getRowType());
        call.transformTo(projectWithoutConstants);
    }

    /** Creates projection with window constants. */
    private RelNode buildProjectWithConstants(RelNode input, List<RexLiteral> constants) {
        RelBuilder relBldr = relBuilderFactory.create(input.getCluster(), null);
        relBldr.push(input);
        relBldr.projectPlus(constants);
        return relBldr.build();
    }

    /** Creates new window without constants. */
    private RelNode buildWindowWithoutConstants(LogicalWindow window, RelNode input, RelNode newInput) {
        int windowFieldCnt = window.getRowType().getFieldCount();
        int inputFieldCnt = input.getRowType().getFieldCount();

        List<RelDataTypeField> newInputFields = newInput.getRowType().getFieldList();
        List<RelDataTypeField> windowFields = window.getRowType().getFieldList();

        assert inputFieldCnt < newInputFields.size();

        RelDataTypeFactory typeFactory = window.getCluster().getTypeFactory();
        RelDataTypeFactory.Builder builder = typeFactory.builder();

        for (int i = 0; i < inputFieldCnt; i++) {
            // add fields from original input, passed through window rel
            builder.add(windowFields.get(i));
        }
        for (int i = inputFieldCnt; i < newInputFields.size(); i++) {
            // add constants from new input
            builder.add(newInputFields.get(i));
        }
        for (int i = inputFieldCnt; i < windowFields.size(); i++) {
            // add fields, provided by window
            builder.add(windowFields.get(i));
        }

        RelDataType type = builder.build();
        assert type.getFieldCount() == newInputFields.size() + (windowFieldCnt - inputFieldCnt);

        // Replace input refs to constants in group bounds with actual constant values.
        // In some cases it lets reuse calculated frame bound for the same peer.
        ImmutableList.Builder<Window.Group> newGrps = ImmutableList.builder();
        for (Window.Group grp : window.groups) {
            if (grp.lowerBound.getOffset() instanceof RexInputRef || grp.upperBound.getOffset() instanceof RexInputRef) {
                Window.Group newGrp = new Window.Group(
                    grp.keys,
                    grp.isRows,
                    replaceInputRefWithConst(grp.lowerBound, inputFieldCnt, window),
                    replaceInputRefWithConst(grp.upperBound, inputFieldCnt, window),
                    grp.exclude,
                    grp.orderKeys,
                    grp.aggCalls
                );
                newGrps.add(newGrp);
            }
            else newGrps.add(grp);
        }

        // agg calls in the original window allready reference fields by index,
        // do not need to remap it
        return LogicalWindow.create(window.getTraitSet(), newInput, ImmutableList.of(), type, newGrps.build());
    }

    /** Creates projection without window constants. */
    private RelNode buildProjectExcludeConstants(LogicalWindow window, RelNode input, RelNode newWindow) {
        int inputFieldCnt = input.getRowType().getFieldCount();
        int windowFieldCnt = window.getRowType().getFieldCount();
        int constantCnt = window.constants.size();

        RexBuilder rexBuilder = window.getCluster().getRexBuilder();
        List<RexNode> projects = new ArrayList<>(windowFieldCnt);
        for (int i = 0; i < inputFieldCnt; i++) {
            projects.add(rexBuilder.makeInputRef(newWindow, i));
        }
        for (int i = inputFieldCnt; i < windowFieldCnt; i++) {
            projects.add(rexBuilder.makeInputRef(newWindow, i + constantCnt));
        }

        RelBuilder relBldr = relBuilderFactory.create(newWindow.getCluster(), null);
        relBldr.push(newWindow);
        relBldr.project(projects);
        return relBldr.build();
    }

    /** Replace provided input ref with a window constatn, if possible */
    private RexWindowBound replaceInputRefWithConst(RexWindowBound bound, int constantStartIdx, LogicalWindow window) {
        assert !bound.isUnbounded() && !bound.isCurrentRow() && bound.getOffset() instanceof RexInputRef;
        assert bound.isPreceding() || bound.isFollowing();

        RexInputRef ref = (RexInputRef)bound.getOffset();
        if (ref.getIndex() < constantStartIdx)
            return bound;

        RexNode cnst = window.getConstants().get(ref.getIndex() - constantStartIdx);
        if (bound.isPreceding())
            return RexWindowBounds.preceding(cnst);
        else
            return RexWindowBounds.following(cnst);
    }

    /** Rule configuration. */
    @Value.Immutable
    public interface Config extends RelRule.Config {
        /** */
        ProjectWindowConstantsRule.Config DEFAULT = ImmutableProjectWindowConstantsRule.Config.of()
            .withOperandSupplier(b -> b.operand(LogicalWindow.class)
                .predicate(it -> !it.constants.isEmpty())
                .anyInputs());

        /** {@inheritDoc} */
        @Override default ProjectWindowConstantsRule toRule() {
            return INSTANCE;
        }
    }
}

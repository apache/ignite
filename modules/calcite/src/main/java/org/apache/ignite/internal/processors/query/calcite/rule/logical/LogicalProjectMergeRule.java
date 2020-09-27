package org.apache.ignite.internal.processors.query.calcite.rule.logical;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.rules.ProjectMergeRule;
import org.apache.calcite.rel.rules.TransformationRule;

public class LogicalProjectMergeRule extends RelOptRule implements TransformationRule {
    /** */
    public static final RelOptRule INSTANCE = new LogicalProjectMergeRule();

    /** */
    private LogicalProjectMergeRule() {
        super(
            operand(LogicalProject.class,
                operand(LogicalProject.class, any())),
            RelFactories.LOGICAL_BUILDER, null);
    }

    /** {@inheritDoc} */
    @Override public void onMatch(RelOptRuleCall call) {
        ProjectMergeRule.INSTANCE.onMatch(call);
    }
}

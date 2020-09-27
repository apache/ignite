package org.apache.ignite.internal.processors.query.calcite.rule.logical;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.rules.ProjectRemoveRule;
import org.apache.calcite.rel.rules.SubstitutionRule;

public class LogicalProjectRemoveRule extends RelOptRule implements SubstitutionRule {
    public static final RelOptRule INSTANCE = new LogicalProjectRemoveRule();

    public LogicalProjectRemoveRule() {
        super(operandJ(Project.class, null, ProjectRemoveRule::isTrivial, any()), RelFactories.LOGICAL_BUILDER, null);
    }

    /** {@inheritDoc} */
    @Override public void onMatch(RelOptRuleCall call) {
        ProjectRemoveRule.INSTANCE.onMatch(call);
    }

    /** {@inheritDoc} */
    @Override public boolean autoPruneOld() {
        return true;
    }
}

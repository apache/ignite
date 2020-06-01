package org.apache.ignite.internal.processors.query.calcite.rule;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.plan.volcano.VolcanoUtils;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;

public class FinishConversionRule extends RelOptRule {
    /** */
    public static final RelOptRule INSTANCE = new FinishConversionRule();

    /** */
    public FinishConversionRule() {
        super(operand(IgniteRel.class, some(operand(RelSubset.class, any()))));
    }

    /** {@inheritDoc} */
    @Override public void onMatch(RelOptRuleCall call) {
        Commons.<RelSubset>cast(call.<IgniteRel>rel(0).getInputs())
            .forEach(this::changeTraits);
    }

    /** */
    private void changeTraits(RelSubset subset) {
        for (RelSubset relSubset : VolcanoUtils.otherSubsets(subset))
            RuleUtils.changeTraits(relSubset, subset.getTraitSet());
    }
}

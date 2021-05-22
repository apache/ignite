package org.apache.ignite.internal.processors.query.calcite.rule.logical;

import com.google.common.collect.ImmutableMap;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;

import java.util.List;

public class LogicalTableModifyAggRule extends RelRule<LogicalTableModifyAggRule.Config> {

    /** Instance. */
    public static final RelOptRule INSTANCE = LogicalTableModifyAggRule.Config.DEFAULT.toRule();

    /**
     * Constructor.
     *
     * @param config Rule configuration.
     */
    private LogicalTableModifyAggRule(LogicalTableModifyAggRule.Config config) {
        super(config);
    }

    /** {@inheritDoc} */
    @Override public void onMatch(RelOptRuleCall call) {
        final LogicalTableModify rel = call.rel(0);
        final RelOptCluster cluster = rel.getCluster();

        RelNode input = rel.getInput(0);

        call.transformTo(rel, new LogicalAggregate(input.));
    }

    /** */
    @SuppressWarnings("ClassNameSameAsAncestorName")
    public interface Config extends RelRule.Config {
        /** */
        LogicalTableModifyAggRule.Config DEFAULT = RelRule.Config.EMPTY
                .withRelBuilderFactory(RelFactories.LOGICAL_BUILDER)
                .withDescription("LogicalTableModifyAggRule")
                .as(LogicalTableModifyAggRule.Config.class)
                .withOperandFor(LogicalTableModify.class);

        /** Defines an operand tree for the given classes. */
        default LogicalTableModifyAggRule.Config withOperandFor(Class<? extends LogicalTableModify> modifyClass) {
            return withOperandSupplier(o -> o.operand(modifyClass).anyInputs())
                    .as(LogicalTableModifyAggRule.Config.class);
        }

        /** {@inheritDoc} */
        @Override default LogicalTableModifyAggRule toRule() {
            return new LogicalTableModifyAggRule(this);
        }
    }
}

package org.apache.ignite.internal.processors.query.calcite.rule;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.PhysicalNode;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteConvention;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableModify;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.processors.query.calcite.trait.RewindabilityTrait;

/**
 *
 */
public class TableModifyConverterRuleWithAgg  extends AbstractIgniteConverterRule<LogicalTableModify> {
    /** */
    public static final RelOptRule INSTANCE = new TableModifyConverterRule();

    /**
     * Creates a ConverterRule.
     */
    public TableModifyConverterRuleWithAgg() {
        super(LogicalTableModify.class, "TableModifyConverterRuleWithAgg");
    }

    /** {@inheritDoc} */
    @Override protected PhysicalNode convert(RelOptPlanner planner, RelMetadataQuery mq, LogicalTableModify rel) {
        RelOptCluster cluster = rel.getCluster();
        RelTraitSet traits = cluster.traitSetOf(IgniteConvention.INSTANCE)
                .replace(IgniteDistributions.single())
                .replace(RewindabilityTrait.ONE_WAY)
                .replace(RelCollations.EMPTY);

        rel.getT
        RelNode input = convert(rel.getInput(), traits);

        return new IgniteTableModify(cluster, traits, rel.getTable(), input,
                rel.getOperation(), rel.getUpdateColumnList(), rel.getSourceExpressionList(), rel.isFlattened());
    }
}

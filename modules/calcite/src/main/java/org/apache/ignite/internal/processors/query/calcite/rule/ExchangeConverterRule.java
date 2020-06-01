package org.apache.ignite.internal.processors.query.calcite.rule;

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalExchange;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.ignite.internal.processors.query.calcite.metadata.IgniteMdDistribution;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteConvention;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteExchange;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTrimExchange;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;

public class ExchangeConverterRule extends RelOptRule {
    /** */
    public static final RelOptRule INSTANCE = new ExchangeConverterRule();

    /** */
    public ExchangeConverterRule() {
        super(operand(LogicalExchange.class, any()));
    }

    /** {@inheritDoc} */
    @Override public void onMatch(RelOptRuleCall call) {
        LogicalExchange rel = call.rel(0);
        RelNode input = convert(rel.getInput(), IgniteConvention.INSTANCE);

        RelOptCluster cluster = rel.getCluster();
        RelMetadataQuery mq = call.getMetadataQuery();

        IgniteDistribution toDistr = IgniteMdDistribution._distribution(rel, mq);
        IgniteDistribution fromDistr = IgniteMdDistribution._distribution(input, mq);

        RelTraitSet traits = rel.getTraitSet()
            .replace(toDistr)
            .replace(IgniteConvention.INSTANCE);

        List<RelNode> newRels = new ArrayList<>();

        newRels.add(new IgniteExchange(cluster, traits, convert(input, IgniteDistributions.any()), toDistr));

        if (trimExchange(fromDistr, toDistr))
            newRels.add(new IgniteTrimExchange(cluster, traits, input, toDistr));

        RuleUtils.transformTo(call, newRels);
    }

    /** */
    private boolean trimExchange(IgniteDistribution fromDistr, IgniteDistribution toDistr) {
        return fromDistr.getType() == RelDistribution.Type.BROADCAST_DISTRIBUTED
            && toDistr.getType() == RelDistribution.Type.HASH_DISTRIBUTED;
    }
}

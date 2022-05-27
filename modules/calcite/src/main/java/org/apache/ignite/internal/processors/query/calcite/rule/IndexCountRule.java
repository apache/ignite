package org.apache.ignite.internal.processors.query.calcite.rule;

import com.google.common.collect.ImmutableMap;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.sql.SqlKind;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteIndexCount;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteIndexScan;
import org.apache.ignite.internal.processors.query.calcite.rel.agg.IgniteMapHashAggregate;
import org.immutables.value.Value;

/** Tries to optimize count(*) using count of the index records. */
@Value.Enclosing
public class IndexCountRule extends RelRule<IndexCountRule.Config> {
    /** */
    public static final IndexCountRule INSTANCE = Config.DEFAULT.toRule();

    /** Ctor. */
    private IndexCountRule(IndexCountRule.Config cfg) {
        super(cfg);
    }

    /** */
    @Override public void onMatch(RelOptRuleCall call) {
        IgniteMapHashAggregate agg = call.rel(0);

        if (agg.getGroupCount() > 0
            || agg.getAggCallList().stream().anyMatch(a -> a.getAggregation().getKind() != SqlKind.COUNT))
            return;

        IgniteIndexScan idx = call.rel(1);

        IgniteIndexCount idxCnt = new IgniteIndexCount(
            agg.getCluster(),
            agg.getTraitSet(),
            idx.getTable(),
            idx.indexName(),
            agg.getRowType());

        call.transformTo(idxCnt, ImmutableMap.of(idxCnt, agg));
    }

    /** The rule config. */
    @Value.Immutable
    public interface Config extends RelRule.Config {
        /** */
        IndexCountRule.Config DEFAULT = ImmutableTestRule.Config.of()
            .withDescription("TestRule")
            .withOperandSupplier(r -> r.operand(IgniteMapHashAggregate.class)
                .oneInput(i -> i.operand(IgniteIndexScan.class).anyInputs()));

        /** {@inheritDoc} */
        @Override default IndexCountRule toRule() {
            return new IndexCountRule(this);
        }
    }
}

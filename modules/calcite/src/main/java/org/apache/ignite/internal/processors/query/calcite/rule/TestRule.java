package org.apache.ignite.internal.processors.query.calcite.rule;

import java.math.BigDecimal;
import java.math.BigInteger;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteIndexCount;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteIndexScan;
import org.apache.ignite.internal.processors.query.calcite.rel.agg.IgniteMapHashAggregate;
import org.immutables.value.Value;

import static java.util.Collections.singletonList;

@Value.Enclosing
public class TestRule extends RelRule<TestRule.Config> {
    public static final TestRule INSTANCE = Config.DEFAULT.toRule();

    private TestRule(TestRule.Config config) {
        super(config);
    }

    @Override public void onMatch(RelOptRuleCall call) {
        IgniteMapHashAggregate agg = call.rel(0);

        if (agg.getGroupCount() > 0
            || agg.getAggCallList().stream().anyMatch(a -> a.getAggregation().getKind() != SqlKind.COUNT))
            return;

        IgniteIndexScan idx = call.rel(1);

        AggregateCall aggFun = AggregateCall.create(
//            SqlStdOperatorTable.SUM,
            SqlStdOperatorTable.COUNT,
            false,
            false,
            false,
            ImmutableIntList.of(0),
            -1,
            RelCollations.EMPTY,
//            idx.getCluster().getTypeFactory().createJavaType(long.class),
//            idx.getCluster().getTypeFactory().createJavaType(BigDecimal.class),
            idx.getCluster().getTypeFactory().createSqlType(SqlTypeName.BIGINT),
            null);

        IgniteIndexCount idxCnt = new IgniteIndexCount(
            idx.getCluster(),
            idx.getTraitSet(),
            idx.getTable(),
            idx.indexName());

        IgniteMapHashAggregate agg2 = new IgniteMapHashAggregate(
            idx.getCluster(),
            agg.getTraitSet(),
            idxCnt,
            agg.getGroupSet(),
            agg.getGroupSets(),
            singletonList(aggFun));

        call.transformTo(agg2);
    }

    @Value.Immutable
    public interface Config extends RelRule.Config {
        /** */
        TestRule.Config DEFAULT = ImmutableTestRule.Config.of()
            .withDescription("TestRule")
            .withOperandSupplier(r ->
                r.operand(IgniteMapHashAggregate.class).oneInput(i->i.operand(IgniteIndexScan.class).anyInputs()));

        @Override default TestRule toRule() {
            return new TestRule(this);
        }
    }
}

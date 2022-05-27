package org.apache.ignite.internal.processors.query.calcite.rule;

import java.math.BigDecimal;
import java.util.Collections;

import com.google.common.collect.ImmutableMap;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteIndexCount;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteIndexScan;
import org.apache.ignite.internal.processors.query.calcite.rel.agg.IgniteMapHashAggregate;
import org.immutables.value.Value;

import static java.util.Collections.singletonList;

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

        RelDataTypeFactory tf = agg.getCluster().getTypeFactory();

        RelDataType type = tf.createJavaType(BigDecimal.class);

        IgniteIndexCount idxCnt = new IgniteIndexCount(
            idx.getCluster(),
            idx.getTraitSet(),
            idx.getTable(),
            idx.indexName(),
            tf.createStructType(Collections.singletonList(type), Collections.singletonList(type.toString()))
        );

        AggregateCall aggFun = AggregateCall.create(
            SqlStdOperatorTable.SUM,
            false,
            false,
            false,
            ImmutableIntList.of(0),
            -1,
            ImmutableBitSet.of(),
            RelCollations.EMPTY,
            0,
            idxCnt,
            null,
            null);

        IgniteMapHashAggregate agg2 = new IgniteMapHashAggregate(
            idx.getCluster(),
            agg.getTraitSet(),
            idxCnt,
            agg.getGroupSet(),
            agg.getGroupSets(),
            singletonList(aggFun));

        call.transformTo(agg2, ImmutableMap.of(agg2, agg));
    }

    /** The rule config. */
    @Value.Immutable
    public interface Config extends RelRule.Config {
        /** */
        IndexCountRule.Config DEFAULT = ImmutableIndexCountRule.Config.of()
            .withDescription("IndexCountRule")
            .withOperandSupplier(r -> r.operand(IgniteMapHashAggregate.class)
                .oneInput(i -> i.operand(IgniteIndexScan.class).anyInputs()));

        /** {@inheritDoc} */
        @Override default IndexCountRule toRule() {
            return new IndexCountRule(this);
        }
    }
}

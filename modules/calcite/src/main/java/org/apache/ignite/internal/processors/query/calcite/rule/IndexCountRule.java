package org.apache.ignite.internal.processors.query.calcite.rule;

import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteConvention;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteIndexCount;
import org.apache.ignite.internal.processors.query.calcite.rel.logical.IgniteLogicalTableScan;
import org.apache.ignite.internal.processors.query.calcite.schema.CacheIndexImpl;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteTable;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.processors.query.calcite.trait.RewindabilityTrait;
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
        LogicalAggregate aggr = call.rel(0);
        IgniteLogicalTableScan scan = call.rel(1);
        IgniteTable table = scan.getTable().unwrap(IgniteTable.class);
        //TODO: const name
        String idxName = "_key_PK";
        //TODO: remove cast
        CacheIndexImpl idx = (CacheIndexImpl)table.getIndex(idxName);

        if (idx == null
            || scan.condition() != null
            || aggr.getGroupCount() > 0
            || aggr.getAggCallList().stream().anyMatch(a -> a.getAggregation().getKind() != SqlKind.COUNT)

        )
            return;

        RelDataTypeFactory tf = aggr.getCluster().getTypeFactory();
//        RelDataType type = tf.createSqlType(SqlTypeName.BIGINT);
//        RelDataType type = tf.createJavaType(BigInteger.class);
//        RelDataType type = aggr.getAggCallList().get(0).getType();
        RelDataType type = aggr.getRowType();
//        type = tf.createTypeWithNullability(type, false);

        RelTraitSet traits = aggr.getTraitSet()
            .replace(IgniteConvention.INSTANCE)
            .replace(IgniteDistributions.random())
            .replace(RewindabilityTrait.REWINDABLE);

//        type = tf.createStructType(Collections.singletonList(type), Collections.singletonList(type.toString()));

        IgniteIndexCount idxCnt = new IgniteIndexCount(
            scan.getCluster(),
            traits,
            scan.getTable(),
            idxName,
            type
        );

        //TODO: several caount(*)
        AggregateCall aggFun = AggregateCall.create(
            SqlStdOperatorTable.SUM0,
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

        LogicalAggregate aggr2 = new LogicalAggregate(
            aggr.getCluster(),
            aggr.getTraitSet(),
            Collections.emptyList(),
            idxCnt,
            aggr.getGroupSet(),
            aggr.getGroupSets(),
            Collections.singletonList(aggFun)
        );

        call.transformTo(aggr2, ImmutableMap.of(aggr2, aggr));

//        IgniteMapHashAggregate agg = call.rel(0);
//        IgniteIndexScan idx = call.rel(1);
//
//        if (idx.getTable().unwrap(IgniteTable.class).isIndexRebuildInProgress()) {
//            agg.getCluster().getPlanner().prune(agg);
//
//            return;
//        }
//
//        if (agg.getGroupCount() > 0
//            || agg.getAggCallList().stream().anyMatch(a -> a.getAggregation().getKind() != SqlKind.COUNT)
//            || idx.condition() != null)
//            return;
//
//        RelDataTypeFactory tf = agg.getCluster().getTypeFactory();
//
//        RelDataType type = tf.createJavaType(BigDecimal.class);
//
//        IgniteIndexCount idxCnt = new IgniteIndexCount(
//            idx.getCluster(),
//            idx.getTraitSet(),
//            idx.getTable(),
//            idx.indexName(),
//            tf.createStructType(Collections.singletonList(type), Collections.singletonList(type.toString()))
//        );

//        AggregateCall aggFun = AggregateCall.create(
//            SqlStdOperatorTable.SUM,
//            false,
//            false,
//            false,
//            ImmutableIntList.of(0),
//            -1,
//            ImmutableBitSet.of(),
//            RelCollations.EMPTY,
//            0,
//            idxCnt,
//            null,
//            null);
//
//        IgniteMapHashAggregate agg2 = new IgniteMapHashAggregate(
//            idx.getCluster(),
//            agg.getTraitSet(),
//            idxCnt,
//            agg.getGroupSet(),
//            agg.getGroupSets(),
//            singletonList(aggFun));

//        call.transformTo(agg2, ImmutableMap.of(agg2, agg));
    }

    /** The rule config. */
    @Value.Immutable
    public interface Config extends RelRule.Config {
//        /** */
//        IndexCountRule.Config DEFAULT = ImmutableIndexCountRule.Config.of()
//            .withDescription("IndexCountRule")
//            .withOperandSupplier(r -> r.operand(IgniteMapHashAggregate.class)
//                .oneInput(i -> i.operand(IgniteIndexScan.class).anyInputs()));

        /** */
        IndexCountRule.Config DEFAULT = ImmutableIndexCountRule.Config.of()
            .withDescription("IndexCountRule")
            .withOperandSupplier(r -> r.operand(LogicalAggregate.class)
                .oneInput(i -> i.operand(IgniteLogicalTableScan.class).anyInputs()));

//        /** */
//        IndexCountRule.Config DEFAULT = ImmutableIndexCountRule.Config.of()
//            .withDescription("IndexCountRule")
//            .withOperandSupplier(r -> r.operand(LogicalAggregate.class).anyInputs());

        /** {@inheritDoc} */
        @Override default IndexCountRule toRule() {
            return new IndexCountRule(this);
        }
    }
}

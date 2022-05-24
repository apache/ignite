package org.apache.ignite.internal.processors.query.calcite.rule;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.sql.SqlKind;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteIndexCount;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteIndexScan;
import org.apache.ignite.internal.processors.query.calcite.rel.agg.IgniteMapHashAggregate;
import org.immutables.value.Value;

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

        System.err.println("TEST | onMatch");

        call.transformTo(new IgniteIndexCount(idx.getCluster(), idx.getTraitSet(), idx.getTable(), idx.indexName()));
    }

//    @Override protected PhysicalNode convert(RelOptPlanner planner, RelMetadataQuery mq, LogicalAggregate rel) {
//        return null;
//    }

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

//        /** Defines an operand tree for the given 2 classes. */
//        default TestRule.Config withOperandFor(Class<? extends Filter> filterClass,
//            Class<? extends Aggregate> aggregateClass) {
//            return withOperandSupplier(b0 ->
//                b0.operand(filterClass).oneInput(b1 ->
//                    b1.operand(aggregateClass).anyInputs()))
//                .as(FilterAggregateTransposeRule.Config.class);
//        }
//
//        /** Defines an operand tree for the given 3 classes. */
//        default TestRule.Config withOperandFor(Class<? extends Filter> filterClass,
//            Class<? extends Aggregate> aggregateClass,
//            Class<? extends RelNode> relClass) {
//            return withOperandSupplier(b0 ->
//                b0.operand(filterClass).oneInput(b1 ->
//                    b1.operand(aggregateClass).oneInput(b2 ->
//                        b2.operand(relClass).anyInputs())))
//                .as(FilterAggregateTransposeRule.Config.class);
//        }
    }
}

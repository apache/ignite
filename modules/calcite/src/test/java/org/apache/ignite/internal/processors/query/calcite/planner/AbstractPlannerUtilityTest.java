package org.apache.ignite.internal.processors.query.calcite.planner;

import java.math.BigDecimal;
import java.util.function.Predicate;
import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.RangeSet;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUnknownAs;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Sarg;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteIndexScan;
import org.apache.ignite.internal.processors.query.calcite.rel.ProjectableFilterableTableScan;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.junit.Test;

import static org.mockito.Mockito.mock;

/** {@link AbstractPlannerTest} inner utility test. */
public class AbstractPlannerUtilityTest extends AbstractPlannerTest {
    @Test
    public void testFlatConditionNormalizer() {
        RelOptCluster cluster = Commons.emptyCluster();
        RexBuilder builder = cluster.getRexBuilder();
        RexLiteral l0 = builder.makeExactLiteral(new BigDecimal(0));
        RexLocalRef lr0 = new RexLocalRef(0, cluster.getTypeFactory().createJavaType(long.class));

        RexLiteral l1 = builder.makeExactLiteral(new BigDecimal(0));
        RexLocalRef lr1 = new RexLocalRef(1, cluster.getTypeFactory().createJavaType(long.class));
        RexLocalRef lr100 = new RexLocalRef(100, cluster.getTypeFactory().createJavaType(long.class));

        final RangeSet<Integer> setNone = ImmutableRangeSet.of();
        final RangeSet<Integer> setAll = setNone.complement();
        final Sarg<Integer> sarg =
            Sarg.of(RexUnknownAs.FALSE, setAll);
        RexNode intLiteral =
            builder.makeLiteral(1, cluster.getTypeFactory().createSqlType(SqlTypeName.INTEGER));
        RexNode rexNode =
            builder.makeCall(SqlStdOperatorTable.SEARCH, intLiteral,
                builder.makeSearchArgumentLiteral(sarg, intLiteral.getType()));
        RexLocalRef lr2 = new RexLocalRef(2, cluster.getTypeFactory().createJavaType(long.class));

        RexNode r0 = builder.makeCall(SqlStdOperatorTable.EQUALS, lr0, l0);
        RexNode r1 = builder.makeCall(SqlStdOperatorTable.EQUALS, lr1, l1);
        RexNode r3 = builder.makeCall(SqlStdOperatorTable.EQUALS, lr2, rexNode);

        {
            RexNode filter = builder.makeCall(SqlStdOperatorTable.AND, r1, r0, r3);
            assertEquals("AND(=($t1, 0), =($t0, 0), =($t2, SEARCH(1, Sarg[IS NOT NULL])))", filter.toString());

            ProjectableFilterableTableScan scan = buildNode(cluster, filter);

            Predicate<ProjectableFilterableTableScan> cond =
                satisfyCondition("AND(=($t0, 0), =($t1, 0), =($t2, SEARCH(1, Sarg[IS NOT NULL])))");

            boolean res = cond.test(scan);
            assertTrue(lastErrorMsg, res);
        }

        {
            RexNode r100 = builder.makeCall(SqlStdOperatorTable.EQUALS, lr100, l1);
            RexNode filter = builder.makeCall(SqlStdOperatorTable.AND, r100, r0, r3);

            assertEquals("AND(=($t100, 0), =($t0, 0), =($t2, SEARCH(1, Sarg[IS NOT NULL])))", filter.toString());

            ProjectableFilterableTableScan scan = buildNode(cluster, filter);

            Predicate<ProjectableFilterableTableScan> cond =
                satisfyCondition("AND(=($t0, 0), =($t2, SEARCH(1, Sarg[IS NOT NULL])), =($t100, 0))");

            boolean res = cond.test(scan);
            assertTrue(lastErrorMsg, res);
        }

        {
            RexNode orOp = builder.makeCall(SqlStdOperatorTable.OR, r0, r1);
            RexNode filter = builder.makeCall(SqlStdOperatorTable.AND, orOp, r3);

            assertEquals("AND(OR(=($t0, 0), =($t1, 0)), =($t2, SEARCH(1, Sarg[IS NOT NULL])))", filter.toString());

            ProjectableFilterableTableScan scan = buildNode(cluster, filter);

            Predicate<ProjectableFilterableTableScan> cond = satisfyCondition("AND(=($t0, 0), =($t100, 0), SEARCH($t2, Sarg[0, 1, 2]))");
            boolean res = cond.test(scan);

            assertFalse(res);
            assertEquals("Unapplicable predicate expected: flat operands", lastErrorMsg);
        }
    }

    private ProjectableFilterableTableScan buildNode(RelOptCluster cluster, RexNode cond) {
        RelDataType type = Commons.typeFactory().createType(int.class);

        return new IgniteIndexScan(
            cluster,
            RelTraitSet.createEmpty(),
            mock(RelOptTableImpl.class),
            null,
            type,
            null,
            cond,
            null,
            null,
            null
        );
    }
}

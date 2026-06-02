
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query.calcite.exec.rel;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.function.Supplier;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexWindowBound;
import org.apache.calcite.rex.RexWindowBounds;
import org.apache.calcite.rex.RexWindowExclusion;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.ignite.internal.processors.query.calcite.exec.ArrayRowHandler;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.RowHandler;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.IgniteRexBuilder;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.window.WindowFunctions;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.window.WindowPartition;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.window.WindowPartitionFactory;
import org.apache.ignite.internal.processors.query.calcite.sql.fun.IgniteOwnSqlOperatorTable;
import org.apache.ignite.internal.processors.query.calcite.trait.TraitUtils;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.util.TypeUtils;
import org.apache.ignite.internal.util.typedef.F;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.apache.calcite.rex.RexWindowBounds.CURRENT_ROW;
import static org.apache.calcite.rex.RexWindowBounds.UNBOUNDED_FOLLOWING;
import static org.apache.calcite.rex.RexWindowBounds.UNBOUNDED_PRECEDING;

/**  */
public class WindowExecutionTest extends AbstractExecutionTest {
    /** row_number() over (partition by {0}). */
    @Test
    public void testRowNumber() {
        Window.RexWinAggCall aggCall = new Window.RexWinAggCall(
            SqlStdOperatorTable.ROW_NUMBER,
            relIntType,
            F.asList(),
            0,
            false,
            false
        );
        Window.Group grp = new Window.Group(
            ImmutableBitSet.of(0),
            true,
            UNBOUNDED_PRECEDING,
            UNBOUNDED_FOLLOWING,
            RexWindowExclusion.EXCLUDE_NO_OTHER,
            RelCollations.EMPTY,
            F.asList(aggCall)
        );

        checkWindow(grp, true,
            new Object[][] {{1}, {2}, {1}, {2}, {3}, {1}});
    }

    /** dense_rank() over (partition by {0} order by {1}). */
    @Test
    public void testDenseRank() {
        Window.RexWinAggCall aggCall = new Window.RexWinAggCall(
            SqlStdOperatorTable.DENSE_RANK,
            relIntType,
            F.asList(),
            0,
            false,
            false
        );
        Window.Group grp = new Window.Group(
            ImmutableBitSet.of(0),
            false,
            UNBOUNDED_PRECEDING,
            CURRENT_ROW,
            RexWindowExclusion.EXCLUDE_NO_OTHER,
            RelCollations.of(1),
            F.asList(aggCall)
        );

        checkWindow(grp, true,
            new Object[][] {{1}, {1}, {1}, {2}, {3}, {1}});
    }

    /** rank() over (partition by {0} order by {1}). */
    @Test
    public void testRank() {
        Window.RexWinAggCall aggCall = new Window.RexWinAggCall(
            SqlStdOperatorTable.RANK,
            relIntType,
            F.asList(),
            0,
            false,
            false
        );
        Window.Group grp = new Window.Group(
            ImmutableBitSet.of(0),
            false,
            UNBOUNDED_PRECEDING,
            CURRENT_ROW,
            RexWindowExclusion.EXCLUDE_NO_OTHER,
            RelCollations.of(1),
            F.asList(aggCall)
        );

        checkWindow(grp, true,
            new Object[][] {{1}, {1}, {1}, {2}, {3}, {1}});
    }

    /** percent_rank() over (partition by {0} order by {1}). */
    @Test
    public void testRercentRank() {
        Window.RexWinAggCall aggCall = new Window.RexWinAggCall(
            SqlStdOperatorTable.PERCENT_RANK,
            relDoubleType,
            F.asList(),
            0,
            false,
            false
        );
        Window.Group grp = new Window.Group(
            ImmutableBitSet.of(0),
            false,
            UNBOUNDED_PRECEDING,
            CURRENT_ROW,
            RexWindowExclusion.EXCLUDE_NO_OTHER,
            RelCollations.of(1),
            F.asList(aggCall)
        );

        checkWindow(grp, false,
            new Object[][] {{0.0}, {0.0}, {0.0}, {0.5}, {1.0}, {0.0}});
    }

    /** cume_dist() over (partition by {0} order by {1}). */
    @Test
    public void testCumeDist() {
        Window.RexWinAggCall aggCall = new Window.RexWinAggCall(
            SqlStdOperatorTable.CUME_DIST,
            relDoubleType,
            F.asList(),
            0,
            false,
            false
        );
        Window.Group grp = new Window.Group(
            ImmutableBitSet.of(0),
            false,
            UNBOUNDED_PRECEDING,
            CURRENT_ROW,
            RexWindowExclusion.EXCLUDE_NO_OTHER,
            RelCollations.of(1),
            F.asList(aggCall)
        );

        checkWindow(grp, false,
            new Object[][] {{1.0}, {1.0}, {1.0 / 3}, {2.0 / 3}, {1.0}, {1.0}});
    }

    /** first_value({1}) over (partition by {0} order by {1}). */
    @Test
    public void testFirstValue() {
        Window.RexWinAggCall aggCall = new Window.RexWinAggCall(
            SqlStdOperatorTable.FIRST_VALUE,
            relIntType,
            F.asList(rexBuilder.makeInputRef(relIntType, 1)),
            0,
            false,
            false
        );
        Window.Group grp = new Window.Group(
            ImmutableBitSet.of(0),
            false,
            UNBOUNDED_PRECEDING,
            CURRENT_ROW,
            RexWindowExclusion.EXCLUDE_NO_OTHER,
            RelCollations.of(1),
            F.asList(aggCall)
        );

        checkWindow(grp, false,
            new Object[][] {{1}, {1}, {1}, {1}, {1}, {0}});
    }

    /** last_value({2}) over (partition by {0} order by {1}). */
    @Test
    public void testLastValue() {
        Window.RexWinAggCall aggCall = new Window.RexWinAggCall(
            SqlStdOperatorTable.LAST_VALUE,
            relIntType,
            F.asList(rexBuilder.makeInputRef(relIntType, 2)),
            0,
            false,
            false
        );
        Window.Group grp = new Window.Group(
            ImmutableBitSet.of(0),
            false,
            UNBOUNDED_PRECEDING,
            CURRENT_ROW,
            RexWindowExclusion.EXCLUDE_NO_OTHER,
            RelCollations.of(1),
            F.asList(aggCall)
        );

        checkWindow(grp, false,
            new Object[][] {{5}, {5}, {1}, {5}, {6}, {1}});
    }

    /** ntile({3}) over (partition by {0}). */
    @Test
    public void testNTile() {
        Window.RexWinAggCall aggCall = new Window.RexWinAggCall(
            SqlStdOperatorTable.NTILE,
            relIntType,
            F.asList(rexBuilder.makeInputRef(relIntType, 3)),
            0,
            false,
            false
        );
        Window.Group grp = new Window.Group(
            ImmutableBitSet.of(0),
            false,
            UNBOUNDED_PRECEDING,
            CURRENT_ROW,
            RexWindowExclusion.EXCLUDE_NO_OTHER,
            RelCollations.EMPTY,
            F.asList(aggCall)
        );

        checkWindow(grp, false,
            new Object[][] {{1}, {2}, {1}, {1}, {2}, {1}});
    }

    /** nth_value({2}, {0}) over (partition by {0}). */
    @Test
    public void testNth() {
        Window.RexWinAggCall aggCall = new Window.RexWinAggCall(
            SqlStdOperatorTable.NTH_VALUE,
            typeFactory.createTypeWithNullability(relIntType, true),
            F.asList(
                rexBuilder.makeInputRef(relIntType, 2),
                rexBuilder.makeInputRef(relIntType, 0)
            ),
            0,
            false,
            false
        );
        Window.Group grp = new Window.Group(
            ImmutableBitSet.of(0),
            false,
            UNBOUNDED_PRECEDING,
            CURRENT_ROW,
            RexWindowExclusion.EXCLUDE_NO_OTHER,
            RelCollations.of(1),
            F.asList(aggCall)
        );

        checkWindow(grp, false,
            new Object[][] {{1}, {1}, {null}, {5}, {5}, {null}});
    }

    /** lag({2}) over (partition by {0} order by {1}). */
    @Test
    public void testLag1() {
        Window.RexWinAggCall aggCall = new Window.RexWinAggCall(
            IgniteOwnSqlOperatorTable.LAG,
            typeFactory.createTypeWithNullability(relIntType, true),
            F.asList(rexBuilder.makeInputRef(relIntType, 2)),
            0,
            false,
            false
        );
        Window.Group grp = new Window.Group(
            ImmutableBitSet.of(0),
            false,
            UNBOUNDED_PRECEDING,
            CURRENT_ROW,
            RexWindowExclusion.EXCLUDE_NO_OTHER,
            RelCollations.of(1),
            F.asList(aggCall)
        );

        checkWindow(grp, false,
            new Object[][] {{null}, {1}, {null}, {1}, {5}, {null}});
    }

    /** lag({2}, {0}) over (partition by {0} order by {1}). */
    @Test
    public void testLag2() {
        Window.RexWinAggCall aggCall = new Window.RexWinAggCall(
            IgniteOwnSqlOperatorTable.LAG,
            typeFactory.createTypeWithNullability(relIntType, true),
            F.asList(
                rexBuilder.makeInputRef(relIntType, 2),
                rexBuilder.makeInputRef(relIntType, 0)
            ),
            0,
            false,
            false
        );
        Window.Group grp = new Window.Group(
            ImmutableBitSet.of(0),
            false,
            UNBOUNDED_PRECEDING,
            CURRENT_ROW,
            RexWindowExclusion.EXCLUDE_NO_OTHER,
            RelCollations.of(1),
            F.asList(aggCall)
        );

        checkWindow(grp, false,
            new Object[][] {{null}, {1}, {null}, {null}, {1}, {null}});
    }

    /** lag({2}, {0}, {1}) over (partition by {0} order by {1}). */
    @Test
    public void testLag3() {
        Window.RexWinAggCall aggCall = new Window.RexWinAggCall(
            IgniteOwnSqlOperatorTable.LAG,
            typeFactory.createTypeWithNullability(relIntType, true),
            F.asList(
                rexBuilder.makeInputRef(relIntType, 2),
                rexBuilder.makeInputRef(relIntType, 0),
                rexBuilder.makeInputRef(relIntType, 1)
            ),
            0,
            false,
            false
        );
        Window.Group grp = new Window.Group(
            ImmutableBitSet.of(0),
            false,
            UNBOUNDED_PRECEDING,
            CURRENT_ROW,
            RexWindowExclusion.EXCLUDE_NO_OTHER,
            RelCollations.of(1),
            F.asList(aggCall)
        );

        checkWindow(grp, false,
            new Object[][] {{1}, {1}, {1}, {2}, {1}, {0}});
    }

    /** lead({2}) over (partition by {0} order by {1}). */
    @Test
    public void testLead1() {
        Window.RexWinAggCall aggCall = new Window.RexWinAggCall(
            IgniteOwnSqlOperatorTable.LEAD,
            typeFactory.createTypeWithNullability(relIntType, true),
            F.asList(rexBuilder.makeInputRef(relIntType, 2)),
            0,
            false,
            false
        );
        Window.Group grp = new Window.Group(
            ImmutableBitSet.of(0),
            false,
            UNBOUNDED_PRECEDING,
            CURRENT_ROW,
            RexWindowExclusion.EXCLUDE_NO_OTHER,
            RelCollations.of(1),
            F.asList(aggCall)
        );

        checkWindow(grp, false,
            new Object[][] {{5}, {null}, {5}, {6}, {null}, {null}});
    }

    /** lead({2}, {0}) over (partition by {0} order by {1}). */
    @Test
    public void testLead2() {
        Window.RexWinAggCall aggCall = new Window.RexWinAggCall(
            IgniteOwnSqlOperatorTable.LEAD,
            typeFactory.createTypeWithNullability(relIntType, true),
            F.asList(
                rexBuilder.makeInputRef(relIntType, 2),
                rexBuilder.makeInputRef(relIntType, 0)
            ),
            0,
            false,
            false
        );
        Window.Group grp = new Window.Group(
            ImmutableBitSet.of(0),
            false,
            UNBOUNDED_PRECEDING,
            CURRENT_ROW,
            RexWindowExclusion.EXCLUDE_NO_OTHER,
            RelCollations.of(1),
            F.asList(aggCall)
        );

        checkWindow(grp, false,
            new Object[][] {{5}, {null}, {6}, {null}, {null}, {null}});
    }

    /** lead({2}, {0}, {1}) over (partition by {0} order by {1}). */
    @Test
    public void testLead3() {
        Window.RexWinAggCall aggCall = new Window.RexWinAggCall(
            IgniteOwnSqlOperatorTable.LEAD,
            typeFactory.createTypeWithNullability(relIntType, true),
            F.asList(
                rexBuilder.makeInputRef(relIntType, 2),
                rexBuilder.makeInputRef(relIntType, 0),
                rexBuilder.makeInputRef(relIntType, 1)
            ),
            0,
            false,
            false
        );
        Window.Group grp = new Window.Group(
            ImmutableBitSet.of(0),
            false,
            UNBOUNDED_PRECEDING,
            CURRENT_ROW,
            RexWindowExclusion.EXCLUDE_NO_OTHER,
            RelCollations.of(1),
            F.asList(aggCall)
        );

        checkWindow(grp, false,
            new Object[][] {{5}, {1}, {6}, {2}, {3}, {0}});
    }

    /** count(*) over (partition by {0} rows between unbounded prescending and current row). */
    @Test
    public void testCountRowsBetweenUnboundedPrescendingAndCurrentRow() {
        checkWindow(count(true, UNBOUNDED_PRECEDING, CURRENT_ROW), true,
            new Object[][] {{1}, {2}, {1}, {2}, {3}, {1}});
    }

    /** count(*) over (partition by {0} rows between unbounded prescending and unbounded following). */
    @Test
    public void testCountRowsBetweenUnboundedPrescendingAndUnboundedFollowing() {
        checkWindow(count(true, UNBOUNDED_PRECEDING, UNBOUNDED_FOLLOWING), false,
            new Object[][] {{2}, {2}, {3}, {3}, {3}, {1}});
    }

    /** count(*) over (partition by {0} order by {1} range between unbounded prescending and current row). */
    @Test
    public void testCountRangeBetweenUnboundedPrescendingAndCurrentRow() {
        checkWindow(count(false, UNBOUNDED_PRECEDING, CURRENT_ROW), false,
            new Object[][] {{2}, {2}, {1}, {2}, {3}, {1}});
    }

    /** count(*) over (partition by {0} order by {1} range between unbounded prescending and unbounded following). */
    @Test
    public void testCountRangeBetweenUnboundedPrescendingAndUnboundedFollowing() {
        checkWindow(count(false, UNBOUNDED_PRECEDING, UNBOUNDED_FOLLOWING), false,
            new Object[][] {{2}, {2}, {3}, {3}, {3}, {1}});
    }

    /** count(*) over (partition by {0} order by {1} range between 2 prescending and 1 following). */
    @Test
    public void testCountRangeBetween2PrescendingAnd1Following() {
        IgniteRexBuilder rexBuilder = new IgniteRexBuilder(typeFactory);
        RexWindowBound lower = RexWindowBounds.preceding(rexBuilder.makeLiteral(2, relIntType));
        RexWindowBound upper = RexWindowBounds.following(rexBuilder.makeLiteral(1, relIntType));
        checkWindow(count(false, lower, upper), false,
            new Object[][] {{2}, {2}, {2}, {3}, {3}, {1}});
    }

    /**
     * sum({0}) over (partition by {1} rows between unbounded prescending and current row),
     * row_number() over (partition by {1} rows between unbounded prescending and current row).
     * */
    @Test
    public void testSumRowsAndRowNumberToCurrentRow() {
        checkWindow(sumRowsAndRowNumber(CURRENT_ROW), true,
            new Object[][] {{1, 1}, {2, 2}, {2, 1}, {4, 2}, {6, 3}, {3, 1}});
    }

    /**
     * sum({0}) over (partition by {1} rows between unbounded prescending and inbounded following),
     * row_number() over (partition by {1} rows between unbounded prescending and inbounded following).
     * */
    @Test
    public void testSumRowsAndRowNumberToUnboundedFollowing() {
        checkWindow(sumRowsAndRowNumber(UNBOUNDED_FOLLOWING), false,
            new Object[][] {{2, 1}, {2, 2}, {6, 1}, {6, 2}, {6, 3}, {3, 1}});
    }

    /** */
    private void checkWindow(Window.Group grp, boolean streaming, Object[][] expRes) {
        Assert.assertEquals(streaming, WindowFunctions.streamable(grp));

        ExecutionContext<Object[]> ctx = executionContext(F.first(nodes()), UUID.randomUUID(), 0);

        Node<Object[]> input = createInputNode(ctx);

        WindowNode<Object[]> window = createWindowNode(ctx, grp, input);

        int resFldShift = input.rowType().getFieldCount();

        try (RootNode<Object[]> root = new RootNode<>(ctx, window.rowType())) {
            root.register(window);

            for (int i = 0; i < expRes.length; i++) {
                assertTrue(root.hasNext());
                Object[] row = root.next();
                for (int j = 0; j < expRes[i].length; j++)
                    assertEquals(expRes[i][j], row[resFldShift + j]);
            }
            assertFalse(root.hasNext());
        }
    }

    /** */
    private WindowNode<Object[]> createWindowNode(ExecutionContext<Object[]> ctx, Window.Group grp, Node<Object[]> input) {
        Class<?>[] outFields = new Class<?>[input.rowType().getFieldCount() + grp.aggCalls.size()];
        Arrays.fill(outFields, int.class);
        RelDataType outRowType = TypeUtils.createRowType(typeFactory, outFields);

        Comparator<Object[]> partCmp = ctx.expressionFactory()
            .comparator(TraitUtils.createCollation(grp.keys.asList()));

        List<AggregateCall> aggCalls = toAggCall(grp);

        Supplier<WindowPartition<Object[]>> partitionFactory =
            new WindowPartitionFactory<>(ctx, grp, aggCalls, input.rowType());

        WindowNode<Object[]> window = new WindowNode<>(
            ctx,
            outRowType,
            partCmp,
            partitionFactory,
            rowFactory()
        );
        window.register(input);
        return window;
    }

    /** */
    private List<AggregateCall> toAggCall(Window.Group grp) {
        List<AggregateCall> calls = new ArrayList<>();
        for (Window.RexWinAggCall aggCall : grp.aggCalls) {
            SqlAggFunction op = (SqlAggFunction)aggCall.op;
            ImmutableIntList args = Window.getProjectOrdinals(aggCall.operands);
            AggregateCall call = AggregateCall.create(
                op,
                aggCall.distinct,
                false,
                aggCall.ignoreNulls,
                aggCall.operands,
                args,
                -1,
                null,
                RelCollations.EMPTY,
                aggCall.type,
                null
            );
            calls.add(call);
        }
        return calls;
    }

    /** */
    private Node<Object[]> createInputNode(ExecutionContext<Object[]> ctx) {
        RelDataType inputRowType = TypeUtils.createRowType(typeFactory, int.class, int.class, int.class, int.class);
        List<Object[]> data = F.asList(
            row(1, 1, 1, 4),
            row(1, 1, 5, 4),
            row(2, 1, 1, 2),
            row(2, 2, 5, 2),
            row(2, 3, 6, 2),
            row(3, 0, 1, 1)
        );
        return new ScanNode<>(ctx, inputRowType, data);
    }

    /**
     * @throws Exception If failed.
     */
    @Before
    @Override public void setup() throws Exception {
        nodesCnt = 1;
        super.setup();
    }

    /**
     * count({0}) over (partition by {0} rows between [lower] and [upper])
     */
    private static Window.Group count(boolean rows, RexWindowBound lower, RexWindowBound upper) {
        Window.RexWinAggCall aggCall = new Window.RexWinAggCall(
            SqlStdOperatorTable.COUNT,
            relIntType,
            F.asList(rexBuilder.makeInputRef(relIntType, 0)),
            0,
            false,
            false
        );
        return new Window.Group(
            ImmutableBitSet.of(0),
            rows,
            lower,
            upper,
            RexWindowExclusion.EXCLUDE_NO_OTHER,
            RelCollations.of(1),
            F.asList(aggCall)
        );
    }

    /**
     * sum({0}) over (partition by {0} rows between unbounded prescending and [upper]),
     * row_number() over (partition by {0} rows between unbounded prescending and [upper])
     */
    private static Window.Group sumRowsAndRowNumber(RexWindowBound upper) {
        Window.RexWinAggCall sumCall = new Window.RexWinAggCall(
            SqlStdOperatorTable.SUM,
            relIntType,
            F.asList(rexBuilder.makeInputRef(relIntType, 0)),
            0,
            false,
            false
        );
        Window.RexWinAggCall rowNumberCall = new Window.RexWinAggCall(
            SqlStdOperatorTable.ROW_NUMBER,
            relIntType,
            F.asList(),
            0,
            false,
            false
        );
        return new Window.Group(
            ImmutableBitSet.of(0),
            true,
            UNBOUNDED_PRECEDING,
            upper,
            RexWindowExclusion.EXCLUDE_NO_OTHER,
            RelCollations.EMPTY,
            F.asList(sumCall, rowNumberCall)
        );
    }

    /** */
    private static final IgniteTypeFactory typeFactory = new IgniteTypeFactory();

    /** */
    private static final RelDataType relIntType = typeFactory.createType(int.class);

    /** */
    private static final RelDataType relDoubleType = typeFactory.createType(double.class);

    /** */
    private static final RexBuilder rexBuilder = new IgniteRexBuilder(typeFactory);

    /** */
    protected RowHandler.RowFactory<Object[]> rowFactory() {
        return new RowHandler.RowFactory<>() {
            /** */
            @Override public RowHandler<Object[]> handler() {
                return ArrayRowHandler.INSTANCE;
            }

            /** */
            @Override public Object[] create() {
                throw new AssertionError();
            }

            /** */
            @Override public Object[] create(Object... fields) {
                return fields;
            }
        };
    }
}

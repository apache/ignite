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
import java.util.stream.Stream;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexWindowBound;
import org.apache.calcite.rex.RexWindowBounds;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.ignite.internal.processors.query.calcite.exec.ArrayRowHandler;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.RowHandler;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.IgniteRexBuilder;
import org.apache.ignite.internal.processors.query.calcite.trait.TraitUtils;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.util.TypeUtils;
import org.apache.ignite.internal.util.typedef.F;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized;

/**
 *
 */
public class WindowExecutionTest extends AbstractExecutionTest {

    private static final int TEST_GRP_PARAM_NUM = LAST_PARAM_NUM + 1;
    private static final int TEST_RES_PARAM_NUM = TEST_GRP_PARAM_NUM + 1;
    private static final int TEST_STREAM = TEST_RES_PARAM_NUM + 1;

    private static final IgniteTypeFactory typeFactory = new IgniteTypeFactory();

    /**  */
    @Parameterized.Parameter(TEST_GRP_PARAM_NUM)
    public Window.Group testGrp;

    /**  */
    @Parameterized.Parameter(TEST_RES_PARAM_NUM)
    public List<List<Integer>> testRes;

    @Parameterized.Parameter(TEST_STREAM)
    public boolean testStream;

    /**  */
    @Parameterized.Parameters(name = PARAMS_STRING
        + ", grp={" + TEST_GRP_PARAM_NUM
        + "}, res={" + TEST_RES_PARAM_NUM
        + "}, stream={" + TEST_STREAM + "}")
    public static List<Object[]> data() {
        List<Object[]> extraParams = new ArrayList<>();
        IgniteRexBuilder rexBuilder = new IgniteRexBuilder(typeFactory);
        ImmutableList<Object[]> newParams = ImmutableList.of(
            new Object[] {
                rowNumber(),
                F.asList(
                    F.asList(1),
                    F.asList(2),
                    F.asList(1),
                    F.asList(2),
                    F.asList(3),
                    F.asList(1)
                ),
                true
            },
            new Object[] {
                rowNumber(),
                F.asList(
                    F.asList(1),
                    F.asList(2),
                    F.asList(1),
                    F.asList(2),
                    F.asList(3),
                    F.asList(1)
                ),
                false
            },
            new Object[] {
                countRows(RexWindowBounds.UNBOUNDED_PRECEDING, RexWindowBounds.CURRENT_ROW),
                F.asList(
                    F.asList(1),
                    F.asList(2),
                    F.asList(1),
                    F.asList(2),
                    F.asList(3),
                    F.asList(1)
                ),
                true
            },
            new Object[] {
                countRows(RexWindowBounds.UNBOUNDED_PRECEDING, RexWindowBounds.CURRENT_ROW),
                F.asList(
                    F.asList(1),
                    F.asList(2),
                    F.asList(1),
                    F.asList(2),
                    F.asList(3),
                    F.asList(1)
                ),
                false
            },
            new Object[] {
                countRange(RexWindowBounds.UNBOUNDED_PRECEDING, RexWindowBounds.CURRENT_ROW),
                F.asList(
                    F.asList(2),
                    F.asList(2),
                    F.asList(1),
                    F.asList(2),
                    F.asList(3),
                    F.asList(1)
                ),
                false
            },
            new Object[] {
                countRange(RexWindowBounds.UNBOUNDED_PRECEDING, RexWindowBounds.UNBOUNDED_FOLLOWING),
                F.asList(
                    F.asList(2),
                    F.asList(2),
                    F.asList(3),
                    F.asList(3),
                    F.asList(3),
                    F.asList(1)
                ),
                false
            },
            new Object[] {
                countRange(
                    RexWindowBounds.preceding(rexBuilder.makeLiteral(2, typeFactory.createType(int.class))),
                    RexWindowBounds.following(rexBuilder.makeLiteral(1, typeFactory.createType(int.class)))
                ),
                F.asList(
                    F.asList(2),
                    F.asList(2),
                    F.asList(2),
                    F.asList(3),
                    F.asList(3),
                    F.asList(1)
                ),
                false
            },
            new Object[] {
                countRange(
                    RexWindowBounds.preceding(new RexInputRef(2, typeFactory.createType(int.class))),
                    RexWindowBounds.following(new RexInputRef(2, typeFactory.createType(int.class)))
                ),
                F.asList(
                    F.asList(2),
                    F.asList(2),
                    F.asList(2),
                    F.asList(3),
                    F.asList(3),
                    F.asList(1)
                ),
                false
            },
            new Object[] {
                sumRowsAndRowNumber(),
                F.asList(
                    F.asList(1, 1),
                    F.asList(2, 2),
                    F.asList(2, 1),
                    F.asList(4, 2),
                    F.asList(6, 3),
                    F.asList(3, 1)
                ),
                true
            },
            new Object[] {
                sumRowsAndRowNumber(),
                F.asList(
                    F.asList(1, 1),
                    F.asList(2, 2),
                    F.asList(2, 1),
                    F.asList(4, 2),
                    F.asList(6, 3),
                    F.asList(3, 1)
                ),
                false
            }
        );

        for (Object[] newParam : newParams) {
            for (Object[] inheritedParam : AbstractExecutionTest.parameters()) {
                Object[] both = Stream.concat(Arrays.stream(inheritedParam), Arrays.stream(newParam))
                    .toArray(Object[]::new);
                extraParams.add(both);
            }
        }

        return extraParams;
    }

    /** */
    @Test
    public void executeWindow() {
        ExecutionContext<Object[]> ctx = executionContext(F.first(nodes()), UUID.randomUUID(), 0);

        IgniteTypeFactory tf = ctx.getTypeFactory();
        RelDataType inputRowType = TypeUtils.createRowType(tf, int.class, int.class, int.class);
        Class<?>[] outFields = new Class<?>[3 + testGrp.aggCalls.size()];
        Arrays.fill(outFields, int.class);
        RelDataType outRowType = TypeUtils.createRowType(tf, outFields);
        ScanNode<Object[]> scan = new ScanNode<>(ctx, inputRowType, Arrays.asList(
            row(1, 1, 1),
            row(1, 1, 5),
            row(2, 1, 1),
            row(2, 2, 5),
            row(2, 3, 6),
            row(3, 0, 1)
        ));

        List<AggregateCall> calls = new ArrayList<>();
        for (Window.RexWinAggCall aggCall : testGrp.aggCalls) {
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

        Comparator<Object[]> partCmp = ctx.expressionFactory().comparator(TraitUtils.createCollation(testGrp.keys.asList()));
        WindowNode<Object[]> window = new WindowNode<>(
            ctx,
            outRowType,
            partCmp,
            ctx.expressionFactory().windowFrameFactory(testGrp, calls, inputRowType, testStream),
            rowFactory()
        );

        window.register(scan);

        RootNode<Object[]> root = new RootNode<>(ctx, outRowType);
        root.register(window);

        for (int i = 0; i < testRes.size(); i++) {
            assertTrue(root.hasNext());
            Object[] row = root.next();
            for (int j = 0; j < testRes.get(i).size(); j++) {
                assertEquals(testRes.get(i).get(j), row[3 + j]);
            }
        }
        assertFalse(root.hasNext());
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
     * row_number() over (partition by {0} rows between unbounded prescending and current row)
     */
    private static Window.Group rowNumber() {
        Window.RexWinAggCall aggCall = new Window.RexWinAggCall(
            SqlStdOperatorTable.ROW_NUMBER,
            typeFactory.createType(long.class),
            ImmutableList.of(),
            0,
            false,
            false
        );
        return new Window.Group(
            ImmutableBitSet.of(0),
            true,
            RexWindowBounds.UNBOUNDED_PRECEDING,
            RexWindowBounds.CURRENT_ROW,
            RelCollations.EMPTY,
            ImmutableList.of(aggCall)
        );
    }

    /**
     * count({0}) over (partition by {0} rows between [lower] and [upper])
     */
    private static Window.Group countRows(RexWindowBound lower, RexWindowBound upper) {
        Window.RexWinAggCall aggCall = new Window.RexWinAggCall(
            SqlStdOperatorTable.COUNT,
            typeFactory.createType(int.class),
            ImmutableList.of(new RexInputRef(0, typeFactory.createType(int.class))),
            0,
            false,
            false
        );
        return new Window.Group(
            ImmutableBitSet.of(0),
            true,
            lower,
            upper,
            RelCollations.EMPTY,
            ImmutableList.of(aggCall)
        );
    }

    /**
     * count({0}) over (partition by {0} order by {1} rows between [lower] and [upper])
     */
    private static Window.Group countRange(RexWindowBound lower, RexWindowBound upper) {
        Window.RexWinAggCall aggCall = new Window.RexWinAggCall(
            SqlStdOperatorTable.COUNT,
            typeFactory.createType(int.class),
            ImmutableList.of(new RexInputRef(0, typeFactory.createType(int.class))),
            0,
            false,
            false
        );
        return new Window.Group(
            ImmutableBitSet.of(0),
            false,
            lower,
            upper,
            TraitUtils.createCollation(ImmutableIntList.of(1)),
            ImmutableList.of(aggCall)
        );
    }

    /**
     * sum({0}) over (partition by {0} rows between unbounded prescending and current row),
     * row_number() over (partition by {0} rows between unbounded prescending and current row)
     */
    private static Window.Group sumRowsAndRowNumber() {
        Window.RexWinAggCall countCall = new Window.RexWinAggCall(
            SqlStdOperatorTable.SUM,
            typeFactory.createType(int.class),
            ImmutableList.of(new RexInputRef(0, typeFactory.createType(int.class))),
            0,
            false,
            false
        );
        Window.RexWinAggCall rowNumberCall = new Window.RexWinAggCall(
            SqlStdOperatorTable.ROW_NUMBER,
            typeFactory.createType(long.class),
            ImmutableList.of(),
            0,
            false,
            false
        );
        return new Window.Group(
            ImmutableBitSet.of(0),
            true,
            RexWindowBounds.UNBOUNDED_PRECEDING,
            RexWindowBounds.CURRENT_ROW,
            RelCollations.EMPTY,
            ImmutableList.of(countCall, rowNumberCall)
        );
    }

    /**  */
    protected RowHandler.RowFactory<Object[]> rowFactory() {
        return new RowHandler.RowFactory<Object[]>() {
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

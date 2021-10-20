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

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.RowHandler;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.agg.AccumulatorWrapper;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.agg.Accumulators;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.agg.AggregateType;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.processors.query.calcite.util.TypeUtils;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrowsWithCause;
import static org.apache.ignite.internal.util.ArrayUtils.asList;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 *
 */
public abstract class BaseAggregateTest extends AbstractExecutionTest {
    /** */
    @ParameterizedTest
    @EnumSource
    public void count(TestAggregateType testAgg) {
        ExecutionContext<Object[]> ctx = executionContext();
        IgniteTypeFactory tf = ctx.getTypeFactory();
        RelDataType rowType = TypeUtils.createRowType(tf, int.class, int.class);
        ScanNode<Object[]> scan = new ScanNode<>(ctx, rowType, Arrays.asList(
            row(0, 200),
            row(1, 300),
            row(1, 1400),
            row(0, 1000)
        ));

        AggregateCall call = AggregateCall.create(
            SqlStdOperatorTable.COUNT,
            false,
            false,
            false,
            ImmutableIntList.of(),
            -1,
            RelCollations.EMPTY,
            tf.createJavaType(int.class),
            null);

        List<ImmutableBitSet> grpSets = List.of(ImmutableBitSet.of(0));

        RelDataType aggRowType = TypeUtils.createRowType(tf, int.class);

        SingleNode<Object[]> aggChain = createAggregateNodesChain(
            testAgg,
            ctx,
            grpSets,
            call,
            rowType,
            aggRowType,
            rowFactory(),
            scan
        );

        RootNode<Object[]> root = new RootNode<>(ctx, aggRowType);
        root.register(aggChain);

        assertTrue(root.hasNext());

        assertArrayEquals(row(0, 2), root.next());
        assertArrayEquals(row(1, 2), root.next());

        assertFalse(root.hasNext());
    }

    /** */
    @ParameterizedTest
    @EnumSource
    public void min(TestAggregateType testAgg) {
        ExecutionContext<Object[]> ctx = executionContext();
        IgniteTypeFactory tf = ctx.getTypeFactory();
        RelDataType rowType = TypeUtils.createRowType(tf, int.class, int.class);
        ScanNode<Object[]> scan = new ScanNode<>(ctx, rowType, Arrays.asList(
            row(0, 200),
            row(1, 300),
            row(1, 1400),
            row(0, 1000)
        ));

        AggregateCall call = AggregateCall.create(
            SqlStdOperatorTable.MIN,
            false,
            false,
            false,
            ImmutableIntList.of(1),
            -1,
            RelCollations.EMPTY,
            tf.createJavaType(int.class),
            null);

        List<ImmutableBitSet> grpSets = List.of(ImmutableBitSet.of(0));

        RelDataType aggRowType = TypeUtils.createRowType(tf, int.class);

        SingleNode<Object[]> aggChain = createAggregateNodesChain(
            testAgg,
            ctx,
            grpSets,
            call,
            rowType,
            aggRowType,
            rowFactory(),
            scan
        );

        RootNode<Object[]> root = new RootNode<>(ctx, aggRowType);
        root.register(aggChain);

        assertTrue(root.hasNext());

        assertArrayEquals(row(0, 200), root.next());
        assertArrayEquals(row(1, 300), root.next());

        assertFalse(root.hasNext());
    }

    /** */
    @ParameterizedTest
    @EnumSource
    public void max(TestAggregateType testAgg) {
        ExecutionContext<Object[]> ctx = executionContext();
        IgniteTypeFactory tf = ctx.getTypeFactory();
        RelDataType rowType = TypeUtils.createRowType(tf, int.class, int.class);
        ScanNode<Object[]> scan = new ScanNode<>(ctx, rowType, Arrays.asList(
            row(0, 200),
            row(1, 300),
            row(1, 1400),
            row(0, 1000)
        ));

        AggregateCall call = AggregateCall.create(
            SqlStdOperatorTable.MAX,
            false,
            false,
            false,
            ImmutableIntList.of(1),
            -1,
            RelCollations.EMPTY,
            tf.createJavaType(int.class),
            null);

        List<ImmutableBitSet> grpSets = List.of(ImmutableBitSet.of(0));

        RelDataType aggRowType = TypeUtils.createRowType(tf, int.class);

        SingleNode<Object[]> aggChain = createAggregateNodesChain(
            testAgg,
            ctx,
            grpSets,
            call,
            rowType,
            aggRowType,
            rowFactory(),
            scan
        );

        RootNode<Object[]> root = new RootNode<>(ctx, aggRowType);
        root.register(aggChain);

        assertTrue(root.hasNext());

        assertArrayEquals(row(0, 1000), root.next());
        assertArrayEquals(row(1, 1400), root.next());

        assertFalse(root.hasNext());
    }

    /** */
    @ParameterizedTest
    @EnumSource
    public void avg(TestAggregateType testAgg) {
        ExecutionContext<Object[]> ctx = executionContext();
        IgniteTypeFactory tf = ctx.getTypeFactory();
        RelDataType rowType = TypeUtils.createRowType(tf, int.class, int.class);
        ScanNode<Object[]> scan = new ScanNode<>(ctx, rowType, Arrays.asList(
            row(0, 200),
            row(1, 300),
            row(1, 1300),
            row(0, 1000)
        ));

        AggregateCall call = AggregateCall.create(
            SqlStdOperatorTable.AVG,
            false,
            false,
            false,
            ImmutableIntList.of(1),
            -1,
            RelCollations.EMPTY,
            tf.createJavaType(int.class),
            null);

        List<ImmutableBitSet> grpSets = List.of(ImmutableBitSet.of(0));

        RelDataType aggRowType = TypeUtils.createRowType(tf, int.class);

        SingleNode<Object[]> aggChain = createAggregateNodesChain(
            testAgg,
            ctx,
            grpSets,
            call,
            rowType,
            aggRowType,
            rowFactory(),
            scan
        );

        RootNode<Object[]> root = new RootNode<>(ctx, aggRowType);
        root.register(aggChain);

        assertTrue(root.hasNext());

        assertArrayEquals(row(0, 600), root.next());
        assertArrayEquals(row(1, 800), root.next());

        assertFalse(root.hasNext());
    }

    /** */
    @ParameterizedTest
    @EnumSource
    public void single(TestAggregateType testAgg) {
        Object[] res = {null, null};

        List<Object[]> arr = Arrays.asList(
            row(0, res[0]),
            row(1, res[1])
        );

        singleAggr(testAgg, arr, res, false);

        res = new Object[]{1, 2};

        arr = Arrays.asList(
            row(0, res[0]),
            row(1, res[1])
        );

        singleAggr(testAgg, arr, res, false);

        arr = Arrays.asList(
            row(0, res[0]),
            row(1, res[1]),
            row(0, res[0]),
            row(1, res[1])
            );

        singleAggr(testAgg, arr, res, true);

        arr = Arrays.asList(
            row(0, null),
            row(1, null),
            row(0, null),
            row(1, null)
        );

        singleAggr(testAgg, arr, res, true);
    }

    /**
     * Checks single aggregate and appropriate {@link Accumulators.SingleVal} implementation.
     *
     * @param scanInput Input data.
     * @param output Expectation result.
     * @param mustFail {@code true} If expression must throw exception.
     **/
    @SuppressWarnings("ThrowableNotThrown")
    public void singleAggr(TestAggregateType testAgg, List<Object[]> scanInput, Object[] output, boolean mustFail) {
        ExecutionContext<Object[]> ctx = executionContext();
        IgniteTypeFactory tf = ctx.getTypeFactory();
        RelDataType rowType = TypeUtils.createRowType(tf, int.class, int.class);
        ScanNode<Object[]> scan = new ScanNode<>(ctx, rowType, scanInput);

        AggregateCall call = AggregateCall.create(
            SqlStdOperatorTable.SINGLE_VALUE,
            false,
            false,
            false,
            ImmutableIntList.of(1),
            -1,
            RelCollations.EMPTY,
            tf.createJavaType(Integer.class),
            null);

        List<ImmutableBitSet> grpSets = List.of(ImmutableBitSet.of(0));

        RelDataType aggRowType = TypeUtils.createRowType(tf, int.class);

        SingleNode<Object[]> aggChain = createAggregateNodesChain(
            testAgg,
            ctx,
            grpSets,
            call,
            rowType,
            aggRowType,
            rowFactory(),
            scan
        );

        RootNode<Object[]> root = new RootNode<>(ctx, aggRowType);
        root.register(aggChain);

        Runnable r = () -> {
            assertTrue(root.hasNext());

            assertArrayEquals(row(0, output[0]), root.next());
            assertArrayEquals(row(1, output[1]), root.next());

            assertFalse(root.hasNext());
        };

        if (mustFail)
            assertThrowsWithCause(r, IllegalArgumentException.class);
        else
            r.run();
    }

    /** */
    @ParameterizedTest
    @EnumSource
    public void distinctSum(TestAggregateType testAgg) {
        ExecutionContext<Object[]> ctx = executionContext();
        IgniteTypeFactory tf = ctx.getTypeFactory();
        RelDataType rowType = TypeUtils.createRowType(tf, int.class, int.class);
        ScanNode<Object[]> scan = new ScanNode<>(ctx, rowType, Arrays.asList(
            row(0, 200),
            row(1, 200),
            row(1, 300),
            row(1, 300),
            row(0, 1000),
            row(0, 1000),
            row(0, 1000),
            row(0, 1000),
            row(0, 200)
        ));

        AggregateCall call = AggregateCall.create(
            SqlStdOperatorTable.SUM,
            true,
            false,
            false,
            ImmutableIntList.of(1),
            -1,
            RelCollations.EMPTY,
            tf.createJavaType(int.class),
            null);

        List<ImmutableBitSet> grpSets = List.of(ImmutableBitSet.of(0));

        RelDataType aggRowType = TypeUtils.createRowType(tf, int.class);

        SingleNode<Object[]> aggChain = createAggregateNodesChain(
            testAgg,
            ctx,
            grpSets,
            call,
            rowType,
            aggRowType,
            rowFactory(),
            scan
        );

        RootNode<Object[]> root = new RootNode<>(ctx, aggRowType);
        root.register(aggChain);

        assertTrue(root.hasNext());

        assertArrayEquals(row(0, 1200), root.next());
        assertArrayEquals(row(1, 500), root.next());

        assertFalse(root.hasNext());
    }

    /** */
    @ParameterizedTest
    @EnumSource
    public void sumOnDifferentRowsCount(TestAggregateType testAgg) {
        int bufSize = Commons.IN_BUFFER_SIZE;

        int[] grpsCount = {1, bufSize / 2, bufSize, bufSize + 1, bufSize * 4};
        int[] rowsInGroups = {1, 5, bufSize};

        for (int grps : grpsCount) {
            for (int rowsInGroup : rowsInGroups) {
                log.info("Check: [grps=" + grps + ", rowsInGroup=" + rowsInGroup + ']');

                ExecutionContext<Object[]> ctx = executionContext();
                IgniteTypeFactory tf = ctx.getTypeFactory();
                RelDataType rowType = TypeUtils.createRowType(tf, int.class, int.class);

                ScanNode<Object[]> scan = new ScanNode<>(
                    ctx,
                    rowType,
                    new TestTable(
                        grps * rowsInGroup,
                        rowType,
                        (r) -> r / rowsInGroup,
                        (r) -> r % rowsInGroup
                    )
                );

                AggregateCall call = AggregateCall.create(
                    SqlStdOperatorTable.SUM,
                    false,
                    false,
                    false,
                    ImmutableIntList.of(1),
                    -1,
                    RelCollations.EMPTY,
                    tf.createJavaType(int.class),
                    null);

                List<ImmutableBitSet> grpSets = List.of(ImmutableBitSet.of(0));

                RelDataType aggRowType = TypeUtils.createRowType(tf, int.class);

                SingleNode<Object[]> aggChain = createAggregateNodesChain(
                    testAgg,
                    ctx,
                    grpSets,
                    call,
                    rowType,
                    aggRowType,
                    rowFactory(),
                    scan
                );

                RootNode<Object[]> root = new RootNode<>(ctx, aggRowType);
                root.register(aggChain);

                Set<Integer> grpId = IntStream.range(0, grps).boxed().collect(Collectors.toSet());

                while (root.hasNext()) {
                    Object[] row = root.next();

                    grpId.remove(row[0]);

                    assertEquals((rowsInGroup - 1) * rowsInGroup / 2, row[1]);
                }

                assertTrue(grpId.isEmpty());
            }
        }
    }

    /** */
    private SingleNode<Object[]> createAggregateNodesChain(
        TestAggregateType testAgg,
        ExecutionContext<Object[]> ctx,
        List<ImmutableBitSet> grpSets,
        AggregateCall aggCall,
        RelDataType inRowType,
        RelDataType aggRowType,
        RowHandler.RowFactory<Object[]> rowFactory,
        ScanNode<Object[]> scan
    ) {
        switch (testAgg) {
            case SINGLE:
                return createSingleAggregateNodesChain(ctx, grpSets, aggCall, inRowType, aggRowType, rowFactory, scan);

            case MAP_REDUCE:
                return createMapReduceAggregateNodesChain(ctx, grpSets, aggCall, inRowType, aggRowType, rowFactory, scan);

            default:
                assert false;

                return null;
        }
    }

    /** */
    protected abstract SingleNode<Object[]> createSingleAggregateNodesChain(
        ExecutionContext<Object[]> ctx,
        List<ImmutableBitSet> grpSets,
        AggregateCall aggCall,
        RelDataType inRowType,
        RelDataType aggRowType,
        RowHandler.RowFactory<Object[]> rowFactory,
        ScanNode<Object[]> scan
    );

    /** */
    protected abstract SingleNode<Object[]> createMapReduceAggregateNodesChain(
        ExecutionContext<Object[]> ctx,
        List<ImmutableBitSet> grpSets,
        AggregateCall call,
        RelDataType inRowType,
        RelDataType aggRowType,
        RowHandler.RowFactory<Object[]> rowFactory,
        ScanNode<Object[]> scan
    );

    /** */
    protected Supplier<List<AccumulatorWrapper<Object[]>>> accFactory(
        ExecutionContext<Object[]> ctx,
        AggregateCall call,
        AggregateType type,
        RelDataType rowType
    ) {
        return ctx.expressionFactory().accumulatorsFactory(type, asList(call), rowType);
    }

    /** */
    enum TestAggregateType {
        /** */
        SINGLE,

        /** */
        MAP_REDUCE
    }
}

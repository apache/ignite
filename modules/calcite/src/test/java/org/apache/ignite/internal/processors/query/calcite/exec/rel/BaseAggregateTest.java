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
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.query.calcite.exec.ArrayRowHandler;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.RowHandler;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.agg.AccumulatorWrapper;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.agg.Accumulators;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.agg.AggregateType;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.util.TypeUtils;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized;

/**
 *
 */
@SuppressWarnings("TypeMayBeWeakened")
@WithSystemProperty(key = "calcite.debug", value = "true")
public abstract class BaseAggregateTest extends AbstractExecutionTest {
    /** Last parameter number. */
    protected static final int TEST_AGG_PARAM_NUM = LAST_PARAM_NUM + 1;

    /** */
    @Parameterized.Parameter(TEST_AGG_PARAM_NUM)
    public TestAggregateType testAgg;

    /** */
    @Parameterized.Parameters(name = PARAMS_STRING + ", type={" + TEST_AGG_PARAM_NUM + "}")
    public static List<Object[]> data() {
        List<Object[]> extraParams = new ArrayList<>();

        ImmutableList<Object[]> newParams = ImmutableList.of(
            new Object[] {TestAggregateType.SINGLE},
            new Object[] {TestAggregateType.MAP_REDUCE}
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

    /**
     * @throws Exception If failed.
     */
    @Before
    @Override public void setup() throws Exception {
        nodesCnt = 1;
        super.setup();
    }

    /** */
    @Test
    public void count() {
        ExecutionContext<Object[]> ctx = executionContext(F.first(nodes()), UUID.randomUUID(), 0);
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

        ImmutableList<ImmutableBitSet> grpSets = ImmutableList.of(ImmutableBitSet.of(0));

        RelDataType aggRowType = TypeUtils.createRowType(tf, int.class);

        SingleNode<Object[]> aggChain = createAggregateNodesChain(
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

        Assert.assertArrayEquals(row(0, 2), root.next());
        Assert.assertArrayEquals(row(1, 2), root.next());

        assertFalse(root.hasNext());
    }

    /** */
    @Test
    public void min() {
        ExecutionContext<Object[]> ctx = executionContext(F.first(nodes()), UUID.randomUUID(), 0);
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

        ImmutableList<ImmutableBitSet> grpSets = ImmutableList.of(ImmutableBitSet.of(0));

        RelDataType aggRowType = TypeUtils.createRowType(tf, int.class);

        SingleNode<Object[]> aggChain = createAggregateNodesChain(
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

        Assert.assertArrayEquals(row(0, 200), root.next());
        Assert.assertArrayEquals(row(1, 300), root.next());

        assertFalse(root.hasNext());
    }

    /** */
    @Test
    public void max() {
        ExecutionContext<Object[]> ctx = executionContext(F.first(nodes()), UUID.randomUUID(), 0);
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

        ImmutableList<ImmutableBitSet> grpSets = ImmutableList.of(ImmutableBitSet.of(0));

        RelDataType aggRowType = TypeUtils.createRowType(tf, int.class);

        SingleNode<Object[]> aggChain = createAggregateNodesChain(
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

        Assert.assertArrayEquals(row(0, 1000), root.next());
        Assert.assertArrayEquals(row(1, 1400), root.next());

        assertFalse(root.hasNext());
    }

    /** */
    @Test
    public void avg() {
        ExecutionContext<Object[]> ctx = executionContext(F.first(nodes()), UUID.randomUUID(), 0);
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

        ImmutableList<ImmutableBitSet> grpSets = ImmutableList.of(ImmutableBitSet.of(0));

        RelDataType aggRowType = TypeUtils.createRowType(tf, int.class);

        SingleNode<Object[]> aggChain = createAggregateNodesChain(
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

        Assert.assertArrayEquals(row(0, 600), root.next());
        Assert.assertArrayEquals(row(1, 800), root.next());

        assertFalse(root.hasNext());
    }

    /** */
    @Test
    public void single() {
        Object[] res = {null, null};

        List<Object[]> arr = Arrays.asList(
            row(0, res[0]),
            row(1, res[1])
        );

        singleAggr(arr, res, false);

        res = new Object[]{1, 2};

        arr = Arrays.asList(
            row(0, res[0]),
            row(1, res[1])
        );

        singleAggr(arr, res, false);

        arr = Arrays.asList(
            row(0, res[0]),
            row(1, res[1]),
            row(0, res[0]),
            row(1, res[1])
            );

        singleAggr(arr, res, true);

        arr = Arrays.asList(
            row(0, null),
            row(1, null),
            row(0, null),
            row(1, null)
        );

        singleAggr(arr, res, true);
    }

    /**
     * Checks single aggregate and appropriate {@link Accumulators.SingleVal} implementation.
     *
     * @param scanInput Input data.
     * @param output Expectation result.
     * @param mustFail {@code true} If expression must throw exception.
     **/
    public void singleAggr(List<Object[]> scanInput, Object[] output, boolean mustFail) {
        ExecutionContext<Object[]> ctx = executionContext(F.first(nodes()), UUID.randomUUID(), 0);
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

        ImmutableList<ImmutableBitSet> grpSets = ImmutableList.of(ImmutableBitSet.of(0));

        RelDataType aggRowType = TypeUtils.createRowType(tf, int.class);

        SingleNode<Object[]> aggChain = createAggregateNodesChain(
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

            Assert.assertArrayEquals(row(0, output[0]), root.next());
            Assert.assertArrayEquals(row(1, output[1]), root.next());

            assertFalse(root.hasNext());
        };

        if (mustFail)
            GridTestUtils.assertThrowsWithCause(r, IllegalArgumentException.class);
        else
            r.run();
    }

    /** */
    @Test
    public void distinctSum() {
        ExecutionContext<Object[]> ctx = executionContext(F.first(nodes()), UUID.randomUUID(), 0);
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

        ImmutableList<ImmutableBitSet> grpSets = ImmutableList.of(ImmutableBitSet.of(0));

        RelDataType aggRowType = TypeUtils.createRowType(tf, int.class);

        SingleNode<Object[]> aggChain = createAggregateNodesChain(
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

        Assert.assertArrayEquals(row(0, 1200), root.next());
        Assert.assertArrayEquals(row(1, 500), root.next());

        assertFalse(root.hasNext());
    }

    /** */
    @Test
    public void sumOnDifferentRowsCount() throws IgniteCheckedException {
        int bufSize = U.field(AbstractNode.class, "IN_BUFFER_SIZE");

        int[] grpsCount = {1, bufSize / 2, bufSize, bufSize + 1, bufSize * 4};
        int[] rowsInGroups = {1, 5, bufSize};

        for (int grps : grpsCount) {
            for (int rowsInGroup : rowsInGroups) {
                log.info("Check: [grps=" + grps + ", rowsInGroup=" + rowsInGroup + ']');

                ExecutionContext<Object[]> ctx = executionContext(F.first(nodes()), UUID.randomUUID(), 0);
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

                ImmutableList<ImmutableBitSet> grpSets = ImmutableList.of(ImmutableBitSet.of(0));

                RelDataType aggRowType = TypeUtils.createRowType(tf, int.class);

                SingleNode<Object[]> aggChain = createAggregateNodesChain(
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
    protected SingleNode<Object[]> createAggregateNodesChain(
        ExecutionContext<Object[]> ctx,
        ImmutableList<ImmutableBitSet> grpSets,
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
        ImmutableList<ImmutableBitSet> grpSets,
        AggregateCall aggCall,
        RelDataType inRowType,
        RelDataType aggRowType,
        RowHandler.RowFactory<Object[]> rowFactory,
        ScanNode<Object[]> scan
    );

    /** */
    protected abstract SingleNode<Object[]> createMapReduceAggregateNodesChain(
        ExecutionContext<Object[]> ctx,
        ImmutableList<ImmutableBitSet> grpSets,
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
        return ctx.expressionFactory().accumulatorsFactory(type, F.asList(call), rowType);
    }

    /** */
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

    /** */
    enum TestAggregateType {
        /** */
        SINGLE,

        /** */
        MAP_REDUCE
    }
}

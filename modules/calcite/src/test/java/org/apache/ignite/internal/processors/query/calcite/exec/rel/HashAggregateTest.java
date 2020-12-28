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
import java.util.UUID;
import java.util.function.Supplier;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.ignite.internal.processors.query.calcite.exec.ArrayRowHandler;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.RowHandler;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.agg.AccumulatorWrapper;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.agg.AggregateType;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteMapAggregateHash;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.util.TypeUtils;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.apache.ignite.internal.processors.query.calcite.exec.exp.agg.AggregateType.MAP;
import static org.apache.ignite.internal.processors.query.calcite.exec.exp.agg.AggregateType.REDUCE;
import static org.apache.ignite.internal.processors.query.calcite.exec.exp.agg.AggregateType.SINGLE;

/**
 *
 */
@SuppressWarnings("TypeMayBeWeakened")
@WithSystemProperty(key = "calcite.debug", value = "true")
public class HashAggregateTest extends AbstractExecutionTest {
    /**
     * @throws Exception If failed.
     */
    @Before
    @Override public void setup() throws Exception {
        nodesCnt = 1;
        super.setup();
    }

    /**
     *
     */
    @Test
    public void mapReduceAvg() {
        ExecutionContext<Object[]> ctx = executionContext(F.first(nodes()), UUID.randomUUID(), 0);
        IgniteTypeFactory tf = ctx.getTypeFactory();
        RelDataType rowType = TypeUtils.createRowType(tf, String.class, int.class);
        ScanNode<Object[]> scan = new ScanNode<>(ctx, rowType, Arrays.asList(
            row("Igor", 200),
            row("Roman", 300),
            row("Ivan", 1400),
            row("Alexey", 1000)
        ));

        AggregateCall call = AggregateCall.create(
            SqlStdOperatorTable.AVG,
            false,
            false,
            false,
            ImmutableIntList.of(1),
            -1,
            RelCollations.EMPTY,
            tf.createJavaType(double.class),
            null);

        ImmutableList<ImmutableBitSet> grpSets = ImmutableList.of(ImmutableBitSet.of());

        RelDataType mapType = IgniteMapAggregateHash.rowType(tf);
        AggregateHashNode<Object[]> map = new AggregateHashNode<>(
            ctx,
            mapType,
            MAP,
            grpSets,
            accFactory(ctx, call, MAP, rowType),
            rowFactory()
        );

        map.register(scan);

        RelDataType reduceType = TypeUtils.createRowType(tf, double.class);
        AggregateHashNode<Object[]> reduce = new AggregateHashNode<>(
            ctx,
            reduceType,
            REDUCE,
            grpSets,
            accFactory(ctx, call, REDUCE, null),
            rowFactory()
        );

        reduce.register(map);

        RootNode<Object[]> root = new RootNode<>(ctx, reduceType);
        root.register(reduce);

        assertTrue(root.hasNext());
        assertEquals(725d, root.next()[0]);
        assertFalse(root.hasNext());
    }

    /** */
    @Test
    public void mapReduceSum() {
        ExecutionContext<Object[]> ctx = executionContext(F.first(nodes()), UUID.randomUUID(), 0);
        IgniteTypeFactory tf = ctx.getTypeFactory();
        RelDataType rowType = TypeUtils.createRowType(tf, String.class, int.class);
        ScanNode<Object[]> scan = new ScanNode<>(ctx, rowType, Arrays.asList(
            row("Igor", 200),
            row("Roman", 300),
            row("Ivan", 1400),
            row("Alexey", 1000)
        ));

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

        ImmutableList<ImmutableBitSet> grpSets = ImmutableList.of(ImmutableBitSet.of());

        RelDataType mapType = IgniteMapAggregateHash.rowType(tf);
        AggregateHashNode<Object[]> map = new AggregateHashNode<>(ctx, mapType, MAP, grpSets, accFactory(ctx, call, MAP, rowType), rowFactory());
        map.register(scan);

        RelDataType reduceType = TypeUtils.createRowType(tf, int.class);
        AggregateHashNode<Object[]> reduce = new AggregateHashNode<>(ctx, reduceType, REDUCE, grpSets, accFactory(ctx, call, REDUCE, null), rowFactory());
        reduce.register(map);

        RootNode<Object[]> root = new RootNode<>(ctx, reduceType);
        root.register(reduce);

        assertTrue(root.hasNext());
        assertEquals(2900, root.next()[0]);
        assertFalse(root.hasNext());
    }

    /** */
    @Test
    public void mapReduceMin() {
        ExecutionContext<Object[]> ctx = executionContext(F.first(nodes()), UUID.randomUUID(), 0);
        IgniteTypeFactory tf = ctx.getTypeFactory();
        RelDataType rowType = TypeUtils.createRowType(tf, String.class, int.class);
        ScanNode<Object[]> scan = new ScanNode<>(ctx, rowType, Arrays.asList(
            row("Igor", 200),
            row("Roman", 300),
            row("Ivan", 1400),
            row("Alexey", 1000)
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

        ImmutableList<ImmutableBitSet> grpSets = ImmutableList.of(ImmutableBitSet.of());

        RelDataType mapType = IgniteMapAggregateHash.rowType(tf);
        AggregateHashNode<Object[]> map = new AggregateHashNode<>(ctx, mapType, MAP, grpSets, accFactory(ctx, call, MAP, rowType), rowFactory());
        map.register(scan);

        RelDataType reduceType = TypeUtils.createRowType(tf, int.class);
        AggregateHashNode<Object[]> reduce = new AggregateHashNode<>(ctx, reduceType, REDUCE, grpSets, accFactory(ctx, call, REDUCE, null), rowFactory());
        reduce.register(map);

        RootNode<Object[]> root = new RootNode<>(ctx, reduceType);
        root.register(reduce);

        assertTrue(root.hasNext());
        assertEquals(200, root.next()[0]);
        assertFalse(root.hasNext());
    }

    /** */
    @Test
    public void mapReduceMax() {
        ExecutionContext<Object[]> ctx = executionContext(F.first(nodes()), UUID.randomUUID(), 0);
        IgniteTypeFactory tf = ctx.getTypeFactory();
        RelDataType rowType = TypeUtils.createRowType(tf, String.class, int.class);
        ScanNode<Object[]> scan = new ScanNode<>(ctx, rowType, Arrays.asList(
            row("Igor", 200),
            row("Roman", 300),
            row("Ivan", 1400),
            row("Alexey", 1000)
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

        ImmutableList<ImmutableBitSet> grpSets = ImmutableList.of(ImmutableBitSet.of());

        RelDataType mapType = IgniteMapAggregateHash.rowType(tf);
        AggregateHashNode<Object[]> map = new AggregateHashNode<>(ctx, mapType, MAP, grpSets, accFactory(ctx, call, MAP, rowType), rowFactory());
        map.register(scan);

        RelDataType reduceType = TypeUtils.createRowType(tf, int.class);
        AggregateHashNode<Object[]> reduce = new AggregateHashNode<>(ctx, reduceType, REDUCE, grpSets, accFactory(ctx, call, REDUCE, null), rowFactory());
        reduce.register(map);

        RootNode<Object[]> root = new RootNode<>(ctx, reduceType);
        root.register(reduce);

        assertTrue(root.hasNext());
        assertEquals(1400, root.next()[0]);
        assertFalse(root.hasNext());
    }

    /** */
    @Test
    public void mapReduceCount() {
        ExecutionContext<Object[]> ctx = executionContext(F.first(nodes()), UUID.randomUUID(), 0);
        IgniteTypeFactory tf = ctx.getTypeFactory();
        RelDataType rowType = TypeUtils.createRowType(tf, String.class, int.class);
        ScanNode<Object[]> scan = new ScanNode<>(ctx, rowType, Arrays.asList(
            row("Igor", 200),
            row("Roman", 300),
            row("Ivan", 1400),
            row("Alexey", 1000)
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

        ImmutableList<ImmutableBitSet> grpSets = ImmutableList.of(ImmutableBitSet.of());

        RelDataType mapType = IgniteMapAggregateHash.rowType(tf);
        AggregateHashNode<Object[]> map = new AggregateHashNode<>(ctx, mapType, MAP, grpSets, accFactory(ctx, call, MAP, rowType), rowFactory());
        map.register(scan);

        RelDataType reduceType = TypeUtils.createRowType(tf, int.class);
        AggregateHashNode<Object[]> reduce = new AggregateHashNode<>(ctx, reduceType, REDUCE, grpSets, accFactory(ctx, call, REDUCE, null), rowFactory());
        reduce.register(map);

        RootNode<Object[]> root = new RootNode<>(ctx, reduceType);
        root.register(reduce);

        assertTrue(root.hasNext());
        assertEquals(4, root.next()[0]);
        assertFalse(root.hasNext());
    }

    /** */
    @Test
    public void singleAvg() {
        ExecutionContext<Object[]> ctx = executionContext(F.first(nodes()), UUID.randomUUID(), 0);
        IgniteTypeFactory tf = ctx.getTypeFactory();
        RelDataType rowType = TypeUtils.createRowType(tf, String.class, int.class);
        ScanNode<Object[]> scan = new ScanNode<>(ctx, rowType, Arrays.asList(
            row("Igor", 200),
            row("Roman", 300),
            row("Ivan", 1400),
            row("Alexey", 1000)
        ));

        AggregateCall call = AggregateCall.create(
            SqlStdOperatorTable.AVG,
            false,
            false,
            false,
            ImmutableIntList.of(1),
            -1,
            RelCollations.EMPTY,
            tf.createJavaType(double.class),
            null);

        ImmutableList<ImmutableBitSet> grpSets = ImmutableList.of(ImmutableBitSet.of());

        RelDataType aggType = TypeUtils.createRowType(tf, int.class);
        AggregateHashNode<Object[]> agg = new AggregateHashNode<>(ctx, aggType, SINGLE, grpSets, accFactory(ctx, call, SINGLE, rowType), rowFactory());
        agg.register(scan);

        RootNode<Object[]> root = new RootNode<>(ctx, aggType);
        root.register(agg);

        assertTrue(root.hasNext());
        assertEquals(725d, root.next()[0]);
        assertFalse(root.hasNext());
    }

    /** */
    @Test
    public void singleSum() {
        ExecutionContext<Object[]> ctx = executionContext(F.first(nodes()), UUID.randomUUID(), 0);
        IgniteTypeFactory tf = ctx.getTypeFactory();
        RelDataType rowType = TypeUtils.createRowType(tf, String.class, int.class);
        ScanNode<Object[]> scan = new ScanNode<>(ctx, rowType, Arrays.asList(
            row("Igor", 200),
            row("Roman", 300),
            row("Ivan", 1400),
            row("Alexey", 1000)
        ));

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

        ImmutableList<ImmutableBitSet> grpSets = ImmutableList.of(ImmutableBitSet.of());

        RelDataType aggType = TypeUtils.createRowType(tf, int.class);
        AggregateHashNode<Object[]> agg = new AggregateHashNode<>(ctx, aggType, SINGLE, grpSets, accFactory(ctx, call, SINGLE, rowType), rowFactory());
        agg.register(scan);

        RootNode<Object[]> root = new RootNode<>(ctx, aggType);
        root.register(agg);

        assertTrue(root.hasNext());
        assertEquals(2900, root.next()[0]);
        assertFalse(root.hasNext());
    }

    /** */
    @Test
    public void singleMin() {
        ExecutionContext<Object[]> ctx = executionContext(F.first(nodes()), UUID.randomUUID(), 0);
        IgniteTypeFactory tf = ctx.getTypeFactory();
        RelDataType rowType = TypeUtils.createRowType(tf, String.class, int.class);
        ScanNode<Object[]> scan = new ScanNode<>(ctx, rowType, Arrays.asList(
            row("Igor", 200),
            row("Roman", 300),
            row("Ivan", 1400),
            row("Alexey", 1000)
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

        ImmutableList<ImmutableBitSet> grpSets = ImmutableList.of(ImmutableBitSet.of());

        RelDataType aggType = TypeUtils.createRowType(tf, int.class);
        AggregateHashNode<Object[]> agg = new AggregateHashNode<>(ctx, aggType, SINGLE, grpSets, accFactory(ctx, call, SINGLE, rowType), rowFactory());
        agg.register(scan);

        RootNode<Object[]> root = new RootNode<>(ctx, aggType);
        root.register(agg);

        assertTrue(root.hasNext());
        assertEquals(200, root.next()[0]);
        assertFalse(root.hasNext());
    }

    /** */
    @Test
    public void singleMax() {
        ExecutionContext<Object[]> ctx = executionContext(F.first(nodes()), UUID.randomUUID(), 0);
        IgniteTypeFactory tf = ctx.getTypeFactory();
        RelDataType rowType = TypeUtils.createRowType(tf, String.class, int.class);
        ScanNode<Object[]> scan = new ScanNode<>(ctx, rowType, Arrays.asList(
            row("Igor", 200),
            row("Roman", 300),
            row("Ivan", 1400),
            row("Alexey", 1000)
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

        ImmutableList<ImmutableBitSet> grpSets = ImmutableList.of(ImmutableBitSet.of());

        RelDataType aggType = TypeUtils.createRowType(tf, int.class);
        AggregateHashNode<Object[]> agg = new AggregateHashNode<>(ctx, aggType, SINGLE, grpSets, accFactory(ctx, call, SINGLE, rowType), rowFactory());
        agg.register(scan);

        RootNode<Object[]> root = new RootNode<>(ctx, aggType);
        root.register(agg);

        assertTrue(root.hasNext());
        assertEquals(1400, root.next()[0]);
        assertFalse(root.hasNext());
    }

    /** */
    @Test
    public void singleCount() {
        ExecutionContext<Object[]> ctx = executionContext(F.first(nodes()), UUID.randomUUID(), 0);
        IgniteTypeFactory tf = ctx.getTypeFactory();
        RelDataType rowType = TypeUtils.createRowType(tf, String.class, int.class);
        ScanNode<Object[]> scan = new ScanNode<>(ctx, rowType, Arrays.asList(
            row("Igor", 200),
            row("Roman", 300),
            row("Ivan", 1400),
            row("Alexey", 1000)
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

        ImmutableList<ImmutableBitSet> grpSets = ImmutableList.of(ImmutableBitSet.of());

        RelDataType aggType = TypeUtils.createRowType(tf, int.class);
        AggregateHashNode<Object[]> agg = new AggregateHashNode<>(ctx, aggType, SINGLE, grpSets, accFactory(ctx, call, SINGLE, rowType), rowFactory());
        agg.register(scan);

        RootNode<Object[]> root = new RootNode<>(ctx, aggType);
        root.register(agg);

        assertTrue(root.hasNext());
        assertEquals(4, root.next()[0]);
        assertFalse(root.hasNext());
    }

    /** */
    @Test
    public void singleCountByGroup() {
        ExecutionContext<Object[]> ctx = executionContext(F.first(nodes()), UUID.randomUUID(), 0);
        IgniteTypeFactory tf = ctx.getTypeFactory();
        RelDataType rowType = TypeUtils.createRowType(tf, String.class, int.class, int.class);
        ScanNode<Object[]> scan = new ScanNode<>(ctx, rowType, Arrays.asList(
            row("Igor", 0, 200),
            row("Roman", 1, 300),
            row("Ivan", 1, 1400),
            row("Alexey", 0, 1000)
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

        ImmutableList<ImmutableBitSet> grpSets = ImmutableList.of(ImmutableBitSet.of(1));

        RelDataType aggType = TypeUtils.createRowType(tf, int.class);
        AggregateHashNode<Object[]> agg = new AggregateHashNode<>(ctx, aggType, SINGLE, grpSets, accFactory(ctx, call, SINGLE, rowType), rowFactory());
        agg.register(scan);

        RootNode<Object[]> root = new RootNode<>(ctx, aggType);
        root.register(agg);

        assertTrue(root.hasNext());
        // TODO needs a sort, relying on an order in a hash table looks strange
        Assert.assertArrayEquals(row(1, 2), root.next());
        Assert.assertArrayEquals(row(0, 2), root.next());
        assertFalse(root.hasNext());
    }

    /** */
    private Supplier<List<AccumulatorWrapper<Object[]>>> accFactory(
        ExecutionContext<Object[]> ctx,
        AggregateCall call,
        AggregateType type,
        RelDataType rowType
    ) {
        return ctx.expressionFactory().accumulatorsFactory(type, F.asList(call), rowType);
    }

    /** */
    private RowHandler.RowFactory<Object[]> rowFactory() {
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
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
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.util.TypeUtils;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 *
 */
@SuppressWarnings("TypeMayBeWeakened")
@WithSystemProperty(key = "calcite.debug", value = "true")
public abstract class BaseAggregateTest extends AbstractExecutionTest {
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
    public void singleCount() {
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
        SingleNode<Object[]> aggChain = createSingleAggregateNodesChain(
            ctx,
            aggType,
            grpSets,
            call,
            rowType,
            rowFactory(),
            scan
        );

        RootNode<Object[]> root = new RootNode<>(ctx, aggType);
        root.register(aggChain);

        assertTrue(root.hasNext());

        Assert.assertArrayEquals(row(0, 2), root.next());
        Assert.assertArrayEquals(row(1, 2), root.next());

        assertFalse(root.hasNext());
    }

    /** */
    @Test
    public void singleMin() {
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
            SqlStdOperatorTable.MIN,
            false,
            false,
            false,
            ImmutableIntList.of(2),
            -1,
            RelCollations.EMPTY,
            tf.createJavaType(int.class),
            null);

        ImmutableList<ImmutableBitSet> grpSets = ImmutableList.of(ImmutableBitSet.of(1));

        RelDataType aggType = TypeUtils.createRowType(tf, int.class);
        SingleNode<Object[]> aggChain = createSingleAggregateNodesChain(
            ctx,
            aggType,
            grpSets,
            call,
            rowType,
            rowFactory(),
            scan
        );

        RootNode<Object[]> root = new RootNode<>(ctx, aggType);
        root.register(aggChain);

        assertTrue(root.hasNext());

        Assert.assertArrayEquals(row(0, 200), root.next());
        Assert.assertArrayEquals(row(1, 300), root.next());

        assertFalse(root.hasNext());
    }

    /** */
    @Test
    public void singleMax() {
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
            SqlStdOperatorTable.MAX,
            false,
            false,
            false,
            ImmutableIntList.of(2),
            -1,
            RelCollations.EMPTY,
            tf.createJavaType(int.class),
            null);

        ImmutableList<ImmutableBitSet> grpSets = ImmutableList.of(ImmutableBitSet.of(1));

        RelDataType aggType = TypeUtils.createRowType(tf, int.class);
        SingleNode<Object[]> aggChain = createSingleAggregateNodesChain(
            ctx,
            aggType,
            grpSets,
            call,
            rowType,
            rowFactory(),
            scan
        );

        RootNode<Object[]> root = new RootNode<>(ctx, aggType);
        root.register(aggChain);

        assertTrue(root.hasNext());

        Assert.assertArrayEquals(row(0, 1000), root.next());
        Assert.assertArrayEquals(row(1, 1400), root.next());

        assertFalse(root.hasNext());
    }

    /** */
    @Test
    public void singleAvg() {
        ExecutionContext<Object[]> ctx = executionContext(F.first(nodes()), UUID.randomUUID(), 0);
        IgniteTypeFactory tf = ctx.getTypeFactory();
        RelDataType rowType = TypeUtils.createRowType(tf, String.class, int.class, int.class);
        ScanNode<Object[]> scan = new ScanNode<>(ctx, rowType, Arrays.asList(
            row("Igor", 0, 200),
            row("Roman", 1, 300),
            row("Ivan", 1, 1300),
            row("Alexey", 0, 1000)
        ));

        AggregateCall call = AggregateCall.create(
            SqlStdOperatorTable.AVG,
            false,
            false,
            false,
            ImmutableIntList.of(2),
            -1,
            RelCollations.EMPTY,
            tf.createJavaType(int.class),
            null);

        ImmutableList<ImmutableBitSet> grpSets = ImmutableList.of(ImmutableBitSet.of(1));

        RelDataType aggType = TypeUtils.createRowType(tf, int.class);
        SingleNode<Object[]> aggChain = createSingleAggregateNodesChain(
            ctx,
            aggType,
            grpSets,
            call,
            rowType,
            rowFactory(),
            scan
        );

        RootNode<Object[]> root = new RootNode<>(ctx, aggType);
        root.register(aggChain);

        assertTrue(root.hasNext());

        Assert.assertArrayEquals(row(0, 600), root.next());
        Assert.assertArrayEquals(row(1, 800), root.next());

        assertFalse(root.hasNext());
    }

    /** */
    protected abstract SingleNode<Object[]> createSingleAggregateNodesChain(
        ExecutionContext<Object[]> ctx,
        RelDataType aggType,
        ImmutableList<ImmutableBitSet> grpSets,
        AggregateCall aggCall,
        RelDataType rowType,
        RowHandler.RowFactory<Object[]> rowFactory,
        ScanNode<Object[]> scan);

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
}

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
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.ignite.internal.processors.query.calcite.exec.ArrayRowHandler;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.RowHandler;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.util.TypeUtils;
import org.apache.ignite.internal.util.typedef.F;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.apache.ignite.internal.processors.query.calcite.exec.exp.agg.AggregateType.MAP;
import static org.apache.ignite.internal.processors.query.calcite.exec.exp.agg.AggregateType.REDUCE;
import static org.apache.ignite.internal.processors.query.calcite.exec.exp.agg.AggregateType.SINGLE;

/**
 * Test execution of MINUS (EXCEPT) operator.
 */
public class MinusExecutionTest extends AbstractExecutionTest {
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
    public void testSingle() {
        checkMinus(true, false);
    }

    /** */
    @Test
    public void testSingleAll() {
        checkMinus(true, true);
    }

    /** */
    @Test
    public void testMapReduce() {
        checkMinus(false, false);
    }

    /** */
    @Test
    public void testMapReduceAll() {
        checkMinus(false, true);
    }

    /** */
    @Test
    public void testSingleWithEmptySet() {
        ExecutionContext<Object[]> ctx = executionContext(F.first(nodes()), UUID.randomUUID(), 0);
        IgniteTypeFactory tf = ctx.getTypeFactory();
        RelDataType rowType = TypeUtils.createRowType(tf, String.class, int.class);

        List<Object[]> data = Arrays.asList(
            row("Igor", 1),
            row("Roman", 1)
        );

        // For "single minus" operation, node should not request rows from the next source if result after the previous
        // source is already empty.
        ScanNode<Object[]> scan1 = new ScanNode<>(ctx, rowType, data);
        ScanNode<Object[]> scan2 = new ScanNode<>(ctx, rowType, data);
        Node<Object[]> node3 = new AbstractNode<Object[]>(ctx, rowType) {
            @Override protected void rewindInternal() {
                // No-op.
            }

            @Override protected Downstream<Object[]> requestDownstream(int idx) {
                return null;
            }

            @Override public void request(int rowsCnt) throws Exception {
                fail("Node should not be requested");
            }
        };

        MinusNode<Object[]> minusNode = new MinusNode<>(ctx, rowType, SINGLE, false, rowFactory());
        minusNode.register(Arrays.asList(scan1, scan2, node3));

        RootNode<Object[]> root = new RootNode<>(ctx, rowType);
        root.register(minusNode);

        assertFalse(root.hasNext());
    }

    /**
     * @param single Single.
     * @param all All.
     */
    private void checkMinus(boolean single, boolean all) {
        ExecutionContext<Object[]> ctx = executionContext(F.first(nodes()), UUID.randomUUID(), 0);
        IgniteTypeFactory tf = ctx.getTypeFactory();
        RelDataType rowType = TypeUtils.createRowType(tf, String.class, int.class);

        ScanNode<Object[]> scan1 = new ScanNode<>(ctx, rowType, Arrays.asList(
            row("Igor", 1),
            row("Roman", 1),
            row("Igor", 1),
            row("Roman", 2),
            row("Igor", 1),
            row("Igor", 1),
            row("Igor", 2)
        ));

        ScanNode<Object[]> scan2 = new ScanNode<>(ctx, rowType, Arrays.asList(
            row("Igor", 1),
            row("Roman", 1),
            row("Igor", 1),
            row("Alexey", 1)
        ));

        MinusNode<Object[]> minusNode;

        if (single) {
            minusNode = new MinusNode<>(
                ctx,
                rowType,
                SINGLE,
                all,
                rowFactory()
            );
        }
        else {
            minusNode = new MinusNode<>(
                ctx,
                rowType,
                MAP,
                all,
                rowFactory()
            );
        }

        minusNode.register(Arrays.asList(scan1, scan2));

        if (!single) {
            MinusNode<Object[]> reduceNode = new MinusNode<>(
                ctx,
                rowType,
                REDUCE,
                all,
                rowFactory()
            );

            reduceNode.register(Collections.singletonList(minusNode));

            minusNode = reduceNode;
        }

        Comparator<Object[]> cmp = ctx.expressionFactory().comparator(RelCollations.of(ImmutableIntList.of(0, 1)));

        // Create sort node on the top to check sorted results.
        SortNode<Object[]> sortNode = new SortNode<>(ctx, rowType, cmp);
        sortNode.register(minusNode);

        RootNode<Object[]> root = new RootNode<>(ctx, rowType);
        root.register(sortNode);

        assertTrue(root.hasNext());

        if (all) {
            Assert.assertArrayEquals(row("Igor", 1), root.next());
            Assert.assertArrayEquals(row("Igor", 1), root.next());
        }

        Assert.assertArrayEquals(row("Igor", 2), root.next());
        Assert.assertArrayEquals(row("Roman", 2), root.next());

        assertFalse(root.hasNext());
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

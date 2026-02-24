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
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.ignite.internal.processors.query.calcite.exec.ArrayRowHandler;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.RowHandler;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.agg.AggregateType;
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
 * Abstract test for set operator (MINUS, INTERSECT) execution.
 */
public abstract class AbstractSetOpExecutionTest extends AbstractExecutionTest {
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
        checkSetOp(true, false);
    }

    /** */
    @Test
    public void testSingleAll() {
        checkSetOp(true, true);
    }

    /** */
    @Test
    public void testMapReduce() {
        checkSetOp(false, false);
    }

    /** */
    @Test
    public void testMapReduceAll() {
        checkSetOp(false, true);
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

        // For single distribution set operations, node should not request rows from the next source if result after
        // the previous source is already empty.
        ScanNode<Object[]> scan1 = new ScanNode<>(ctx, rowType, data);
        ScanNode<Object[]> scan2 = new ScanNode<>(ctx, rowType, data);
        ScanNode<Object[]> scan3 = new ScanNode<>(ctx, rowType, Collections.emptyList());
        Node<Object[]> node4 = new AbstractNode<Object[]>(ctx, rowType) {
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

        List<Node<Object[]>> inputs = Arrays.asList(scan1, scan2, scan3, node4);

        AbstractSetOpNode<Object[]> setOpNode = setOpNodeFactory(ctx, rowType, SINGLE, false, inputs.size());
        setOpNode.register(inputs);

        RootNode<Object[]> root = new RootNode<>(ctx, rowType);
        root.register(setOpNode);

        assertFalse(root.hasNext());
    }

    /** */
    protected abstract void checkSetOp(boolean single, boolean all);

    /**
     * @param single Single.
     * @param all All.
     */
    protected void checkSetOp(boolean single, boolean all, List<List<Object[]>> dataSets, List<Object[]> expectedResult) {
        ExecutionContext<Object[]> ctx = executionContext(F.first(nodes()), UUID.randomUUID(), 0);
        IgniteTypeFactory tf = ctx.getTypeFactory();
        RelDataType rowType = TypeUtils.createRowType(tf, String.class, int.class);

        List<Node<Object[]>> inputs = dataSets.stream().map(ds -> new ScanNode<>(ctx, rowType, ds))
            .collect(Collectors.toList());

        AbstractSetOpNode<Object[]> setOpNode;

        if (single)
            setOpNode = setOpNodeFactory(ctx, rowType, SINGLE, all, inputs.size());
        else
            setOpNode = setOpNodeFactory(ctx, rowType, MAP, all, inputs.size());

        setOpNode.register(inputs);

        if (!single) {
            AbstractSetOpNode<Object[]> reduceNode = setOpNodeFactory(ctx, rowType, REDUCE, all, 1);

            reduceNode.register(Collections.singletonList(setOpNode));

            setOpNode = reduceNode;
        }

        Comparator<Object[]> cmp = ctx.expressionFactory().comparator(RelCollations.of(ImmutableIntList.of(0, 1)));

        // Create sort node on the top to check sorted results.
        SortNode<Object[]> sortNode = new SortNode<>(ctx, rowType, cmp);
        sortNode.register(setOpNode);

        RootNode<Object[]> root = new RootNode<>(ctx, rowType);
        root.register(sortNode);

        assertTrue(F.isEmpty(expectedResult) || root.hasNext());

        for (Object[] row : expectedResult)
            Assert.assertArrayEquals(row, root.next());

        assertFalse(root.hasNext());
    }

    /** */
    @Test
    public void testDistributedInputs() {
        // Check all variants containing from 0 to 2 duplicated rows for 2 inputs.
        int[][] inputVariants = {
            {0, 0}, {0, 1}, {0, 2},
            {1, 0}, {1, 1}, {1, 2},
            {2, 0}, {2, 1}, {2, 2}
        };

        // Check 2 cluster nodes.
        int[][] rowsCnt = new int[2][];

        for (int node0input = 0; node0input < inputVariants.length; node0input++) {
            for (int node1input = 0; node1input < inputVariants.length; node1input++) {
                rowsCnt[0] = inputVariants[node0input];
                rowsCnt[1] = inputVariants[node1input];
                checkDistributedSetOp(false, rowsCnt);
                checkDistributedSetOp(true, rowsCnt);
            }
        }
    }

    /**
     * @param all All.
     * @param rowsCnt Count of duplicated rows per node per input.
     */
    protected void checkDistributedSetOp(boolean all, int[][] rowsCnt) {
        ExecutionContext<Object[]> ctx = executionContext();
        RelDataType rowType = TypeUtils.createRowType(ctx.getTypeFactory(), String.class);

        List<Node<Object[]>> mapNodes = new ArrayList<>();
        int[] totalRowsCnt = new int[rowsCnt[0].length];

        for (int i = 0; i < rowsCnt.length; i++) {
            for (int j = 0; j < rowsCnt[i].length; j++)
                totalRowsCnt[j] += rowsCnt[i][j];

            List<Node<Object[]>> inputs = Arrays.stream(rowsCnt[i])
                .mapToObj(cnt -> new ScanNode<>(ctx, rowType, new TestTable(cnt, rowType, r -> "test")))
                .collect(Collectors.toList());

            AbstractSetOpNode<Object[]> mapNode;

            mapNode = setOpNodeFactory(ctx, rowType, MAP, all, inputs.size());

            mapNode.register(inputs);

            mapNodes.add(mapNode);
        }

        // Use union all to emulate streams from different cluster nodes.
        Node<Object[]> unionNode = new UnionAllNode<>(ctx, rowType);
        unionNode.register(mapNodes);

        AbstractSetOpNode<Object[]> reduceNode = setOpNodeFactory(ctx, rowType, REDUCE, all, 1);

        reduceNode.register(Collections.singletonList(unionNode));

        RootNode<Object[]> root = new RootNode<>(ctx, rowType);
        root.register(reduceNode);

        assertEquals("Unexpected result [rowsCnt=" + Arrays.deepToString(rowsCnt) + ", all=" + all + ']',
            expectedResultSize(totalRowsCnt, all), F.size(root));
    }

    /** */
    protected abstract int expectedResultSize(int[] totalRowsCnt, boolean all);

    /** */
    protected abstract AbstractSetOpNode<Object[]> setOpNodeFactory(ExecutionContext<Object[]> ctx, RelDataType rowType,
        AggregateType type, boolean all, int inputsCnt);

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

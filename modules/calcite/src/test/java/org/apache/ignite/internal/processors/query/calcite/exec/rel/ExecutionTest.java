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

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.UUID;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Pair;
import org.apache.ignite.internal.processors.query.calcite.exec.ArrayRowHandler;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.RowHandler;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.agg.WrappersFactoryImpl;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.util.typedef.F;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.apache.ignite.internal.processors.query.calcite.exec.rel.AggregateNode.AggregateType.MAP;
import static org.apache.ignite.internal.processors.query.calcite.exec.rel.AggregateNode.AggregateType.REDUCE;
import static org.apache.ignite.internal.processors.query.calcite.exec.rel.AggregateNode.AggregateType.SINGLE;

/**
 *
 */
public class ExecutionTest extends AbstractExecutionTest {
    /**
     * @throws Exception If failed.
     */
    @Before
    @Override public void setup() throws Exception {
        nodesCount = 1;
        super.setup();
    }

    @Test
    public void testSimpleExecution() throws Exception {
        // SELECT P.ID, P.NAME, PR.NAME AS PROJECT
        // FROM PERSON P
        // INNER JOIN PROJECT PR
        // ON P.ID = PR.RESP_ID
        // WHERE P.ID >= 2

        ExecutionContext ctx = executionContext(F.first(nodes()), UUID.randomUUID(), 0);

        ScanNode persons = new ScanNode(ctx, Arrays.asList(
            new Object[]{0, "Igor", "Seliverstov"},
            new Object[]{1, "Roman", "Kondakov"},
            new Object[]{2, "Ivan", "Pavlukhin"},
            new Object[]{3, "Alexey", "Goncharuk"}
        ));

        ScanNode projects = new ScanNode(ctx, Arrays.asList(
            new Object[]{0, 2, "Calcite"},
            new Object[]{1, 1, "SQL"},
            new Object[]{2, 2, "Ignite"},
            new Object[]{3, 0, "Core"}
        ));

        JoinNode join = new JoinNode(ctx, r -> r[0] == r[4]);
        join.register(F.asList(persons, projects));

        ProjectNode project = new ProjectNode(ctx, r -> new Object[]{r[0], r[1], r[5]});
        project.register(join);

        FilterNode filter = new FilterNode(ctx, r -> (Integer) r[0] >= 2);
        filter.register(project);

        RootNode node = new RootNode(ctx, r -> {});
        node.register(filter);

        assert node.hasNext();

        ArrayList<Object[]> rows = new ArrayList<>();

        while (node.hasNext())
            rows.add(node.next());

        assertEquals(2, rows.size());

        Assert.assertArrayEquals(new Object[]{2, "Ivan", "Calcite"}, rows.get(0));
        Assert.assertArrayEquals(new Object[]{2, "Ivan", "Ignite"}, rows.get(1));
    }

    @Test
    public void testAggregateMapReduceAvg() throws Exception {
        ExecutionContext ctx = executionContext(F.first(nodes()), UUID.randomUUID(), 0);

        RowHandler<Object[]> rowHnd = ArrayRowHandler.INSTANCE;

        ScanNode scan = new ScanNode(ctx, Arrays.asList(
            rowHnd.create("Igor", 200),
            rowHnd.create("Roman", 300),
            rowHnd.create("Ivan", 1400),
            rowHnd.create("Alexey", 1000)
        ));

        IgniteTypeFactory typeFactory = ctx.getTypeFactory();

        RelDataType rowType = typeFactory.createStructType(F.asList(
            Pair.of("NAME", typeFactory.createSqlType(SqlTypeName.VARCHAR)),
            Pair.of("SALARY", typeFactory.createSqlType(SqlTypeName.INTEGER))));

        ImmutableList<ImmutableBitSet> groupSets = ImmutableList.of(ImmutableBitSet.of());

        AggregateCall call = AggregateCall.create(
            SqlStdOperatorTable.AVG,
            false,
            false,
            false,
            ImmutableIntList.of(1),
            -1,
            RelCollations.EMPTY,
            typeFactory.createSqlType(SqlTypeName.DOUBLE),
            null);

        WrappersFactoryImpl factory;

        factory = new WrappersFactoryImpl(ctx, MAP, rowHnd, F.asList(call), rowType);

        AggregateNode<Object[]> map = new AggregateNode<>(ctx, MAP, groupSets, factory, rowHnd);
        map.register(scan);

        factory = new WrappersFactoryImpl(ctx, REDUCE, rowHnd, F.asList(call), rowType);

        AggregateNode<Object[]> reduce = new AggregateNode<>(ctx, REDUCE, groupSets, factory, rowHnd);
        reduce.register(map);

        RootNode root = new RootNode(ctx, c -> {});
        root.register(reduce);

        assertTrue(root.hasNext());
        assertEquals(725d, rowHnd.get(0, root.next()));
        assertFalse(root.hasNext());
    }

    @Test
    public void testAggregateMapReduceSum() throws Exception {
        ExecutionContext ctx = executionContext(F.first(nodes()), UUID.randomUUID(), 0);

        RowHandler<Object[]> rowHnd = ArrayRowHandler.INSTANCE;

        ScanNode scan = new ScanNode(ctx, Arrays.asList(
            rowHnd.create("Igor", 200),
            rowHnd.create("Roman", 300),
            rowHnd.create("Ivan", 1400),
            rowHnd.create("Alexey", 1000)
        ));

        IgniteTypeFactory typeFactory = ctx.getTypeFactory();

        RelDataType rowType = typeFactory.createStructType(F.asList(
            Pair.of("NAME", typeFactory.createSqlType(SqlTypeName.VARCHAR)),
            Pair.of("SALARY", typeFactory.createSqlType(SqlTypeName.INTEGER))));

        // empty groups means SELECT SUM(field) FROM table
        ImmutableList<ImmutableBitSet> groupSets = ImmutableList.of(ImmutableBitSet.of());

        // AVG on second field
        AggregateCall call = AggregateCall.create(
            SqlStdOperatorTable.SUM,
            false,
            false,
            false,
            ImmutableIntList.of(1),
            -1,
            RelCollations.EMPTY,
            typeFactory.createSqlType(SqlTypeName.INTEGER),
            null);

        WrappersFactoryImpl factory;

        factory = new WrappersFactoryImpl(ctx, MAP, rowHnd, F.asList(call), rowType);

        AggregateNode<Object[]> map = new AggregateNode<>(ctx, MAP, groupSets, factory, rowHnd);
        map.register(scan);

        factory = new WrappersFactoryImpl(ctx, REDUCE, rowHnd, F.asList(call), rowType);

        AggregateNode<Object[]> reduce = new AggregateNode<>(ctx, REDUCE, groupSets, factory, rowHnd);
        reduce.register(map);

        RootNode root = new RootNode(ctx, c -> {});
        root.register(reduce);

        assertTrue(root.hasNext());
        assertEquals((Integer)2900, rowHnd.get(0, root.next()));
        assertFalse(root.hasNext());
    }

    @Test
    public void testAggregateMapReduceMin() throws Exception {
        ExecutionContext ctx = executionContext(F.first(nodes()), UUID.randomUUID(), 0);

        RowHandler<Object[]> rowHnd = ArrayRowHandler.INSTANCE;

        ScanNode scan = new ScanNode(ctx, Arrays.asList(
            rowHnd.create("Igor", 200),
            rowHnd.create("Roman", 300),
            rowHnd.create("Ivan", 1400),
            rowHnd.create("Alexey", 1000)
        ));

        IgniteTypeFactory typeFactory = ctx.getTypeFactory();

        RelDataType rowType = typeFactory.createStructType(F.asList(
            Pair.of("NAME", typeFactory.createSqlType(SqlTypeName.VARCHAR)),
            Pair.of("SALARY", typeFactory.createSqlType(SqlTypeName.INTEGER))));

        ImmutableList<ImmutableBitSet> groupSets = ImmutableList.of(ImmutableBitSet.of());

        AggregateCall call = AggregateCall.create(
            SqlStdOperatorTable.MIN,
            false,
            false,
            false,
            ImmutableIntList.of(1),
            -1,
            RelCollations.EMPTY,
            typeFactory.createSqlType(SqlTypeName.INTEGER),
            null);

        WrappersFactoryImpl factory;

        factory = new WrappersFactoryImpl(ctx, MAP, rowHnd, F.asList(call), rowType);

        AggregateNode<Object[]> map = new AggregateNode<>(ctx, MAP, groupSets, factory, rowHnd);
        map.register(scan);

        factory = new WrappersFactoryImpl(ctx, REDUCE, rowHnd, F.asList(call), rowType);

        AggregateNode<Object[]> reduce = new AggregateNode<>(ctx, REDUCE, groupSets, factory, rowHnd);
        reduce.register(map);

        RootNode root = new RootNode(ctx, c -> {});
        root.register(reduce);

        assertTrue(root.hasNext());
        assertEquals((Integer)200, rowHnd.get(0, root.next()));
        assertFalse(root.hasNext());
    }

    @Test
    public void testAggregateMapReduceMax() throws Exception {
        ExecutionContext ctx = executionContext(F.first(nodes()), UUID.randomUUID(), 0);

        RowHandler<Object[]> rowHnd = ArrayRowHandler.INSTANCE;

        ScanNode scan = new ScanNode(ctx, Arrays.asList(
            rowHnd.create("Igor", 200),
            rowHnd.create("Roman", 300),
            rowHnd.create("Ivan", 1400),
            rowHnd.create("Alexey", 1000)
        ));

        IgniteTypeFactory typeFactory = ctx.getTypeFactory();

        RelDataType rowType = typeFactory.createStructType(F.asList(
            Pair.of("NAME", typeFactory.createSqlType(SqlTypeName.VARCHAR)),
            Pair.of("SALARY", typeFactory.createSqlType(SqlTypeName.INTEGER))));

        ImmutableList<ImmutableBitSet> groupSets = ImmutableList.of(ImmutableBitSet.of());

        AggregateCall call = AggregateCall.create(
            SqlStdOperatorTable.MAX,
            false,
            false,
            false,
            ImmutableIntList.of(1),
            -1,
            RelCollations.EMPTY,
            typeFactory.createSqlType(SqlTypeName.INTEGER),
            null);

        WrappersFactoryImpl factory;

        factory = new WrappersFactoryImpl(ctx, MAP, rowHnd, F.asList(call), rowType);

        AggregateNode<Object[]> map = new AggregateNode<>(ctx, MAP, groupSets, factory, rowHnd);
        map.register(scan);

        factory = new WrappersFactoryImpl(ctx, REDUCE, rowHnd, F.asList(call), rowType);

        AggregateNode<Object[]> reduce = new AggregateNode<>(ctx, REDUCE, groupSets, factory, rowHnd);
        reduce.register(map);

        RootNode root = new RootNode(ctx, c -> {});
        root.register(reduce);

        assertTrue(root.hasNext());
        assertEquals((Integer)1400, rowHnd.get(0, root.next()));
        assertFalse(root.hasNext());
    }

    @Test
    public void testAggregateMapReduceCount() throws Exception {
        ExecutionContext ctx = executionContext(F.first(nodes()), UUID.randomUUID(), 0);

        RowHandler<Object[]> rowHnd = ArrayRowHandler.INSTANCE;

        ScanNode scan = new ScanNode(ctx, Arrays.asList(
            rowHnd.create("Igor", 200),
            rowHnd.create("Roman", 300),
            rowHnd.create("Ivan", 1400),
            rowHnd.create("Alexey", 1000)
        ));

        IgniteTypeFactory typeFactory = ctx.getTypeFactory();

        RelDataType rowType = typeFactory.createStructType(F.asList(
            Pair.of("NAME", typeFactory.createSqlType(SqlTypeName.VARCHAR)),
            Pair.of("SALARY", typeFactory.createSqlType(SqlTypeName.INTEGER))));

        ImmutableList<ImmutableBitSet> groupSets = ImmutableList.of(ImmutableBitSet.of());

        AggregateCall call = AggregateCall.create(
            SqlStdOperatorTable.COUNT,
            false,
            false,
            false,
            ImmutableIntList.of(),
            -1,
            RelCollations.EMPTY,
            typeFactory.createSqlType(SqlTypeName.INTEGER),
            null);

        WrappersFactoryImpl factory;

        factory = new WrappersFactoryImpl(ctx, MAP, rowHnd, F.asList(call), rowType);

        AggregateNode<Object[]> map = new AggregateNode<>(ctx, MAP, groupSets, factory, rowHnd);
        map.register(scan);

        factory = new WrappersFactoryImpl(ctx, REDUCE, rowHnd, F.asList(call), rowType);

        AggregateNode<Object[]> reduce = new AggregateNode<>(ctx, REDUCE, groupSets, factory, rowHnd);
        reduce.register(map);

        RootNode root = new RootNode(ctx, c -> {});
        root.register(reduce);

        assertTrue(root.hasNext());
        assertEquals((Integer)4, rowHnd.get(0, root.next()));
        assertFalse(root.hasNext());
    }


    @Test
    public void testAggregateAvg() throws Exception {
        ExecutionContext ctx = executionContext(F.first(nodes()), UUID.randomUUID(), 0);

        RowHandler<Object[]> rowHnd = ArrayRowHandler.INSTANCE;

        ScanNode scan = new ScanNode(ctx, Arrays.asList(
            rowHnd.create("Igor", 200),
            rowHnd.create("Roman", 300),
            rowHnd.create("Ivan", 1400),
            rowHnd.create("Alexey", 1000)
        ));

        IgniteTypeFactory typeFactory = ctx.getTypeFactory();

        RelDataType rowType = typeFactory.createStructType(F.asList(
            Pair.of("NAME", typeFactory.createSqlType(SqlTypeName.VARCHAR)),
            Pair.of("SALARY", typeFactory.createSqlType(SqlTypeName.INTEGER))));

        ImmutableList<ImmutableBitSet> groupSets = ImmutableList.of(ImmutableBitSet.of());

        AggregateCall call = AggregateCall.create(
            SqlStdOperatorTable.AVG,
            false,
            false,
            false,
            ImmutableIntList.of(1),
            -1,
            RelCollations.EMPTY,
            typeFactory.createSqlType(SqlTypeName.DOUBLE),
            null);

        WrappersFactoryImpl factory = new WrappersFactoryImpl(ctx, SINGLE, rowHnd, F.asList(call), rowType);

        AggregateNode<Object[]> agg = new AggregateNode<>(ctx, SINGLE, groupSets, factory, rowHnd);
        agg.register(scan);

        RootNode root = new RootNode(ctx, c -> {});
        root.register(agg);

        assertTrue(root.hasNext());
        assertEquals(725d, rowHnd.get(0, root.next()));
        assertFalse(root.hasNext());
    }

    @Test
    public void testAggregateSum() throws Exception {
        ExecutionContext ctx = executionContext(F.first(nodes()), UUID.randomUUID(), 0);

        RowHandler<Object[]> rowHnd = ArrayRowHandler.INSTANCE;

        ScanNode scan = new ScanNode(ctx, Arrays.asList(
            rowHnd.create("Igor", 200),
            rowHnd.create("Roman", 300),
            rowHnd.create("Ivan", 1400),
            rowHnd.create("Alexey", 1000)
        ));

        IgniteTypeFactory typeFactory = ctx.getTypeFactory();

        RelDataType rowType = typeFactory.createStructType(F.asList(
            Pair.of("NAME", typeFactory.createSqlType(SqlTypeName.VARCHAR)),
            Pair.of("SALARY", typeFactory.createSqlType(SqlTypeName.INTEGER))));

        // empty groups means SELECT SUM(field) FROM table
        ImmutableList<ImmutableBitSet> groupSets = ImmutableList.of(ImmutableBitSet.of());

        // AVG on second field
        AggregateCall call = AggregateCall.create(
            SqlStdOperatorTable.SUM,
            false,
            false,
            false,
            ImmutableIntList.of(1),
            -1,
            RelCollations.EMPTY,
            typeFactory.createSqlType(SqlTypeName.INTEGER),
            null);

        WrappersFactoryImpl factory = new WrappersFactoryImpl(ctx, SINGLE, rowHnd, F.asList(call), rowType);

        AggregateNode<Object[]> agg = new AggregateNode<>(ctx, SINGLE, groupSets, factory, rowHnd);
        agg.register(scan);

        RootNode root = new RootNode(ctx, c -> {});
        root.register(agg);

        assertTrue(root.hasNext());
        assertEquals((Integer)2900, rowHnd.get(0, root.next()));
        assertFalse(root.hasNext());
    }

    @Test
    public void testAggregateMin() throws Exception {
        ExecutionContext ctx = executionContext(F.first(nodes()), UUID.randomUUID(), 0);

        RowHandler<Object[]> rowHnd = ArrayRowHandler.INSTANCE;

        ScanNode scan = new ScanNode(ctx, Arrays.asList(
            rowHnd.create("Igor", 200),
            rowHnd.create("Roman", 300),
            rowHnd.create("Ivan", 1400),
            rowHnd.create("Alexey", 1000)
        ));

        IgniteTypeFactory typeFactory = ctx.getTypeFactory();

        RelDataType rowType = typeFactory.createStructType(F.asList(
            Pair.of("NAME", typeFactory.createSqlType(SqlTypeName.VARCHAR)),
            Pair.of("SALARY", typeFactory.createSqlType(SqlTypeName.INTEGER))));

        ImmutableList<ImmutableBitSet> groupSets = ImmutableList.of(ImmutableBitSet.of());

        AggregateCall call = AggregateCall.create(
            SqlStdOperatorTable.MIN,
            false,
            false,
            false,
            ImmutableIntList.of(1),
            -1,
            RelCollations.EMPTY,
            typeFactory.createSqlType(SqlTypeName.INTEGER),
            null);

        WrappersFactoryImpl factory = new WrappersFactoryImpl(ctx, SINGLE, rowHnd, F.asList(call), rowType);

        AggregateNode<Object[]> agg = new AggregateNode<>(ctx, SINGLE, groupSets, factory, rowHnd);
        agg.register(scan);

        RootNode root = new RootNode(ctx, c -> {});
        root.register(agg);

        assertTrue(root.hasNext());
        assertEquals((Integer)200, rowHnd.get(0, root.next()));
        assertFalse(root.hasNext());
    }

    @Test
    public void testAggregateMax() throws Exception {
        ExecutionContext ctx = executionContext(F.first(nodes()), UUID.randomUUID(), 0);

        RowHandler<Object[]> rowHnd = ArrayRowHandler.INSTANCE;

        ScanNode scan = new ScanNode(ctx, Arrays.asList(
            rowHnd.create("Igor", 200),
            rowHnd.create("Roman", 300),
            rowHnd.create("Ivan", 1400),
            rowHnd.create("Alexey", 1000)
        ));

        IgniteTypeFactory typeFactory = ctx.getTypeFactory();

        RelDataType rowType = typeFactory.createStructType(F.asList(
            Pair.of("NAME", typeFactory.createSqlType(SqlTypeName.VARCHAR)),
            Pair.of("SALARY", typeFactory.createSqlType(SqlTypeName.INTEGER))));

        ImmutableList<ImmutableBitSet> groupSets = ImmutableList.of(ImmutableBitSet.of());

        AggregateCall call = AggregateCall.create(
            SqlStdOperatorTable.MAX,
            false,
            false,
            false,
            ImmutableIntList.of(1),
            -1,
            RelCollations.EMPTY,
            typeFactory.createSqlType(SqlTypeName.INTEGER),
            null);

        WrappersFactoryImpl factory = new WrappersFactoryImpl(ctx, SINGLE, rowHnd, F.asList(call), rowType);

        AggregateNode<Object[]> agg = new AggregateNode<>(ctx, SINGLE, groupSets, factory, rowHnd);
        agg.register(scan);

        RootNode root = new RootNode(ctx, c -> {});
        root.register(agg);

        assertTrue(root.hasNext());
        assertEquals((Integer)1400, rowHnd.get(0, root.next()));
        assertFalse(root.hasNext());
    }

    @Test
    public void testAggregateCount() throws Exception {
        ExecutionContext ctx = executionContext(F.first(nodes()), UUID.randomUUID(), 0);

        RowHandler<Object[]> rowHnd = ArrayRowHandler.INSTANCE;

        ScanNode scan = new ScanNode(ctx, Arrays.asList(
            rowHnd.create("Igor", 200),
            rowHnd.create("Roman", 300),
            rowHnd.create("Ivan", 1400),
            rowHnd.create("Alexey", 1000)
        ));

        IgniteTypeFactory typeFactory = ctx.getTypeFactory();

        RelDataType rowType = typeFactory.createStructType(F.asList(
            Pair.of("NAME", typeFactory.createSqlType(SqlTypeName.VARCHAR)),
            Pair.of("SALARY", typeFactory.createSqlType(SqlTypeName.INTEGER))));

        ImmutableList<ImmutableBitSet> groupSets = ImmutableList.of(ImmutableBitSet.of());

        AggregateCall call = AggregateCall.create(
            SqlStdOperatorTable.COUNT,
            false,
            false,
            false,
            ImmutableIntList.of(),
            -1,
            RelCollations.EMPTY,
            typeFactory.createSqlType(SqlTypeName.INTEGER),
            null);

        WrappersFactoryImpl factory = new WrappersFactoryImpl(ctx, SINGLE, rowHnd, F.asList(call), rowType);

        AggregateNode<Object[]> agg = new AggregateNode<>(ctx, SINGLE, groupSets, factory, rowHnd);
        agg.register(scan);

        RootNode root = new RootNode(ctx, c -> {});
        root.register(agg);

        assertTrue(root.hasNext());
        assertEquals((Integer)4, rowHnd.get(0, root.next()));
        assertFalse(root.hasNext());
    }

    @Test
    public void testAggregateCountByGroup() throws Exception {
        ExecutionContext ctx = executionContext(F.first(nodes()), UUID.randomUUID(), 0);

        RowHandler<Object[]> rowHnd = ArrayRowHandler.INSTANCE;

        ScanNode scan = new ScanNode(ctx, Arrays.asList(
            rowHnd.create("Igor", 0, 200),
            rowHnd.create("Roman", 1, 300),
            rowHnd.create("Ivan", 1, 1400),
            rowHnd.create("Alexey", 0, 1000)
        ));

        IgniteTypeFactory typeFactory = ctx.getTypeFactory();

        RelDataType rowType = typeFactory.createStructType(F.asList(
            Pair.of("NAME", typeFactory.createSqlType(SqlTypeName.VARCHAR)),
            Pair.of("PROJECT_ID", typeFactory.createSqlType(SqlTypeName.INTEGER)),
            Pair.of("SALARY", typeFactory.createSqlType(SqlTypeName.INTEGER))));

        ImmutableList<ImmutableBitSet> groupSets = ImmutableList.of(ImmutableBitSet.of(1));

        AggregateCall call = AggregateCall.create(
            SqlStdOperatorTable.COUNT,
            false,
            false,
            false,
            ImmutableIntList.of(),
            -1,
            RelCollations.EMPTY,
            typeFactory.createSqlType(SqlTypeName.INTEGER),
            null);

        WrappersFactoryImpl factory = new WrappersFactoryImpl(ctx, SINGLE, rowHnd, F.asList(call), rowType);

        AggregateNode<Object[]> agg = new AggregateNode<>(ctx, SINGLE, groupSets, factory, rowHnd);
        agg.register(scan);

        RootNode root = new RootNode(ctx, c -> {});
        root.register(agg);

        assertTrue(root.hasNext());
        // TODO needs a sort, relying on an order in a hash table looks strange
        Assert.assertArrayEquals(rowHnd.create(1, 2), root.next());
        Assert.assertArrayEquals(rowHnd.create(0, 2), root.next());
        assertFalse(root.hasNext());
    }
}

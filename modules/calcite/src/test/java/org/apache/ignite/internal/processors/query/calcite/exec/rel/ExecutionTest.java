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
import java.util.List;
import java.util.UUID;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Pair;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.RowHandler.RowFactory;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.agg.WrappersFactoryImpl;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.util.typedef.F;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.apache.ignite.internal.processors.query.calcite.exec.rel.AggregateNode.AggregateType.MAP;
import static org.apache.ignite.internal.processors.query.calcite.exec.rel.AggregateNode.AggregateType.REDUCE;
import static org.apache.ignite.internal.processors.query.calcite.exec.rel.AggregateNode.AggregateType.SINGLE;

/**
 *
 */
@SuppressWarnings("TypeMayBeWeakened") public class ExecutionTest extends AbstractExecutionTest {
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
    public void testSimpleExecution() {
        // SELECT P.ID, P.NAME, PR.NAME AS PROJECT
        // FROM PERSON P
        // INNER JOIN PROJECT PR
        // ON P.ID = PR.RESP_ID
        // WHERE P.ID >= 2

        ExecutionContext<Object[]> ctx = executionContext(F.first(nodes()), UUID.randomUUID(), 0);

        ScanNode<Object[]> persons = new ScanNode<>(ctx, Arrays.asList(
            new Object[]{0, "Igor", "Seliverstov"},
            new Object[]{1, "Roman", "Kondakov"},
            new Object[]{2, "Ivan", "Pavlukhin"},
            new Object[]{3, "Alexey", "Goncharuk"}
        ));

        ScanNode<Object[]> projects = new ScanNode<>(ctx, Arrays.asList(
            new Object[]{0, 2, "Calcite"},
            new Object[]{1, 1, "SQL"},
            new Object[]{2, 2, "Ignite"},
            new Object[]{3, 0, "Core"}
        ));

        JoinNode<Object[]> join = new JoinNode<>(ctx, r -> r[0] == r[4]);
        join.register(F.asList(persons, projects));

        ProjectNode<Object[]> project = new ProjectNode<>(ctx, r -> new Object[]{r[0], r[1], r[5]});
        project.register(join);

        FilterNode<Object[]> filter = new FilterNode<>(ctx, r -> (Integer) r[0] >= 2);
        filter.register(project);

        RootNode<Object[]> node = new RootNode<>(ctx, r -> {});
        node.register(filter);

        assert node.hasNext();

        ArrayList<Object[]> rows = new ArrayList<>();

        while (node.hasNext())
            rows.add(node.next());

        assertEquals(2, rows.size());

        Assert.assertArrayEquals(new Object[]{2, "Ivan", "Calcite"}, rows.get(0));
        Assert.assertArrayEquals(new Object[]{2, "Ivan", "Ignite"}, rows.get(1));
    }

    /** */
    @Test
    public void testUnionAll() {
        ExecutionContext<Object[]> ctx = executionContext(F.first(nodes()), UUID.randomUUID(), 0);
        ScanNode<Object[]> scan1 = new ScanNode<>(ctx, Arrays.asList(
            row("Igor", 200),
            row("Roman", 300),
            row("Ivan", 1400),
            row("Alexey", 1000)
        ));

        ScanNode<Object[]> scan2 = new ScanNode<>(ctx, Arrays.asList(
            row("Igor", 200),
            row("Roman", 300),
            row("Ivan", 1400),
            row("Alexey", 1000)
        ));

        ScanNode<Object[]> scan3 = new ScanNode<>(ctx, Arrays.asList(
            row("Igor", 200),
            row("Roman", 300),
            row("Ivan", 1400),
            row("Alexey", 1000)
        ));

        UnionAllNode<Object[]> union = new UnionAllNode<>(ctx);
        union.register(F.asList(scan1, scan2, scan3));

        RootNode<Object[]> root = new RootNode<>(ctx, c -> {});
        root.register(union);

        assertTrue(root.hasNext());

        List<Object[]> res = new ArrayList<>();

        while (root.hasNext())
            res.add(root.next());

        assertEquals(12, res.size());
    }

    /** */
    @Test
    public void testAggregateMapReduceAvg() {
        ExecutionContext<Object[]> ctx = executionContext(F.first(nodes()), UUID.randomUUID(), 0);

        ScanNode<Object[]> scan = new ScanNode<>(ctx, Arrays.asList(
            row("Igor", 200),
            row("Roman", 300),
            row("Ivan", 1400),
            row("Alexey", 1000)
        ));

        IgniteTypeFactory typeFactory = ctx.getTypeFactory();

        RelDataType rowType = typeFactory.createStructType(F.asList(
            Pair.of("NAME", typeFactory.createSqlType(SqlTypeName.VARCHAR)),
            Pair.of("SALARY", typeFactory.createSqlType(SqlTypeName.INTEGER))));

        ImmutableList<ImmutableBitSet> grpSets = ImmutableList.of(ImmutableBitSet.of());

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

        AggregateNode<Object[]> map = new AggregateNode<>(ctx, MAP, grpSets, factory(ctx, call, MAP, rowType), rowFactory());
        map.register(scan);

        AggregateNode<Object[]> reduce = new AggregateNode<>(ctx, REDUCE, grpSets, factory(ctx, call, REDUCE, rowType), rowFactory());
        reduce.register(map);

        RootNode<Object[]> root = new RootNode<>(ctx, c -> {});
        root.register(reduce);

        assertTrue(root.hasNext());
        assertEquals(725d, root.next()[0]);
        assertFalse(root.hasNext());
    }

    /** */
    @Test
    public void testAggregateMapReduceSum() {
        ExecutionContext<Object[]> ctx = executionContext(F.first(nodes()), UUID.randomUUID(), 0);
        ScanNode<Object[]> scan = new ScanNode<>(ctx, Arrays.asList(
            row("Igor", 200),
            row("Roman", 300),
            row("Ivan", 1400),
            row("Alexey", 1000)
        ));

        IgniteTypeFactory typeFactory = ctx.getTypeFactory();

        RelDataType rowType = typeFactory.createStructType(F.asList(
            Pair.of("NAME", typeFactory.createSqlType(SqlTypeName.VARCHAR)),
            Pair.of("SALARY", typeFactory.createSqlType(SqlTypeName.INTEGER))));

        // empty groups means SELECT SUM(field) FROM table
        ImmutableList<ImmutableBitSet> grpSets = ImmutableList.of(ImmutableBitSet.of());

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

        AggregateNode<Object[]> map = new AggregateNode<>(ctx, MAP, grpSets, factory(ctx, call, MAP, rowType), rowFactory());
        map.register(scan);

        AggregateNode<Object[]> reduce = new AggregateNode<>(ctx, REDUCE, grpSets, factory(ctx, call, REDUCE, rowType), rowFactory());
        reduce.register(map);

        RootNode<Object[]> root = new RootNode<>(ctx, c -> {});
        root.register(reduce);

        assertTrue(root.hasNext());
        assertEquals(2900, root.next()[0]);
        assertFalse(root.hasNext());
    }

    /** */
    @Test
    public void testAggregateMapReduceMin() {
        ExecutionContext<Object[]> ctx = executionContext(F.first(nodes()), UUID.randomUUID(), 0);

        ScanNode<Object[]>scan = new ScanNode<>(ctx, Arrays.asList(
            row("Igor", 200),
            row("Roman", 300),
            row("Ivan", 1400),
            row("Alexey", 1000)
        ));

        IgniteTypeFactory typeFactory = ctx.getTypeFactory();

        RelDataType rowType = typeFactory.createStructType(F.asList(
            Pair.of("NAME", typeFactory.createSqlType(SqlTypeName.VARCHAR)),
            Pair.of("SALARY", typeFactory.createSqlType(SqlTypeName.INTEGER))));

        ImmutableList<ImmutableBitSet> grpSets = ImmutableList.of(ImmutableBitSet.of());

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

        AggregateNode<Object[]> map = new AggregateNode<>(ctx, MAP, grpSets, factory(ctx, call, MAP, rowType), rowFactory());
        map.register(scan);

        AggregateNode<Object[]> reduce = new AggregateNode<>(ctx, REDUCE, grpSets, factory(ctx, call, REDUCE, rowType), rowFactory());
        reduce.register(map);

        RootNode<Object[]>root = new RootNode<>(ctx, c -> {});
        root.register(reduce);

        assertTrue(root.hasNext());
        assertEquals(200, root.next()[0]);
        assertFalse(root.hasNext());
    }

    /** */
    @NotNull private WrappersFactoryImpl<Object[]> factory(ExecutionContext<Object[]> ctx, AggregateCall call,
        AggregateNode.AggregateType type, RelDataType rowType) {
        return new WrappersFactoryImpl<>(ctx, type, F.asList(call), rowType);
    }

    /** */
    @Test
    public void testAggregateMapReduceMax() {
        ExecutionContext<Object[]> ctx = executionContext(F.first(nodes()), UUID.randomUUID(), 0);

        ScanNode<Object[]>scan = new ScanNode<>(ctx, Arrays.asList(
            row("Igor", 200),
            row("Roman", 300),
            row("Ivan", 1400),
            row("Alexey", 1000)
        ));

        IgniteTypeFactory typeFactory = ctx.getTypeFactory();

        RelDataType rowType = typeFactory.createStructType(F.asList(
            Pair.of("NAME", typeFactory.createSqlType(SqlTypeName.VARCHAR)),
            Pair.of("SALARY", typeFactory.createSqlType(SqlTypeName.INTEGER))));

        ImmutableList<ImmutableBitSet> grpSets = ImmutableList.of(ImmutableBitSet.of());

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

        AggregateNode<Object[]> map = new AggregateNode<>(ctx, MAP, grpSets, factory(ctx, call, MAP, rowType), rowFactory());
        map.register(scan);

        AggregateNode<Object[]> reduce = new AggregateNode<>(ctx, REDUCE, grpSets, factory(ctx, call, REDUCE, rowType), rowFactory());
        reduce.register(map);

        RootNode<Object[]>root = new RootNode<>(ctx, c -> {});
        root.register(reduce);

        assertTrue(root.hasNext());
        assertEquals(1400, root.next()[0]);
        assertFalse(root.hasNext());
    }

    /** */
    @Test
    public void testAggregateMapReduceCount() {
        ExecutionContext<Object[]> ctx = executionContext(F.first(nodes()), UUID.randomUUID(), 0);

        ScanNode<Object[]>scan = new ScanNode<>(ctx, Arrays.asList(
            row("Igor", 200),
            row("Roman", 300),
            row("Ivan", 1400),
            row("Alexey", 1000)
        ));

        IgniteTypeFactory typeFactory = ctx.getTypeFactory();

        RelDataType rowType = typeFactory.createStructType(F.asList(
            Pair.of("NAME", typeFactory.createSqlType(SqlTypeName.VARCHAR)),
            Pair.of("SALARY", typeFactory.createSqlType(SqlTypeName.INTEGER))));

        ImmutableList<ImmutableBitSet> grpSets = ImmutableList.of(ImmutableBitSet.of());

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

        AggregateNode<Object[]> map = new AggregateNode<>(ctx, MAP, grpSets, factory(ctx, call, MAP, rowType), rowFactory());
        map.register(scan);

        AggregateNode<Object[]> reduce = new AggregateNode<>(ctx, REDUCE, grpSets, factory(ctx, call, REDUCE, rowType), rowFactory());
        reduce.register(map);

        RootNode<Object[]>root = new RootNode<>(ctx, c -> {});
        root.register(reduce);

        assertTrue(root.hasNext());
        assertEquals(4, root.next()[0]);
        assertFalse(root.hasNext());
    }

    /** */
    @Test
    public void testAggregateAvg() {
        ExecutionContext<Object[]> ctx = executionContext(F.first(nodes()), UUID.randomUUID(), 0);

        ScanNode<Object[]>scan = new ScanNode<>(ctx, Arrays.asList(
            row("Igor", 200),
            row("Roman", 300),
            row("Ivan", 1400),
            row("Alexey", 1000)
        ));

        IgniteTypeFactory typeFactory = ctx.getTypeFactory();

        RelDataType rowType = typeFactory.createStructType(F.asList(
            Pair.of("NAME", typeFactory.createSqlType(SqlTypeName.VARCHAR)),
            Pair.of("SALARY", typeFactory.createSqlType(SqlTypeName.INTEGER))));

        ImmutableList<ImmutableBitSet> grpSets = ImmutableList.of(ImmutableBitSet.of());

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

        AggregateNode<Object[]> agg = new AggregateNode<>(ctx, SINGLE, grpSets, factory(ctx, call, SINGLE, rowType), rowFactory());
        agg.register(scan);

        RootNode<Object[]>root = new RootNode<>(ctx, c -> {});
        root.register(agg);

        assertTrue(root.hasNext());
        assertEquals(725d, root.next()[0]);
        assertFalse(root.hasNext());
    }

    /** */
    @Test
    public void testAggregateSum() {
        ExecutionContext<Object[]> ctx = executionContext(F.first(nodes()), UUID.randomUUID(), 0);

        ScanNode<Object[]>scan = new ScanNode<>(ctx, Arrays.asList(
            row("Igor", 200),
            row("Roman", 300),
            row("Ivan", 1400),
            row("Alexey", 1000)
        ));

        IgniteTypeFactory typeFactory = ctx.getTypeFactory();

        RelDataType rowType = typeFactory.createStructType(F.asList(
            Pair.of("NAME", typeFactory.createSqlType(SqlTypeName.VARCHAR)),
            Pair.of("SALARY", typeFactory.createSqlType(SqlTypeName.INTEGER))));

        // empty groups means SELECT SUM(field) FROM table
        ImmutableList<ImmutableBitSet> grpSets = ImmutableList.of(ImmutableBitSet.of());

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

        AggregateNode<Object[]> agg = new AggregateNode<>(ctx, SINGLE, grpSets, factory(ctx, call, SINGLE, rowType), rowFactory());
        agg.register(scan);

        RootNode<Object[]>root = new RootNode<>(ctx, c -> {});
        root.register(agg);

        assertTrue(root.hasNext());
        assertEquals(2900, root.next()[0]);
        assertFalse(root.hasNext());
    }

    /** */
    @Test
    public void testAggregateMin() {
        ExecutionContext<Object[]> ctx = executionContext(F.first(nodes()), UUID.randomUUID(), 0);

        ScanNode<Object[]>scan = new ScanNode<>(ctx, Arrays.asList(
            row("Igor", 200),
            row("Roman", 300),
            row("Ivan", 1400),
            row("Alexey", 1000)
        ));

        IgniteTypeFactory typeFactory = ctx.getTypeFactory();

        RelDataType rowType = typeFactory.createStructType(F.asList(
            Pair.of("NAME", typeFactory.createSqlType(SqlTypeName.VARCHAR)),
            Pair.of("SALARY", typeFactory.createSqlType(SqlTypeName.INTEGER))));

        ImmutableList<ImmutableBitSet> grpSets = ImmutableList.of(ImmutableBitSet.of());

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

        AggregateNode<Object[]> agg = new AggregateNode<>(ctx, SINGLE, grpSets, factory(ctx, call, SINGLE, rowType), rowFactory());
        agg.register(scan);

        RootNode<Object[]>root = new RootNode<>(ctx, c -> {});
        root.register(agg);

        assertTrue(root.hasNext());
        assertEquals(200, root.next()[0]);
        assertFalse(root.hasNext());
    }

    /** */
    @Test
    public void testAggregateMax() {
        ExecutionContext<Object[]> ctx = executionContext(F.first(nodes()), UUID.randomUUID(), 0);

        ScanNode<Object[]>scan = new ScanNode<>(ctx, Arrays.asList(
            row("Igor", 200),
            row("Roman", 300),
            row("Ivan", 1400),
            row("Alexey", 1000)
        ));

        IgniteTypeFactory typeFactory = ctx.getTypeFactory();

        RelDataType rowType = typeFactory.createStructType(F.asList(
            Pair.of("NAME", typeFactory.createSqlType(SqlTypeName.VARCHAR)),
            Pair.of("SALARY", typeFactory.createSqlType(SqlTypeName.INTEGER))));

        ImmutableList<ImmutableBitSet> grpSets = ImmutableList.of(ImmutableBitSet.of());

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

        AggregateNode<Object[]> agg = new AggregateNode<>(ctx, SINGLE, grpSets, factory(ctx, call, SINGLE, rowType), rowFactory());
        agg.register(scan);

        RootNode<Object[]>root = new RootNode<>(ctx, c -> {});
        root.register(agg);

        assertTrue(root.hasNext());
        assertEquals(1400, root.next()[0]);
        assertFalse(root.hasNext());
    }

    /** */
    @Test
    public void testAggregateCount() {
        ExecutionContext<Object[]> ctx = executionContext(F.first(nodes()), UUID.randomUUID(), 0);

        ScanNode<Object[]>scan = new ScanNode<>(ctx, Arrays.asList(
            row("Igor", 200),
            row("Roman", 300),
            row("Ivan", 1400),
            row("Alexey", 1000)
        ));

        IgniteTypeFactory typeFactory = ctx.getTypeFactory();

        RelDataType rowType = typeFactory.createStructType(F.asList(
            Pair.of("NAME", typeFactory.createSqlType(SqlTypeName.VARCHAR)),
            Pair.of("SALARY", typeFactory.createSqlType(SqlTypeName.INTEGER))));

        ImmutableList<ImmutableBitSet> grpSets = ImmutableList.of(ImmutableBitSet.of());

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

        AggregateNode<Object[]> agg = new AggregateNode<>(ctx, SINGLE, grpSets, factory(ctx, call, SINGLE, rowType), rowFactory());
        agg.register(scan);

        RootNode<Object[]>root = new RootNode<>(ctx, c -> {});
        root.register(agg);

        assertTrue(root.hasNext());
        assertEquals(4, root.next()[0]);
        assertFalse(root.hasNext());
    }

    /** */
    @Test
    public void testAggregateCountByGroup() {
        ExecutionContext<Object[]> ctx = executionContext(F.first(nodes()), UUID.randomUUID(), 0);

        ScanNode<Object[]>scan = new ScanNode<>(ctx, Arrays.asList(
            row("Igor", 0, 200),
            row("Roman", 1, 300),
            row("Ivan", 1, 1400),
            row("Alexey", 0, 1000)
        ));

        IgniteTypeFactory typeFactory = ctx.getTypeFactory();

        RelDataType rowType = typeFactory.createStructType(F.asList(
            Pair.of("NAME", typeFactory.createSqlType(SqlTypeName.VARCHAR)),
            Pair.of("PROJECT_ID", typeFactory.createSqlType(SqlTypeName.INTEGER)),
            Pair.of("SALARY", typeFactory.createSqlType(SqlTypeName.INTEGER))));

        ImmutableList<ImmutableBitSet> grpSets = ImmutableList.of(ImmutableBitSet.of(1));

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

        AggregateNode<Object[]> agg = new AggregateNode<>(ctx, SINGLE, grpSets, factory(ctx, call, SINGLE, rowType), rowFactory());
        agg.register(scan);

        RootNode<Object[]>root = new RootNode<>(ctx, c -> {});
        root.register(agg);

        assertTrue(root.hasNext());
        // TODO needs a sort, relying on an order in a hash table looks strange
        Assert.assertArrayEquals(row(1, 2), root.next());
        Assert.assertArrayEquals(row(0, 2), root.next());
        assertFalse(root.hasNext());
    }

    /** */
    private Object[] row(Object... fields) {
        return fields;
    }

    /** */
    private RowFactory<Object[]> rowFactory() {
        return new RowFactory<Object[]>() {
            @Override public Object[] create() {
                throw new AssertionError();
            }

            @Override public Object[] create(Object... fields) {
                return fields;
            }
        };
    } 
}

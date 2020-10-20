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
import org.apache.ignite.internal.processors.query.calcite.exec.RowHandler.RowFactory;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.agg.AccumulatorWrapper;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteMapAggregate;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.util.TypeUtils;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.apache.calcite.rel.core.JoinRelType.ANTI;
import static org.apache.calcite.rel.core.JoinRelType.FULL;
import static org.apache.calcite.rel.core.JoinRelType.INNER;
import static org.apache.calcite.rel.core.JoinRelType.LEFT;
import static org.apache.calcite.rel.core.JoinRelType.RIGHT;
import static org.apache.calcite.rel.core.JoinRelType.SEMI;
import static org.apache.ignite.internal.processors.query.calcite.exec.rel.AggregateNode.AggregateType.MAP;
import static org.apache.ignite.internal.processors.query.calcite.exec.rel.AggregateNode.AggregateType.REDUCE;
import static org.apache.ignite.internal.processors.query.calcite.exec.rel.AggregateNode.AggregateType.SINGLE;

/**
 *
 */
@SuppressWarnings("TypeMayBeWeakened")
@WithSystemProperty(key = "calcite.debug", value = "true")
public class ExecutionTest extends AbstractExecutionTest {
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
        IgniteTypeFactory tf = ctx.getTypeFactory();
        RelDataType rowType = TypeUtils.createRowType(tf, int.class, String.class, String.class);

        ScanNode<Object[]> persons = new ScanNode<>(ctx, rowType, Arrays.asList(
            new Object[]{0, "Igor", "Seliverstov"},
            new Object[]{1, "Roman", "Kondakov"},
            new Object[]{2, "Ivan", "Pavlukhin"},
            new Object[]{3, "Alexey", "Goncharuk"}
        ));

        rowType = TypeUtils.createRowType(tf, int.class, int.class, String.class);
        ScanNode<Object[]> projects = new ScanNode<>(ctx, rowType, Arrays.asList(
            new Object[]{0, 2, "Calcite"},
            new Object[]{1, 1, "SQL"},
            new Object[]{2, 2, "Ignite"},
            new Object[]{3, 0, "Core"}
        ));

        RelDataType outType = TypeUtils.createRowType(tf, int.class, String.class, String.class, int.class, int.class, String.class);
        RelDataType leftType = TypeUtils.createRowType(tf, int.class, String.class, String.class);
        RelDataType rightType = TypeUtils.createRowType(tf, int.class, int.class, String.class);

        NestedLoopJoinNode<Object[]> join = NestedLoopJoinNode.create(ctx, outType, leftType, rightType, INNER, r -> r[0] == r[4]);
        join.register(F.asList(persons, projects));

        rowType = TypeUtils.createRowType(tf, int.class, String.class, String.class);
        ProjectNode<Object[]> project = new ProjectNode<>(ctx, rowType, r -> new Object[]{r[0], r[1], r[5]});
        project.register(join);

        FilterNode<Object[]> filter = new FilterNode<>(ctx, rowType, r -> (Integer) r[0] >= 2);
        filter.register(project);

        RootNode<Object[]> node = new RootNode<>(ctx, rowType);
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
        IgniteTypeFactory tf = ctx.getTypeFactory();
        RelDataType rowType = TypeUtils.createRowType(tf, String.class, int.class);

        ScanNode<Object[]> scan1 = new ScanNode<>(ctx, rowType, Arrays.asList(
            row("Igor", 200),
            row("Roman", 300),
            row("Ivan", 1400),
            row("Alexey", 1000)
        ));

        ScanNode<Object[]> scan2 = new ScanNode<>(ctx, rowType, Arrays.asList(
            row("Igor", 200),
            row("Roman", 300),
            row("Ivan", 1400),
            row("Alexey", 1000)
        ));

        ScanNode<Object[]> scan3 = new ScanNode<>(ctx, rowType, Arrays.asList(
            row("Igor", 200),
            row("Roman", 300),
            row("Ivan", 1400),
            row("Alexey", 1000)
        ));

        UnionAllNode<Object[]> union = new UnionAllNode<>(ctx, rowType);
        union.register(F.asList(scan1, scan2, scan3));

        RootNode<Object[]> root = new RootNode<>(ctx, rowType);
        root.register(union);

        assertTrue(root.hasNext());

        List<Object[]> res = new ArrayList<>();

        while (root.hasNext())
            res.add(root.next());

        assertEquals(12, res.size());
    }

    /** */
    @Test
    public void testLeftJoin() {
        //    select e.id, e.name, d.name as dep_name
        //      from emp e
        // left join dep d
        //        on e.depno = d.depno

        ExecutionContext<Object[]> ctx = executionContext(F.first(nodes()), UUID.randomUUID(), 0);

        IgniteTypeFactory tf = ctx.getTypeFactory();
        RelDataType rowType = TypeUtils.createRowType(tf, int.class, String.class, Integer.class);

        ScanNode<Object[]> persons = new ScanNode<>(ctx, rowType, Arrays.asList(
            new Object[]{0, "Igor", 1},
            new Object[]{1, "Roman", 2},
            new Object[]{2, "Ivan", null},
            new Object[]{3, "Alexey", 1}
        ));

        rowType = TypeUtils.createRowType(tf, int.class, String.class);
        ScanNode<Object[]> deps = new ScanNode<>(ctx, rowType, Arrays.asList(
            new Object[]{1, "Core"},
            new Object[]{2, "SQL"}
        ));

        RelDataType outType = TypeUtils.createRowType(ctx.getTypeFactory(), int.class, String.class, Integer.class, int.class, String.class);
        RelDataType leftType = TypeUtils.createRowType(ctx.getTypeFactory(), int.class, String.class, Integer.class);
        RelDataType rightType = TypeUtils.createRowType(ctx.getTypeFactory(), int.class, String.class);

        NestedLoopJoinNode<Object[]> join = NestedLoopJoinNode.create(ctx, outType, leftType, rightType, LEFT, r -> r[2] == r[3]);
        join.register(F.asList(persons, deps));

        rowType = TypeUtils.createRowType(tf, int.class, String.class, String.class);
        ProjectNode<Object[]> project = new ProjectNode<>(ctx, rowType, r -> new Object[]{r[0], r[1], r[4]});
        project.register(join);

        RootNode<Object[]> node = new RootNode<>(ctx, rowType);
        node.register(project);

        assert node.hasNext();

        ArrayList<Object[]> rows = new ArrayList<>();

        while (node.hasNext())
            rows.add(node.next());

        assertEquals(4, rows.size());

        Assert.assertArrayEquals(new Object[]{0, "Igor", "Core"}, rows.get(0));
        Assert.assertArrayEquals(new Object[]{1, "Roman", "SQL"}, rows.get(1));
        Assert.assertArrayEquals(new Object[]{2, "Ivan", null}, rows.get(2));
        Assert.assertArrayEquals(new Object[]{3, "Alexey", "Core"}, rows.get(3));
    }

    /** */
    @Test
    public void testRightJoin() {
        //     select e.id, e.name, d.name as dep_name
        //       from dep d
        // right join emp e
        //         on e.depno = d.depno

        ExecutionContext<Object[]> ctx = executionContext(F.first(nodes()), UUID.randomUUID(), 0);
        IgniteTypeFactory tf = ctx.getTypeFactory();
        RelDataType rowType = TypeUtils.createRowType(tf, int.class, String.class, Integer.class);

        ScanNode<Object[]> persons = new ScanNode<>(ctx, rowType, Arrays.asList(
            new Object[]{0, "Igor", 1},
            new Object[]{1, "Roman", 2},
            new Object[]{2, "Ivan", null},
            new Object[]{3, "Alexey", 1}
        ));

        rowType = TypeUtils.createRowType(tf, int.class, String.class);
        ScanNode<Object[]> deps = new ScanNode<>(ctx, rowType, Arrays.asList(
            new Object[]{1, "Core"},
            new Object[]{2, "SQL"},
            new Object[]{3, "QA"}
        ));

        RelDataType outType = TypeUtils.createRowType(ctx.getTypeFactory(), int.class, String.class, int.class, String.class, Integer.class);
        RelDataType leftType = TypeUtils.createRowType(ctx.getTypeFactory(), int.class, String.class);
        RelDataType rightType = TypeUtils.createRowType(ctx.getTypeFactory(), int.class, String.class, Integer.class);

        NestedLoopJoinNode<Object[]> join = NestedLoopJoinNode.create(ctx, outType, leftType, rightType, RIGHT, r -> r[0] == r[4]);
        join.register(F.asList(deps, persons));

        rowType = TypeUtils.createRowType(tf, int.class, String.class, String.class);
        ProjectNode<Object[]> project = new ProjectNode<>(ctx, rowType, r -> new Object[]{r[2], r[3], r[1]});
        project.register(join);

        RootNode<Object[]> node = new RootNode<>(ctx, rowType);
        node.register(project);

        assert node.hasNext();

        ArrayList<Object[]> rows = new ArrayList<>();

        while (node.hasNext())
            rows.add(node.next());

        assertEquals(4, rows.size());

        Assert.assertArrayEquals(new Object[]{0, "Igor", "Core"}, rows.get(0));
        Assert.assertArrayEquals(new Object[]{3, "Alexey", "Core"}, rows.get(1));
        Assert.assertArrayEquals(new Object[]{1, "Roman", "SQL"}, rows.get(2));
        Assert.assertArrayEquals(new Object[]{2, "Ivan", null}, rows.get(3));
    }

    /** */
    @Test
    public void testFullOuterJoin() {
        //          select e.id, e.name, d.name as dep_name
        //            from emp e
        // full outer join dep d
        //              on e.depno = d.depno

        ExecutionContext<Object[]> ctx = executionContext(F.first(nodes()), UUID.randomUUID(), 0);
        IgniteTypeFactory tf = ctx.getTypeFactory();
        RelDataType rowType = TypeUtils.createRowType(tf, int.class, String.class, Integer.class);

        ScanNode<Object[]> persons = new ScanNode<>(ctx, rowType, Arrays.asList(
            new Object[]{0, "Igor", 1},
            new Object[]{1, "Roman", 2},
            new Object[]{2, "Ivan", null},
            new Object[]{3, "Alexey", 1}
        ));

        rowType = TypeUtils.createRowType(tf, int.class, String.class);
        ScanNode<Object[]> deps = new ScanNode<>(ctx, rowType, Arrays.asList(
            new Object[]{1, "Core"},
            new Object[]{2, "SQL"},
            new Object[]{3, "QA"}
        ));

        RelDataType outType = TypeUtils.createRowType(ctx.getTypeFactory(), int.class, String.class, Integer.class, int.class, String.class);
        RelDataType leftType = TypeUtils.createRowType(ctx.getTypeFactory(), int.class, String.class, Integer.class);
        RelDataType rightType = TypeUtils.createRowType(ctx.getTypeFactory(), int.class, String.class);

        NestedLoopJoinNode<Object[]> join = NestedLoopJoinNode.create(ctx, outType, leftType, rightType, FULL, r -> r[2] == r[3]);
        join.register(F.asList(persons, deps));

        rowType = TypeUtils.createRowType(tf, Integer.class, String.class, String.class);
        ProjectNode<Object[]> project = new ProjectNode<>(ctx, rowType, r -> new Object[]{r[0], r[1], r[4]});
        project.register(join);

        RootNode<Object[]> node = new RootNode<>(ctx, rowType);
        node.register(project);

        assert node.hasNext();

        ArrayList<Object[]> rows = new ArrayList<>();

        while (node.hasNext())
            rows.add(node.next());

        assertEquals(5, rows.size());

        Assert.assertArrayEquals(new Object[]{0, "Igor", "Core"}, rows.get(0));
        Assert.assertArrayEquals(new Object[]{1, "Roman", "SQL"}, rows.get(1));
        Assert.assertArrayEquals(new Object[]{2, "Ivan", null}, rows.get(2));
        Assert.assertArrayEquals(new Object[]{3, "Alexey", "Core"}, rows.get(3));
        Assert.assertArrayEquals(new Object[]{null, null, "QA"}, rows.get(4));
    }

    /** */
    @Test
    public void testSemiJoin() {
        //    select d.name as dep_name
        //      from dep d
        // semi join emp e
        //        on e.depno = d.depno

        ExecutionContext<Object[]> ctx = executionContext(F.first(nodes()), UUID.randomUUID(), 0);
        IgniteTypeFactory tf = ctx.getTypeFactory();
        RelDataType rowType = TypeUtils.createRowType(tf, int.class, String.class, Integer.class);

        ScanNode<Object[]> persons = new ScanNode<>(ctx, rowType, Arrays.asList(
            new Object[]{0, "Igor", 1},
            new Object[]{1, "Roman", 2},
            new Object[]{2, "Ivan", null},
            new Object[]{3, "Alexey", 1}
        ));

        rowType = TypeUtils.createRowType(tf, int.class, String.class);
        ScanNode<Object[]> deps = new ScanNode<>(ctx, rowType, Arrays.asList(
            new Object[]{1, "Core"},
            new Object[]{2, "SQL"},
            new Object[]{3, "QA"}
        ));

        RelDataType outType = TypeUtils.createRowType(ctx.getTypeFactory(), int.class, String.class, Integer.class);
        RelDataType leftType = TypeUtils.createRowType(ctx.getTypeFactory(), int.class, String.class, Integer.class);
        RelDataType rightType = TypeUtils.createRowType(ctx.getTypeFactory(), int.class, String.class);

        NestedLoopJoinNode<Object[]> join = NestedLoopJoinNode.create(ctx, outType, leftType, rightType, SEMI, r -> r[0] == r[4]);
        join.register(F.asList(deps, persons));

        rowType = TypeUtils.createRowType(tf, String.class);
        ProjectNode<Object[]> project = new ProjectNode<>(ctx, rowType, r -> new Object[]{r[1]});
        project.register(join);

        RootNode<Object[]> node = new RootNode<>(ctx, rowType);
        node.register(project);

        assert node.hasNext();

        ArrayList<Object[]> rows = new ArrayList<>();

        while (node.hasNext())
            rows.add(node.next());

        assertEquals(2, rows.size());

        Assert.assertArrayEquals(new Object[]{"Core"}, rows.get(0));
        Assert.assertArrayEquals(new Object[]{"SQL"}, rows.get(1));
    }

    /** */
    @Test
    public void testAntiJoin() {
        //    select d.name as dep_name
        //      from dep d
        // anti join emp e
        //        on e.depno = d.depno

        ExecutionContext<Object[]> ctx = executionContext(F.first(nodes()), UUID.randomUUID(), 0);
        IgniteTypeFactory tf = ctx.getTypeFactory();
        RelDataType rowType = TypeUtils.createRowType(tf, int.class, String.class, Integer.class);

        ScanNode<Object[]> persons = new ScanNode<>(ctx, rowType, Arrays.asList(
            new Object[]{0, "Igor", 1},
            new Object[]{1, "Roman", 2},
            new Object[]{2, "Ivan", null},
            new Object[]{3, "Alexey", 1}
        ));

        rowType = TypeUtils.createRowType(tf, int.class, String.class);
        ScanNode<Object[]> deps = new ScanNode<>(ctx, rowType, Arrays.asList(
            new Object[]{1, "Core"},
            new Object[]{2, "SQL"},
            new Object[]{3, "QA"}
        ));

        RelDataType outType = TypeUtils.createRowType(ctx.getTypeFactory(), int.class, String.class, Integer.class);
        RelDataType leftType = TypeUtils.createRowType(ctx.getTypeFactory(), int.class, String.class, Integer.class);
        RelDataType rightType = TypeUtils.createRowType(ctx.getTypeFactory(), int.class, String.class);

        NestedLoopJoinNode<Object[]> join = NestedLoopJoinNode.create(ctx, outType, leftType, rightType, ANTI, r -> r[0] == r[4]);
        join.register(F.asList(deps, persons));

        rowType = TypeUtils.createRowType(tf, String.class);
        ProjectNode<Object[]> project = new ProjectNode<>(ctx, rowType, r -> new Object[]{r[1]});
        project.register(join);

        RootNode<Object[]> node = new RootNode<>(ctx, rowType);
        node.register(project);

        assert node.hasNext();

        ArrayList<Object[]> rows = new ArrayList<>();

        while (node.hasNext())
            rows.add(node.next());

        assertEquals(1, rows.size());

        Assert.assertArrayEquals(new Object[]{"QA"}, rows.get(0));
    }

    /** */
    @Test
    public void testAggregateMapReduceAvg() {
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

        RelDataType mapType = IgniteMapAggregate.rowType(tf);
        AggregateNode<Object[]> map = new AggregateNode<>(ctx, mapType, MAP, grpSets, accFactory(ctx, call, MAP, rowType), rowFactory());
        map.register(scan);

        RelDataType reduceType = TypeUtils.createRowType(tf, double.class);
        AggregateNode<Object[]> reduce = new AggregateNode<>(ctx, reduceType, REDUCE, grpSets, accFactory(ctx, call, REDUCE, null), rowFactory());
        reduce.register(map);

        RootNode<Object[]> root = new RootNode<>(ctx, reduceType);
        root.register(reduce);

        assertTrue(root.hasNext());
        assertEquals(725d, root.next()[0]);
        assertFalse(root.hasNext());
    }

    /** */
    @Test
    public void testAggregateMapReduceSum() {
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

        RelDataType mapType = IgniteMapAggregate.rowType(tf);
        AggregateNode<Object[]> map = new AggregateNode<>(ctx, mapType, MAP, grpSets, accFactory(ctx, call, MAP, rowType), rowFactory());
        map.register(scan);

        RelDataType reduceType = TypeUtils.createRowType(tf, int.class);
        AggregateNode<Object[]> reduce = new AggregateNode<>(ctx, reduceType, REDUCE, grpSets, accFactory(ctx, call, REDUCE, null), rowFactory());
        reduce.register(map);

        RootNode<Object[]> root = new RootNode<>(ctx, reduceType);
        root.register(reduce);

        assertTrue(root.hasNext());
        assertEquals(2900, root.next()[0]);
        assertFalse(root.hasNext());
    }

    /** */
    @Test
    public void testAggregateMapReduceMin() {
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

        RelDataType mapType = IgniteMapAggregate.rowType(tf);
        AggregateNode<Object[]> map = new AggregateNode<>(ctx, mapType, MAP, grpSets, accFactory(ctx, call, MAP, rowType), rowFactory());
        map.register(scan);

        RelDataType reduceType = TypeUtils.createRowType(tf, int.class);
        AggregateNode<Object[]> reduce = new AggregateNode<>(ctx, reduceType, REDUCE, grpSets, accFactory(ctx, call, REDUCE, null), rowFactory());
        reduce.register(map);

        RootNode<Object[]> root = new RootNode<>(ctx, reduceType);
        root.register(reduce);

        assertTrue(root.hasNext());
        assertEquals(200, root.next()[0]);
        assertFalse(root.hasNext());
    }

    /** */
    @Test
    public void testAggregateMapReduceMax() {
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

        RelDataType mapType = IgniteMapAggregate.rowType(tf);
        AggregateNode<Object[]> map = new AggregateNode<>(ctx, mapType, MAP, grpSets, accFactory(ctx, call, MAP, rowType), rowFactory());
        map.register(scan);

        RelDataType reduceType = TypeUtils.createRowType(tf, int.class);
        AggregateNode<Object[]> reduce = new AggregateNode<>(ctx, reduceType, REDUCE, grpSets, accFactory(ctx, call, REDUCE, null), rowFactory());
        reduce.register(map);

        RootNode<Object[]> root = new RootNode<>(ctx, reduceType);
        root.register(reduce);

        assertTrue(root.hasNext());
        assertEquals(1400, root.next()[0]);
        assertFalse(root.hasNext());
    }

    /** */
    @Test
    public void testAggregateMapReduceCount() {
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

        RelDataType mapType = IgniteMapAggregate.rowType(tf);
        AggregateNode<Object[]> map = new AggregateNode<>(ctx, mapType, MAP, grpSets, accFactory(ctx, call, MAP, rowType), rowFactory());
        map.register(scan);

        RelDataType reduceType = TypeUtils.createRowType(tf, int.class);
        AggregateNode<Object[]> reduce = new AggregateNode<>(ctx, reduceType, REDUCE, grpSets, accFactory(ctx, call, REDUCE, null), rowFactory());
        reduce.register(map);

        RootNode<Object[]> root = new RootNode<>(ctx, reduceType);
        root.register(reduce);

        assertTrue(root.hasNext());
        assertEquals(4, root.next()[0]);
        assertFalse(root.hasNext());
    }

    /** */
    @Test
    public void testAggregateAvg() {
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
        AggregateNode<Object[]> agg = new AggregateNode<>(ctx, aggType, SINGLE, grpSets, accFactory(ctx, call, SINGLE, rowType), rowFactory());
        agg.register(scan);

        RootNode<Object[]> root = new RootNode<>(ctx, aggType);
        root.register(agg);

        assertTrue(root.hasNext());
        assertEquals(725d, root.next()[0]);
        assertFalse(root.hasNext());
    }

    /** */
    @Test
    public void testAggregateSum() {
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
        AggregateNode<Object[]> agg = new AggregateNode<>(ctx, aggType, SINGLE, grpSets, accFactory(ctx, call, SINGLE, rowType), rowFactory());
        agg.register(scan);

        RootNode<Object[]> root = new RootNode<>(ctx, aggType);
        root.register(agg);

        assertTrue(root.hasNext());
        assertEquals(2900, root.next()[0]);
        assertFalse(root.hasNext());
    }

    /** */
    @Test
    public void testAggregateMin() {
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
        AggregateNode<Object[]> agg = new AggregateNode<>(ctx, aggType, SINGLE, grpSets, accFactory(ctx, call, SINGLE, rowType), rowFactory());
        agg.register(scan);

        RootNode<Object[]> root = new RootNode<>(ctx, aggType);
        root.register(agg);

        assertTrue(root.hasNext());
        assertEquals(200, root.next()[0]);
        assertFalse(root.hasNext());
    }

    /** */
    @Test
    public void testAggregateMax() {
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
        AggregateNode<Object[]> agg = new AggregateNode<>(ctx, aggType, SINGLE, grpSets, accFactory(ctx, call, SINGLE, rowType), rowFactory());
        agg.register(scan);

        RootNode<Object[]> root = new RootNode<>(ctx, aggType);
        root.register(agg);

        assertTrue(root.hasNext());
        assertEquals(1400, root.next()[0]);
        assertFalse(root.hasNext());
    }

    /** */
    @Test
    public void testAggregateCount() {
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
        AggregateNode<Object[]> agg = new AggregateNode<>(ctx, aggType, SINGLE, grpSets, accFactory(ctx, call, SINGLE, rowType), rowFactory());
        agg.register(scan);

        RootNode<Object[]> root = new RootNode<>(ctx, aggType);
        root.register(agg);

        assertTrue(root.hasNext());
        assertEquals(4, root.next()[0]);
        assertFalse(root.hasNext());
    }

    /** */
    @Test
    public void testAggregateCountByGroup() {
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
        AggregateNode<Object[]> agg = new AggregateNode<>(ctx, aggType, SINGLE, grpSets, accFactory(ctx, call, SINGLE, rowType), rowFactory());
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
    private Object[] row(Object... fields) {
        return fields;
    }

    /** */
    private Supplier<List<AccumulatorWrapper<Object[]>>> accFactory(ExecutionContext<Object[]> ctx, AggregateCall call,
        AggregateNode.AggregateType type, RelDataType rowType) {
        return ctx.expressionFactory().accumulatorsFactory(type, F.asList(call), rowType);
    }

    /** */
    private RowFactory<Object[]> rowFactory() {
        return new RowFactory<Object[]>() {
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

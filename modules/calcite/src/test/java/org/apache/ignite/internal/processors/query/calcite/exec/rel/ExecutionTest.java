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
import com.google.common.collect.ImmutableSet;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.RowHandler;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.util.TypeUtils;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static java.lang.Math.max;
import static java.lang.Math.min;
import static org.apache.calcite.rel.core.JoinRelType.ANTI;
import static org.apache.calcite.rel.core.JoinRelType.FULL;
import static org.apache.calcite.rel.core.JoinRelType.INNER;
import static org.apache.calcite.rel.core.JoinRelType.LEFT;
import static org.apache.calcite.rel.core.JoinRelType.RIGHT;
import static org.apache.calcite.rel.core.JoinRelType.SEMI;
import static org.apache.ignite.internal.processors.query.calcite.util.Commons.getFieldFromBiRows;

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

    /**
     *
     */
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
            new Object[] {0, "Igor", "Seliverstov"},
            new Object[] {1, "Roman", "Kondakov"},
            new Object[] {2, "Ivan", "Pavlukhin"},
            new Object[] {3, "Alexey", "Goncharuk"}
        ));

        rowType = TypeUtils.createRowType(tf, int.class, int.class, String.class);
        ScanNode<Object[]> projects = new ScanNode<>(ctx, rowType, Arrays.asList(
            new Object[] {0, 2, "Calcite"},
            new Object[] {1, 1, "SQL"},
            new Object[] {2, 2, "Ignite"},
            new Object[] {3, 0, "Core"}
        ));

        RelDataType outType = TypeUtils.createRowType(tf, int.class, String.class, String.class, int.class, int.class, String.class);
        RelDataType leftType = TypeUtils.createRowType(tf, int.class, String.class, String.class);
        RelDataType rightType = TypeUtils.createRowType(tf, int.class, int.class, String.class);

        RowHandler<Object[]> hnd = ctx.rowHandler();

        NestedLoopJoinNode<Object[]> join = NestedLoopJoinNode.create(ctx, outType, leftType, rightType, INNER,
            (r1, r2) -> getFieldFromBiRows(hnd, 0, r1, r2) == getFieldFromBiRows(hnd, 4, r1, r2));
        join.register(F.asList(persons, projects));

        rowType = TypeUtils.createRowType(tf, int.class, String.class, String.class);
        ProjectNode<Object[]> project = new ProjectNode<>(ctx, rowType, r -> new Object[] {r[0], r[1], r[5]});
        project.register(join);

        FilterNode<Object[]> filter = new FilterNode<>(ctx, rowType, r -> (Integer)r[0] >= 2);
        filter.register(project);

        RootNode<Object[]> node = new RootNode<>(ctx, rowType);
        node.register(filter);

        assert node.hasNext();

        ArrayList<Object[]> rows = new ArrayList<>();

        while (node.hasNext())
            rows.add(node.next());

        assertEquals(2, rows.size());

        Assert.assertArrayEquals(new Object[] {2, "Ivan", "Calcite"}, rows.get(0));
        Assert.assertArrayEquals(new Object[] {2, "Ivan", "Ignite"}, rows.get(1));
    }

    /**
     *
     */
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

    /**
     *
     */
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
            new Object[] {0, "Igor", 1},
            new Object[] {1, "Roman", 2},
            new Object[] {2, "Ivan", null},
            new Object[] {3, "Alexey", 1}
        ));

        rowType = TypeUtils.createRowType(tf, int.class, String.class);
        ScanNode<Object[]> deps = new ScanNode<>(ctx, rowType, Arrays.asList(
            new Object[] {1, "Core"},
            new Object[] {2, "SQL"}
        ));

        RelDataType outType = TypeUtils.createRowType(ctx.getTypeFactory(), int.class, String.class, Integer.class,
            int.class, String.class);
        RelDataType leftType = TypeUtils.createRowType(ctx.getTypeFactory(), int.class, String.class, Integer.class);
        RelDataType rightType = TypeUtils.createRowType(ctx.getTypeFactory(), int.class, String.class);

        RowHandler<Object[]> hnd = ctx.rowHandler();

        NestedLoopJoinNode<Object[]> join = NestedLoopJoinNode.create(ctx, outType, leftType, rightType, LEFT,
            (r1, r2) -> getFieldFromBiRows(hnd, 2, r1, r2) == getFieldFromBiRows(hnd, 3, r1, r2));
        join.register(F.asList(persons, deps));

        rowType = TypeUtils.createRowType(tf, int.class, String.class, String.class);
        ProjectNode<Object[]> project = new ProjectNode<>(ctx, rowType, r -> new Object[] {r[0], r[1], r[4]});
        project.register(join);

        RootNode<Object[]> node = new RootNode<>(ctx, rowType);
        node.register(project);

        assert node.hasNext();

        ArrayList<Object[]> rows = new ArrayList<>();

        while (node.hasNext())
            rows.add(node.next());

        assertEquals(4, rows.size());

        Assert.assertArrayEquals(new Object[] {0, "Igor", "Core"}, rows.get(0));
        Assert.assertArrayEquals(new Object[] {1, "Roman", "SQL"}, rows.get(1));
        Assert.assertArrayEquals(new Object[] {2, "Ivan", null}, rows.get(2));
        Assert.assertArrayEquals(new Object[] {3, "Alexey", "Core"}, rows.get(3));
    }

    /**
     *
     */
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
            new Object[] {0, "Igor", 1},
            new Object[] {1, "Roman", 2},
            new Object[] {2, "Ivan", null},
            new Object[] {3, "Alexey", 1}
        ));

        rowType = TypeUtils.createRowType(tf, int.class, String.class);
        ScanNode<Object[]> deps = new ScanNode<>(ctx, rowType, Arrays.asList(
            new Object[] {1, "Core"},
            new Object[] {2, "SQL"},
            new Object[] {3, "QA"}
        ));

        RelDataType outType = TypeUtils.createRowType(ctx.getTypeFactory(), int.class, String.class, int.class,
            String.class, Integer.class);
        RelDataType leftType = TypeUtils.createRowType(ctx.getTypeFactory(), int.class, String.class);
        RelDataType rightType = TypeUtils.createRowType(ctx.getTypeFactory(), int.class, String.class, Integer.class);

        RowHandler<Object[]> hnd = ctx.rowHandler();

        NestedLoopJoinNode<Object[]> join = NestedLoopJoinNode.create(ctx, outType, leftType, rightType, RIGHT,
            (r1, r2) -> getFieldFromBiRows(hnd, 0, r1, r2) == getFieldFromBiRows(hnd, 4, r1, r2));
        join.register(F.asList(deps, persons));

        rowType = TypeUtils.createRowType(tf, int.class, String.class, String.class);
        ProjectNode<Object[]> project = new ProjectNode<>(ctx, rowType, r -> new Object[] {r[2], r[3], r[1]});
        project.register(join);

        RootNode<Object[]> node = new RootNode<>(ctx, rowType);
        node.register(project);

        assert node.hasNext();

        ArrayList<Object[]> rows = new ArrayList<>();

        while (node.hasNext())
            rows.add(node.next());

        assertEquals(4, rows.size());

        Assert.assertArrayEquals(new Object[] {0, "Igor", "Core"}, rows.get(0));
        Assert.assertArrayEquals(new Object[] {3, "Alexey", "Core"}, rows.get(1));
        Assert.assertArrayEquals(new Object[] {1, "Roman", "SQL"}, rows.get(2));
        Assert.assertArrayEquals(new Object[] {2, "Ivan", null}, rows.get(3));
    }

    /**
     *
     */
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
            new Object[] {0, "Igor", 1},
            new Object[] {1, "Roman", 2},
            new Object[] {2, "Ivan", null},
            new Object[] {3, "Alexey", 1}
        ));

        rowType = TypeUtils.createRowType(tf, int.class, String.class);
        ScanNode<Object[]> deps = new ScanNode<>(ctx, rowType, Arrays.asList(
            new Object[] {1, "Core"},
            new Object[] {2, "SQL"},
            new Object[] {3, "QA"}
        ));

        RelDataType outType = TypeUtils.createRowType(ctx.getTypeFactory(), int.class, String.class, Integer.class,
            int.class, String.class);
        RelDataType leftType = TypeUtils.createRowType(ctx.getTypeFactory(), int.class, String.class, Integer.class);
        RelDataType rightType = TypeUtils.createRowType(ctx.getTypeFactory(), int.class, String.class);

        RowHandler<Object[]> hnd = ctx.rowHandler();

        NestedLoopJoinNode<Object[]> join = NestedLoopJoinNode.create(ctx, outType, leftType, rightType, FULL,
            (r1, r2) -> getFieldFromBiRows(hnd, 2, r1, r2) == getFieldFromBiRows(hnd, 3, r1, r2));
        join.register(F.asList(persons, deps));

        rowType = TypeUtils.createRowType(tf, Integer.class, String.class, String.class);
        ProjectNode<Object[]> project = new ProjectNode<>(ctx, rowType, r -> new Object[] {r[0], r[1], r[4]});
        project.register(join);

        RootNode<Object[]> node = new RootNode<>(ctx, rowType);
        node.register(project);

        assert node.hasNext();

        ArrayList<Object[]> rows = new ArrayList<>();

        while (node.hasNext())
            rows.add(node.next());

        assertEquals(5, rows.size());

        Assert.assertArrayEquals(new Object[] {0, "Igor", "Core"}, rows.get(0));
        Assert.assertArrayEquals(new Object[] {1, "Roman", "SQL"}, rows.get(1));
        Assert.assertArrayEquals(new Object[] {2, "Ivan", null}, rows.get(2));
        Assert.assertArrayEquals(new Object[] {3, "Alexey", "Core"}, rows.get(3));
        Assert.assertArrayEquals(new Object[] {null, null, "QA"}, rows.get(4));
    }

    /**
     *
     */
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
            new Object[] {0, "Igor", 1},
            new Object[] {1, "Roman", 2},
            new Object[] {2, "Ivan", null},
            new Object[] {3, "Alexey", 1}
        ));

        rowType = TypeUtils.createRowType(tf, int.class, String.class);
        ScanNode<Object[]> deps = new ScanNode<>(ctx, rowType, Arrays.asList(
            new Object[] {1, "Core"},
            new Object[] {2, "SQL"},
            new Object[] {3, "QA"}
        ));

        RelDataType outType = TypeUtils.createRowType(ctx.getTypeFactory(), int.class, String.class, Integer.class);
        RelDataType leftType = TypeUtils.createRowType(ctx.getTypeFactory(), int.class, String.class, Integer.class);
        RelDataType rightType = TypeUtils.createRowType(ctx.getTypeFactory(), int.class, String.class);

        RowHandler<Object[]> hnd = ctx.rowHandler();

        NestedLoopJoinNode<Object[]> join = NestedLoopJoinNode.create(ctx, outType, leftType, rightType, SEMI,
            (r1, r2) -> getFieldFromBiRows(hnd, 0, r1, r2) == getFieldFromBiRows(hnd, 4, r1, r2));
        join.register(F.asList(deps, persons));

        rowType = TypeUtils.createRowType(tf, String.class);
        ProjectNode<Object[]> project = new ProjectNode<>(ctx, rowType, r -> new Object[] {r[1]});
        project.register(join);

        RootNode<Object[]> node = new RootNode<>(ctx, rowType);
        node.register(project);

        assert node.hasNext();

        ArrayList<Object[]> rows = new ArrayList<>();

        while (node.hasNext())
            rows.add(node.next());

        assertEquals(2, rows.size());

        Assert.assertArrayEquals(new Object[] {"Core"}, rows.get(0));
        Assert.assertArrayEquals(new Object[] {"SQL"}, rows.get(1));
    }

    /**
     *
     */
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
            new Object[] {0, "Igor", 1},
            new Object[] {1, "Roman", 2},
            new Object[] {2, "Ivan", null},
            new Object[] {3, "Alexey", 1}
        ));

        rowType = TypeUtils.createRowType(tf, int.class, String.class);
        ScanNode<Object[]> deps = new ScanNode<>(ctx, rowType, Arrays.asList(
            new Object[] {1, "Core"},
            new Object[] {2, "SQL"},
            new Object[] {3, "QA"}
        ));

        RelDataType outType = TypeUtils.createRowType(ctx.getTypeFactory(), int.class, String.class, Integer.class);
        RelDataType leftType = TypeUtils.createRowType(ctx.getTypeFactory(), int.class, String.class, Integer.class);
        RelDataType rightType = TypeUtils.createRowType(ctx.getTypeFactory(), int.class, String.class);

        RowHandler<Object[]> hnd = ctx.rowHandler();

        NestedLoopJoinNode<Object[]> join = NestedLoopJoinNode.create(ctx, outType, leftType, rightType, ANTI,
            (r1, r2) -> getFieldFromBiRows(hnd, 0, r1, r2) == getFieldFromBiRows(hnd, 4, r1, r2));
        join.register(F.asList(deps, persons));

        rowType = TypeUtils.createRowType(tf, String.class);
        ProjectNode<Object[]> project = new ProjectNode<>(ctx, rowType, r -> new Object[] {r[1]});
        project.register(join);

        RootNode<Object[]> node = new RootNode<>(ctx, rowType);
        node.register(project);

        assert node.hasNext();

        ArrayList<Object[]> rows = new ArrayList<>();

        while (node.hasNext())
            rows.add(node.next());

        assertEquals(1, rows.size());

        Assert.assertArrayEquals(new Object[] {"QA"}, rows.get(0));
    }

    /**
     *
     */
    @Test
    public void testCorrelatedNestedLoopJoin() {
        ExecutionContext<Object[]> ctx = executionContext(F.first(nodes()), UUID.randomUUID(), 0);
        IgniteTypeFactory tf = ctx.getTypeFactory();
        RelDataType rowType = TypeUtils.createRowType(tf, int.class, String.class, int.class);

        int[] leftSizes = {1, 99, 100, 101, 512, 513, 2000};
        int[] rightSizes = {1, 99, 100, 101, 512, 513, 2000};
        int[] rightBufSizes = {1, 100, 512};
        JoinRelType[] joinTypes = {INNER, LEFT};

        for (JoinRelType joinType : joinTypes) {
            for (int rightBufSize : rightBufSizes) {
                for (int leftSize : leftSizes) {
                    for (int rightSize : rightSizes) {
                        log.info("Check: joinType=" + joinType + ", rightBufSize=" + rightBufSize +
                            ", leftSize=" + leftSize + ", rightSize=" + rightSize);

                        ScanNode<Object[]> left = new ScanNode<>(ctx, rowType, new TestTable(leftSize, rowType));
                        ScanNode<Object[]> right = new ScanNode<>(ctx, rowType, new TestTable(rightSize, rowType));

                        RelDataType joinRowType = TypeUtils.createRowType(
                            tf,
                            int.class, String.class, int.class,
                            int.class, String.class, int.class);

                        RowHandler<Object[]> hnd = ctx.rowHandler();

                        CorrelatedNestedLoopJoinNode<Object[]> join = new CorrelatedNestedLoopJoinNode<>(
                            ctx,
                            joinRowType,
                            (r1, r2) -> getFieldFromBiRows(hnd, 0, r1, r2).equals(getFieldFromBiRows(hnd, 3, r1, r2)),
                            ImmutableSet.of(new CorrelationId(0)),
                            joinType
                        );

                        GridTestUtils.setFieldValue(join, "rightInBufferSize", rightBufSize);

                        join.register(Arrays.asList(left, right));

                        FilterNode<Object[]> filter = new FilterNode<>(ctx, joinRowType, r -> true);
                        filter.register(join);

                        RootNode<Object[]> root = new RootNode<>(ctx, joinRowType);
                        root.register(filter);

                        int cnt = 0;
                        while (root.hasNext()) {
                            root.next();

                            cnt++;
                        }

                        assertEquals(
                            "Invalid result size. [left=" + leftSize + ", right=" + rightSize + ", results=" + cnt,
                            joinType == INNER ? min(leftSize, rightSize) : leftSize,
                            cnt);
                    }
                }
            }
        }
    }

    /** */
    @Test
    public void testMergeJoin() throws IgniteCheckedException {
        ExecutionContext<Object[]> ctx = executionContext(F.first(nodes()), UUID.randomUUID(), 0);
        IgniteTypeFactory tf = ctx.getTypeFactory();
        RelDataType rowType = TypeUtils.createRowType(tf, int.class, String.class, int.class);

        int[] sizes = {1, max(inBufSize / 3, 1), max(inBufSize / 2, 1), max(inBufSize - 1, 1), inBufSize,
            inBufSize + 1, 2 * inBufSize - 1, 2 * inBufSize, 2 * inBufSize + 1};

        for (int leftSize : sizes) {
            for (int rightSize : sizes) {
                for (boolean distr : new boolean[] {false, true}) {
                    log.info("Check: leftSize=" + leftSize + ", rightSize=" + rightSize + ", distributed=" + distr);

                    ScanNode<Object[]> left = new ScanNode<>(ctx, rowType, new TestTable(leftSize, rowType));
                    ScanNode<Object[]> right = new ScanNode<>(ctx, rowType, new TestTable(rightSize, rowType));

                    RelDataType joinRowType = TypeUtils.createRowType(
                        tf,
                        int.class, String.class, int.class,
                        int.class, String.class, int.class);

                    MergeJoinNode<Object[]> join = MergeJoinNode.create(
                        ctx,
                        joinRowType,
                        null,
                        null,
                        INNER,
                        (r1, r2) -> {
                            Object o1 = r1[0];
                            Object o2 = r2[0];

                            if (o1 == null || o2 == null) {
                                if (o1 != null)
                                    return 1;
                                else if (o2 != null)
                                    return -1;
                                else
                                    return 0;
                            }

                            return Integer.compare((Integer)o1, (Integer)o2);
                        },
                        distr
                    );

                    join.register(Arrays.asList(left, right));

                    RootNode<Object[]> root = new RootNode<>(ctx, joinRowType);
                    root.register(join);

                    int cnt = 0;
                    while (root.hasNext()) {
                        root.next();

                        cnt++;
                    }

                    assertEquals(
                        "Invalid result size. [left=" + leftSize + ", right=" + rightSize + ", results=" + cnt,
                        min(leftSize, rightSize),
                        cnt);
                }
            }
        }
    }

    /**
     * Test verifies that an AssertionError thrown from an execution node
     * properly handled by a task executor.
     */
    @Test
    @SuppressWarnings("ThrowableNotThrown")
    public void assertionHandlingTest() {
        ExecutionContext<Object[]> ctx = executionContext(F.first(nodes()), UUID.randomUUID(), 0);
        IgniteTypeFactory tf = ctx.getTypeFactory();
        RelDataType rowType = TypeUtils.createRowType(tf, int.class, String.class);

        CorruptedNode<Object[]> node = new CorruptedNode<>();

        RootNode<Object[]> root = new RootNode<>(ctx, rowType);
        root.register(node);

        Thread watchDog = new Thread(() -> {
            try {
                U.sleep(5_000);
            }
            catch (IgniteInterruptedCheckedException ignored) {
            }

            if (!root.isClosed())
                root.close();
        }, "test-watchdog");

        watchDog.start();

        GridTestUtils.assertThrowsWithCause(root::hasNext, AssertionError.class);

        watchDog.interrupt();
    }

    /** {@inheritDoc} */
    @Override protected Object[] row(Object... fields) {
        return fields;
    }

    /**
     * Node that always throws assertion error except for {@link #close()}
     * and {@link #onRegister(Downstream)} methods.
     */
    static class CorruptedNode<T> implements Node<T> {
        /** {@inheritDoc} */
        @Override public ExecutionContext<T> context() {
            throw new AssertionError();
        }

        /** {@inheritDoc} */
        @Override public RelDataType rowType() {
            throw new AssertionError();
        }

        /** {@inheritDoc} */
        @Override public Downstream<T> downstream() {
            throw new AssertionError();
        }

        /** {@inheritDoc} */
        @Override public void register(List<Node<T>> sources) {
            throw new AssertionError();
        }

        /** {@inheritDoc} */
        @Override public List<Node<T>> sources() {
            throw new AssertionError();
        }

        /** {@inheritDoc} */
        @Override public void onRegister(Downstream<T> downstream) {

        }

        /** {@inheritDoc} */
        @Override public void request(int rowsCnt) throws Exception {
            throw new AssertionError();
        }

        /** {@inheritDoc} */
        @Override public void rewind() {
            throw new AssertionError();
        }

        /** {@inheritDoc} */
        @Override public void close() throws Exception {
        }
    }
}

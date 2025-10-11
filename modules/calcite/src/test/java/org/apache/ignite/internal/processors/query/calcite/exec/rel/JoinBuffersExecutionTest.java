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
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteJoinInfo;
import org.apache.ignite.internal.processors.query.calcite.util.TypeUtils;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.calcite.rel.core.JoinRelType.ANTI;
import static org.apache.calcite.rel.core.JoinRelType.FULL;
import static org.apache.calcite.rel.core.JoinRelType.INNER;
import static org.apache.calcite.rel.core.JoinRelType.LEFT;
import static org.apache.calcite.rel.core.JoinRelType.RIGHT;
import static org.apache.calcite.rel.core.JoinRelType.SEMI;

/** Tests that buffers of join nodes are cleared at the join end and that a join node is not stuck. */
@RunWith(Parameterized.class)
public class JoinBuffersExecutionTest extends AbstractExecutionTest {
    /** */
    @Test
    public void testMergeJoinBuffers() throws Exception {
        JoinFactory joinFactory = (ctx, outType, leftType, rightType, joinType) ->
            MergeJoinNode.create(ctx, outType, leftType, rightType, joinType, Comparator.comparingInt(r -> (Integer)r[0]), true);

        Consumer<AbstractNode<?>> bufChecker = (node) -> {
            assertTrue(((MergeJoinNode<?>)node).leftInBuf.size() <= IN_BUFFER_SIZE);

            assertTrue(((MergeJoinNode<?>)node).rightInBuf.size() <= IN_BUFFER_SIZE);
        };

        doTestJoinBuffer(joinFactory, bufChecker, false);
    }

    /** */
    @Test
    public void testNLJoinBuffers() throws Exception {
        JoinFactory joinFactory = (ctx, outType, leftType, rightType, joinType) ->
            NestedLoopJoinNode.create(ctx, outType, leftType, rightType, joinType, (r1, r2) -> r1[0].equals(r2[0]));

        Consumer<AbstractNode<?>> bufChecker = (node) ->
            assertTrue(((AbstractRightMaterializedJoinNode<?>)node).leftInBuf.size() <= IN_BUFFER_SIZE);

        doTestJoinBuffer(joinFactory, bufChecker, false);
    }

    /** */
    @Test
    public void testHashJoinBuffers() throws Exception {
        IgniteJoinInfo joinInfo = IgniteJoinInfo.of(ImmutableIntList.of(0), ImmutableIntList.of(0));

        JoinFactory joinFactory = (ctx, outType, leftType, rightType, joinType) ->
            HashJoinNode.create(ctx, outType, leftType, rightType, joinType, joinInfo, null);

        Consumer<AbstractNode<?>> bufChecker = (node) ->
            assertTrue(((AbstractRightMaterializedJoinNode<?>)node).leftInBuf.size() <= IN_BUFFER_SIZE);

        doTestJoinBuffer(joinFactory, bufChecker, true);
    }

    /**
     * Tests a join with input bigger that the buffer size.
     *
     * @param joinFactory Creates certain join node.
     * @param joinBufChecker Finally check node after successfull run.
     * @param sortResults If {@code true}, sorts results before checking.
     */
    private void doTestJoinBuffer(
        JoinFactory joinFactory,
        Consumer<AbstractNode<?>> joinBufChecker,
        boolean sortResults
    ) throws Exception {
        for (JoinRelType joinType : F.asList(LEFT, INNER, RIGHT, FULL, SEMI, ANTI)) {
            if (log.isInfoEnabled())
                log.info("Testing join of type '" + joinType + "'...");

            int size = IN_BUFFER_SIZE * 2 + IN_BUFFER_SIZE / 2;
            int intersect = Math.max(10, IN_BUFFER_SIZE / 10);

            int leftTo = size + intersect;
            int rightTo = size * 2;

            ExecutionContext<Object[]> ctx = executionContext(F.first(nodes()), UUID.randomUUID(), 0);

            Iterator<Object[]> leftIter = IntStream.range(0, leftTo).boxed().map(i -> new Object[] {i}).iterator();
            Iterator<Object[]> rightIter = IntStream.range(size, rightTo).boxed().map(i -> new Object[] {i}).iterator();

            RelDataType leftType = TypeUtils.createRowType(ctx.getTypeFactory(), int.class);
            ScanNode<Object[]> leftNode = new ScanNode<>(ctx, leftType, () -> leftIter);

            RelDataType rightType = TypeUtils.createRowType(ctx.getTypeFactory(), int.class);
            ScanNode<Object[]> rightNode = new ScanNode<>(ctx, rightType, () -> rightIter);

            RelDataType outType = TypeUtils.createRowType(ctx.getTypeFactory(), int.class, int.class);

            AbstractNode<Object[]> join = joinFactory.create(ctx, outType, leftType, rightType, joinType);

            join.register(F.asList(leftNode, rightNode));

            List<Object[]> res = new ArrayList<>();

            AtomicBoolean finished = new AtomicBoolean();

            join.onRegister(new Downstream<>() {
                @Override public void push(Object[] objects) {
                    res.add(objects);
                }

                @Override public void end() {
                    finished.set(true);
                }

                @Override public void onError(Throwable e) {
                    // No-op.
                }
            });

            join.request(1);

            assertTrue(GridTestUtils.waitForCondition(() -> !res.isEmpty(), getTestTimeout()));

            joinBufChecker.accept(join);

            join.request(size * size);

            assertTrue(GridTestUtils.waitForCondition(finished::get, getTestTimeout()));

            // Sorting might be needed because join may not produce a sorted result.
            if (sortResults)
                sortResults(res, joinType);

            switch (joinType) {
                case LEFT:
                    assertEquals(size + intersect, res.size());

                    for (int i = 0; i < size; ++i) {
                        assertEquals(i, res.get(i)[0]);
                        assertEquals(null, res.get(i)[1]);
                    }

                    for (int i = size; i < size + intersect; ++i) {
                        assertEquals(i, res.get(i)[0]);
                        assertEquals(i, res.get(i)[1]);
                    }
                    break;

                case INNER:
                    assertEquals(intersect, res.size());

                    for (int i = size; i < size + intersect; ++i) {
                        assertEquals(i, res.get(i - size)[0]);
                        assertEquals(i, res.get(i - size)[1]);
                    }
                    break;

                case RIGHT:
                    assertEquals(rightTo - size, res.size());

                    for (int i = size; i < size + intersect; ++i) {
                        assertEquals(i, res.get(i - size)[0]);
                        assertEquals(i, res.get(i - size)[1]);
                    }

                    for (int i = size + intersect; i < size << 1; ++i) {
                        assertEquals(null, res.get(i - size)[0]);
                        assertEquals(i, res.get(i - size)[1]);
                    }
                    break;

                case FULL:
                    assertEquals(size * 2, res.size());

                    for (int i = 0; i < size; ++i) {
                        assertEquals(i, res.get(i)[0]);
                        assertEquals(null, res.get(i)[1]);
                    }

                    for (int i = size; i < size + intersect; ++i) {
                        assertEquals(i, res.get(i)[0]);
                        assertEquals(i, res.get(i)[1]);
                    }

                    for (int i = size + intersect; i < size << 1; ++i) {
                        assertEquals(null, res.get(i)[0]);
                        assertEquals(i, res.get(i)[1]);
                    }
                    break;

                case SEMI:
                    assertEquals(intersect, res.size());

                    for (int i = 0; i < intersect; ++i) {
                        assertEquals(1, res.get(i).length);
                        assertEquals(size + i, res.get(i)[0]);
                    }
                    break;

                case ANTI:
                    assertEquals(size, res.size());

                    for (int i = 0; i < size; ++i) {
                        assertEquals(1, res.get(i).length);
                        assertEquals(i, res.get(i)[0]);
                    }
                    break;

                default:
                    throw new UnsupportedOperationException("Unsupported join type: " + join);
            }

            joinBufChecker.accept(join);
        }
    }

    /** */
    private static void sortResults(List<Object[]> res, JoinRelType joinType) {
        res.sort(new Comparator<>() {
            @Override public int compare(Object[] row0, Object[] row1) {
                assert row0.length == row1.length;

                int v1;
                int v2;

                if (joinType == SEMI || joinType == ANTI) {
                    assert row0.length == 1;
                    assert row0[0] != null && row1[0] != null;

                    v1 = (int)row0[0];
                    v2 = (int)row1[0];
                }
                else {
                    assert row0.length == 2;
                    assert (row0[0] == row0[1] && row0[0] != null) || row0[0] != null || row0[1] != null;
                    assert (row1[0] == row1[1] && row1[0] != null) || row1[0] != null || row1[1] != null;

                    v1 = (int)(row0[0] == null ? row0[1] : row0[0]);
                    v2 = (int)(row1[0] == null ? row1[1] : row1[0]);
                }

                return Integer.compare(v1, v2);
            }
        });
    }

    /** */
    @FunctionalInterface
    protected interface JoinFactory {
        /** */
        AbstractNode<Object[]> create(
            ExecutionContext<Object[]> ctx,
            RelDataType outType,
            RelDataType leftType,
            RelDataType rightType,
            JoinRelType joinType
        );
    }
}

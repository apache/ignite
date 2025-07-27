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
import java.util.Comparator;
import java.util.function.BiPredicate;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteJoinInfo;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.util.TypeUtils;
import org.apache.ignite.internal.util.typedef.F;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static org.apache.calcite.rel.core.JoinRelType.ANTI;
import static org.apache.calcite.rel.core.JoinRelType.FULL;
import static org.apache.calcite.rel.core.JoinRelType.INNER;
import static org.apache.calcite.rel.core.JoinRelType.LEFT;
import static org.apache.calcite.rel.core.JoinRelType.RIGHT;
import static org.apache.calcite.rel.core.JoinRelType.SEMI;

/** */
@SuppressWarnings("TypeMayBeWeakened")
public class HashJoinExecutionTest extends AbstractExecutionTest {
    /** */
    @Test
    public void testHashJoinRewind() {
        ExecutionContext<Object[]> ctx = executionContext();

        RelDataType leftType = TypeUtils.createRowType(ctx.getTypeFactory(), Integer.class, String.class);
        RelDataType rightType = TypeUtils.createRowType(ctx.getTypeFactory(), Integer.class, String.class, Integer.class);

        ScanNode<Object[]> deps = new ScanNode<>(
            ctx,
            rightType,
            Arrays.asList(
                new Object[] {1, "Core"},
                new Object[] {2, "SQL"},
                new Object[] {3, "QA"}
            ));

        ScanNode<Object[]> persons = new ScanNode<>(
            ctx,
            leftType,
            Arrays.asList(
                new Object[] {0, "Igor", 1},
                new Object[] {1, "Roman", 2},
                new Object[] {2, "Ivan", 5},
                new Object[] {3, "Alexey", 1}
            ));

        HashJoinNode<Object[]> join = createJoinNode(ctx, LEFT, leftType, rightType, null);

        join.register(F.asList(persons, deps));

        ProjectNode<Object[]> project = new ProjectNode<>(ctx, join.rowType(), r -> new Object[] {r[2], r[3], r[1]});
        project.register(join);

        RootRewindable<Object[]> node = new RootRewindable<>(ctx, project.rowType());

        node.register(project);

        assert node.hasNext();

        ArrayList<Object[]> rows = new ArrayList<>();

        while (node.hasNext())
            rows.add(node.next());

        Object[][] expected = {
            {1, 1, "Igor"},
            {1, 1, "Alexey"},
            {2, 2, "Roman"},
            {5, null, "Ivan"},
        };

        checkResults(expected, rows);

        node.rewind();

        assert node.hasNext();

        rows.clear();

        while (node.hasNext())
            rows.add(node.next());

        checkResults(expected, rows);
    }

    /** */
    @Test
    public void testEquiJoinWithDifferentBufferSize() {
        for (JoinRelType joinType : F.asList(INNER, LEFT, RIGHT, FULL, SEMI, ANTI)) {
            validateEquiJoin(joinType, 0, 0);
            validateEquiJoin(joinType, 0, 1);
            validateEquiJoin(joinType, 0, 10);
            validateEquiJoin(joinType, 1, 0);
            validateEquiJoin(joinType, 1, 1);
            validateEquiJoin(joinType, 1, 10);
            validateEquiJoin(joinType, 10, 0);
            validateEquiJoin(joinType, 10, 1);
            validateEquiJoin(joinType, 10, 10);

            int testSize = IN_BUFFER_SIZE;

            validateEquiJoin(joinType, 0, testSize - 1);
            validateEquiJoin(joinType, 0, testSize);
            validateEquiJoin(joinType, 0, testSize + 1);

            validateEquiJoin(joinType, testSize - 1, 0);
            validateEquiJoin(joinType, testSize - 1, testSize - 1);
            validateEquiJoin(joinType, testSize - 1, testSize);
            validateEquiJoin(joinType, testSize - 1, testSize + 1);

            validateEquiJoin(joinType, testSize, 0);
            validateEquiJoin(joinType, testSize, testSize - 1);
            validateEquiJoin(joinType, testSize, testSize);
            validateEquiJoin(joinType, testSize, testSize + 1);

            validateEquiJoin(joinType, testSize + 1, 0);
            validateEquiJoin(joinType, testSize + 1, testSize - 1);
            validateEquiJoin(joinType, testSize + 1, testSize);
            validateEquiJoin(joinType, testSize + 1, testSize + 1);

            validateEquiJoin(joinType, 2 * testSize, 0);
            validateEquiJoin(joinType, 0, 2 * testSize);
            validateEquiJoin(joinType, 2 * testSize, 2 * testSize);
        }
    }

    /** */
    @Test
    public void testNonEquiJoinWithDifferentBufferSize() {
        JoinRelType joinType = INNER;

        validateNonEquiJoin(joinType, 0, 0);
        validateNonEquiJoin(joinType, 0, 1);
        validateNonEquiJoin(joinType, 0, 10);
        validateNonEquiJoin(joinType, 1, 0);
        validateNonEquiJoin(joinType, 1, 1);
        validateNonEquiJoin(joinType, 1, 10);
        validateNonEquiJoin(joinType, 10, 0);
        validateNonEquiJoin(joinType, 10, 1);
        validateNonEquiJoin(joinType, 10, 10);

        int testSize = IN_BUFFER_SIZE;

        validateNonEquiJoin(joinType, 0, testSize - 1);
        validateNonEquiJoin(joinType, 0, testSize);
        validateNonEquiJoin(joinType, 0, testSize + 1);

        validateNonEquiJoin(joinType, testSize - 1, 0);
        validateNonEquiJoin(joinType, testSize - 1, testSize - 1);
        validateNonEquiJoin(joinType, testSize - 1, testSize);
        validateNonEquiJoin(joinType, testSize - 1, testSize + 1);

        validateNonEquiJoin(joinType, testSize, 0);
        validateNonEquiJoin(joinType, testSize, testSize - 1);
        validateNonEquiJoin(joinType, testSize, testSize);
        validateNonEquiJoin(joinType, testSize, testSize + 1);

        validateNonEquiJoin(joinType, testSize + 1, 0);
        validateNonEquiJoin(joinType, testSize + 1, testSize - 1);
        validateNonEquiJoin(joinType, testSize + 1, testSize);
        validateNonEquiJoin(joinType, testSize + 1, testSize + 1);

        validateNonEquiJoin(joinType, 2 * testSize, 0);
        validateNonEquiJoin(joinType, 0, 2 * testSize);
        validateNonEquiJoin(joinType, 2 * testSize, 2 * testSize);
    }

    /** */
    @Test
    public void testInnerJoinWithPostFiltration() {
        Object[][] persons = {
            new Object[] {0, "Igor", 1},
            new Object[] {1, "Roman", 2},
            new Object[] {2, "Ivan", 5},
            new Object[] {3, "Alexey", 1}
        };

        Object[][] deps = {
            new Object[] {1, "Core"},
            new Object[] {2, "SQL"},
            new Object[] {3, "QA"}
        };

        BiPredicate<Object[], Object[]> condition = (l, r) -> ((String)r[1]).length() > 3 && ((String)l[1]).length() > 4;

        Object[][] expected = {{3, "Alexey", 1, 1, "Core"}};

        validate(INNER, Stream.of(persons)::iterator, Stream.of(deps)::iterator, expected, -1, condition);
    }

    /** */
    @Test
    public void testSemiJoinWithPostFiltration() {
        Object[][] persons = {
            new Object[] {0, "Igor", 1},
            new Object[] {1, "Roman", 2},
            new Object[] {2, "Ivan", 5},
            new Object[] {3, "Alexey", 1}
        };

        Object[][] deps = {
            new Object[] {1, "Core"},
            new Object[] {2, "SQL"},
            new Object[] {3, "QA"}
        };

        BiPredicate<Object[], Object[]> condition = (l, r) -> ((String)r[1]).length() > 3 && ((String)l[1]).length() > 4;

        Object[][] expected = {{3, "Alexey", 1}};

        validate(SEMI, Stream.of(persons)::iterator, Stream.of(deps)::iterator, expected, -1, condition);
    }

    /** */
    private void validateEquiJoin(JoinRelType joinType, int leftSize, int rightSize) {
        if (log.isInfoEnabled()) {
            log.info("Testing eq. join with different buffer sizes. Join type: " + joinType + ", leftSz: "
                + leftSize + ", rightSz: " + rightSize);
        }

        { // Distinct inputs
            Object[] person = {1, "name", 2};
            Object[] department = {1, "department"};

            int resultSize = estimateResultSizeForDistinctInputs(joinType, leftSize, rightSize);

            validate(
                joinType,
                () -> IntStream.range(0, leftSize).mapToObj(i -> person).iterator(),
                () -> IntStream.range(0, rightSize).mapToObj(i -> department).iterator(),
                null,
                resultSize,
                null
            );
        }

        { // Matching inputs
            Object[] person = {1, "name", 2};
            Object[] department = {2, "department"};

            int resultSize = estimateResultSizeForEqualInputs(joinType, leftSize, rightSize);

            validate(
                joinType,
                () -> IntStream.range(0, leftSize).mapToObj(i -> person).iterator(),
                () -> IntStream.range(0, rightSize).mapToObj(i -> department).iterator(),
                null,
                resultSize,
                null
            );
        }
    }

    /** */
    protected void validateNonEquiJoin(JoinRelType joinType, int leftSize, int rightSize) {
        Object[] person = {1, "name", 2};
        Object[] department = {2, "department"};

        int resultSize = estimateResultSizeForEqualInputs(joinType, leftSize, rightSize);

        validate(
            joinType,
            () -> IntStream.range(0, leftSize).mapToObj(i -> person).iterator(),
            () -> IntStream.range(0, rightSize).mapToObj(i -> department).iterator(),
            null,
            resultSize,
            (l, r) -> true
        );

        validate(
            joinType,
            () -> IntStream.range(0, leftSize).mapToObj(i -> person).iterator(),
            () -> IntStream.range(0, rightSize).mapToObj(i -> department).iterator(),
            null,
            0,
            (l, r) -> false
        );
    }

    /** */
    private static int estimateResultSizeForDistinctInputs(JoinRelType joinType, int leftSize, int rightSize) {
        switch (joinType) {
            case SEMI: // Fallthrough
            case INNER:
                return 0;
            case ANTI: // Fallthrough
            case LEFT:
                return leftSize;
            case RIGHT:
                return rightSize;
            case FULL:
                return leftSize + rightSize;
            default:
                throw new IllegalArgumentException("Unsupported join type: " + joinType);
        }
    }

    /** */
    private static int estimateResultSizeForEqualInputs(
        JoinRelType joinType,
        int leftSize,
        int rightSize
    ) {
        switch (joinType) {
            case SEMI:
                return rightSize == 0 ? 0 : leftSize;
            case ANTI:
                return rightSize == 0 ? leftSize : 0;
            case LEFT:
                return rightSize == 0 ? leftSize : leftSize * rightSize;
            case RIGHT:
                return leftSize == 0 ? rightSize : leftSize * rightSize;
            case FULL:
                return leftSize == 0 ? rightSize : rightSize == 0 ? leftSize : leftSize * rightSize;
            case INNER:
                return leftSize * rightSize;
            default:
                throw new IllegalArgumentException("Unsupported join type: " + joinType);
        }
    }

    /** */
    private void validate(
        JoinRelType joinType,
        Iterable<Object[]> leftSrc,
        Iterable<Object[]> rightSrc,
        @Nullable Object[][] expected,
        int resultSize,
        @Nullable BiPredicate<Object[], Object[]> postCondition
    ) {
        ExecutionContext<Object[]> ctx = executionContext();

        IgniteTypeFactory tf = ctx.getTypeFactory();

        RelDataType leftType = TypeUtils.createRowType(tf, Integer.class, String.class, Integer.class);
        RelDataType rightType = TypeUtils.createRowType(tf, Integer.class, String.class);

        ScanNode<Object[]> left = new ScanNode<>(ctx, leftType, leftSrc);
        ScanNode<Object[]> right = new ScanNode<>(ctx, rightType, rightSrc);

        AbstractRightMaterializedJoinNode<Object[]> join = createJoinNode(ctx, joinType, leftType, rightType, postCondition);

        join.register(F.asList(left, right));

        RootNode<Object[]> node = new RootNode<>(ctx, join.rowType());

        node.register(join);

        ArrayList<Object[]> result = new ArrayList<>();

        while (node.hasNext())
            result.add(node.next());

        if (resultSize >= 0)
            assertEquals(resultSize, result.size());

        if (expected != null)
            checkResults(expected, result);
    }

    /** */
    private static HashJoinNode<Object[]> createJoinNode(
        ExecutionContext<Object[]> ctx,
        JoinRelType joinType,
        RelDataType leftType,
        RelDataType rightType,
        @Nullable BiPredicate<Object[], Object[]> postCondition
    ) {
        IgniteTypeFactory tf = ctx.getTypeFactory();

        RelDataType outType = (joinType == ANTI || joinType == SEMI)
            ? leftType
            : TypeUtils.combinedRowType(tf, leftType, rightType);

        return HashJoinNode.create(ctx, outType, leftType, rightType, joinType,
            IgniteJoinInfo.of(ImmutableIntList.of(2), ImmutableIntList.of(0)), postCondition);
    }

    /** */
    private static void checkResults(Object[][] expected, ArrayList<Object[]> actual) {
        assertEquals(expected.length, actual.size());

        actual.sort(Comparator.comparing(r -> (int)r[0]));

        int length = expected.length;

        for (int i = 0; i < length; ++i) {
            Object[] exp = expected[i];
            Object[] act = actual.get(i);

            assertEquals(exp.length, act.length);
            assertEquals(0, F.compareArrays(exp, act));
        }
    }
}

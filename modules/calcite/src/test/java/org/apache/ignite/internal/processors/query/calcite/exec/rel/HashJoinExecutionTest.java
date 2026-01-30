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
import java.util.function.BiPredicate;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.util.ImmutableBitSet;
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
            {2, 2, "Roman"},
            {5, null, "Ivan"},
            {1, 1, "Alexey"},
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
        Object[][] expected = {
            // No rows for cases 0, 1, 2, 10 - empty left side, no rows for cases, 0, 3, 6, 9 - empty right side.
            {1, "Roman4", 4, 4, "SQL4"},
            {1, "Roman5", 5, 5, "SQL5"},
            {1, "Roman5", 5, 5, "QA5"},
            {1, "Roman7", 7, 7, "SQL7"},
            {2, "Ivan7", 7, 7, "SQL7"},
            {1, "Roman8", 8, 8, "SQL8"},
            {1, "Roman8", 8, 8, "QA8"},
            {2, "Ivan8", 8, 8, "SQL8"},
            {2, "Ivan8", 8, 8, "QA8"},
        };

        doTestJoinWithPostFiltration(INNER, expected);
    }

    /** */
    @Test
    public void testLeftJoinWithPostFiltration() {
        Object[][] expected = {
            // Case 0.
            {0, "Igor0", 0, null, null},

            // Case 1.
            {0, "Igor1", 1, null, null},

            // Case 2.
            {0, "Igor2", 2, null, null},

            // Case 3.
            {0, "Igor3", 3, null, null},
            {1, "Roman3", 3, null, null},

            // Case 4.
            {0, "Igor4", 4, null, null},
            {1, "Roman4", 4, 4, "SQL4"},

            // Case 5.
            {0, "Igor5", 5, null, null},
            {1, "Roman5", 5, 5, "SQL5"},
            {1, "Roman5", 5, 5, "QA5"},

            // Case 6.
            {0, "Igor6", 6, null, null},
            {1, "Roman6", 6, null, null},
            {2, "Ivan6", 6, null, null},

            // Case 7.
            {0, "Igor7", 7, null, null},
            {1, "Roman7", 7, 7, "SQL7"},
            {2, "Ivan7", 7, 7, "SQL7"},

            // Case 8.
            {0, "Igor8", 8, null, null},
            {1, "Roman8", 8, 8, "SQL8"},
            {1, "Roman8", 8, 8, "QA8"},
            {2, "Ivan8", 8, 8, "SQL8"},
            {2, "Ivan8", 8, 8, "QA8"},

            // Case 9.
            {0, "Igor9", 9, null, null},
            {1, "Roman9", 9, null, null},
            {2, "Ivan9", 9, null, null},
        };

        doTestJoinWithPostFiltration(LEFT, expected);
    }

    /** */
    @Test
    public void testRightJoinWithPostFiltration() {
        Object[][] expected = {
            // Case 0.
            {null, null, null, 0, "Core0"},

            // Case 1.
            {null, null, null, 1, "Core1"},
            {null, null, null, 1, "SQL1"},

            // Case 2.
            {null, null, null, 2, "Core2"},
            {null, null, null, 2, "SQL2"},
            {null, null, null, 2, "QA2"},

            // Case 3.
            {null, null, null, 3, "Core3"},

            // Case 4.
            {1, "Roman4", 4, 4, "SQL4"},
            {null, null, null, 4, "Core4"},

            // Case 5.
            {1, "Roman5", 5, 5, "SQL5"},
            {1, "Roman5", 5, 5, "QA5"},
            {null, null, null, 5, "Core5"},

            // Case 6.
            {null, null, null, 6, "Core6"},

            // Case 7.
            {1, "Roman7", 7, 7, "SQL7"},
            {2, "Ivan7", 7, 7, "SQL7"},
            {null, null, null, 7, "Core7"},

            // Case 8.
            {1, "Roman8", 8, 8, "SQL8"},
            {1, "Roman8", 8, 8, "QA8"},
            {2, "Ivan8", 8, 8, "SQL8"},
            {2, "Ivan8", 8, 8, "QA8"},
            {null, null, null, 8, "Core8"},

            // Case 10.
            {null, null, null, 10, "Core10"},
            {null, null, null, 10, "SQL10"},
            {null, null, null, 10, "QA10"},
        };

        doTestJoinWithPostFiltration(RIGHT, expected);
    }

    /** */
    @Test
    public void testFullJoinWithPostFiltration() {
        Object[][] expected = {
            // Case 0.
            {0, "Igor0", 0, null, null},
            {null, null, null, 0, "Core0"},

            // Case 1.
            {0, "Igor1", 1, null, null},
            {null, null, null, 1, "Core1"},
            {null, null, null, 1, "SQL1"},

            // Case 2.
            {0, "Igor2", 2, null, null},
            {null, null, null, 2, "Core2"},
            {null, null, null, 2, "SQL2"},
            {null, null, null, 2, "QA2"},

            // Case 3.
            {0, "Igor3", 3, null, null},
            {1, "Roman3", 3, null, null},
            {null, null, null, 3, "Core3"},

            // Case 4.
            {0, "Igor4", 4, null, null},
            {1, "Roman4", 4, 4, "SQL4"},
            {null, null, null, 4, "Core4"},

            // Case 5.
            {0, "Igor5", 5, null, null},
            {1, "Roman5", 5, 5, "SQL5"},
            {1, "Roman5", 5, 5, "QA5"},
            {null, null, null, 5, "Core5"},

            // Case 6.
            {0, "Igor6", 6, null, null},
            {1, "Roman6", 6, null, null},
            {2, "Ivan6", 6, null, null},
            {null, null, null, 6, "Core6"},

            // Case 7.
            {0, "Igor7", 7, null, null},
            {1, "Roman7", 7, 7, "SQL7"},
            {2, "Ivan7", 7, 7, "SQL7"},
            {null, null, null, 7, "Core7"},

            // Case 8.
            {0, "Igor8", 8, null, null},
            {1, "Roman8", 8, 8, "SQL8"},
            {1, "Roman8", 8, 8, "QA8"},
            {2, "Ivan8", 8, 8, "SQL8"},
            {2, "Ivan8", 8, 8, "QA8"},
            {null, null, null, 8, "Core8"},

            // Case 9.
            {0, "Igor9", 9, null, null},
            {1, "Roman9", 9, null, null},
            {2, "Ivan9", 9, null, null},

            // Case 10.
            {null, null, null, 10, "Core10"},
            {null, null, null, 10, "SQL10"},
            {null, null, null, 10, "QA10"},
        };

        doTestJoinWithPostFiltration(FULL, expected);
    }

    /** */
    @Test
    public void testSemiJoinWithPostFiltration() {
        Object[][] expected = {
            // No rows for cases 0, 1, 2, 10 - empty left side, no rows for cases, 0, 3, 6, 9 - empty right side.
            {1, "Roman4", 4},
            {1, "Roman5", 5},
            {1, "Roman7", 7},
            {2, "Ivan7", 7},
            {1, "Roman8", 8},
            {2, "Ivan8", 8},
        };

        doTestJoinWithPostFiltration(SEMI, expected);
    }

    /** */
    @Test
    public void testAntiJoinWithPostFiltration() {
        Object[][] expected = {
            {0, "Igor0", 0},
            {0, "Igor1", 1},
            {0, "Igor2", 2},
            {0, "Igor3", 3},
            {1, "Roman3", 3},
            {0, "Igor4", 4},
            {0, "Igor5", 5},
            {0, "Igor6", 6},
            {1, "Roman6", 6},
            {2, "Ivan6", 6},
            {0, "Igor7", 7},
            {0, "Igor8", 8},
            {0, "Igor9", 9},
            {1, "Roman9", 9},
            {2, "Ivan9", 9},
        };

        doTestJoinWithPostFiltration(ANTI, expected);
    }

    /** */
    private void doTestJoinWithPostFiltration(JoinRelType joinType, Object[][] expected) {
        Object[][] persons = {
            // Case 0: 1 rows on left (-1 filtered) to 1 rows on right (-1 filtered).
            {0, "Igor0", 0},

            // Case 1: 1 rows on left (-1 filtered) to 2 rows on right (-1 filtered).
            {0, "Igor1", 1},

            // Case 2: 1 rows on left (-1 filtered) to 3 rows on right (-1 filtered).
            {0, "Igor2", 2},

            // Case 3: 2 rows on left (-1 filtered) to 1 rows on right (-1 filtered).
            {0, "Igor3", 3},
            {1, "Roman3", 3},

            // Case 4: 2 rows on left (-1 filtered) to 2 rows on right (-1 filtered).
            {0, "Igor4", 4},
            {1, "Roman4", 4},

            // Case 5: 2 rows on left (-1 filtered) to 3 rows on right (-1 filtered).
            {0, "Igor5", 5},
            {1, "Roman5", 5},

            // Case 6: 3 rows on left (-1 filtered) to 1 rows on right (-1 filtered).
            {0, "Igor6", 6},
            {1, "Roman6", 6},
            {2, "Ivan6", 6},

            // Case 7: 3 rows on left (-1 filtered) to 2 rows on right (-1 filtered).
            {0, "Igor7", 7},
            {1, "Roman7", 7},
            {2, "Ivan7", 7},

            // Case 8: 3 rows on left (-1 filtered) to 3 rows on right (-1 filtered).
            {0, "Igor8", 8},
            {1, "Roman8", 8},
            {2, "Ivan8", 8},

            // Case 9: 3 rows on left (-1 filtered) to 0 rows on right.
            {0, "Igor9", 9},
            {1, "Roman9", 9},
            {2, "Ivan9", 9},

            // Case 10: 0 rows on left to 3 rows on right (-1 filtered).
        };

        Object[][] deps = {
            // Case 0: 1 rows on left (-1 filtered) to 1 rows on right (-1 filtered).
            {0, "Core0"},

            // Case 1: 1 rows on left (-1 filtered) to 2 rows on right (-1 filtered).
            {1, "Core1"},
            {1, "SQL1"},

            // Case 2: 1 rows on left (-1 filtered) to 3 rows on right (-1 filtered).
            {2, "Core2"},
            {2, "SQL2"},
            {2, "QA2"},

            // Case 3: 2 rows on left (-1 filtered) to 1 rows on right (-1 filtered).
            {3, "Core3"},

            // Case 4: 2 rows on left (-1 filtered) to 2 rows on right (-1 filtered).
            {4, "Core4"},
            {4, "SQL4"},

            // Case 5: 2 rows on left (-1 filtered) to 3 rows on right (-1 filtered).
            {5, "Core5"},
            {5, "SQL5"},
            {5, "QA5"},

            // Case 6: 3 rows on left (-1 filtered) to 1 rows on right (-1 filtered).
            {6, "Core6"},

            // Case 7: 3 rows on left (-1 filtered) to 2 rows on right (-1 filtered).
            {7, "Core7"},
            {7, "SQL7"},

            // Case 8: 3 rows on left (-1 filtered) to 3 rows on right (-1 filtered).
            {8, "Core8"},
            {8, "SQL8"},
            {8, "QA8"},

            // Case 9: 3 rows on left (-1 filtered) to 0 rows on right.

            // Case 10: 0 rows on left to 3 rows on right (-1 filtered).
            {10, "Core10"},
            {10, "SQL10"},
            {10, "QA10"},
        };

        BiPredicate<Object[], Object[]> condition =
            (l, r) -> !((String)l[1]).startsWith("Igor") && !((String)r[1]).startsWith("Core");

        validate(joinType, Stream.of(persons)::iterator, Stream.of(deps)::iterator, expected, -1, condition);
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

        IgniteJoinInfo joinInfo = new IgniteJoinInfo(ImmutableIntList.of(2), ImmutableIntList.of(0),
            ImmutableBitSet.of(), ImmutableList.of());

        return HashJoinNode.create(ctx, outType, leftType, rightType, joinType, joinInfo, postCondition);
    }

    /** */
    private static void checkResults(Object[][] expected, ArrayList<Object[]> actual) {
        assertEquals(expected.length, actual.size());

        actual.sort(F::compareArrays);
        Arrays.sort(expected, F::compareArrays);

        int length = expected.length;

        for (int i = 0; i < length; ++i) {
            Object[] exp = expected[i];
            Object[] act = actual.get(i);

            assertEqualsArraysAware(exp, act);
        }
    }
}

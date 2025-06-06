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

import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.util.TypeUtils;
import org.apache.ignite.internal.util.typedef.F;
import org.junit.Test;

import static org.apache.calcite.rel.core.JoinRelType.ANTI;
import static org.apache.calcite.rel.core.JoinRelType.FULL;
import static org.apache.calcite.rel.core.JoinRelType.RIGHT;
import static org.apache.calcite.rel.core.JoinRelType.SEMI;

/** */
@SuppressWarnings("TypeMayBeWeakened")
public class HashJoinExecutionTest extends AbstractExecutionTest {
    /** */
    @Test
    public void testEquiJoinWithDifferentBufferSize() {
        for (JoinRelType joinType : JoinRelType.values()) {
            validateEquiJoin(executionContext(), joinType, 0, 0);
            validateEquiJoin(executionContext(), joinType, 0, 1);
            validateEquiJoin(executionContext(), joinType, 0, 10);
            validateEquiJoin(executionContext(), joinType, 1, 0);
            validateEquiJoin(executionContext(), joinType, 1, 1);
            validateEquiJoin(executionContext(), joinType, 1, 10);
            validateEquiJoin(executionContext(), joinType, 10, 0);
            validateEquiJoin(executionContext(), joinType, 10, 1);
            validateEquiJoin(executionContext(), joinType, 10, 10);

            int testSize = IN_BUFFER_SIZE;

            validateEquiJoin(executionContext(), joinType, 0, testSize - 1);
            validateEquiJoin(executionContext(), joinType, 0, testSize);
            validateEquiJoin(executionContext(), joinType, 0, testSize + 1);

            validateEquiJoin(executionContext(), joinType, testSize - 1, 0);
            validateEquiJoin(executionContext(), joinType, testSize - 1, testSize - 1);
            validateEquiJoin(executionContext(), joinType, testSize - 1, testSize);
            validateEquiJoin(executionContext(), joinType, testSize - 1, testSize + 1);

            validateEquiJoin(executionContext(), joinType, testSize, 0);
            validateEquiJoin(executionContext(), joinType, testSize, testSize - 1);
            validateEquiJoin(executionContext(), joinType, testSize, testSize);
            validateEquiJoin(executionContext(), joinType, testSize, testSize + 1);

            validateEquiJoin(executionContext(), joinType, testSize + 1, 0);
            validateEquiJoin(executionContext(), joinType, testSize + 1, testSize - 1);
            validateEquiJoin(executionContext(), joinType, testSize + 1, testSize);
            validateEquiJoin(executionContext(), joinType, testSize + 1, testSize + 1);

            validateEquiJoin(executionContext(), joinType, 2 * testSize, 0);
            validateEquiJoin(executionContext(), joinType, 0, 2 * testSize);
            validateEquiJoin(executionContext(), joinType, 2 * testSize, 2 * testSize);
        }
    }

//    /** */
//    @Test
//    public void nonEquiJoinWithDifferentBufferSize() {
//        for (JoinRelType joinType : F.asList(INNER, SEMI)) {
//            validateNonEquiJoin(executionContext(), joinType, 0, 0);
//            validateNonEquiJoin(executionContext(), joinType, 0, 1);
//            validateNonEquiJoin(executionContext(), joinType, 0, 10);
//            validateNonEquiJoin(executionContext(), joinType, 1, 0);
//            validateNonEquiJoin(executionContext(), joinType, 1, 1);
//            validateNonEquiJoin(executionContext(), joinType, 1, 10);
//            validateNonEquiJoin(executionContext(), joinType, 10, 0);
//            validateNonEquiJoin(executionContext(), joinType, 10, 1);
//            validateNonEquiJoin(executionContext(), joinType, 10, 10);
//
//            int testSize = IN_BUFFER_SIZE;
//
//            validateNonEquiJoin(executionContext(), joinType, 0, testSize - 1);
//            validateNonEquiJoin(executionContext(), joinType, 0, testSize);
//            validateNonEquiJoin(executionContext(), joinType, 0, testSize + 1);
//
//            validateNonEquiJoin(executionContext(), joinType, testSize - 1, 0);
//            validateNonEquiJoin(executionContext(), joinType, testSize - 1, testSize - 1);
//            validateNonEquiJoin(executionContext(), joinType, testSize - 1, testSize);
//            validateNonEquiJoin(executionContext(), joinType, testSize - 1, testSize + 1);
//
//            validateNonEquiJoin(executionContext(), joinType, testSize, 0);
//            validateNonEquiJoin(executionContext(), joinType, testSize, testSize - 1);
//            validateNonEquiJoin(executionContext(), joinType, testSize, testSize);
//            validateNonEquiJoin(executionContext(), joinType, testSize, testSize + 1);
//
//            validateNonEquiJoin(executionContext(), joinType, testSize + 1, 0);
//            validateNonEquiJoin(executionContext(), joinType, testSize + 1, testSize - 1);
//            validateNonEquiJoin(executionContext(), joinType, testSize + 1, testSize);
//            validateNonEquiJoin(executionContext(), joinType, testSize + 1, testSize + 1);
//
//            validateNonEquiJoin(executionContext(), joinType, 2 * testSize, 0);
//            validateNonEquiJoin(executionContext(), joinType, 0, 2 * testSize);
//            validateNonEquiJoin(executionContext(), joinType, 2 * testSize, 2 * testSize);
//        }
//    }

    /** */
    private void validateEquiJoin(ExecutionContext<Object[]> ctx, JoinRelType joinType, int leftSize, int rightSize) {
        if (log.isInfoEnabled()) {
            log.info("Testing eq. join with different buffer sizes. Join type: " + joinType + ", leftSz: "
                + leftSize + ", rightSz: " + rightSize);
        }

        IgniteTypeFactory tf = ctx.getTypeFactory();

        { // Distinct inputs
            Object[] person = {1, "name", 2};
            Object[] department = {1, "department"};

            int resultSize = estimateResultSizeForDistinctInputs(joinType, leftSize, rightSize);

            RelDataType leftType = TypeUtils.createRowType(tf, Integer.class, String.class, Integer.class);
            RelDataType righType = TypeUtils.createRowType(tf, Integer.class, String.class);

            validate(
                ctx,
                joinType,
                () -> IntStream.range(0, leftSize).mapToObj(i -> person).iterator(),
                leftType,
                () -> IntStream.range(0, rightSize).mapToObj(i -> department).iterator(),
                righType,
                resultSize
            );
        }

//        { // Matching inputs
//            Object[] person = {1, "name", 2};
//            Object[] department = {2, "department"};
//
//            int resultSize = estimateResultSizeForEqualInputs(joinType, leftSize, rightSize);
//
//            validate(
//                ctx,
//                joinType,
//                () -> IntStream.range(0, leftSize).mapToObj(i -> person).iterator(),
//                () -> IntStream.range(0, rightSize).mapToObj(i -> department).iterator(),
//                resultSize
//            );
//        }
    }

//    /** */
//    protected void validateNonEquiJoin(ExecutionContext<Object[]> ctx, JoinRelType joinType, int leftSize, int rightSize) {
//        Object[] person = {1, "name", 2};
//        Object[] department = {2, "department"};
//
//        int resultSize = estimateResultSizeForEqualInputs(joinType, leftSize, rightSize);
//
//        validate(
//            ctx,
//            joinType,
//            () -> IntStream.range(0, leftSize).mapToObj(i -> person).iterator(),
//            () -> IntStream.range(0, rightSize).mapToObj(i -> department).iterator(),
//            resultSize
//        );
//
//        validate(
//            ctx,
//            joinType,
//            () -> IntStream.range(0, leftSize).mapToObj(i -> person).iterator(),
//            () -> IntStream.range(0, rightSize).mapToObj(i -> department).iterator(),
//            0
//        );
//    }

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
        ExecutionContext<Object[]> ctx,
        JoinRelType joinType,
        Iterable<Object[]> leftSrc,
        RelDataType leftType,
        Iterable<Object[]> rightSrc,
        RelDataType rightType,
        int resultSize
    ) {
        ScanNode<Object[]> left = new ScanNode<>(ctx, leftType, leftSrc);
        ScanNode<Object[]> right = new ScanNode<>(ctx, rightType, rightSrc);

        AbstractRightMaterializedJoinNode<Object[]> join = createJoinNode(ctx, joinType);

        join.register(F.asList(left, right));

        RootNode<Object[]> node = new RootNode<>(ctx, join.rowType());

        node.register(join);

        long cnt = StreamSupport.stream(Spliterators.spliteratorUnknownSize(node, Spliterator.ORDERED), false).count();

        assertEquals(resultSize, cnt);
    }

    /** */
    private static HashJoinNode<Object[]> createJoinNode(ExecutionContext<Object[]> ctx, JoinRelType joinType) {
        IgniteTypeFactory tf = ctx.getTypeFactory();

        RelDataType leftType = TypeUtils.createRowType(tf, tf.createSqlType(SqlTypeName.INTEGER),
            tf.createSqlType(SqlTypeName.VARCHAR));

        RelDataType rightType = TypeUtils.createRowType(tf, tf.createSqlType(SqlTypeName.INTEGER),
            tf.createSqlType(SqlTypeName.VARCHAR), tf.createSqlType(SqlTypeName.INTEGER));

        RelDataType outType = (joinType == ANTI || joinType == SEMI)
            ? leftType
            : TypeUtils.combinedRowType(tf, leftType, rightType);

        return HashJoinNode.create(ctx, outType, leftType, rightType, joinType,
            JoinInfo.of(ImmutableIntList.of(2), ImmutableIntList.of(0)));
    }
}

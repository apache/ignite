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
import java.util.BitSet;
import java.util.List;
import java.util.function.BiPredicate;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.RowHandler;

/** */
public abstract class NestedLoopJoinNode<Row> extends AbstractRightMaterializedJoinNode<Row> {
    /** */
    private static final int HALF_BUF_SIZE = IN_BUFFER_SIZE >> 1;

    /** */
    protected final BiPredicate<Row, Row> cond;

    /** */
    protected final RowHandler<Row> rowHnd;

    /** */
    protected final List<Row> rightMaterialized = new ArrayList<>(IN_BUFFER_SIZE);

    /**
     * @param ctx Execution context.
     * @param rowType Row type.
     * @param cond Join expression.
     */
    private NestedLoopJoinNode(ExecutionContext<Row> ctx, RelDataType rowType, BiPredicate<Row, Row> cond) {
        super(ctx, rowType);

        this.cond = cond;
        rowHnd = ctx.rowHandler();
    }

    /** {@inheritDoc} */
    @Override protected void rewindInternal() {
        super.rewindInternal();

        rightMaterialized.clear();
    }

    /** {@inheritDoc} */
    @Override protected void pushRight(Row row) throws Exception {
        assert downstream() != null;
        assert waitingRight > 0;

        checkState();

        nodeMemoryTracker.onRowAdded(row);

        waitingRight--;

        rightMaterialized.add(row);

        if (waitingRight == 0)
            rightSource().request(waitingRight = IN_BUFFER_SIZE);
    }

    /** */
    public static <Row> NestedLoopJoinNode<Row> create(
        ExecutionContext<Row> ctx,
        RelDataType outputRowType,
        RelDataType leftRowType,
        RelDataType rightRowType,
        JoinRelType joinType,
        BiPredicate<Row, Row> cond
    ) {
        switch (joinType) {
            case INNER:
                return new InnerJoin<>(ctx, outputRowType, cond);

            case LEFT: {
                RowHandler.RowFactory<Row> rightRowFactory = ctx.rowHandler().factory(ctx.getTypeFactory(), rightRowType);

                return new LeftJoin<>(ctx, outputRowType, cond, rightRowFactory);
            }

            case RIGHT: {
                RowHandler.RowFactory<Row> leftRowFactory = ctx.rowHandler().factory(ctx.getTypeFactory(), leftRowType);

                return new RightJoin<>(ctx, outputRowType, cond, leftRowFactory);
            }

            case FULL: {
                RowHandler.RowFactory<Row> leftRowFactory = ctx.rowHandler().factory(ctx.getTypeFactory(), leftRowType);
                RowHandler.RowFactory<Row> rightRowFactory = ctx.rowHandler().factory(ctx.getTypeFactory(), rightRowType);

                return new FullOuterJoin<>(ctx, outputRowType, cond, leftRowFactory, rightRowFactory);
            }

            case SEMI:
                return new SemiJoin<>(ctx, outputRowType, cond);

            case ANTI:
                return new AntiJoin<>(ctx, outputRowType, cond);

            default:
                throw new IllegalArgumentException("Join type '" + joinType + "' is not supported.");
        }
    }

    /** */
    private static class InnerJoin<Row> extends NestedLoopJoinNode<Row> {
        /** */
        private int rightIdx;

        /** */
        public InnerJoin(ExecutionContext<Row> ctx, RelDataType rowType, BiPredicate<Row, Row> cond) {
            super(ctx, rowType, cond);
        }

        /** {@inheritDoc} */
        @Override protected void rewindInternal() {
            super.rewindInternal();

            rightIdx = 0;
        }

        /** {@inheritDoc} */
        @Override protected void join() throws Exception {
            if (waitingRight == NOT_WAITING) {
                inLoop = true;
                try {
                    while (requested > 0 && (left != null || !leftInBuf.isEmpty())) {
                        if (left == null)
                            left = leftInBuf.remove();

                        while (requested > 0 && rightIdx < rightMaterialized.size()) {
                            checkState();

                            if (!cond.test(left, rightMaterialized.get(rightIdx++)))
                                continue;

                            requested--;
                            Row row = handler.concat(left, rightMaterialized.get(rightIdx - 1));
                            downstream().push(row);
                        }

                        if (rightIdx == rightMaterialized.size()) {
                            left = null;
                            rightIdx = 0;
                        }
                    }
                }
                finally {
                    inLoop = false;
                }
            }

            if (checkJoinFinished())
                return;

            tryToRequestInputs();
        }
    }

    /** */
    private static class LeftJoin<Row> extends NestedLoopJoinNode<Row> {
        /** Right row factory. */
        private final RowHandler.RowFactory<Row> rightRowFactory;

        /** Shows whether current left row was matched. */
        private boolean matched;

        /** */
        private int rightIdx;

        /** */
        public LeftJoin(
            ExecutionContext<Row> ctx,
            RelDataType rowType,
            BiPredicate<Row, Row> cond,
            RowHandler.RowFactory<Row> rightRowFactory
        ) {
            super(ctx, rowType, cond);

            this.rightRowFactory = rightRowFactory;
        }

        /** {@inheritDoc} */
        @Override protected void rewindInternal() {
            super.rewindInternal();

            matched = false;
            rightIdx = 0;
        }

        /** {@inheritDoc} */
        @Override protected void join() throws Exception {
            if (waitingRight == NOT_WAITING) {
                inLoop = true;
                try {
                    while (requested > 0 && (left != null || !leftInBuf.isEmpty())) {
                        if (left == null) {
                            left = leftInBuf.remove();

                            matched = false;
                        }

                        while (requested > 0 && rightIdx < rightMaterialized.size()) {
                            checkState();

                            if (!cond.test(left, rightMaterialized.get(rightIdx++)))
                                continue;

                            requested--;
                            matched = true;

                            Row row = handler.concat(left, rightMaterialized.get(rightIdx - 1));
                            downstream().push(row);
                        }

                        if (rightIdx == rightMaterialized.size()) {
                            boolean wasPushed = false;

                            if (!matched && requested > 0) {
                                requested--;
                                wasPushed = true;

                                downstream().push(rowHnd.concat(left, rightRowFactory.create()));
                            }

                            if (matched || wasPushed) {
                                left = null;
                                rightIdx = 0;
                            }
                        }
                    }
                }
                finally {
                    inLoop = false;
                }
            }

            if (checkJoinFinished())
                return;

            tryToRequestInputs();
        }
    }

    /** */
    private static class RightJoin<Row> extends NestedLoopJoinNode<Row> {
        /** Left row factory. */
        private final RowHandler.RowFactory<Row> leftRowFactory;

        /** */
        private BitSet rightNotMatchedIndexes;

        /** */
        private int lastPushedInd;

        /** */
        private int rightIdx;

        /** */
        public RightJoin(
            ExecutionContext<Row> ctx,
            RelDataType rowType,
            BiPredicate<Row, Row> cond,
            RowHandler.RowFactory<Row> leftRowFactory
        ) {
            super(ctx, rowType, cond);

            this.leftRowFactory = leftRowFactory;
        }

        /** {@inheritDoc} */
        @Override protected void rewindInternal() {
            super.rewindInternal();

            rightNotMatchedIndexes.clear();
            lastPushedInd = 0;
            rightIdx = 0;
        }

        /** {@inheritDoc} */
        @Override protected void join() throws Exception {
            if (waitingRight == NOT_WAITING) {
                if (rightNotMatchedIndexes == null) {
                    rightNotMatchedIndexes = new BitSet(rightMaterialized.size());

                    rightNotMatchedIndexes.set(0, rightMaterialized.size());
                }

                inLoop = true;
                try {
                    while (requested > 0 && (left != null || !leftInBuf.isEmpty())) {
                        if (left == null)
                            left = leftInBuf.remove();

                        while (requested > 0 && rightIdx < rightMaterialized.size()) {
                            checkState();

                            Row right = rightMaterialized.get(rightIdx++);

                            if (!cond.test(left, right))
                                continue;

                            requested--;
                            rightNotMatchedIndexes.clear(rightIdx - 1);

                            Row joined = handler.concat(left, right);
                            downstream().push(joined);
                        }

                        if (rightIdx == rightMaterialized.size()) {
                            left = null;
                            rightIdx = 0;
                        }
                    }
                }
                finally {
                    inLoop = false;
                }
            }

            if (waitingLeft == NOT_WAITING && requested > 0 && (rightNotMatchedIndexes != null && !rightNotMatchedIndexes.isEmpty())) {
                assert lastPushedInd >= 0;

                inLoop = true;
                try {
                    for (lastPushedInd = rightNotMatchedIndexes.nextSetBit(lastPushedInd);;
                        lastPushedInd = rightNotMatchedIndexes.nextSetBit(lastPushedInd + 1)
                    ) {
                        checkState();

                        if (lastPushedInd < 0)
                            break;

                        Row row = rowHnd.concat(leftRowFactory.create(), rightMaterialized.get(lastPushedInd));

                        rightNotMatchedIndexes.clear(lastPushedInd);

                        requested--;
                        downstream().push(row);

                        if (lastPushedInd == Integer.MAX_VALUE || requested <= 0)
                            break;
                    }
                }
                finally {
                    inLoop = false;
                }
            }

            if ((rightNotMatchedIndexes == null || rightNotMatchedIndexes.isEmpty()) && checkJoinFinished())
                return;

            tryToRequestInputs();
        }
    }

    /** */
    private static class FullOuterJoin<Row> extends NestedLoopJoinNode<Row> {
        /** Left row factory. */
        private final RowHandler.RowFactory<Row> leftRowFactory;

        /** Right row factory. */
        private final RowHandler.RowFactory<Row> rightRowFactory;

        /** Shows whether current left row was matched. */
        private boolean leftMatched;

        /** */
        private BitSet rightNotMatchedIndexes;

        /** */
        private int lastPushedInd;

        /** */
        private Row left;

        /** */
        private int rightIdx;

        /** */
        public FullOuterJoin(
            ExecutionContext<Row> ctx,
            RelDataType rowType,
            BiPredicate<Row, Row> cond,
            RowHandler.RowFactory<Row> leftRowFactory,
            RowHandler.RowFactory<Row> rightRowFactory
        ) {
            super(ctx, rowType, cond);

            this.leftRowFactory = leftRowFactory;
            this.rightRowFactory = rightRowFactory;
        }

        /** {@inheritDoc} */
        @Override protected void rewindInternal() {
            super.rewindInternal();

            left = null;
            leftMatched = false;
            rightNotMatchedIndexes.clear();
            lastPushedInd = 0;
            rightIdx = 0;
        }

        /** {@inheritDoc} */
        @Override protected void join() throws Exception {
            if (waitingRight == NOT_WAITING) {
                if (rightNotMatchedIndexes == null) {
                    rightNotMatchedIndexes = new BitSet(rightMaterialized.size());

                    rightNotMatchedIndexes.set(0, rightMaterialized.size());
                }

                inLoop = true;
                try {
                    while (requested > 0 && (left != null || !leftInBuf.isEmpty())) {
                        if (left == null) {
                            left = leftInBuf.remove();

                            leftMatched = false;
                        }

                        while (requested > 0 && rightIdx < rightMaterialized.size()) {
                            checkState();

                            Row right = rightMaterialized.get(rightIdx++);

                            if (!cond.test(left, right))
                                continue;

                            requested--;
                            leftMatched = true;
                            rightNotMatchedIndexes.clear(rightIdx - 1);

                            Row joined = handler.concat(left, right);
                            downstream().push(joined);
                        }

                        if (rightIdx == rightMaterialized.size()) {
                            boolean wasPushed = false;

                            if (!leftMatched && requested > 0) {
                                requested--;
                                wasPushed = true;

                                downstream().push(rowHnd.concat(left, rightRowFactory.create()));
                            }

                            if (leftMatched || wasPushed) {
                                left = null;
                                rightIdx = 0;
                            }
                        }
                    }
                }
                finally {
                    inLoop = false;
                }
            }

            if (waitingLeft == NOT_WAITING && requested > 0 && (rightNotMatchedIndexes != null && !rightNotMatchedIndexes.isEmpty())) {
                assert lastPushedInd >= 0;

                inLoop = true;
                try {
                    for (lastPushedInd = rightNotMatchedIndexes.nextSetBit(lastPushedInd);;
                        lastPushedInd = rightNotMatchedIndexes.nextSetBit(lastPushedInd + 1)
                    ) {
                        checkState();

                        if (lastPushedInd < 0)
                            break;

                        Row row = rowHnd.concat(leftRowFactory.create(), rightMaterialized.get(lastPushedInd));

                        rightNotMatchedIndexes.clear(lastPushedInd);

                        requested--;
                        downstream().push(row);

                        if (lastPushedInd == Integer.MAX_VALUE || requested <= 0)
                            break;
                    }
                }
                finally {
                    inLoop = false;
                }
            }

            if ((rightNotMatchedIndexes == null || rightNotMatchedIndexes.isEmpty()) && checkJoinFinished())
                return;

            tryToRequestInputs();
        }
    }

    /** */
    private static class SemiJoin<Row> extends NestedLoopJoinNode<Row> {
        /** */
        private int rightIdx;

        /** */
        public SemiJoin(ExecutionContext<Row> ctx, RelDataType rowType, BiPredicate<Row, Row> cond) {
            super(ctx, rowType, cond);
        }

        /** {@inheritDoc} */
        @Override protected void rewindInternal() {
            super.rewindInternal();

            rightIdx = 0;
        }

        /** {@inheritDoc} */
        @Override protected void join() throws Exception {
            if (waitingRight == NOT_WAITING) {
                while (requested > 0 && (left != null || !leftInBuf.isEmpty())) {
                    if (left == null)
                        left = leftInBuf.remove();

                    boolean matched = false;

                    while (!matched && requested > 0 && rightIdx < rightMaterialized.size()) {
                        checkState();

                        if (!cond.test(left, rightMaterialized.get(rightIdx++)))
                            continue;

                        requested--;
                        downstream().push(left);

                        matched = true;
                    }

                    if (matched || rightIdx == rightMaterialized.size()) {
                        left = null;
                        rightIdx = 0;
                    }
                }
            }

            if (checkJoinFinished())
                return;

            tryToRequestInputs();
        }
    }

    /** */
    private static class AntiJoin<Row> extends NestedLoopJoinNode<Row> {
        /** */
        private int rightIdx;

        /** */
        public AntiJoin(ExecutionContext<Row> ctx, RelDataType rowType, BiPredicate<Row, Row> cond) {
            super(ctx, rowType, cond);
        }

        /** {@inheritDoc} */
        @Override protected void rewindInternal() {
            super.rewindInternal();

            rightIdx = 0;
        }

        /** {@inheritDoc} */
        @Override protected void join() throws Exception {
            if (waitingRight == NOT_WAITING) {
                inLoop = true;
                try {
                    while (requested > 0 && (left != null || !leftInBuf.isEmpty())) {
                        if (left == null)
                            left = leftInBuf.remove();

                        boolean matched = false;

                        while (!matched && rightIdx < rightMaterialized.size()) {
                            checkState();

                            if (cond.test(left, rightMaterialized.get(rightIdx++)))
                                matched = true;
                        }

                        if (!matched) {
                            requested--;
                            downstream().push(left);
                        }

                        left = null;
                        rightIdx = 0;
                    }
                }
                finally {
                    inLoop = false;
                }
            }

            if (checkJoinFinished())
                return;

            tryToRequestInputs();
        }
    }

    /** */
    protected boolean checkJoinFinished() throws Exception {
        if (requested > 0 && waitingLeft == NOT_WAITING && waitingRight == NOT_WAITING && left == null && leftInBuf.isEmpty()) {
            requested = 0;
            rightMaterialized.clear();

            downstream().end();

            return true;
        }

        return false;
    }

    /** */
    protected void tryToRequestInputs() throws Exception {
        if (waitingLeft == 0 && leftInBuf.size() <= HALF_BUF_SIZE)
            leftSource().request(waitingLeft = IN_BUFFER_SIZE - leftInBuf.size());

        if (waitingRight == 0 && requested > 0)
            rightSource().request(waitingRight = IN_BUFFER_SIZE);
    }
}

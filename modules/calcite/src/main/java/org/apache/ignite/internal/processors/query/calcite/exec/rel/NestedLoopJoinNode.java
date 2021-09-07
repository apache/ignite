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

import java.util.ArrayDeque;
import java.util.BitSet;
import java.util.Deque;
import java.util.function.BiPredicate;

import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.RowHandler;
import org.apache.ignite.internal.util.typedef.F;
import org.jetbrains.annotations.NotNull;

/** */
public abstract class NestedLoopJoinNode<Row> extends AbstractNode<Row> {
    /** */
    protected final BiPredicate<Row, Row> cond;

    /** */
    protected final RowHandler<Row> handler;

    /** */
    protected int requested;

    /** */
    protected int waitingLeft;

    /** */
    protected int waitingRight;

    /** */
    protected final Deque<Row> leftInBuf = new ArrayDeque<>(IN_BUFFER_SIZE);

    /** */
    protected final Deque<Row> rightInBuf = new ArrayDeque<>(IN_BUFFER_SIZE);

    /** */
    protected boolean inLoop;

    /**
     * @param ctx Execution context.
     * @param cond Join expression.
     */
    private NestedLoopJoinNode(ExecutionContext<Row> ctx, RelDataType rowType, BiPredicate<Row, Row> cond) {
        super(ctx, rowType);

        this.cond = cond;
        handler = ctx.rowHandler();
    }

    /** {@inheritDoc} */
    @Override public void request(int rowsCnt) throws Exception {
        assert !F.isEmpty(sources()) && sources().size() == 2;
        assert rowsCnt > 0 && requested == 0;

        checkState();

        requested = rowsCnt;

        if (!inLoop)
            context().execute(this::doJoin, this::onError);
    }

    /** */
    private void doJoin() throws Exception {
        checkState();

        join();
    }

    /** {@inheritDoc} */
    @Override protected void rewindInternal() {
        requested = 0;
        waitingLeft = 0;
        waitingRight = 0;

        leftInBuf.clear();
        rightInBuf.clear();
    }

    /** {@inheritDoc} */
    @Override protected Downstream<Row> requestDownstream(int idx) {
        if (idx == 0)
            return new Downstream<Row>() {
                /** {@inheritDoc} */
                @Override public void push(Row row) throws Exception {
                    pushLeft(row);
                }

                /** {@inheritDoc} */
                @Override public void end() throws Exception {
                    endLeft();
                }

                /** {@inheritDoc} */
                @Override public void onError(Throwable e) {
                    NestedLoopJoinNode.this.onError(e);
                }
            };
        else if (idx == 1)
            return new Downstream<Row>() {
                /** {@inheritDoc} */
                @Override public void push(Row row) throws Exception {
                    pushRight(row);
                }

                /** {@inheritDoc} */
                @Override public void end() throws Exception {
                    endRight();
                }

                /** {@inheritDoc} */
                @Override public void onError(Throwable e) {
                    NestedLoopJoinNode.this.onError(e);
                }
            };

        throw new IndexOutOfBoundsException();
    }

    /** */
    private void pushLeft(Row row) throws Exception {
        assert downstream() != null;
        assert waitingLeft > 0;

        waitingLeft--;

        leftInBuf.add(row);

        doJoin();
    }

    /** */
    private void pushRight(Row row) throws Exception {
        assert downstream() != null;
        assert waitingRight > 0;

        rightInBuf.add(row);

        if (--waitingRight == 0)
            doJoin();
    }

    /** */
    private void endLeft() throws Exception {
        assert downstream() != null;
        assert waitingLeft > 0;

        checkState();

        waitingLeft = NOT_WAITING;

        join();
    }

    /** */
    private void endRight() throws Exception {
        assert downstream() != null;
        assert waitingRight > 0;

        checkState();

        waitingRight = NOT_WAITING;

        join();
    }

    /** */
    protected void requestRight() throws Exception {
        rightSource().request(waitingRight = IN_BUFFER_SIZE);
    }

    /** */
    protected void requestLeft() throws Exception {
        leftSource().request(waitingLeft = IN_BUFFER_SIZE);
    }

    /** */
    protected Node<Row> leftSource() {
        return sources().get(0);
    }

    /** */
    protected Node<Row> rightSource() {
        return sources().get(1);
    }

    /** */
    protected abstract void join() throws Exception;

    /** */
    @NotNull public static <Row> NestedLoopJoinNode<Row> create(ExecutionContext<Row> ctx, RelDataType outputRowType,
        RelDataType leftRowType, RelDataType rightRowType, JoinRelType joinType, BiPredicate<Row, Row> cond) {
        switch (joinType) {
            case INNER:
            case SEMI:
            case ANTI:
                return new InnerJoin<>(ctx, outputRowType, cond, joinType);

            case LEFT: {
                RowHandler.RowFactory<Row> rightRowFactory = ctx.rowHandler().factory(ctx.getTypeFactory(), rightRowType);

                return new LeftJoin<>(ctx, outputRowType, cond, rightRowFactory);
            }

            case FULL:
            case RIGHT: {
                RowHandler.RowFactory<Row> leftRowFactory = ctx.rowHandler().factory(ctx.getTypeFactory(), leftRowType);
                RowHandler.RowFactory<Row> rightRowFactory = ctx.rowHandler().factory(ctx.getTypeFactory(), rightRowType);

                return new RightOrFullJoin<>(ctx, outputRowType, cond, leftRowFactory, rightRowFactory, joinType);
            }

            default:
                throw new IllegalStateException("Join type \"" + joinType + "\" is not supported yet");
        }
    }

    /** */
    private static class InnerJoin<Row> extends NestedLoopJoinNode<Row> {
        /** */
        private Row left;

        /** */
        private Row right;

        /** */
        private final JoinRelType joinType;

        /** Whether current left row was matched or not. */
        private boolean matched;

        /**
         * @param ctx Execution context.
         * @param cond Join expression.
         * @param joinType Join operation type.
         */
        public InnerJoin(
            ExecutionContext<Row> ctx,
            RelDataType rowType,
            BiPredicate<Row, Row> cond,
            JoinRelType joinType
        ) {
            super(ctx, rowType, cond);

            this.joinType = joinType;
        }

        /** {@inheritDoc} */
        @Override protected void rewindInternal() {
            left = null;
            right = null;

            super.rewindInternal();
        }

        /** */
        private boolean leftIsEmpty() {
            return left == null && F.isEmpty(leftInBuf);
        }

        /** */
        private boolean rightIsEmptying() {
            return waitingRight == NOT_WAITING && F.isEmpty(rightInBuf);
        }

        /** */
        @Override protected void join() throws Exception {
            inLoop = true;

            try {
                while (requested > 0 && !leftIsEmpty() && !F.isEmpty(rightInBuf)) {
                    checkState();

                    if (left == null)
                        left = leftInBuf.remove();

                    right = rightInBuf.remove();

                    if (!cond.test(left, right))
                        continue;

                    matched = true;

                    if (joinType == JoinRelType.SEMI) {
                        requested--;
                        downstream().push(left);
                        left = null;
                    } else if (joinType == JoinRelType.INNER) {
                        requested--;

                        final Row row = handler.concat(left, right);
                        
                        downstream().push(row);
                    }
                }

                if (waitingRight == NOT_WAITING && left != null && joinType == JoinRelType.ANTI) {
                    if (!matched && requested > 0) {
                        requested--;

                        downstream().push(left);
                    }
                }
            } finally {
                inLoop = false;
            }

            if (requested > 0) {
                if (waitingLeft == 0 && F.isEmpty(leftInBuf))
                    requestLeft();

                if (waitingRight == 0 && F.isEmpty(rightInBuf))
                    requestRight();
            }

            // rewind right if left is not empty.
            if (requested > 0 && rightIsEmptying() && !leftIsEmpty()) {
                // if left == null seems no progress and right is empty.
                if (left != null) {
                    left = null;
                    matched = false;
                    rightSource().rewind();
                    requestRight();
                }
            }

            // empty right side, only for ANTI join purposes.
            if (requested > 0 && joinType == JoinRelType.ANTI &&
                rightIsEmptying() && left == null && !F.isEmpty(leftInBuf)) {
                while (requested > 0 && !F.isEmpty(leftInBuf)) {
                    requested--;

                    left = leftInBuf.remove();

                    downstream().push(left);

                    left = null;
                }
            }

            // complete if:
            // 1) left is empty.
            // 2) right is empty and not ANTI.
            // 3) ANTI and left is empty.
            if (requested > 0) {
                if (((waitingLeft == NOT_WAITING && leftIsEmpty()) ||
                    (!F.isEmpty(leftInBuf) && rightIsEmptying() && left == null && joinType != JoinRelType.ANTI) ||
                    (joinType == JoinRelType.ANTI && waitingLeft == NOT_WAITING && F.isEmpty(leftInBuf) &&
                        (waitingRight == NOT_WAITING || !F.isEmpty(rightInBuf))))) {
                    requested = 0;
                    downstream().end();
                }
            }
        }
    }

    /** */
    private static class LeftJoin<Row> extends NestedLoopJoinNode<Row> {
        /** Right row factory. */
        private final RowHandler.RowFactory<Row> rightRowFactory;

        /** Whether current left row was matched or not. */
        private boolean matched;

        /** */
        private Row left;

        /** */
        private Row right;

        /**
         * @param ctx Execution context.
         * @param cond Join expression.
         */
        public LeftJoin(
            ExecutionContext<Row> ctx,
            RelDataType rowType,
            BiPredicate<Row, Row> cond,
            RowHandler.RowFactory<Row> rightRowFactory
        ) {
            super(ctx, rowType, cond);

            this.rightRowFactory = rightRowFactory;
        }

        /** */
        @Override protected void rewindInternal() {
            matched = false;
            left = null;
            right = null;

            super.rewindInternal();
        }

        /** {@inheritDoc} */
        @Override protected void join() throws Exception {
            inLoop = true;
            try {
                while (requested > 0 && (left != null || !F.isEmpty(leftInBuf)) && !F.isEmpty(rightInBuf)) {
                    checkState();

                    if (left == null)
                        left = leftInBuf.remove();

                    right = rightInBuf.remove();

                    if (!cond.test(left, right))
                        continue;

                    final Row row = handler.concat(left, right);

                    requested--;
                    matched = true;
                    downstream().push(row);
                }

                if (waitingRight == NOT_WAITING && left != null) {
                    if (!matched && requested > 0) {
                        requested--;

                        downstream().push(handler.concat(left, rightRowFactory.create()));
                    }
                }
            }
            finally {
                inLoop = false;
            }

            if (requested > 0 && waitingLeft == 0 && F.isEmpty(leftInBuf))
                requestLeft();

            if (requested > 0 && (left != null || !F.isEmpty(leftInBuf))) {
                if (waitingRight == NOT_WAITING) {
                    // no progress, seems right is empty.
                    if (left == null) {
                        while (requested > 0 && !F.isEmpty(leftInBuf)) {
                            left = leftInBuf.remove();
                            requested--;
                            downstream().push(handler.concat(left, rightRowFactory.create()));
                            left = null;
                        }
                    }
                    else {
                        left = null;
                        matched = false;
                        rightSource().rewind();
                        requestRight();
                    }
                }
                else if (requested > 0 && waitingRight == 0 && F.isEmpty(rightInBuf))
                    requestRight();
            }

            // no more data from left side.
            if (requested > 0 && waitingLeft == NOT_WAITING && left == null && F.isEmpty(leftInBuf)) {
                requested = 0;
                downstream().end();
            }
        }
    }

    /** */
    private static class RightOrFullJoin<Row> extends NestedLoopJoinNode<Row> {
        /** Left row factory. */
        private final RowHandler.RowFactory<Row> leftRowFactory;

        /** Right row factory. */
        private final RowHandler.RowFactory<Row> rightRowFactory;

        /** Rows from right side that have no matching with left. */
        private BitSet rightNotMatchedRows;

        /** Last checked position from {@link RightOrFullJoin#rightNotMatchedRows}. */
        private int lastPushedIdx;

        /** */
        private Row left;

        /** */
        private Row right;

        /** */
        private int rightIdx;

        /** */
        private int leftIdx;

        /** If {@code true} start to processing right side. */
        private boolean doRight;

        /** If {@code true} fill right matching structure. */
        private boolean initRight = true;

        /** Whether current left row was matched or not, used only with {@link JoinRelType.FULL} type. */
        private boolean leftMatched;

        /** Join type. */
        private final JoinRelType joinType;

        /**
         * @param ctx Execution context.
         * @param cond Join expression.
         */
        public RightOrFullJoin(
            ExecutionContext<Row> ctx,
            RelDataType rowType,
            BiPredicate<Row, Row> cond,
            RowHandler.RowFactory<Row> leftRowFactory,
            RowHandler.RowFactory<Row> rightRowFactory,
            JoinRelType joinType
        ) {
            super(ctx, rowType, cond);

            this.leftRowFactory = leftRowFactory;
            this.rightRowFactory = rightRowFactory;

            this.joinType = joinType;
        }

        /** {@inheritDoc} */
        @Override protected void rewindInternal() {
            left = null;
            right = null;
            rightNotMatchedRows.clear();
            lastPushedIdx = 0;
            rightIdx = 0;
            leftIdx = 0;
            doRight = false;
            leftMatched = false;

            super.rewindInternal();
        }

        /** */
        private boolean leftIsEmpty() {
            return left == null && F.isEmpty(leftInBuf);
        }

        /** */
        private void doRight() throws Exception {
            if (requested > 0 && !F.isEmpty(rightInBuf) && !rightNotMatchedRows.isEmpty()) {
                inLoop = true;
                try {
                    lastPushedIdx = rightNotMatchedRows.nextSetBit(lastPushedIdx);

                    while (requested > 0 && !F.isEmpty(rightInBuf)) {
                        checkState();

                        if (lastPushedIdx < 0) {
                            doRight = false;
                            rightNotMatchedRows.clear();
                            break;
                        }

                        right = rightInBuf.remove();

                        if (leftIdx++ == lastPushedIdx) {
                            Row row = handler.concat(leftRowFactory.create(), right);

                            rightNotMatchedRows.clear(lastPushedIdx);

                            requested--;
                            downstream().push(row);

                            lastPushedIdx = rightNotMatchedRows.nextSetBit(lastPushedIdx + 1);
                        }
                    }
                }
                finally {
                    inLoop = false;
                }
            }

            // complete if right is empty or there is no items on matching set.
            if ((waitingRight == NOT_WAITING && F.isEmpty(rightInBuf)) || rightNotMatchedRows.isEmpty()) {
                doRight = false;
                rightNotMatchedRows.clear();

                if (requested > 0)
                    complete();

                return;
            }

            // reads all from right.
            if (waitingRight == 0 && F.isEmpty(rightInBuf) && doRight)
                requestRight();
        }

        /** {@inheritDoc} */
        @Override protected void join() throws Exception {
            if (doRight) {
                doRight();

                return;
            }

            if (rightNotMatchedRows == null)
                rightNotMatchedRows = new BitSet(IN_BUFFER_SIZE);

            if (initRight && !F.isEmpty(rightInBuf))
                rightNotMatchedRows.set(rightIdx, rightIdx + rightInBuf.size());

            try {
                inLoop = true;

                while (requested > 0 && !leftIsEmpty() && !F.isEmpty(rightInBuf)) {
                    checkState();

                    if (left == null) {
                        left = leftInBuf.remove();
                        leftMatched = false;
                    }

                    right = rightInBuf.remove();

                    rightIdx++;

                    if (!cond.test(left, right))
                        continue;

                    final Row row = handler.concat(left, right);

                    requested--;
                    leftMatched = true;
                    rightNotMatchedRows.clear(rightIdx - 1);
                    downstream().push(row);
                }
            }
            finally {
                inLoop = false;
            }

            if (requested > 0 && waitingLeft == 0 && F.isEmpty(leftInBuf))
                requestLeft();

            if (requested > 0 && waitingRight == 0 && F.isEmpty(rightInBuf))
                requestRight();

            // right is empty.
            if (requested > 0 && waitingRight == NOT_WAITING && F.isEmpty(rightInBuf) && left == null &&
                joinType == JoinRelType.FULL) {
                if (!F.isEmpty(leftInBuf)) {
                    while (!F.isEmpty(leftInBuf) && requested > 0) {
                        final Row left0 = leftInBuf.remove();
                        requested--;
                        downstream().push(handler.concat(left0, rightRowFactory.create()));
                    }
                }
                else {
                    if (waitingLeft == NOT_WAITING && F.isEmpty(leftInBuf))
                        complete();
                }
            }

            if (requested > 0 && waitingLeft == NOT_WAITING && leftIsEmpty() && initRight) {
                if (!F.isEmpty(rightInBuf)) {
                    while (!F.isEmpty(rightInBuf)) {
                        rightInBuf.remove();
                        rightNotMatchedRows.set(rightIdx++);
                    }

                    if (waitingRight != NOT_WAITING)
                        requestRight();
                }
            }

            // do final stuff
            if (requested > 0 && waitingLeft == NOT_WAITING && waitingRight == NOT_WAITING
                && F.isEmpty(rightInBuf) && F.isEmpty(leftInBuf)) {
                if (rightNotMatchedRows.isEmpty())
                    complete();
                else {
                    doRight = true;
                    initRight = false;

                    if (!leftMatched && requested > 0 && joinType == JoinRelType.FULL && left != null) {
                        requested--;

                        downstream().push(handler.concat(left, rightRowFactory.create()));
                    }

                    left = null;

                    rightSource().rewind();
                    requestRight();
                }

                return;
            }

            if (requested > 0 && ((waitingLeft == NOT_WAITING && !initRight && leftIsEmpty()) ||
                (waitingRight == NOT_WAITING && rightIdx == 0 && F.isEmpty(rightInBuf) && joinType != JoinRelType.FULL))) {
                assert rightNotMatchedRows.isEmpty() : "rightIdxs: " + rightNotMatchedRows;
                complete();
            }

            // rewind right.
            if (requested > 0 && waitingRight == NOT_WAITING && !F.isEmpty(leftInBuf) && F.isEmpty(rightInBuf)) {
                if (!leftMatched && requested > 0 && joinType == JoinRelType.FULL) {
                    requested--;

                    downstream().push(handler.concat(left, rightRowFactory.create()));
                }

                left = null;

                rightIdx = 0;

                rightSource().rewind();
                requestRight();

                initRight = false;
            }
        }

        /** */
        private void complete() throws Exception {
            rightNotMatchedRows = null;
            requested = 0;
            downstream().end();
        }
    }
}

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
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Deque;
import java.util.List;
import java.util.function.BiPredicate;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.RowHandler;
import org.apache.ignite.internal.util.typedef.F;
import org.jetbrains.annotations.NotNull;

/** */
public abstract class NestedLoopJoinNode<Row> extends MemoryTrackingNode<Row> {
    /** Special value to highlights that all row were received and we are not waiting any more. */
    protected static final int NOT_WAITING = -1;

    /** */
    protected final BiPredicate<Row, Row> cond;

    /** */
    protected final RowHandler<Row> handler;

    /** */
    protected int requested;

    /** */
    protected Row left;

    /** */
    protected int rightIdx;

    /** */
    protected int waitingLeft;

    /** */
    protected int waitingRight;

    /** */
    protected final List<Row> rightMaterialized = new ArrayList<>(IN_BUFFER_SIZE);

    /** */
    protected final Deque<Row> leftInBuf = new ArrayDeque<>(IN_BUFFER_SIZE);

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

        rightMaterialized.clear();
        leftInBuf.clear();

        left = null;
        rightIdx = 0;
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

        checkState();

        waitingLeft--;

        leftInBuf.add(row);

        join();
    }

    /** */
    private void pushRight(Row row) throws Exception {
        assert downstream() != null;
        assert waitingRight > 0;

        checkState();

        waitingRight--;

        rightMaterialized.add(row);

        nodeMemoryTracker.onRowAdded(row);

        if (waitingRight == 0)
            rightSource().request(waitingRight = IN_BUFFER_SIZE);
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
                throw new IllegalStateException("Join type \"" + joinType + "\" is not supported yet");
        }
    }

    /** */
    private static class InnerJoin<Row> extends NestedLoopJoinNode<Row> {
        /**
         * @param ctx Execution context.
         * @param cond Join expression.
         */
        public InnerJoin(ExecutionContext<Row> ctx, RelDataType rowType, BiPredicate<Row, Row> cond) {
            super(ctx, rowType, cond);
        }

        /** */
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

            tryToRequestInputs();

            if (requested > 0 && waitingLeft == NOT_WAITING && waitingRight == NOT_WAITING && left == null && leftInBuf.isEmpty()) {
                requested = 0;
                downstream().end();
            }
        }
    }

    /** */
    private static class LeftJoin<Row> extends NestedLoopJoinNode<Row> {
        /** Right row factory. */
        private final RowHandler.RowFactory<Row> rightRowFactory;

        /** Whether current left row was matched or not. */
        private boolean matched;

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

            super.rewindInternal();
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

                                downstream().push(handler.concat(left, rightRowFactory.create()));
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

            tryToRequestInputs();

            if (requested > 0 && waitingLeft == NOT_WAITING && waitingRight == NOT_WAITING && left == null && leftInBuf.isEmpty()) {
                requested = 0;
                downstream().end();
            }
        }
    }

    /** */
    private static class RightJoin<Row> extends NestedLoopJoinNode<Row> {
        /** Right row factory. */
        private final RowHandler.RowFactory<Row> leftRowFactory;

        /** */
        private BitSet rightNotMatchedIndexes;

        /** */
        private int lastPushedInd;

        /**
         * @param ctx Execution context.
         * @param cond Join expression.
         */
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
            rightNotMatchedIndexes.clear();
            lastPushedInd = 0;

            super.rewindInternal();
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

                        Row row = handler.concat(leftRowFactory.create(), rightMaterialized.get(lastPushedInd));

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

            tryToRequestInputs();

            if (requested > 0 && waitingLeft == NOT_WAITING && waitingRight == NOT_WAITING && left == null
                && leftInBuf.isEmpty() && rightNotMatchedIndexes.isEmpty()) {
                requested = 0;
                downstream().end();
            }
        }
    }

    /** */
    private static class FullOuterJoin<Row> extends NestedLoopJoinNode<Row> {
        /** Left row factory. */
        private final RowHandler.RowFactory<Row> leftRowFactory;

        /** Right row factory. */
        private final RowHandler.RowFactory<Row> rightRowFactory;

        /** Whether current left row was matched or not. */
        private boolean leftMatched;

        /** */
        private BitSet rightNotMatchedIndexes;

        /** */
        private int lastPushedInd;

        /**
         * @param ctx Execution context.
         * @param cond Join expression.
         */
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
            leftMatched = false;
            rightNotMatchedIndexes.clear();
            lastPushedInd = 0;

            super.rewindInternal();
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

                                downstream().push(handler.concat(left, rightRowFactory.create()));
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

                        Row row = handler.concat(leftRowFactory.create(), rightMaterialized.get(lastPushedInd));

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

            tryToRequestInputs();

            if (requested > 0 && waitingLeft == NOT_WAITING && waitingRight == NOT_WAITING && left == null
                && leftInBuf.isEmpty() && rightNotMatchedIndexes.isEmpty()) {
                requested = 0;
                downstream().end();
            }
        }
    }

    /** */
    private static class SemiJoin<Row> extends NestedLoopJoinNode<Row> {
        /**
         * @param ctx  Execution context.
         * @param cond Join expression.
         */
        public SemiJoin(ExecutionContext<Row> ctx, RelDataType rowType, BiPredicate<Row, Row> cond) {
            super(ctx, rowType, cond);
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

            tryToRequestInputs();

            if (requested > 0 && waitingLeft == NOT_WAITING && waitingRight == NOT_WAITING && left == null && leftInBuf.isEmpty()) {
                downstream().end();
                requested = 0;
            }
        }
    }

    /** */
    private static class AntiJoin<Row> extends NestedLoopJoinNode<Row> {
        /**
         * @param ctx Execution context.
         * @param cond Join expression.
         */
        public AntiJoin(ExecutionContext<Row> ctx, RelDataType rowType, BiPredicate<Row, Row> cond) {
            super(ctx, rowType, cond);
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

            tryToRequestInputs();

            if (requested > 0 && waitingLeft == NOT_WAITING && waitingRight == NOT_WAITING && left == null && leftInBuf.isEmpty()) {
                requested = 0;
                downstream().end();
            }
        }
    }

    /** */
    protected void tryToRequestInputs() throws Exception {
        if (waitingLeft == 0 && requested > 0 && leftInBuf.size() <= HALF_BUF_SIZE)
            leftSource().request(waitingLeft = IN_BUFFER_SIZE - leftInBuf.size());

        if (waitingRight == 0 && requested > 0 && rightMaterialized.size() <= HALF_BUF_SIZE)
            rightSource().request(waitingRight = IN_BUFFER_SIZE - rightMaterialized.size());
    }
}

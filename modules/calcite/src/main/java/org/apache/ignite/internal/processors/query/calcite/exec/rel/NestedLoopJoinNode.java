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

import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Deque;
import java.util.List;
import java.util.function.Predicate;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.RowHandler;
import org.jetbrains.annotations.NotNull;

/**
 *
 */
public abstract class NestedLoopJoinNode<RowT> extends AbstractNode<RowT> {
    /** Special value to highlights that all row were received and we are not waiting any more. */
    protected static final int NOT_WAITING = -1;

    /**
     *
     */
    protected final Predicate<RowT> cond;

    /**
     *
     */
    protected final RowHandler<RowT> handler;

    /**
     *
     */
    protected int requested;

    /**
     *
     */
    protected int waitingLeft;

    /**
     *
     */
    protected int waitingRight;

    /**
     *
     */
    protected final List<RowT> rightMaterialized = new ArrayList<>(inBufSize);

    /**
     *
     */
    protected final Deque<RowT> leftInBuf = new ArrayDeque<>(inBufSize);

    /**
     *
     */
    protected boolean inLoop;

    /**
     * @param ctx  Execution context.
     * @param cond Join expression.
     */
    private NestedLoopJoinNode(ExecutionContext<RowT> ctx, RelDataType rowType, Predicate<RowT> cond) {
        super(ctx, rowType);

        this.cond = cond;
        handler = ctx.rowHandler();
    }

    /** {@inheritDoc} */
    @Override
    public void request(int rowsCnt) throws Exception {
        assert !nullOrEmpty(sources()) && sources().size() == 2;
        assert rowsCnt > 0 && requested == 0;

        checkState();

        requested = rowsCnt;

        if (!inLoop) {
            context().execute(this::doJoin, this::onError);
        }
    }

    /**
     *
     */
    private void doJoin() throws Exception {
        checkState();

        join();
    }

    /** {@inheritDoc} */
    @Override
    protected void rewindInternal() {
        requested = 0;
        waitingLeft = 0;
        waitingRight = 0;

        rightMaterialized.clear();
        leftInBuf.clear();
    }

    /** {@inheritDoc} */
    @Override
    protected Downstream<RowT> requestDownstream(int idx) {
        if (idx == 0) {
            return new Downstream<RowT>() {
                /** {@inheritDoc} */
                @Override
                public void push(RowT row) throws Exception {
                    pushLeft(row);
                }

                /** {@inheritDoc} */
                @Override
                public void end() throws Exception {
                    endLeft();
                }

                /** {@inheritDoc} */
                @Override
                public void onError(Throwable e) {
                    NestedLoopJoinNode.this.onError(e);
                }
            };
        } else if (idx == 1) {
            return new Downstream<RowT>() {
                /** {@inheritDoc} */
                @Override
                public void push(RowT row) throws Exception {
                    pushRight(row);
                }

                /** {@inheritDoc} */
                @Override
                public void end() throws Exception {
                    endRight();
                }

                /** {@inheritDoc} */
                @Override
                public void onError(Throwable e) {
                    NestedLoopJoinNode.this.onError(e);
                }
            };
        }

        throw new IndexOutOfBoundsException();
    }

    /**
     *
     */
    private void pushLeft(RowT row) throws Exception {
        assert downstream() != null;
        assert waitingLeft > 0;

        checkState();

        waitingLeft--;

        leftInBuf.add(row);

        join();
    }

    /**
     *
     */
    private void pushRight(RowT row) throws Exception {
        assert downstream() != null;
        assert waitingRight > 0;

        checkState();

        waitingRight--;

        rightMaterialized.add(row);

        if (waitingRight == 0) {
            rightSource().request(waitingRight = inBufSize);
        }
    }

    /**
     *
     */
    private void endLeft() throws Exception {
        assert downstream() != null;
        assert waitingLeft > 0;

        checkState();

        waitingLeft = NOT_WAITING;

        join();
    }

    /**
     *
     */
    private void endRight() throws Exception {
        assert downstream() != null;
        assert waitingRight > 0;

        checkState();

        waitingRight = NOT_WAITING;

        join();
    }

    /**
     *
     */
    protected Node<RowT> leftSource() {
        return sources().get(0);
    }

    /**
     *
     */
    protected Node<RowT> rightSource() {
        return sources().get(1);
    }

    /**
     *
     */
    protected abstract void join() throws Exception;

    /**
     *
     */
    @NotNull
    public static <RowT> NestedLoopJoinNode<RowT> create(ExecutionContext<RowT> ctx, RelDataType outputRowType,
            RelDataType leftRowType, RelDataType rightRowType, JoinRelType joinType, Predicate<RowT> cond) {
        switch (joinType) {
            case INNER:
                return new InnerJoin<>(ctx, outputRowType, cond);

            case LEFT: {
                RowHandler.RowFactory<RowT> rightRowFactory = ctx.rowHandler().factory(ctx.getTypeFactory(), rightRowType);

                return new LeftJoin<>(ctx, outputRowType, cond, rightRowFactory);
            }

            case RIGHT: {
                RowHandler.RowFactory<RowT> leftRowFactory = ctx.rowHandler().factory(ctx.getTypeFactory(), leftRowType);

                return new RightJoin<>(ctx, outputRowType, cond, leftRowFactory);
            }

            case FULL: {
                RowHandler.RowFactory<RowT> leftRowFactory = ctx.rowHandler().factory(ctx.getTypeFactory(), leftRowType);
                RowHandler.RowFactory<RowT> rightRowFactory = ctx.rowHandler().factory(ctx.getTypeFactory(), rightRowType);

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

    /**
     *
     */
    private static class InnerJoin<RowT> extends NestedLoopJoinNode<RowT> {
        /**
         *
         */
        private RowT left;

        /**
         *
         */
        private int rightIdx;

        /**
         * @param ctx  Execution context.
         * @param cond Join expression.
         */
        private InnerJoin(ExecutionContext<RowT> ctx, RelDataType rowType, Predicate<RowT> cond) {
            super(ctx, rowType, cond);
        }

        /** {@inheritDoc} */
        @Override
        protected void rewindInternal() {
            left = null;
            rightIdx = 0;

            super.rewindInternal();
        }

        /**
         *
         */
        @Override
        protected void join() throws Exception {
            if (waitingRight == NOT_WAITING) {
                inLoop = true;
                try {
                    while (requested > 0 && (left != null || !leftInBuf.isEmpty())) {
                        if (left == null) {
                            left = leftInBuf.remove();
                        }

                        while (requested > 0 && rightIdx < rightMaterialized.size()) {
                            checkState();

                            RowT row = handler.concat(left, rightMaterialized.get(rightIdx++));

                            if (!cond.test(row)) {
                                continue;
                            }

                            requested--;
                            downstream().push(row);
                        }

                        if (rightIdx == rightMaterialized.size()) {
                            left = null;
                            rightIdx = 0;
                        }
                    }
                } finally {
                    inLoop = false;
                }
            }

            if (waitingRight == 0) {
                rightSource().request(waitingRight = inBufSize);
            }

            if (waitingLeft == 0 && leftInBuf.isEmpty()) {
                leftSource().request(waitingLeft = inBufSize);
            }

            if (requested > 0 && waitingLeft == NOT_WAITING && waitingRight == NOT_WAITING && left == null && leftInBuf.isEmpty()) {
                requested = 0;
                downstream().end();
            }
        }
    }

    /**
     *
     */
    private static class LeftJoin<RowT> extends NestedLoopJoinNode<RowT> {
        /** Right row factory. */
        private final RowHandler.RowFactory<RowT> rightRowFactory;

        /** Whether current left row was matched or not. */
        private boolean matched;

        /**
         *
         */
        private RowT left;

        /**
         *
         */
        private int rightIdx;

        /**
         * @param ctx  Execution context.
         * @param cond Join expression.
         */
        private LeftJoin(
                ExecutionContext<RowT> ctx,
                RelDataType rowType,
                Predicate<RowT> cond,
                RowHandler.RowFactory<RowT> rightRowFactory
        ) {
            super(ctx, rowType, cond);

            this.rightRowFactory = rightRowFactory;
        }

        /**
         *
         */
        @Override
        protected void rewindInternal() {
            matched = false;
            left = null;
            rightIdx = 0;

            super.rewindInternal();
        }

        /** {@inheritDoc} */
        @Override
        protected void join() throws Exception {
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

                            RowT row = handler.concat(left, rightMaterialized.get(rightIdx++));

                            if (!cond.test(row)) {
                                continue;
                            }

                            requested--;
                            matched = true;
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
                } finally {
                    inLoop = false;
                }
            }

            if (waitingRight == 0) {
                rightSource().request(waitingRight = inBufSize);
            }

            if (waitingLeft == 0 && leftInBuf.isEmpty()) {
                leftSource().request(waitingLeft = inBufSize);
            }

            if (requested > 0 && waitingLeft == NOT_WAITING && waitingRight == NOT_WAITING && left == null && leftInBuf.isEmpty()) {
                requested = 0;
                downstream().end();
            }
        }
    }

    /**
     *
     */
    private static class RightJoin<RowT> extends NestedLoopJoinNode<RowT> {
        /** Right row factory. */
        private final RowHandler.RowFactory<RowT> leftRowFactory;

        /**
         *
         */
        private BitSet rightNotMatchedIndexes;

        /**
         *
         */
        private int lastPushedInd;

        /**
         *
         */
        private RowT left;

        /**
         *
         */
        private int rightIdx;

        /**
         * @param ctx  Execution context.
         * @param cond Join expression.
         */
        private RightJoin(
                ExecutionContext<RowT> ctx,
                RelDataType rowType,
                Predicate<RowT> cond,
                RowHandler.RowFactory<RowT> leftRowFactory
        ) {
            super(ctx, rowType, cond);

            this.leftRowFactory = leftRowFactory;
        }

        /** {@inheritDoc} */
        @Override
        protected void rewindInternal() {
            left = null;
            rightNotMatchedIndexes.clear();
            lastPushedInd = 0;
            rightIdx = 0;

            super.rewindInternal();
        }

        /** {@inheritDoc} */
        @Override
        protected void join() throws Exception {
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
                        }

                        while (requested > 0 && rightIdx < rightMaterialized.size()) {
                            checkState();

                            RowT right = rightMaterialized.get(rightIdx++);
                            RowT joined = handler.concat(left, right);

                            if (!cond.test(joined)) {
                                continue;
                            }

                            requested--;
                            rightNotMatchedIndexes.clear(rightIdx - 1);
                            downstream().push(joined);
                        }

                        if (rightIdx == rightMaterialized.size()) {
                            left = null;
                            rightIdx = 0;
                        }
                    }
                } finally {
                    inLoop = false;
                }
            }

            if (waitingLeft == NOT_WAITING && requested > 0 && (rightNotMatchedIndexes != null && !rightNotMatchedIndexes.isEmpty())) {
                assert lastPushedInd >= 0;

                inLoop = true;
                try {
                    for (lastPushedInd = rightNotMatchedIndexes.nextSetBit(lastPushedInd); ;
                            lastPushedInd = rightNotMatchedIndexes.nextSetBit(lastPushedInd + 1)
                    ) {
                        checkState();

                        if (lastPushedInd < 0) {
                            break;
                        }

                        RowT row = handler.concat(leftRowFactory.create(), rightMaterialized.get(lastPushedInd));

                        rightNotMatchedIndexes.clear(lastPushedInd);

                        requested--;
                        downstream().push(row);

                        if (lastPushedInd == Integer.MAX_VALUE || requested <= 0) {
                            break;
                        }
                    }
                } finally {
                    inLoop = false;
                }
            }

            if (waitingRight == 0) {
                rightSource().request(waitingRight = inBufSize);
            }

            if (waitingLeft == 0 && leftInBuf.isEmpty()) {
                leftSource().request(waitingLeft = inBufSize);
            }

            if (requested > 0 && waitingLeft == NOT_WAITING && waitingRight == NOT_WAITING && left == null
                    && leftInBuf.isEmpty() && rightNotMatchedIndexes.isEmpty()) {
                requested = 0;
                downstream().end();
            }
        }
    }

    /**
     *
     */
    private static class FullOuterJoin<RowT> extends NestedLoopJoinNode<RowT> {
        /** Left row factory. */
        private final RowHandler.RowFactory<RowT> leftRowFactory;

        /** Right row factory. */
        private final RowHandler.RowFactory<RowT> rightRowFactory;

        /** Whether current left row was matched or not. */
        private boolean leftMatched;

        /**
         *
         */
        private BitSet rightNotMatchedIndexes;

        /**
         *
         */
        private int lastPushedInd;

        /**
         *
         */
        private RowT left;

        /**
         *
         */
        private int rightIdx;

        /**
         * @param ctx  Execution context.
         * @param cond Join expression.
         */
        private FullOuterJoin(ExecutionContext<RowT> ctx, RelDataType rowType, Predicate<RowT> cond,
                RowHandler.RowFactory<RowT> leftRowFactory,
                RowHandler.RowFactory<RowT> rightRowFactory) {
            super(ctx, rowType, cond);

            this.leftRowFactory = leftRowFactory;
            this.rightRowFactory = rightRowFactory;
        }

        /** {@inheritDoc} */
        @Override
        protected void rewindInternal() {
            left = null;
            leftMatched = false;
            rightNotMatchedIndexes.clear();
            lastPushedInd = 0;
            rightIdx = 0;

            super.rewindInternal();
        }

        /** {@inheritDoc} */
        @Override
        protected void join() throws Exception {
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

                            RowT right = rightMaterialized.get(rightIdx++);
                            RowT joined = handler.concat(left, right);

                            if (!cond.test(joined)) {
                                continue;
                            }

                            requested--;
                            leftMatched = true;
                            rightNotMatchedIndexes.clear(rightIdx - 1);
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
                } finally {
                    inLoop = false;
                }
            }

            if (waitingLeft == NOT_WAITING && requested > 0 && (rightNotMatchedIndexes != null && !rightNotMatchedIndexes.isEmpty())) {
                assert lastPushedInd >= 0;

                inLoop = true;
                try {
                    for (lastPushedInd = rightNotMatchedIndexes.nextSetBit(lastPushedInd); ;
                            lastPushedInd = rightNotMatchedIndexes.nextSetBit(lastPushedInd + 1)
                    ) {
                        checkState();

                        if (lastPushedInd < 0) {
                            break;
                        }

                        RowT row = handler.concat(leftRowFactory.create(), rightMaterialized.get(lastPushedInd));

                        rightNotMatchedIndexes.clear(lastPushedInd);

                        requested--;
                        downstream().push(row);

                        if (lastPushedInd == Integer.MAX_VALUE || requested <= 0) {
                            break;
                        }
                    }
                } finally {
                    inLoop = false;
                }
            }

            if (waitingRight == 0) {
                rightSource().request(waitingRight = inBufSize);
            }

            if (waitingLeft == 0 && leftInBuf.isEmpty()) {
                leftSource().request(waitingLeft = inBufSize);
            }

            if (requested > 0 && waitingLeft == NOT_WAITING && waitingRight == NOT_WAITING && left == null
                    && leftInBuf.isEmpty() && rightNotMatchedIndexes.isEmpty()) {
                requested = 0;
                downstream().end();
            }
        }
    }

    /**
     *
     */
    private static class SemiJoin<RowT> extends NestedLoopJoinNode<RowT> {
        /**
         *
         */
        private RowT left;

        /**
         *
         */
        private int rightIdx;

        /**
         * @param ctx  Execution context.
         * @param cond Join expression.
         */
        private SemiJoin(ExecutionContext<RowT> ctx, RelDataType rowType, Predicate<RowT> cond) {
            super(ctx, rowType, cond);
        }

        /** {@inheritDoc} */
        @Override
        protected void rewindInternal() {
            left = null;
            rightIdx = 0;

            super.rewindInternal();
        }

        /** {@inheritDoc} */
        @Override
        protected void join() throws Exception {
            if (waitingRight == NOT_WAITING) {
                while (requested > 0 && (left != null || !leftInBuf.isEmpty())) {
                    if (left == null) {
                        left = leftInBuf.remove();
                    }

                    boolean matched = false;

                    while (!matched && requested > 0 && rightIdx < rightMaterialized.size()) {
                        checkState();

                        RowT row = handler.concat(left, rightMaterialized.get(rightIdx++));

                        if (!cond.test(row)) {
                            continue;
                        }

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

            if (waitingRight == 0) {
                rightSource().request(waitingRight = inBufSize);
            }

            if (waitingLeft == 0 && leftInBuf.isEmpty()) {
                leftSource().request(waitingLeft = inBufSize);
            }

            if (requested > 0 && waitingLeft == NOT_WAITING && waitingRight == NOT_WAITING && left == null
                    && leftInBuf.isEmpty()) {
                downstream().end();
                requested = 0;
            }
        }
    }

    /**
     *
     */
    private static class AntiJoin<RowT> extends NestedLoopJoinNode<RowT> {
        /**
         *
         */
        private RowT left;

        /**
         *
         */
        private int rightIdx;

        /**
         * @param ctx  Execution context.
         * @param cond Join expression.
         */
        private AntiJoin(ExecutionContext<RowT> ctx, RelDataType rowType, Predicate<RowT> cond) {
            super(ctx, rowType, cond);
        }

        /**
         *
         */
        @Override
        protected void rewindInternal() {
            left = null;
            rightIdx = 0;

            super.rewindInternal();
        }

        /** {@inheritDoc} */
        @Override
        protected void join() throws Exception {
            if (waitingRight == NOT_WAITING) {
                inLoop = true;
                try {
                    while (requested > 0 && (left != null || !leftInBuf.isEmpty())) {
                        if (left == null) {
                            left = leftInBuf.remove();
                        }

                        boolean matched = false;

                        while (!matched && rightIdx < rightMaterialized.size()) {
                            checkState();

                            RowT row = handler.concat(left, rightMaterialized.get(rightIdx++));

                            if (cond.test(row)) {
                                matched = true;
                            }
                        }

                        if (!matched) {
                            requested--;
                            downstream().push(left);
                        }

                        left = null;
                        rightIdx = 0;
                    }
                } finally {
                    inLoop = false;
                }
            }

            if (waitingRight == 0) {
                rightSource().request(waitingRight = inBufSize);
            }

            if (waitingLeft == 0 && leftInBuf.isEmpty()) {
                leftSource().request(waitingLeft = inBufSize);
            }

            if (requested > 0 && waitingLeft == NOT_WAITING && waitingRight == NOT_WAITING && left == null && leftInBuf.isEmpty()) {
                requested = 0;
                downstream().end();
            }
        }
    }
}
